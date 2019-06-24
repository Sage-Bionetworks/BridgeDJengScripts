package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.security.spec.KeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.codec.binary.Base64;

/**
 * <p>
 * This script will take Stormpath hashes and double-hash them with PBKDF2 for enhanced security.
 * </p>
 * <p>
 * To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.StormpathRehashPasswords -Dexec.args=[path to config JSON]"
 * </p>
 */
public class StormpathRehashPasswords {
    private static final int BATCH_SIZE = 100;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int LOG_PERIOD = 50;
    private static final int PBKDF2_ITERATIONS = 250000;

    // This account limit is to make sure we don't end up in some weird infinite loop. Since there are only 110518
    // accounts with Stormpath hashes, and we aren't creating new accounts with Stormpath hashes, if we see 200k
    // accounts, there's almost certainly a bug in the code.
    private static final int BATCH_LIMIT = 200000 / BATCH_SIZE;

    private static final String SELECT_STATEMENT = "select id, passwordHash, version from Accounts where " +
            "passwordAlgorithm='STORMPATH_HMAC_SHA_256' limit " + BATCH_SIZE;
    private static final String UPDATE_STATEMENT = "update Accounts set passwordAlgorithm='STORMPATH_PBKDF2_DOUBLE_HASH', " +
            "passwordHash=?, passwordModifiedOn=?, modifiedOn=?, version=? where id=?";

    private final Connection dbConnection;
    private final RateLimiter rateLimiter = RateLimiter.create(1.0);

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length != 1) {
            logInfo("Usage: StormpathRehashPasswords [path to config JSON]");
            return;
        }

        // This is to make sure whatever script runs this Java application is properly capturing stdout and stderr and
        // sending them to the logs
        System.out.println("Verifying stdout...");
        System.err.println("Verifying stderr...");

        // Init.
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        StormpathRehashPasswords job = new StormpathRehashPasswords(configNode);

        // Execute.
        try {
            job.execute();
        } finally {
            job.shutdown();
        }
    }

    public StormpathRehashPasswords(JsonNode configNode) throws SQLException {
        logInfo("Initializing...");
        String url = configNode.get("dbUrl").textValue();
        String username = configNode.get("dbUsername").textValue();
        String password = configNode.get("dbPassword").textValue();

        // This fixes a timezone bug in the MySQL Connector/J
        url = url + "?serverTimezone=UTC";

        // Append SSL props to URL if needed
        boolean useSsl = configNode.get("dbUseSsl").booleanValue();
        if (useSsl) {
            url = url + "&requireSSL=true&useSSL=true&verifyServerCertificate=false";
        }

        // Connect to DB
        dbConnection = DriverManager.getConnection(url, username, password);
    }

    public void shutdown() throws SQLException {
        dbConnection.close();
    }

    public void execute() {
        logInfo("Executing...");

        Stopwatch stopwatch = Stopwatch.createStarted();
        int numBatches = 0;
        while (true) {
            // Rate limit at 1 batch per second.
            rateLimiter.acquire();

            // The strategy is to get a batch of 100 accounts that have passwordAlgorithm=STORMPATH_PBKDF2_DOUBLE_HASH,
            // process those 100 accounts, and then repeat until there are no more accounts with
            // passwordAlgorithm=STORMPATH_PBKDF2_DOUBLE_HASH.
            long updateTimeMillis = System.currentTimeMillis();
            try {
                try (PreparedStatement selectStatement = dbConnection.prepareStatement(SELECT_STATEMENT);
                    ResultSet resultSet = selectStatement.executeQuery();
                    PreparedStatement updateStatement = dbConnection.prepareStatement(UPDATE_STATEMENT)) {

                    int numRows = 0;
                    while(resultSet.next()) {
                        // Get queried row.
                        numRows++;
                        String id = resultSet.getString("id");
                        String passwordHash = resultSet.getString("passwordHash");
                        int version = resultSet.getInt("version");

                        // Password is in the form "$stormpath1$[base64-encoded salt]$[base64-encoded hashed password]"
                        String[] stormpathHashParts = passwordHash.split("\\$");
                        String base64Salt = stormpathHashParts[2];
                        byte[] salt = Base64.decodeBase64(base64Salt);
                        String stormpathHashedPassword = stormpathHashParts[3];

                        // Hash the hash with PBKDF2.
                        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
                        KeySpec keySpec = new PBEKeySpec(stormpathHashedPassword.toCharArray(), salt,
                                PBKDF2_ITERATIONS, 256);
                        String doubleHashedPassword = Base64.encodeBase64String(keyFactory.generateSecret(keySpec)
                                .getEncoded());

                        // Output format will be "[iterations]$[base64-encoded salt]$[base64-encoded hashed password]"
                        String newPasswordHash = PBKDF2_ITERATIONS + "$" + base64Salt + "$" + doubleHashedPassword;

                        // Add to batch update.
                        updateStatement.setString(1, newPasswordHash);
                        updateStatement.setLong(2, updateTimeMillis);
                        updateStatement.setLong(3, updateTimeMillis);
                        updateStatement.setInt(4, version + 1);
                        updateStatement.setString(5, id);
                        updateStatement.addBatch();
                    }

                    if (numRows > 0) {
                        // Execute batch.
                        updateStatement.executeBatch();
                    } else {
                        // There are no more passwords to update. Break.
                        break;
                    }
                }
            } catch (Exception ex) {
                logError("Error processing batch: " + ex.getMessage(), ex);
            }

            // Batch counting. Logging and batch limit.
            numBatches++;
            if (numBatches >= BATCH_LIMIT) {
                logError("Batch limit exceeded!");
                break;
            }
            if (numBatches % LOG_PERIOD == 0) {
                logInfo(numBatches + " batches in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
            }
        }

        logInfo("Finished processing " + numBatches + " batches in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds...");
    }
}
