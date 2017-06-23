package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

// After running TransformStormpathAccounts, run this script.
// To execute:
// mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.StormpathToMySqlMigration -Dexec.args=[path to config file]
public class StormpathToMySqlMigration {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");
    private static final int LOG_PERIOD = 1000;
    private static final Joiner SQL_VALUES_JOINER = Joiner.on(", ");

    // Rate limiter of 10 accounts per second seems reasonable.
    private static final RateLimiter RATE_LIMITER = RateLimiter.create(10.0);

    private static Path accountsRootPath;
    private static final Set<String> accountWhitelist = new HashSet<>();
    private static final Set<String> envSet = new HashSet<>();
    private static final Map<String, Connection> mySqlConnectionsByEnv = new HashMap<>();

    public static void main(String[] args) throws IOException, SQLException {
        // input params
        if (args.length != 1){
            System.out.println("Usage: StormpathToMySqlMigration [config file]");
            return;
        }
        String configFilePath = args[0];

        // This is to make sure whatever script runs this Java application is properly capturing stdout and stderr and
        // sending them to the logs
        System.out.println("Verifying stdout...");
        System.err.println("Verifying stderr...");

        // init and execute
        //noinspection finally
        try {
            init(configFilePath);
            execute();
            shutdown();
        } catch (Exception ex) {
            System.err.println("ERROR " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            // For whatever reason, something here keeps the JVM alive even after we shut everything down. Force close the
            // application.
            System.out.println("Exiting...");
            System.exit(0);
        }
    }

    private static void init(String configFilePath) throws IOException, SQLException {
        System.out.println("Initializing...");

        // load config, which is a JSON file
        JsonNode configNode = JSON_MAPPER.readTree(new File(configFilePath));

        // This is the output of the Transform scripts, but the input of the Migration script.
        String transformOutputDirPath = configNode.get("outputDirPath").textValue();
        accountsRootPath = Paths.get(transformOutputDirPath, "accounts");

        // get list of envs
        JsonNode envListNode = configNode.get("envs");
        for (JsonNode oneEnvNode : envListNode) {
            envSet.add(oneEnvNode.textValue());
        }

        // account whitelist, also used for testing
        if (configNode.has("accountWhitelist")) {
            JsonNode accountWhitelistNode = configNode.get("accountWhitelist");
            for (JsonNode oneAccountId : accountWhitelistNode) {
                accountWhitelist.add(oneAccountId.textValue());
            }
        }

        // config MySQL connections
        JsonNode mySqlConnectionsByEnvNode = configNode.get("mySqlConnectionsByEnv");
        for (String oneEnv : envSet) {
            JsonNode oneConnectionNode = mySqlConnectionsByEnvNode.get(oneEnv);
            String url = oneConnectionNode.get("url").textValue();
            String username = oneConnectionNode.get("username").textValue();
            String password = oneConnectionNode.get("password").textValue();

            // This fixes a timezone bug in the MySQL Connector/J
            url = url + "?serverTimezone=UTC";

            // Append SSL props to URL if needed
            boolean useSsl = oneConnectionNode.get("useSsl").booleanValue();
            if (useSsl) {
                url = url + "&requireSSL=true&useSSL=true&verifyServerCertificate=false";
            }

            // Connect to DB
            Connection oneConnection = DriverManager.getConnection(url, username, password);
            mySqlConnectionsByEnv.put(oneEnv, oneConnection);
        }
    }

    private static void shutdown() throws SQLException {
        // Close MySQL connection.
        for (Connection oneConnection : mySqlConnectionsByEnv.values()) {
            oneConnection.close();
        }
    }

    @SuppressWarnings("ConstantConditions")
    private static void execute() throws IOException, SQLException {
        // Metrics
        int numAccounts = 0;
        int numInserted = 0;
        int numDeleted = 0;
        int numSkipped = 0;
        Multiset<String> numAccountsByEnv = HashMultiset.create(envSet.size());

        // Read transformed accounts and upload them to MySQL.
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (Stream<Path> accountPathStream = getPathStream()) {
            Iterator<Path> accountPathIter = accountPathStream.iterator();
            while (accountPathIter.hasNext()) {
                Path oneAccountPath = accountPathIter.next();
                try {
                    JsonNode accountNode = JSON_MAPPER.readTree(oneAccountPath.toFile());
                    String env = accountNode.get("env").textValue();
                    String accountId = accountNode.get("id").textValue();
                    long createdOn = accountNode.get("createdOn").longValue();
                    long jsonModifiedOn = accountNode.get("modifiedOn").longValue();

                    // env whitelist - We ended up putting accounts for all 4 envs in the same directory to make it
                    // easier to implement the account whitelist. It doesn't matter that much, since our test data set
                    // is small, and our prod data set is mostly in the prod env anyway.
                    if (!envSet.contains(env)) {
                        continue;
                    }
                    numAccountsByEnv.add(env);

                    if (numAccounts % LOG_PERIOD == 0) {
                        System.out.println(DateTime.now(LOCAL_TIME_ZONE) + ": " + numAccounts +
                                " accounts processed in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
                    }
                    numAccounts++;

                    // Similarly, rate limit, also after env whitelist check.
                    RATE_LIMITER.acquire();

                    // This is not the most efficient way to use JDBC. However, the code is simple, and doing it one
                    // query at a time allows us to log something at the time an error happens, rather than later on if
                    // doing a batch request. Plus, this code will be defunct after Aug 17 after the Stormpath
                    // migration is complete and irreversible.

                    Connection mySqlConnection = mySqlConnectionsByEnv.get(env);

                    // Determine if an account already exists and when the last time it was updated.
                    boolean hasResult;
                    Long sqlModifiedOn = null;
                    try (Statement selectStatement = mySqlConnection.createStatement();
                            ResultSet selectResultSet = selectStatement.executeQuery("select modifiedOn from " +
                                    "Accounts where id='" + accountId + "'")) {
                        // id is a primary key, so there should only be at most one
                        hasResult = selectResultSet.first();
                        if (hasResult) {
                            sqlModifiedOn = selectResultSet.getLong("modifiedOn");
                        }
                    }

                    if (hasResult) {
                        long delta = Math.abs(jsonModifiedOn - sqlModifiedOn);
                        if (sqlModifiedOn >= jsonModifiedOn || delta < 5000) {
                            // SQL account is newer than account in Stormpath dump. Alternatively, because of clock
                            // skew and because we aren't updating both simultaneously, if the SQL account is older,
                            // but the difference is less than, say, 5 seconds, we consider it up-to-date.
                            // Skip up-to-date accounts.
                            numSkipped++;
                            continue;
                        } else {
                            // If we found an outdated row in MySQL, this means our Migration Auth Dao is failing to
                            // keep the account info up-to-date. Log, so we can determine how often this is happening
                            // and fix as appropriate.
                            System.out.println("WARN Found outdated account ID " + accountId +
                                    ", Stormpath account is newer by " + delta + " milliseconds");

                            // The result set exists and is older than the JSON account info. Delete the old row and
                            // insert new ones. This is certainly not the most efficient way to update SQL, but this
                            // code is simpler than trying to query across 4 tables and figuring out which rows to
                            // update and how.
                            //
                            // Note: Just delete the row from Accounts. Foreign key constraints and CASCADE ON DELETE
                            // will take care of the rest.
                            try (Statement deleteStatement = mySqlConnection.createStatement()) {
                                int rowsDeleted = deleteStatement.executeUpdate("delete from Accounts where id='" +
                                        accountId + "'");
                                if (rowsDeleted != 1) {
                                    throw new IllegalStateException("Attempted to delete 1 row for account ID " +
                                            accountId + ", actually deleted " + rowsDeleted);
                                }
                            }

                            // Metrics
                            numDeleted++;
                        }
                    }

                    // At this point, there's definitely no MySQL entry for the account. Transform the account info
                    // into SQL statements and insert.

                    // insert into Accounts
                    String insertIntoAccountsQuery = makeInsertIntoAccountsQuery(accountNode);
                    try (Statement insertIntoAccountsStatement = mySqlConnection.createStatement()) {
                        int rowsInserted = insertIntoAccountsStatement.executeUpdate(insertIntoAccountsQuery);
                        if (rowsInserted != 1) {
                            throw new IllegalStateException("Attempted to insert 1 row into Accounts for account ID " +
                                    accountId + ", actally inserted " + rowsInserted);
                        }
                    }

                    // insert into AccountAttributes
                    JsonNode attributesNode = accountNode.get("attributes");
                    if (attributesNode != null && !attributesNode.isNull() && attributesNode.size() > 0) {
                        int numAttributes = attributesNode.size();
                        String insertIntoAttributesQuery = makeInsertIntoAttributesQuery(accountId, attributesNode);
                        try (Statement insertIntoAttributesStatement = mySqlConnection.createStatement()) {
                            int rowsInserted = insertIntoAttributesStatement.executeUpdate(insertIntoAttributesQuery);
                            if (rowsInserted != numAttributes) {
                                throw new IllegalStateException("Attempted to insert " + numAttributes + " rows " +
                                        "into AccountAttributes for account ID " + accountId + ", actually inserted " +
                                        rowsInserted);
                            }
                        }
                    }

                    // insert into AccountConsents
                    JsonNode consentsBySubpop = accountNode.get("consents");
                    int numConsents = countConsents(accountId, consentsBySubpop);
                    if (numConsents > 0) {
                        String insertIntoConsentsQuery = makeInsertIntoConsentsQuery(accountId, createdOn,
                                consentsBySubpop);
                        try (Statement insertIntoConsentsStatement = mySqlConnection.createStatement()) {
                            int rowsInserted = insertIntoConsentsStatement.executeUpdate(insertIntoConsentsQuery);
                            if (rowsInserted != numConsents) {
                                throw new IllegalStateException("Attempted to insert " + numConsents + " rows into " +
                                        "AccountConsents for account ID " + accountId + ", actually inserted " +
                                        rowsInserted);
                            }
                        }
                    }

                    // insert into AccountRoles
                    JsonNode roleListNode = accountNode.get("roles");
                    if (roleListNode != null && !roleListNode.isNull() && roleListNode.size() > 0) {
                        int numRoles = roleListNode.size();
                        String insertIntoRolesQuery = makeInsertIntoRolesQuery(accountId, roleListNode);
                        try (Statement insertIntoRolesStatement = mySqlConnection.createStatement()) {
                            int rowsInserted = insertIntoRolesStatement.executeUpdate(insertIntoRolesQuery);
                            if (rowsInserted != numRoles) {
                                throw new IllegalStateException("Attempted to insert " + numRoles + " rows into " +
                                        "AccountRoles for account ID " + accountId + ", actually inserted " +
                                        rowsInserted);
                            }
                        }
                    }

                    // We've already backfilled all the accounts, and new accounts are being written to both sources
                    // now. If we need to insert another account, this may be an indication of a bug in our MySQL Auth
                    // DAO or the migration DAO. Log so we can track down.
                    System.out.println("Inserted account " + accountId);

                    numInserted++;
                } catch (Exception ex) {
                    System.err.println("ERROR Error processing account file " + oneAccountPath.toString() + ": " +
                            ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }

        // timing
        System.out.println(DateTime.now(LOCAL_TIME_ZONE) + ": " + numAccounts + " accounts processed in " +
                stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");

        // metrics
        System.out.println("# total accounts processed: " + numAccounts);
        System.out.println("# up-to-date accounts skipped: " + numSkipped);
        System.out.println("# outdated accounts deleted: " + numDeleted);
        System.out.println("# accounts inserted: " + numInserted);

        for (String oneEnv : envSet) {
            System.out.println("# accounts (" + oneEnv + "): " + numAccountsByEnv.count(oneEnv));
        }
    }

    private static Stream<Path> getPathStream() throws IOException {
        if (!accountWhitelist.isEmpty()) {
            return accountWhitelist.stream().map(accountId -> accountsRootPath.resolve(accountId + ".json"));
        } else {
            return Files.list(accountsRootPath);
        }
    }

    private static String makeInsertIntoAccountsQuery(JsonNode accountNode) {
        //noinspection StringBufferReplaceableByString
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("insert into Accounts (id, studyId, email, createdOn, healthCode, healthId, modifiedOn, " +
                "firstName, lastName, passwordAlgorithm, passwordHash, passwordModifiedOn, status) values (");
        queryBuilder.append(serializeJsonTextField(accountNode, "id"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "studyId"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "email"));
        queryBuilder.append(", ");
        queryBuilder.append(getJsonNumberField(accountNode, "createdOn"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "healthCode"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "healthId"));
        queryBuilder.append(", ");
        queryBuilder.append(getJsonNumberField(accountNode, "modifiedOn"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "firstName"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "lastName"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "passwordAlgorithm"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "passwordHash"));
        queryBuilder.append(", ");
        queryBuilder.append(getJsonNumberField(accountNode, "passwordModifiedOn"));
        queryBuilder.append(", ");
        queryBuilder.append(serializeJsonTextField(accountNode, "status"));
        queryBuilder.append(")");
        return queryBuilder.toString();
    }

    private static String makeInsertIntoAttributesQuery(String accountId, JsonNode attributesNode) {
        List<String> valueList = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> attrFieldIter = attributesNode.fields();
        while (attrFieldIter.hasNext()) {
            Map.Entry<String, JsonNode> attrFieldEntry = attrFieldIter.next();
            String attrName = attrFieldEntry.getKey();
            String attrValue = attrFieldEntry.getValue().textValue();
            valueList.add("('" + accountId + "', '" + attrName + "', '" + attrValue + "')");
        }

        return "insert into AccountAttributes (accountId, attributeKey, attributeValue) values " +
                SQL_VALUES_JOINER.join(valueList);
    }

    private static int countConsents(String accountId, JsonNode consentsBySubpop) {
        if (consentsBySubpop == null || consentsBySubpop.isNull() || consentsBySubpop.size() == 0) {
            return 0;
        }

        // Sometimes, we have multiple consents that are signed at the same millisecond. This is likely a bug, since
        // that's not actually possible. If it happens, log a warning and validate manually.
        Set<Long> uniqueSignedOnSet = new HashSet<>();
        for (JsonNode consentListForSubpop : consentsBySubpop) {
            for (JsonNode oneConsent : consentListForSubpop) {
                Long signedOn = getJsonNumberField(oneConsent, "signedOn");
                if (uniqueSignedOnSet.contains(signedOn)) {
                    System.out.println("WARN Found duplicate consent signedOn " + signedOn + " for account " +
                            accountId);
                }

                uniqueSignedOnSet.add(signedOn);
            }
        }
        return uniqueSignedOnSet.size();
    }

    private static String makeInsertIntoConsentsQuery(String accountId, long createdOn,
            JsonNode consentsBySubpopNode) {
        Map<Long, String> valuesBySignedOn = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> consentsBySubpopIter = consentsBySubpopNode.fields();
        while (consentsBySubpopIter.hasNext()) {
            Map.Entry<String, JsonNode> consentsBySubpopEntry = consentsBySubpopIter.next();
            String subpopGuid = consentsBySubpopEntry.getKey();
            JsonNode consentListForSubpop = consentsBySubpopEntry.getValue();

            for (JsonNode oneConsent : consentListForSubpop) {
                Long signedOn = getJsonNumberField(oneConsent,"signedOn");
                if (signedOn == null) {
                    signedOn = createdOn;
                }

                Long consentCreatedOn = getJsonNumberField(oneConsent, "consentCreatedOn");
                if (consentCreatedOn == null) {
                    consentCreatedOn = 0L;
                }

                //noinspection StringBufferReplaceableByString
                StringBuilder valueBuilder = new StringBuilder();
                valueBuilder.append("('");
                valueBuilder.append(accountId);
                valueBuilder.append("', '");
                valueBuilder.append(subpopGuid);
                valueBuilder.append("', ");
                valueBuilder.append(signedOn);
                valueBuilder.append(", ");
                valueBuilder.append(serializeJsonTextField(oneConsent, "birthdate"));
                valueBuilder.append(", ");
                valueBuilder.append(consentCreatedOn);
                valueBuilder.append(", ");
                valueBuilder.append(serializeJsonTextField(oneConsent, "name"));
                valueBuilder.append(", ");
                valueBuilder.append(serializeJsonTextField(oneConsent, "imageData"));
                valueBuilder.append(", ");
                valueBuilder.append(serializeJsonTextField(oneConsent, "imageMimeType"));
                valueBuilder.append(", ");
                valueBuilder.append(getJsonNumberField(oneConsent, "withdrewOn"));
                valueBuilder.append(")");
                valuesBySignedOn.put(signedOn, valueBuilder.toString());
            }
        }

        return "insert into AccountConsents (accountId, subpopulationGuid, signedOn, birthdate, consentCreatedOn, " +
                "name, signatureImageData, signatureImageMimeType, withdrewOn) values " +
                SQL_VALUES_JOINER.join(valuesBySignedOn.values());
    }

    private static String makeInsertIntoRolesQuery(String accountId, JsonNode roleListNode) {
        List<String> valueList = new ArrayList<>();
        for (JsonNode oneRoleNode : roleListNode) {
            valueList.add("('" + accountId + "', '" + oneRoleNode.textValue() + "')");
        }

        return "insert into AccountRoles (accountId, role) values " + SQL_VALUES_JOINER.join(valueList);
    }

    private static String serializeJsonTextField(JsonNode parent, String key) {
        JsonNode child = parent.get(key);
        if (child != null && !child.isNull()) {
            return "'" + StringEscapeUtils.escapeSql(child.textValue()) + "'";
        } else {
            return null;
        }
    }

    private static Long getJsonNumberField(JsonNode parent, String key) {
        JsonNode child = parent.get(key);
        if (child != null && !child.isNull()) {
            return child.longValue();
        } else {
            return null;
        }
    }
}
