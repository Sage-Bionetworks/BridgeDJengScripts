package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.sagebionetworks.bridge.crypto.AesGcmEncryptor;
import org.sagebionetworks.bridge.crypto.Encryptor;

// Transforms the Stormpath export to an intermediate format, which makes it easier for our scripts to upload
// everything to MySQL.
//
// Pre-req: Run TransformStormpathDirectoriesAndGroups first.
//
// To execute:
// mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.TransformStormpathAccounts -Dexec.args=[path to config file]
public class TransformStormpathAccounts {
    private static final Map<String, String> BRIDGE_NAME_TO_STORMPATH_NAME_MAP = ImmutableMap.<String, String>builder()
            .put("id", "id").put("email", "email").put("firstName", "givenName").put("lastName", "surname")
            .put("status", "status").build();
    private static final Set<String> CUSTOM_DATA_BLACKLIST = ImmutableSet.of("createdAt", "href", "id", "modifiedAt");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");
    private static final int LOG_PERIOD = 2500;

    // Rate limiter of 10 accounts per second seems reasonable.
    private static final RateLimiter RATE_LIMITER = RateLimiter.create(10.0);

    private static final Set<String> accountWhitelist = new HashSet<>();
    private static AmazonDynamoDB ddbClient;
    private static final Map<String, String> ddbPrefixByEnv = new HashMap<>();
    private static JsonNode directoriesById;
    private static final Map<String, Encryptor> encryptorsByEnv = new HashMap<>();
    private static final Set<String> envSet = new HashSet<>();
    private static JsonNode groupsByAccountId;
    private static Integer maxAccounts;
    private static String stormpathExportRootPath;
    private static String outputDirPath;

    public static void main(String[] args) throws IOException {
        // input params
        if (args.length != 1){
            System.out.println("Usage: TransformStormpathAccounts [config file]");
            return;
        }
        String configFilePath = args[0];

        // This is to make sure whatever script runs this Java application is properly capturing stdout and stderr and
        // sending them to the logs
        System.out.println("Verifying stdout...");
        System.err.println("Verifying stderr...");

        // init and execute
        init(configFilePath);
        loadDirectoriesAndGroups();
        transformAccounts();
        cleanup();
    }

    private static void init(String configFilePath) throws IOException {
        System.out.println("Initializing...");

        // load config, which is a JSON file
        JsonNode configNode = JSON_MAPPER.readTree(new File(configFilePath));
        stormpathExportRootPath = configNode.get("stormpathExportRootPath").textValue();
        outputDirPath = configNode.get("outputDirPath").textValue();

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

        // maxAccounts. This is used for tests. Since we have over 100k accounts, we don't want to process that many
        // accounts while testing.
        if (configNode.has("maxAccounts")) {
            maxAccounts = configNode.get("maxAccounts").intValue();
        }

        // init DDB
        ddbClient = new AmazonDynamoDBClient();
        JsonNode ddbPrefixByEnvNode = configNode.get("ddbPrefixByEnv");
        for (String oneEnv : envSet) {
            String oneDdbPrefix = ddbPrefixByEnvNode.get(oneEnv).textValue();
            ddbPrefixByEnv.put(oneEnv, oneDdbPrefix);
        }

        // init encryptors
        JsonNode encryptionKeysByEnv = configNode.get("encryptionKeysByEnv");
        for (String oneEnv : envSet) {
            String oneEncryptionKey = encryptionKeysByEnv.get(oneEnv).textValue();
            Encryptor oneEncryptor = new AesGcmEncryptor(oneEncryptionKey);
            encryptorsByEnv.put(oneEnv, oneEncryptor);
        }
    }

    public static void cleanup() {
        ddbClient.shutdown();
    }

    private static void loadDirectoriesAndGroups() throws IOException {
        directoriesById = JSON_MAPPER.readTree(Paths.get(outputDirPath, "directoriesById.json").toFile());
        groupsByAccountId = JSON_MAPPER.readTree(Paths.get(outputDirPath, "groupsByAccountId.json").toFile());
    }

    @SuppressWarnings("ConstantConditions")
    private static void transformAccounts() throws IOException {
        // Create output accounts subdirectory.
        Path outputAccountsRootPath = Files.createDirectories(Paths.get(outputDirPath, "accounts"));

        // Read accounts from Stormpath dump.
        int numAccounts = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (Stream<Path> accountPathStream = getPathStream()) {
            Iterator<Path> accountPathIter = accountPathStream.iterator();
            while (accountPathIter.hasNext()) {
                Path oneAccountPath = accountPathIter.next();
                try {
                    JsonNode inputAccountNode = JSON_MAPPER.readTree(oneAccountPath.toFile());
                    ObjectNode outputAccountNode = JSON_MAPPER.createObjectNode();
                    String accountId = inputAccountNode.get("id").textValue();

                    // Simple attributes
                    for (Map.Entry<String, String> oneBridgeToStormpathPair : BRIDGE_NAME_TO_STORMPATH_NAME_MAP
                            .entrySet()) {
                        String bridgeName = oneBridgeToStormpathPair.getKey();
                        String stormpathName = oneBridgeToStormpathPair.getValue();
                        outputAccountNode.set(bridgeName, inputAccountNode.get(stormpathName));
                    }

                    // get env and study from dirs
                    String dirId = inputAccountNode.get("directory").get("id").textValue();
                    JsonNode dirNode = directoriesById.get(dirId);
                    if (dirNode == null) {
                        // This is possible, for example, if the directory is "Stormpath Adminstrators".
                        System.out.println("WARN No directory found for directory ID " + dirId + ", account ID " +
                                accountId);
                        continue;
                    }
                    String env = dirNode.get("env").textValue();
                    outputAccountNode.put("env", env);
                    String studyId = dirNode.get("studyId").textValue();
                    outputAccountNode.put("studyId", studyId);

                    // env whitelist - We have to parse the account first, because Stormpath raw data isn't organized
                    // by directory.
                    if (!envSet.contains(env)) {
                        continue;
                    }

                    // Max accounts check _after_ the env whitelist. Otherwise, we might waste our "quota" on accounts
                    // that we don't even process.
                    if (maxAccounts != null && numAccounts >= maxAccounts) {
                        break;
                    }
                    if (numAccounts % LOG_PERIOD == 0) {
                        System.out.println(DateTime.now(LOCAL_TIME_ZONE) + ": " + numAccounts +
                                " accounts processed in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
                    }
                    numAccounts++;

                    // Similarly, rate limit, also after env whitelist check.
                    RATE_LIMITER.acquire();

                    // Timestamps in Stormpath are ISO8601 (always UTC). Bridge expects them as epoch milliseconds.
                    // createdOn
                    String isoCreatedOnString = inputAccountNode.get("createdAt").textValue();
                    long createdOnEpochMillis;
                    if (StringUtils.isNotBlank(isoCreatedOnString)) {
                        createdOnEpochMillis = DateTime.parse(isoCreatedOnString).getMillis();
                    } else {
                        // If Stormpath doesn't know when we created the account, fill in a dummy createdOn timestamp
                        // (aka now).
                        createdOnEpochMillis = DateTime.now().getMillis();
                        System.err.println("ERROR No createdAt found for account " + accountId);
                    }
                    outputAccountNode.put("createdOn", createdOnEpochMillis);

                    // modifiedOn
                    String isoModifiedOnString = inputAccountNode.get("modifiedAt").textValue();
                    long modifiedOnEpochMillis;
                    if (StringUtils.isNotBlank(isoModifiedOnString)) {
                        modifiedOnEpochMillis = DateTime.parse(isoModifiedOnString).getMillis();
                    } else {
                        // Similarly, fill this in with createdOn
                        modifiedOnEpochMillis = createdOnEpochMillis;
                        System.err.println("ERROR No modifiedAt found for account " + accountId);
                    }
                    outputAccountNode.put("modifiedOn", modifiedOnEpochMillis);

                    // passwordModifiedOn (which is required even if passwordHash isn't)
                    String isoPasswordModifiedOnString = inputAccountNode.get("passwordModifiedAt").textValue();
                    long passwordModifiedOnEpochMillis;
                    if (StringUtils.isNotBlank(isoPasswordModifiedOnString)) {
                        passwordModifiedOnEpochMillis = DateTime.parse(isoPasswordModifiedOnString).getMillis();
                    } else {
                        // For whatever reason, most accounts in Stormpath don't have this value filled in. Assume
                        // that the password has never been changed and fill in with the createdOn.
                        //
                        // Also, don't log an error, since this happens more often than not.
                        passwordModifiedOnEpochMillis = createdOnEpochMillis;
                    }
                    outputAccountNode.put("passwordModifiedOn", passwordModifiedOnEpochMillis);

                    // passwords (algorithm, hash, modifiedOn)
                    String passwordAlgorithm = null;
                    String passwordHash = null;
                    if (inputAccountNode.has("password")) {
                        passwordHash = inputAccountNode.get("password").textValue();
                        // determine algorithm from hash
                        if (passwordHash.startsWith("$stormpath")) {
                            passwordAlgorithm = "STORMPATH_HMAC_SHA_256";
                        } else if (passwordHash.startsWith("$2")) {
                            passwordAlgorithm = "BCRYPT";
                        } else {
                            System.err.println("ERROR Cannot identify password algorithm for account " + accountId +
                                    " password hash " + passwordHash);
                        }
                    }
                    outputAccountNode.put("passwordAlgorithm", passwordAlgorithm);
                    outputAccountNode.put("passwordHash", passwordHash);

                    // decrypt custom data
                    Map<String, String> customDataMap = new HashMap<>();
                    JsonNode customDataNode = inputAccountNode.get("customData");
                    Iterator<String> customDataFieldNameIter = customDataNode.fieldNames();
                    while (customDataFieldNameIter.hasNext()) {
                        String oneFieldName = customDataFieldNameIter.next();
                        if (CUSTOM_DATA_BLACKLIST.contains(oneFieldName)) {
                            // metadata fields in Stormpath that we don't need to copy
                            continue;
                        }
                        if (oneFieldName.endsWith("_version")) {
                            // Encryption version, which we also don't need to copy. Encryption version is always "2"
                            // anyway (we never had version 1, and we never made version 3), so we don't even need to
                            // look.
                            continue;
                        }

                        // decrypt
                        String encrypted = customDataNode.get(oneFieldName).textValue();
                        String decrypted = encryptorsByEnv.get(env).decrypt(encrypted);
                        customDataMap.put(oneFieldName, decrypted);
                    }

                    // parse custom data
                    String healthId = null;
                    ObjectNode attrMap = JSON_MAPPER.createObjectNode();
                    ObjectNode consentsBySubpop = JSON_MAPPER.createObjectNode();
                    for (Map.Entry<String, String> oneCustomData : customDataMap.entrySet()) {
                        String customDataKey = oneCustomData.getKey();
                        String customDataValue = oneCustomData.getValue();

                        if (customDataKey.equals(studyId + "_code")) {
                            healthId = customDataValue;
                        } else if (customDataKey.endsWith("_consent_signature")) {
                            // Old-style consent sig (without the plural). We've migrated most, but not all of these.
                            // If we have a singular consent sig that doesn't also have a list, parse it, wrap it in a
                            // list, and stick it in the consents map.
                            if (!customDataMap.containsKey(customDataKey + "s")) {
                                // extract subpop
                                String subpopGuid = customDataKey.substring(0, customDataKey.lastIndexOf(
                                        "_consent_signature"));

                                // parse consent
                                JsonNode consentNode = JSON_MAPPER.readTree(customDataValue);
                                ArrayNode consentListNode = JSON_MAPPER.createArrayNode();
                                consentListNode.add(consentNode);
                                consentsBySubpop.set(subpopGuid, consentListNode);
                            }
                        } else if (customDataKey.endsWith("_consent_signatures")) {
                            // extract subpop
                            String subpopGuid = customDataKey.substring(0, customDataKey.lastIndexOf(
                                    "_consent_signatures"));

                            // parse consent list
                            JsonNode consentListNode = JSON_MAPPER.readTree(customDataValue);
                            consentsBySubpop.set(subpopGuid, consentListNode);
                        } else {
                            // Anything else is an attribute.
                            attrMap.put(customDataKey, customDataValue);
                        }
                    }
                    outputAccountNode.put("healthId", healthId);
                    outputAccountNode.set("consents", consentsBySubpop);
                    outputAccountNode.set("attributes", attrMap);

                    // get health code from DynamoDB
                    String healthCode = null;
                    if (StringUtils.isNotBlank(healthId)) {
                        GetItemResult healthIdResult = ddbClient.getItem(ddbPrefixByEnv.get(env) + "HealthId",
                                ImmutableMap.of("id", new AttributeValue(healthId)));
                        Map<String, AttributeValue> healthIdItem = healthIdResult.getItem();
                        if (healthIdItem == null) {
                            // This should never happen, but sometimes comes up during testing.
                            System.err.println("ERROR Couldn't find healthCode for healthId " + healthId +
                                    ", accountId " + accountId + ", env " + env);
                        } else {
                            healthCode = healthIdItem.get("code").getS();
                        }
                    }
                    outputAccountNode.put("healthCode", healthCode);

                    // roles
                    JsonNode groupList = groupsByAccountId.get(accountId);
                    outputAccountNode.set("roles", groupList);

                    // write account
                    JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                            outputAccountsRootPath.resolve(accountId + ".json").toFile(), outputAccountNode);
                } catch (Exception ex) {
                    System.err.println("ERROR Error processing account file " + oneAccountPath.toString() + ": " +
                            ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }

        System.out.println(DateTime.now(LOCAL_TIME_ZONE) + ": " + numAccounts + " accounts processed in " +
                stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
    }

    private static Stream<Path> getPathStream() throws IOException {
        if (!accountWhitelist.isEmpty()) {
            return accountWhitelist.stream().map(accountId -> Paths.get(stormpathExportRootPath, "accounts",
                    accountId + ".json"));
        } else {
            return Files.list(Paths.get(stormpathExportRootPath, "accounts"));
        }
    }
}
