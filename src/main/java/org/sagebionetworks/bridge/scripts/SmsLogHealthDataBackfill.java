package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.sagebionetworks.bridge.helper.BridgeHelper;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.HealthDataSubmission;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

public class SmsLogHealthDataBackfill {
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("SmsLogHealthDataBackfill").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // If there are a lot of entries, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 25;

    private final BridgeHelper bridgeHelper;
    private final DynamoDB ddbClient;
    private final String ddbPrefix;
    private final RateLimiter perMessageRateLimiter = RateLimiter.create(1.0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: SmsLogHealthDataBackfill [path to config JSON]");
            return;
        }

        logInfo("Initializing...");

        // Init Bridge.
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        String researcherStudyId = configNode.get("researcherStudyId").textValue();
        String researcherEmail = configNode.get("researcherEmail").textValue();
        String researcherPassword = configNode.get("researcherPassword").textValue();

        SignIn researcherSignIn = new SignIn().appId(researcherStudyId).email(researcherEmail).password(
                researcherPassword);
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO)
                .withSignIn(researcherSignIn).build();

        BridgeHelper bridgeHelper = new BridgeHelper(clientManager);

        // Init DynamoDB.
        String ddbPrefix = configNode.get("ddbPrefix").textValue();
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());

        SmsLogHealthDataBackfill backfill = new SmsLogHealthDataBackfill(bridgeHelper, ddbClient, ddbPrefix);

        // Execute.
        try {
            backfill.execute();
        } finally {
            backfill.cleanup();
        }
    }

    public SmsLogHealthDataBackfill(BridgeHelper bridgeHelper, DynamoDB ddbClient, String ddbPrefix) {
        this.bridgeHelper = bridgeHelper;
        this.ddbClient = ddbClient;
        this.ddbPrefix = ddbPrefix;
    }

    public void execute() {
        logInfo("Starting backfill...");

        // Scan notification log table and process each row.
        Table notificationLogTable = ddbClient.getTable(ddbPrefix + "NotificationLog");
        Iterable<Item> notificationLogTableIter = notificationLogTable.scan();
        int numEntries = 0;
        int numBackfilled = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (Item notificationLogEntry : notificationLogTableIter) {
            // Rate limit.
            perMessageRateLimiter.acquire();

            // Process.
            try {
                // Get message log attributes.
                String userId = notificationLogEntry.getString("userId");
                long sentOnMillis = notificationLogEntry.getLong("notificationTime");
                String messageBody = notificationLogEntry.getString("message");

                // We neglected to add study ID to the notification log. Also, the user might have been deleted. Check
                // Bridge Server to see if the user exists and is in our study.
                StudyParticipant participant = null;
                try {
                    participant = bridgeHelper.getParticipant(userId);
                } catch (EntityNotFoundException ex) {
                    logInfo("User " + userId + " does not exist");
                }

                if (participant != null) {
                    // Set sentOn w/ user's time zone, if it exists.
                    DateTimeZone timeZone;
                    if (participant.getTimeZone() != null) {
                        timeZone = parseTimeZone(participant.getTimeZone());
                    } else {
                        timeZone = DateTimeZone.UTC;
                    }
                    DateTime sentOn = new DateTime(sentOnMillis, timeZone);

                    // Create health data. (Use a map instead of a Jackson JSON node, because mixing JSON libraries
                    // causes bad things to happen.) All messages sent by the notification worker are promotional.
                    Map<String, String> dataMap = new HashMap<>();
                    dataMap.put("sentOn", sentOn.toString());
                    dataMap.put("smsType", "Promotional");
                    dataMap.put("messageBody", messageBody);

                    // Health Data Service requires app version and phone info. However, this health data is submitted
                    // by Bridge, not by the app, so fill those in with artificial values.
                    HealthDataSubmission healthData = new HealthDataSubmission().appVersion("SmsLogHealthDataBackfill")
                            .phoneInfo("SmsLogHealthDataBackfill").createdOn(sentOn)
                            .schemaId("sms-messages-sent-from-bridge").schemaRevision(1L).data(dataMap);
                    bridgeHelper.submitHealthDataForParticipant(userId, healthData);

                    numBackfilled++;
                }
            } catch (Exception ex) {
                logError("Error processing log entry: " + ex.getMessage(), ex);
            }

            // Reporting.
            numEntries++;
            if (numEntries % REPORTING_INTERVAL == 0) {
                logInfo("Processing entries in progress: " + numEntries + " entries in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }
        logInfo("Finished processing " + numEntries + " entries in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds");
        logInfo("Backfilled " + numBackfilled + " entries");
    }

    public void cleanup() {
        ddbClient.shutdown();
    }

    private static DateTimeZone parseTimeZone(String value) {
        // There's no simple way to parse a timezone (in the format "+09:00"). The fastest way is to concatenate it
        // with an ISO date time and the parse it.
        DateTime dateTime = DateTime.parse("1970-01-01T0:00" + value);
        return dateTime.getZone();
    }
}
