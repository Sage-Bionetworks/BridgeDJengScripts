package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SmsTemplate;

@SuppressWarnings("UnstableApiUsage")
public class BulkSmsScript {
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("BulkSmsScript").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // If there are a lot of entries, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 25;

    // As per https://sagebionetworks.jira.com/browse/BRIDGE-3193
    private static final String SMS_MESSAGE_CONTENT =
            "Howzit! Are you keen on sharing more MindKind qualitative insights? Don't worry about data, we've got you. Email mindkindsa@gmail.com for more info.";

    private final String appId;
    private final ClientManager clientManager;
    private final List<String> healthCodeList;
    private final RateLimiter rateLimiter = RateLimiter.create(1.0);

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            logInfo("Usage: BulkSmsScript [path to config JSON] [app ID] [path to list of health codes]");
            return;
        }
        String configFilePath = args[0];
        String appId = args[1];
        String healthCodeFilePath = args[2];

        logInfo("Initializing...");

        // Init Bridge.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configFilePath));
        String workerAppId = configNode.get("workerAppId").textValue();
        String workerEmail = configNode.get("workerEmail").textValue();
        String workerPassword = configNode.get("workerPassword").textValue();

        SignIn workerSignIn = new SignIn().appId(workerAppId).email(workerEmail).password(workerPassword);
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(workerSignIn)
                .build();

        // Load health code list.
        List<String> healthCodeList;
        try (FileReader healthCodeListReader = new FileReader(healthCodeFilePath)) {
            healthCodeList = CharStreams.readLines(healthCodeListReader);
        }

        // Execute.
        BulkSmsScript script = new BulkSmsScript(appId, clientManager, healthCodeList);
        script.execute();
    }

    public BulkSmsScript(String appId, ClientManager clientManager, List<String> healthCodeList) {
        this.appId = appId;
        this.clientManager = clientManager;
        this.healthCodeList = healthCodeList;
    }

    public void execute() throws IOException {
        logInfo("Starting bulk SMS send...");

        // Make SMS template.
        SmsTemplate smsTemplate = new SmsTemplate();
        smsTemplate.setMessage(SMS_MESSAGE_CONTENT);

        // Send SMS messages.
        int numParticipants = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        ForWorkersApi forWorkersApi = clientManager.getClient(ForWorkersApi.class);
        for (String healthCode : healthCodeList) {
            // Rate limit.
            rateLimiter.acquire();

            // Send.
            try {
                forWorkersApi.sendSmsMessageToParticipantForApp(appId, "healthcode:" + healthCode, smsTemplate)
                        .execute();
            } catch (Exception ex) {
                logError("Error sending SMS for healthcode " + healthCode, ex);
            }

            // Reporting.
            numParticipants++;
            if (numParticipants % REPORTING_INTERVAL == 0) {
                logInfo("Sending in progress: " + numParticipants + " participants in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        logInfo("Finished sending to " + numParticipants + " participants in " +
                stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
    }
}
