package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import org.sagebionetworks.bridge.helper.AccountSummaryIterator;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;

// BRIDGE-3349 - We have code on the server side to fix the duplicate client info bug. We just need to read the
// RequestInfo and write it back to the server.
public class UpdateClientInfo {
    private static final String APP_ID = "mtb-alpha";
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("UpdateClientInfo").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int MAX_ERRORS = 50;
    private static final int REPORTING_INTERVAL = 250;

    private static ClientManager clientManager;
    private static ParticipantsApi participantsApi;
    private static ForSuperadminsApi superadminsApi;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: UpdateClientInfo [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        init(args[0]);

        // Execute.
        execute();
    }

    private static void init(String configPath) throws IOException {
        // Init Bridge.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));
        String adminEmail = configNode.get("adminEmail").textValue();
        String adminPassword = configNode.get("adminPassword").textValue();

        SignIn workerSignIn = new SignIn().appId(APP_ID).email(adminEmail).password(adminPassword);
        clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(workerSignIn)
                .withAcceptLanguage(ImmutableList.of("en-us")).build();
        participantsApi = clientManager.getClient(ParticipantsApi.class);
        superadminsApi = clientManager.getClient(ForSuperadminsApi.class);
    }

    private static void execute() throws IOException {
        AccountSummaryIterator accountSummaryIterator = new AccountSummaryIterator(clientManager, APP_ID,
                100, 0.1);
        String lastUserId = null;
        int numErrors = 0;
        int numUsers = 0;
        int numUpdated = 0;
        while (accountSummaryIterator.hasNext()) {
            if (numUsers > 0 && numUsers % REPORTING_INTERVAL == 0) {
                logInfo("Updated " + numUpdated + " out of " + numUsers + " users, last user ID " + lastUserId);
            }
            numUsers++;

            // Fetch upload.
            AccountSummary accountSummary;
            try {
                accountSummary = accountSummaryIterator.next();
            } catch (RuntimeException ex) {
                logError("Error getting next user. Last user is " + lastUserId, ex);
                numErrors++;
                if (numErrors >= MAX_ERRORS) {
                    logError("Too many errors. Aborting.");
                    break;
                } else {
                    continue;
                }
            }
            lastUserId = accountSummary.getId();

            // Get request info.
            try {
                RequestInfo requestInfo = participantsApi.getParticipantRequestInfo(lastUserId).execute().body();
                if (requestInfo != null) {
                    String userAgent = requestInfo.getUserAgent();
                    if (isDuplicateClientInfo(userAgent)) {
                        // Write request info back.
                        superadminsApi.updateParticipantRequestInfo(lastUserId, requestInfo).execute();
                        numUpdated++;
                    }
                }
            } catch (RuntimeException ex) {
                logError("Error processing user " + lastUserId, ex);
            }
        }
    }

    private static boolean isDuplicateClientInfo(String userAgent) {
        if (userAgent != null) {
            int midpoint = userAgent.length() / 2;
            if (userAgent.charAt(midpoint) == ',') {
                String frontHalf = userAgent.substring(0, midpoint);
                String backHalf = userAgent.substring(midpoint + 1);
                return frontHalf.equals(backHalf);
            }
        }
        return false;
    }
}
