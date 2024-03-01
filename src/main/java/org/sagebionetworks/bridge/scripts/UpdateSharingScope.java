package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;

@SuppressWarnings({ "ConstantConditions", "UnstableApiUsage" })
public class UpdateSharingScope {
    private static final String APP_ID = "mobile-toolbox";
    private static final String STUDY_ID = "htshxm";

    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("UpdateSharingScope").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String OUTPUT_FILE_PATH = "/Users/dwaynejeng/Documents/backfill/sharing-scope-upload-ids-" +
            STUDY_ID;
    private static final int PAGE_SIZE = 100;
    private static final RateLimiter RATE_LIMITER = RateLimiter.create(10.0);

    private static final String[] EXTERNAL_ID_LIST = {
            "PM0502",
            "CA514",
            "PM050",
            "26913",
    };

    private static ClientManager clientManager;
    private static PrintWriter fileWriter;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: UpdateSharingScope [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        init(args[0]);

        // Execute.
        try {
            execute();
        } finally {
            // Close file writers.
            fileWriter.close();
        }

        // Need to force exit, because JavaSDK doesn't close itself.
        System.exit(0);
    }

    private static void init(String configPath) throws IOException {
        // Init Bridge.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));
        String adminEmail = configNode.get("adminEmail").textValue();
        String adminPassword = configNode.get("adminPassword").textValue();

        SignIn adminSignIn = new SignIn().appId(APP_ID).email(adminEmail).password(adminPassword);
        clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(adminSignIn)
                .withAcceptLanguage(ImmutableList.of("en-us")).build();

        // Init file writer.
        fileWriter = new PrintWriter(new FileWriter(OUTPUT_FILE_PATH), true);
    }

    private static void execute() {
        for (String externalId : EXTERNAL_ID_LIST) {
            RATE_LIMITER.acquire();

            logInfo("Querying for external ID " + externalId);
            try {
                // Query for the participant.
                // The full external ID is in the format [externalId]:[studyId]
                String fullExternalId = externalId + ':' + STUDY_ID;
                ParticipantsApi participantsApi = clientManager.getClient(ParticipantsApi.class);
                AccountSummarySearch search = new AccountSummarySearch().externalIdFilter(fullExternalId);
                List<AccountSummary> accountSummaryList = participantsApi.searchAccountSummaries(search).execute()
                        .body().getItems();

                AccountSummary accountSummary = null;
                for (AccountSummary oneAccountSummary : accountSummaryList) {
                    if (oneAccountSummary.getExternalIds().containsValue(fullExternalId)) {
                        accountSummary = oneAccountSummary;
                    }
                }
                if (accountSummary == null) {
                    throw new RuntimeException("Account not found for external ID " + externalId);
                }
                String userId = accountSummary.getId();

                StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute()
                        .body();
                String healthCode = participant.getHealthCode();

                logInfo("For external ID " + externalId + " found user ID " + userId + " with health code " +
                        healthCode);

                // Set sharing scope.
                if (participant.getSharingScope() == SharingScope.NO_SHARING) {
                    logInfo("Setting sharing scope for external ID " + externalId);
                    participant.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);
                    participantsApi.updateParticipant(userId, participant).execute();
                }

                // Query uploads.
                DateTime queryStartTime = participant.getCreatedOn();
                DateTime queryEndTime = DateTime.now();
                String nextPageOffsetKey = null;
                do {
                    RATE_LIMITER.acquire();
                    UploadList uploadListResponse = participantsApi.getParticipantUploads(userId, queryStartTime,
                            queryEndTime, PAGE_SIZE, nextPageOffsetKey).execute().body();
                    nextPageOffsetKey = uploadListResponse.getNextPageOffsetKey();

                    // Redrives automatically handle re-setting the sharing status.
                    for (Upload upload : uploadListResponse.getItems()) {
                        fileWriter.println(upload.getUploadId());
                    }
                } while (nextPageOffsetKey != null);
            } catch (Exception ex) {
                logError("Error processing external ID " + externalId, ex);
            }
        }
    }
}
