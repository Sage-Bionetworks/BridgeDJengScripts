package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.helper.AppUploadIterator;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;

@SuppressWarnings("ConstantConditions")
public class GetUploadsForStudies {
    private static final String APP_ID = "open-bridge";
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("GetUploadsForStudies").appVersion(1);
    private static final DateTime START_DATE_TIME = DateTime.parse("2023-12-01T0:00-0700");
    private static final DateTime END_DATE_TIME = DateTime.parse("2023-12-07T00:00-0700");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int MAX_ERRORS = 50;
    private static final String OUTPUT_PATH_PREFIX = "/Users/dwaynejeng/Documents/backfill/all-uploads-";
    private static final int REPORTING_INTERVAL = 1000;
    private static final Set<String> STUDY_ID_SET = ImmutableSet.of("rvrccc");

    private static ClientManager clientManager;
    private static Map<String, PrintWriter> fileWritersByStudy = new HashMap<>();
    private static Map<String, Set<String>> studyIdsByHealthCode = new HashMap<>();
    private static ForWorkersApi workersApi;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: GetUploadsForStudies [path to config JSON]");
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
            for (PrintWriter writer : fileWritersByStudy.values()) {
                writer.close();
            }
        }
    }

    private static void init(String configPath) throws IOException {
        // Init Bridge.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));
        String workerStudyId = configNode.get("workerAppId").textValue();
        String workerEmail = configNode.get("workerEmail").textValue();
        String workerPassword = configNode.get("workerPassword").textValue();

        SignIn workerSignIn = new SignIn().appId(workerStudyId).email(workerEmail).password(workerPassword);
        clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(workerSignIn)
                .withAcceptLanguage(ImmutableList.of("en-us")).build();
        workersApi = clientManager.getClient(ForWorkersApi.class);
    }

    private static void execute() {
        AppUploadIterator appUploadIterator = new AppUploadIterator(clientManager, APP_ID, START_DATE_TIME,
                END_DATE_TIME);
        String lastUploadId = null;
        DateTime lastRequestedOn = null;
        int numErrors = 0;
        int numUploads = 0;
        while (appUploadIterator.hasNext()) {
            if (numUploads > 0 && numUploads % REPORTING_INTERVAL == 0) {
                logInfo("Processed " + numUploads + " uploads, last requested on " + lastRequestedOn.toString());
            }
            numUploads++;

            // Fetch upload.
            Upload upload;
            try {
                upload = appUploadIterator.next();
            } catch (RuntimeException ex) {
                logError("Error getting next upload. Last upload is " + lastUploadId, ex);
                numErrors++;
                if (numErrors >= MAX_ERRORS) {
                    logError("Too many errors. Aborting.");
                    break;
                } else {
                    continue;
                }
            }

            lastUploadId = upload.getUploadId();
            lastRequestedOn = upload.getRequestedOn();
            try {
                appendToFile("rvrccc", lastUploadId);
            } catch (IOException ex) {
                logError("Error appending upload " + lastUploadId + " to file", ex);
            }

            //if (upload.getStatus() == UploadStatus.REQUESTED) {
            //    // Upload requested but never completed. Skip.
            //    continue;
            //}
            //
            // Get study IDs for health code. Note that if health code is inactive, this returns an empty set.
            // TODO We originally got health code from upload.getHealthCode(), but that was never merged into JavaSDK.
            //String healthCode = upload.getHealthCode();
            //String healthCode = null;
            //Set<String> studyIdSet = getStudyIdsForHealthCode(healthCode, lastUploadId);
            //for (String studyId : studyIdSet) {
            //    try {
            //        appendToFile(studyId, lastUploadId);
            //    } catch (IOException ex) {
            //        logError("Error appending upload " + lastUploadId + " to file for study " + studyId,
            //                ex);
            //    }
            //}
        }
    }

    private static Set<String> getStudyIdsForHealthCode(String healthCode, String uploadId) {
        // Check cached value.
        Set<String> studyIdSet = studyIdsByHealthCode.get(healthCode);
        if (studyIdSet != null) {
            return studyIdSet;
        }

        // Fetch participant.
        StudyParticipant participant;
        try {
            participant = workersApi.getParticipantByHealthCodeForApp(APP_ID, healthCode, false).execute()
                    .body();
        } catch (IOException | RuntimeException ex) {
            logError("Error getting participant with healthcode " + healthCode + " for upload " + uploadId, ex);
            return Collections.emptySet();
        }

        // Make empty set and add it to the cache.
        studyIdSet = new HashSet<>();
        studyIdsByHealthCode.put(healthCode, studyIdSet);

        // Filter out: no_sharing, test_users, users with roles.
        if (participant.getSharingScope() == SharingScope.NO_SHARING) {
            return studyIdSet;
        }
        List<String> dataGroupList = participant.getDataGroups();
        if (dataGroupList != null && dataGroupList.contains("test_user")) {
            return studyIdSet;
        }
        List<Role> roleList = participant.getRoles();
        if (roleList != null && !roleList.isEmpty()) {
            return studyIdSet;
        }

        // Sort by study ID and add to the set.
        for (String studyId : participant.getStudyIds()) {
            if (STUDY_ID_SET.contains(studyId)) {
                studyIdSet.add(studyId);
            }
        }
        return studyIdSet;
    }

    private static void appendToFile(String studyId, String uploadId) throws IOException {
        PrintWriter writer = fileWritersByStudy.get(studyId);
        if (writer == null) {
            writer = new PrintWriter(new FileWriter(OUTPUT_PATH_PREFIX + studyId), true);
            fileWritersByStudy.put(studyId, writer);
        }
        writer.println(uploadId);
    }
}
