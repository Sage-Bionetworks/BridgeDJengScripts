package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.util.ModelConstants;

import org.sagebionetworks.bridge.helper.BridgeHelper;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;

/**
 * <p>
 * As it turns out, adding permissions to the tables but not the study is kinda useless. In this second backfill, we
 * first remove the Bridge Admin and Staff teams from the tables' permissions, then we add them to the study.
 * </p>
 * <p>
 * See https://sagebionetworks.jira.com/browse/BRIDGE-2396 for more details.
 * </p>
 * <p>
 * To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.ExporterPermissionsBackfill2 -Dexec.args=[args]"
 * </p>
 */
public class ExporterPermissionsBackfill2 {
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("ExporterPermissionsBackfill").appVersion(2);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final Set<ACCESS_TYPE> ACCESS_TYPE_ADMIN = ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS;
    private static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);

    private final long bridgeAdminTeamId;
    private final long bridgeStaffTeamId;
    private final BridgeHelper bridgeHelper;
    private final SynapseClient synapseClient;

    // Rate limiter, used to limit the amount of traffic to Synapse. Synapse throttles at 10 requests per second.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: ExporterPermissionsBackfill2 [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        ExporterPermissionsBackfill2 backfill = new ExporterPermissionsBackfill2(configNode);

        // Execute.
        backfill.addTeamsToStudies();
    }

    public ExporterPermissionsBackfill2(JsonNode configNode) {
        // Init team IDs.
        bridgeAdminTeamId = configNode.get("bridgeAdminTeamId").longValue();
        bridgeStaffTeamId = configNode.get("bridgeStaffTeamId").longValue();

        // Init Bridge.
        String workerStudyId = configNode.get("workerStudyId").textValue();
        String workerEmail = configNode.get("workerEmail").textValue();
        String workerPassword = configNode.get("workerPassword").textValue();

        SignIn workerSignIn = new SignIn().appId(workerStudyId).email(workerEmail).password(workerPassword);
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(workerSignIn)
                .build();
        bridgeHelper = new BridgeHelper(clientManager);

        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public void addTeamsToStudies() throws IOException {
        logInfo("Adding teams to studies...");

        // Iterate over studies.
        List<App> studySummaryList = bridgeHelper.getAppSummaries();
        for (App studySummary : studySummaryList) {
            String studyId = studySummary.getIdentifier();
            try {
                handleStudy(studyId);
            } catch (Exception ex) {
                logError("Error handling study " + studyId + ": " + ex.getMessage(), ex);
            }
        }
    }

    private void handleStudy(String studyId) throws IOException, SynapseException {
        // Need 3 permits, 1 to check if the Synapse project exists, 1 to get existing ACls, 1 to update ACLs.
        rateLimiter.acquire(3);

        // Get Synapse project from study and check if project exists.
        App study = bridgeHelper.getApp(studyId);
        String projectId = study.getSynapseProjectId();
        if (projectId == null) {
            logInfo("Study " + studyId + " has no Synapse project, skipping...");
            return;
        }
        try {
            synapseClient.getEntity(projectId, Project.class);
        } catch (SynapseNotFoundException ex) {
            logInfo("Study " + studyId + "'s configured project " + projectId + " doesn't exist, skipping...");
            return;
        }

        logInfo("Processing study " + studyId);

        // ResourceAccess is a mutable object, but the Synapse API takes them in a Set. This is a little weird.
        // IMPORTANT: Do not modify ResourceAccess objects after adding them to the set. This will break the set.
        boolean isModified = false;
        AccessControlList acl = synapseClient.getACL(projectId);
        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();
        Set<Long> principalIdSet = resourceAccessSet.stream().map(ResourceAccess::getPrincipalId)
                .collect(Collectors.toSet());

        // BridgeAdmin Team.
        if (!principalIdSet.contains(bridgeAdminTeamId)) {
            ResourceAccess bridgeAdminAccess = new ResourceAccess();
            bridgeAdminAccess.setPrincipalId(bridgeAdminTeamId);
            bridgeAdminAccess.setAccessType(ACCESS_TYPE_ADMIN);
            resourceAccessSet.add(bridgeAdminAccess);
            isModified = true;
        }

        // BridgeStaff Team.
        if (!principalIdSet.contains(bridgeStaffTeamId)) {
            ResourceAccess bridgeStaffAccess = new ResourceAccess();
            bridgeStaffAccess.setPrincipalId(bridgeStaffTeamId);
            bridgeStaffAccess.setAccessType(ACCESS_TYPE_READ);
            resourceAccessSet.add(bridgeStaffAccess);
            isModified = true;
        }

        if (isModified) {
            synapseClient.updateACL(acl);
        }
    }
}
