package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.file.UploadDestinationLocation;
import org.sagebionetworks.repo.model.project.ProjectSetting;
import org.sagebionetworks.repo.model.project.ProjectSettingsType;

/**
 * To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.RemoveStorageLocation -Dexec.args=[args]"
 */
public class RemoveStorageLocation {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final SynapseClient synapseClient;

    public static void main(String[] args) throws IOException, SynapseException {
        if (args.length != 2) {
            logInfo("Usage: RemoveStorageLocation [project ID] [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[1]));
        RemoveStorageLocation backfill = new RemoveStorageLocation(configNode);

        // Execute.
        backfill.execute(args[0]);
    }

    public RemoveStorageLocation(JsonNode configNode) {
        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public void execute(String projectId) throws SynapseException {
        try {
            UploadDestinationLocation[] locationArray = synapseClient.getUploadDestinationLocations(projectId);
            logInfo(String.valueOf(locationArray.length));
            ProjectSetting projectSetting = synapseClient.getProjectSetting(projectId, ProjectSettingsType.upload);
            logInfo(projectSetting.getId());
            logInfo("Deleting upload setting.");
            synapseClient.deleteProjectSetting(projectSetting.getId());
        } catch (SynapseNotFoundException ex) {
            logInfo("No upload setting found.");
        }
    }
}
