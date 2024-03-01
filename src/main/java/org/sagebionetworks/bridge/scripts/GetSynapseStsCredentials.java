package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.sts.StsCredentials;
import org.sagebionetworks.repo.model.sts.StsPermission;

public class GetSynapseStsCredentials {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static SynapseClient synapseClient;

    public static void main(String[] args) throws IOException, SynapseException {
        if (args.length != 2) {
            System.out.println("Usage: GetSynapseStsCredentials [path to config JSON] [folder ID]");
            return;
        }
        init(args[0]);
        execute(args[1]);

        logInfo("Done");
    }

    public static void init(String configPath) throws IOException {
        // Load config JSON.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));

        // Init Synapse client.
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseAccessToken = configNode.get("synapseAccessToken").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setBearerAuthorizationToken(synapseAccessToken);
    }

    public static void execute(String folderId) throws SynapseException {
        // Get STS credentials.
        StsCredentials credentials = synapseClient.getTemporaryCredentialsForEntity(folderId, StsPermission.read_only);

        // Print out STS credentials.
        System.out.println("STS session credentials: " + credentials);
    }
}
