package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.auth.NewIntegrationTestUser;

public class CreateSynapseDevAccount {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String SYNAPSE_ENDPOINT = "https://repo-dev.dev.sagebase.org/";

    private static SynapseAdminClient synapseClient;
    private static String username;
    private static String password;

    public static void main(String[] args) throws IOException, SynapseException {
        username = args[0];
        password = args[1];

        init();
        execute();

        System.out.println("Done");
    }

    public static void init() throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File("/Users/dwaynejeng/scripts-config-local.json"));

        // init Synapse client
        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUsername(configNode.get("synapseUser").textValue());
        synapseClient.setApiKey(configNode.get("synapseApiKey").textValue());

        synapseClient.setAuthEndpoint(SYNAPSE_ENDPOINT + "auth/v1");
        synapseClient.setFileEndpoint(SYNAPSE_ENDPOINT + "file/v1");
        synapseClient.setRepositoryEndpoint(SYNAPSE_ENDPOINT + "repo/v1");
    }

    public static void execute() throws SynapseException {
        NewIntegrationTestUser user = new NewIntegrationTestUser();
        user.setTou(true);
        user.setEmail("bridge-testing+" + username + "@sagebase.org");
        user.setUsername(username);
        user.setPassword(password);
        synapseClient.createIntegrationTestUser(user);
    }
}
