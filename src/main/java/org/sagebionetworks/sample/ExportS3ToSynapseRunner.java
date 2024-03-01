package org.sagebionetworks.sample;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.exceptions.SynapseException;

public class ExportS3ToSynapseRunner {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    public static void main(String[] args) throws IOException, SynapseException {
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        String awsAccessKey = configNode.get("awsAccessKey").textValue();
        String awsSecretKey = configNode.get("awsSecretKey").textValue();
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        ExportS3ToSynapse export = new ExportS3ToSynapse(awsAccessKey, awsSecretKey, synapseUser, synapseApiKey,
                "djengtest", null, "syn21558155");
        export.initialize();
        export.execute();
    }
}
