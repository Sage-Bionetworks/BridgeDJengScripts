package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.FileEntity;
import org.sagebionetworks.repo.model.file.S3FileHandle;

/**
 * To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.UploadS3FileHandleTest -Dexec.args=[args]"
 */
public class UploadS3FileHandleTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String PROJECT_ID = "syn4727563";
    private static final String S3_BUCKET = "dev.djeng.test";
    private static final String S3_KEY = "folder1";
    private static final long STORAGE_LOCATION_ID = 40855;

    private final SynapseClient synapseClient;

    public static void main(String[] args) throws IOException, SynapseException {
        if (args.length != 1) {
            logInfo("Usage: UploadS3FileHandleTest [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        UploadS3FileHandleTest test = new UploadS3FileHandleTest(configNode);

        // Execute.
        test.execute();
    }

    public UploadS3FileHandleTest(JsonNode configNode) {
        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public void execute() throws SynapseException {
        // Create file handle.
        S3FileHandle s3FileHandle = new S3FileHandle();
        s3FileHandle.setBucketName(S3_BUCKET);
        s3FileHandle.setContentMd5("dummyvalue");
        s3FileHandle.setFileName(S3_KEY);
        s3FileHandle.setKey(S3_KEY);
        s3FileHandle.setStorageLocationId(STORAGE_LOCATION_ID);
        s3FileHandle = synapseClient.createExternalS3FileHandle(s3FileHandle);

        // Create file entity.
        FileEntity fileEntity = new FileEntity();
        fileEntity.setDataFileHandleId(s3FileHandle.getId());
        fileEntity.setName(S3_KEY);
        fileEntity.setParentId(PROJECT_ID);
        synapseClient.createEntity(fileEntity);
    }
}
