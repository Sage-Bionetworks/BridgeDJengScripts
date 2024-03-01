package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.text.MessageFormat;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * To run, use
 * mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.STSTest
 */
public class STSTest {
    private static final String BUCKET = "djengtest";
    private static final int DURATION_SECONDS = 15 * 60; // 15 min
    private static final String FOLDER = "folder1";

    private static final String ACCESSIBLE_FILE = FOLDER + "/dummy.txt";
    private static final String OUT_OF_SCOPE_FILE = "owner.txt";
    private static final String RESTRICTED_FILE = FOLDER + "/dummy2.txt";

    private static final String POLICY_TEMPLATE = "{\n" +
            "    \"Version\": \"2012-10-17\",\n" +
            "    \"Statement\": [\n" +
            "        {\n" +
            "            \"Sid\": \"ListParentBuckets\",\n" +
            "            \"Action\": [\"s3:ListBucket*\"],\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Resource\": [\"arn:aws:s3:::$bucket\"],\n" +
            "            \"Condition\":{\"StringEquals\":{\"s3:prefix\":[\"$prefix\"]}}\n" +
            "        },\n" +
            "        {\n" +
            "            \"Sid\": \"ListBucketAccess\",\n" +
            "            \"Action\": [\"s3:ListBucket*\"],\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Resource\": [\"arn:aws:s3:::$bucket\"],\n" +
            "            \"Condition\":{\"StringLike\":{\"s3:prefix\":[\"$prefix/*\"]}}\n" +
            "        },\n" +
            "        {\n" +
            "            \"Sid\": \"FolderAccess\",\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Action\": [\n" +
            "                \"s3:*\"\n" +
            "            ],\n" +
            "            \"Resource\": [\n" +
            "                \"arn:aws:s3:::$bucket/$prefix\",\n" +
            "                \"arn:aws:s3:::$bucket/$prefix/*\"\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    private final AWSSecurityTokenService stsClient;

    private AmazonS3 s3Client;

    public static void main(String[] args) {
        STSTest test = new STSTest();
        try {
            test.execute();
        } catch (Throwable t) {
            logError(t.getMessage(), t);
        } finally {
            test.shutdown();
        }
    }

    public STSTest() {
        logInfo("Initializing...");
        AWSCredentialsProvider awsCredentialsProvider = new ProfileCredentialsProvider("djeng-sts-test");
        stsClient = AWSSecurityTokenServiceClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
    }

    public void execute() {
        logInfo("Getting temporary credentials from STS...");
        String name = "test-user-" + RandomStringUtils.randomAlphabetic(4);
        String policy = POLICY_TEMPLATE.replace("$bucket", BUCKET).replace("$prefix", FOLDER);
        GetFederationTokenRequest request = new GetFederationTokenRequest(name);
        request.setDurationSeconds(DURATION_SECONDS);
        request.setPolicy(policy);
        GetFederationTokenResult result = stsClient.getFederationToken(request);
        Credentials credentials = result.getCredentials();

        logInfo("Creating S3 client with temporary credentials...");
        AWSCredentials awsCredentials = new BasicSessionCredentials(credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(), credentials.getSessionToken());
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        s3Client = AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();

        File tmpDir = Files.createTempDir();
        logInfo("Created temp dir " + tmpDir.getAbsolutePath());

        downloadFile(ACCESSIBLE_FILE, tmpDir, "accessible-file");
        logInfo("Downloaded accessible file " + ACCESSIBLE_FILE);

        try {
            downloadFile(RESTRICTED_FILE, tmpDir, "restricted-file");
            logError("Downloaded restricted file " + RESTRICTED_FILE + " but shouldn't be able to");
        } catch (AmazonClientException ex) {
            logInfo("Failed to download restricted file " + RESTRICTED_FILE + ": " + ex.getMessage());
        }

        try {
            downloadFile(OUT_OF_SCOPE_FILE, tmpDir, "out-of-scope-file");
            logError("Downloaded out-of-scope file " + OUT_OF_SCOPE_FILE + " but shouldn't be able to");
        } catch (AmazonClientException ex) {
            logInfo("Failed to download out-of-scope file " + OUT_OF_SCOPE_FILE + ": " + ex.getMessage());
        }
    }

    public void shutdown() {
        logInfo("Shutting down...");
        stsClient.shutdown();
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }

    private void downloadFile(String key, File tmpDir, String filename) {
        GetObjectRequest request = new GetObjectRequest(BUCKET, key);
        s3Client.getObject(request, new File(tmpDir, filename));
    }
}
