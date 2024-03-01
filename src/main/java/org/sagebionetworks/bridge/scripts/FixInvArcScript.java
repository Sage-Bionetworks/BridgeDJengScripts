package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.RateLimiter;
import org.bouncycastle.cms.CMSException;

import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.CmsEncryptorCacheLoader;
import org.sagebionetworks.bridge.s3.S3Helper;

public class FixInvArcScript {
    private static final String DOWNLOADED_FILES_ROOT = "/Users/dwaynejeng/Documents/backfill/inv-arc/";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int REPORTING_INTERVAL = 100;
    private static final String UPLOAD_BUCKET = "org-sagebridge-upload-prod";

    private static CmsEncryptor arcEncryptor;
    private static CmsEncryptor invArcEncryptor;
    private static S3Helper s3Helper;
    private static final RateLimiter perUploadRateLimiter = RateLimiter.create(0.5);

    public static void main(String[] args) throws CertificateEncodingException, CMSException,
            IOException {
        if (args.length != 2) {
            logInfo("Usage: FixInvArcScript [path to config JSON] [path to list of upload IDs]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        init(args[0]);

        // Execute.
        execute(args[1]);
    }

    private static void init(String configPath) throws CertificateEncodingException, IOException {
        // Init S3 helper.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));
        String awsKey = configNode.get("awsKey").textValue();
        String awsSecretKey = configNode.get("awsSecretKey").textValue();
        String awsSessionToken = configNode.get("awsSessionToken").textValue();

        AWSCredentials credentials = new BasicSessionCredentials(awsKey, awsSecretKey, awsSessionToken);
        AWSCredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);
        AmazonS3Client s3Client = new AmazonS3Client(credentialsProvider);
        s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // Load encryptors.
        CmsEncryptorCacheLoader encryptorCacheLoader = new CmsEncryptorCacheLoader();
        encryptorCacheLoader.setCertBucket("org-sagebridge-upload-cms-cert-prod");
        encryptorCacheLoader.setPrivateKeyBucket("org-sagebridge-upload-cms-priv-prod");
        encryptorCacheLoader.setS3Helper(s3Helper);

        arcEncryptor = encryptorCacheLoader.load("arc");
        invArcEncryptor = encryptorCacheLoader.load("inv-arc");
    }

    private static void execute(String uploadIdListPath) throws CertificateEncodingException, CMSException,
            IOException {
        // Fix InvArc.
        logInfo("Fixing InvArc...");

        // Read upload ID list.
        List<String> uploadIdList = Files.readLines(new File(uploadIdListPath), StandardCharsets.UTF_8);

        Stopwatch stopwatch = Stopwatch.createStarted();
        int numUploads = 0;
        for (String uploadId : uploadIdList) {
            // Rate limit.
            perUploadRateLimiter.acquire();

            // Download file.
            File encryptedArcFile = new File(DOWNLOADED_FILES_ROOT + uploadId + ".encrypted.arc");
            String encryptedInvArcFilePath = DOWNLOADED_FILES_ROOT + uploadId + ".encrypted.inv-arc";
            s3Helper.downloadS3File(UPLOAD_BUCKET, uploadId, encryptedArcFile);

            // Decrypt.
            File decryptedFile = new File(DOWNLOADED_FILES_ROOT + uploadId + ".decrypted");
            try (FileInputStream encryptedArcStream = new FileInputStream(encryptedArcFile);
                    InputStream decryptedInputStream = arcEncryptor.decrypt(encryptedArcStream);
                    FileOutputStream decryptedOutStream = new FileOutputStream(decryptedFile)) {
                ByteStreams.copy(decryptedInputStream, decryptedOutStream);
            }

            // Report progress.
            numUploads++;
            if (numUploads % REPORTING_INTERVAL == 0) {
                logInfo("Fixed " + numUploads + " uploads in " + stopwatch);
            }
        }
    }
}
