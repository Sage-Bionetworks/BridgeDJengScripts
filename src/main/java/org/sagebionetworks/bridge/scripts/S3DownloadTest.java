package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import org.apache.http.client.fluent.Request;

import com.amazonaws.services.s3.AmazonS3Client;
import org.joda.time.DateTime;

/**
 * To run, use
 * mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.S3DownloadTest
 */
public class S3DownloadTest {
    private static final String BUCKET = "djengtest";
    private static final String FILE_NAME = "10mb-test-file";
    private static final int NUM_ITERATIONS = 10;
    private static final String TEST_FILE_PATH = "/Users/dwaynejeng/Documents/scratch/" + FILE_NAME;

    private final AmazonS3Client s3Client;

    public static void main(String[] args) {
        S3DownloadTest test = new S3DownloadTest();
        try {
            test.execute();
        } catch (Throwable t) {
            logError(t.getMessage(), t);
        } finally {
            test.shutdown();
        }
    }

    public S3DownloadTest() {
        logInfo("Initializing...");
        s3Client = new AmazonS3Client();
    }

    public void execute() throws IOException, URISyntaxException {
        File tmpDir = Files.createTempDir();
        logInfo("Created temp dir " + tmpDir.getAbsolutePath());

        logInfo("Uploading test file...");
        s3Client.putObject(BUCKET, FILE_NAME, new File(TEST_FILE_PATH));

        DateTime expirationTimeJoda = DateTime.now().plusHours(1);
        Date expirationTime = expirationTimeJoda.toDate();
        long presignedUrlDownloadTimeMillis = 0;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            logInfo("Testing pre-signed URL, iteration " + i);
            File file = new File(tmpDir, "downloaded-presigned-url-" + i);
            URL presignedUrl = s3Client.generatePresignedUrl(BUCKET, FILE_NAME, expirationTime);
            Stopwatch stopwatch = Stopwatch.createStarted();
            Request.Get(presignedUrl.toURI()).execute().saveContent(file);
            presignedUrlDownloadTimeMillis += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        logInfo("Total download time (presigned URLs): " + presignedUrlDownloadTimeMillis);
        logInfo("Average download time (presigned URLs): " + presignedUrlDownloadTimeMillis / NUM_ITERATIONS);

        long apiDownloadTimeMillis = 0;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            logInfo("Testing API download, iteration " + i);
            File file = new File(tmpDir, "api-download-" + i);
            GetObjectRequest request = new GetObjectRequest(BUCKET, FILE_NAME);
            Stopwatch stopwatch = Stopwatch.createStarted();
            s3Client.getObject(request, file);
            apiDownloadTimeMillis += stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        logInfo("Total download time (API download): " + apiDownloadTimeMillis);
        logInfo("Average download time (API download): " + apiDownloadTimeMillis / NUM_ITERATIONS);
    }

    public void shutdown() {
        logInfo("Shutting down...");
        s3Client.shutdown();
    }
}
