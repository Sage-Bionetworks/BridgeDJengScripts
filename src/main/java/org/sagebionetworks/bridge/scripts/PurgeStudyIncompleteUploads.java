package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

// Incomplete uploads don't have uploadDate. This means if someone in StJ uploaded to S3 but didn't call upload
// complete, we have no way of knowing whether it's before or after the cut-off date. To be safe, delete the upload
// anyway.
public class PurgeStudyIncompleteUploads {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int PAGE_SIZE = 40;
    private static final int SLEEP_TIME_MILLIS = 1000;

    // DDB
    private static AmazonDynamoDB ddbClient;
    private static String ddbPrefix;
    private static boolean debug;

    // S3
    private static AmazonS3Client s3Client;
    private static String uploadBucket;

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.out.println("Usage: PurgeStudyIncompleteUploads [path to config JSON] [study ID] " +
                    "[last evaluated upload ID] [max total uploads] [debug/release]");
            return;
        }

        if ("debug".equals(args[4])) {
            System.out.println("Running in DEBUG mode");
            debug = true;
        } else if ("release".equals(args[4])) {
            System.out.println("Running in RELEASE mode");
            System.out.println("IMPORTANT: This will delete rows from DDB and files from S3");
            debug = false;
        } else {
            System.out.println("Must specify debug or release");
            return;
        }

        String lastEvaluatedUploadId = null;
        if (!"null".equals(args[2])) {
            lastEvaluatedUploadId = args[2];
        }

        init(args[0]);
        execute(args[1], lastEvaluatedUploadId, Integer.parseInt(args[3]));
        cleanup();

        System.out.println("Done running PurgeStudyIncompleteUploads");
    }

    public static void init(String configPath) throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));

        // init ddb
        ddbClient = new AmazonDynamoDBClient();
        ddbPrefix = configNode.get("ddbBridgePrefix").textValue();

        // init S3 client
        s3Client = new AmazonS3Client();
        uploadBucket = configNode.get("uploadBucket").textValue();
    }

    public static void cleanup() {
        ddbClient.shutdown();
        s3Client.shutdown();
    }

    public static void execute(String studyId, String lastEvaluatedUploadId, int maxTotalUploads) {
        // Scan DDB page by page
        boolean hasNext = true;
        int countSoFar = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        do {
            // logging
            if (countSoFar % 10000 < PAGE_SIZE) {
                System.out.println(countSoFar + " records seen in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                        " seconds...");
            }

            // If we've gone over the max, shortcut.
            if (maxTotalUploads > 0 && countSoFar >= maxTotalUploads) {
                break;
            }

            // sleep to avoid browning out DDB
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException ex) {
                System.err.println("Interrupted while sleeping: " + ex.getMessage());
                ex.printStackTrace();
            }

            // Scan for next page.
            System.out.println("Querying uploads table with exclusive start key=" + lastEvaluatedUploadId);
            ScanRequest scanRequest = new ScanRequest().withTableName(ddbPrefix + "Upload2").withLimit(PAGE_SIZE);
            // Eventual reads can double our throughput. Plus, there's no new data coming into StJ, so we don't need
            // consistent reads anyway.
            scanRequest.setConsistentRead(false);
            if (StringUtils.isNotBlank(lastEvaluatedUploadId)) {
                scanRequest.addExclusiveStartKeyEntry("uploadId", new AttributeValue(lastEvaluatedUploadId));
            }
            ScanResult scanResult = ddbClient.scan(scanRequest);

            // increment count so far
            countSoFar += scanResult.getScannedCount();

            // Double check and make sure last evaluated key is set up properly.
            Map<String, AttributeValue> lastEvaluatedKey = scanResult.getLastEvaluatedKey();
            if (lastEvaluatedKey != null) {
                lastEvaluatedUploadId = lastEvaluatedKey.get("uploadId").getS();
                if (StringUtils.isBlank(lastEvaluatedUploadId)) {
                    throw new IllegalStateException("No lastEvaluatedKey from the last scan");
                }
            } else {
                // We're at the last page. Clear last evaluated upload ID and hasNext.
                lastEvaluatedUploadId = null;
                hasNext = false;
            }

            // Iterate over page
            List<Map<String, AttributeValue>> itemList = scanResult.getItems();
            for (Map<String, AttributeValue> oneItem : itemList) {
                String uploadId = oneItem.get("uploadId").getS();
                try {
                    // If it contains uploadDate key, which means this is already covered by other scripts. Can ignore.
                    if (oneItem.containsKey("uploadDate")) {
                        if (debug) {
                            System.out.println("Filtered out uploadId=" + uploadId +
                                    ": Upload already has uploadDate");
                            System.out.println();
                        }
                        continue;
                    }

                    // Cross-ref health code table to see if this is in our target study.
                    String healthCode = oneItem.get("healthCode").getS();
                    GetItemResult healthCodeResult = ddbClient.getItem(ddbPrefix + "HealthCode", ImmutableMap.of(
                            "code", new AttributeValue(healthCode)));
                    Map<String, AttributeValue> healthCodeItem = healthCodeResult.getItem();
                    if (healthCodeItem == null) {
                        // This should never happen, but sometimes comes up during testing.
                        System.out.println("Upload has healthCode with no entry in HealthCode table, uploadId=" +
                                uploadId);
                        System.out.println();
                        continue;
                    }
                    String uploadStudyId = healthCodeItem.get("studyIdentifier").getS();
                    if (!studyId.equals(uploadStudyId)) {
                        if (debug) {
                            System.out.println("Filtered out uploadId=" + uploadId + ": Upload not in study " + studyId
                                    + ", instead in study " + uploadStudyId);
                            System.out.println();
                        }
                        continue;
                    }

                    System.out.println("Found qualifying upload with uploadId=" + uploadId);
                    if (!debug) {
                        // Delete from S3 before deleting from DDB. This way, if the delete from S3 fails, we can find
                        // it again.

                        // delete from S3
                        s3Client.deleteObject(uploadBucket, uploadId);

                        // delete upload
                        ddbClient.deleteItem(ddbPrefix + "Upload2", ImmutableMap.of("uploadId",
                                new AttributeValue(uploadId)));

                        System.out.println("Done deleting for uploadId=" + uploadId);
                    }
                    System.out.println();
                } catch (RuntimeException ex) {
                    System.err.println("Error processing uploadId="  + uploadId + ": " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            System.out.println();
        } while (hasNext);

        System.out.println("Total " + countSoFar + " records seen in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds...");
    }
}
