package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDate;

public class PurgeStudyUploadsByDate {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int SLEEP_TIME_MILLIS = 1000;

    // DDB
    private static DynamoDB ddbClient;
    private static boolean debug;
    private static Index attachmentRecordIdIndex;
    private static Table attachmentTable;
    private static Table healthCodeTable;
    private static Table recordTable;
    private static Index uploadDateIndex;
    private static Table uploadTable;

    // S3
    private static String attachmentBucket;
    private static AmazonS3Client s3Client;
    private static String uploadBucket;

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage: PurgeStudyUploadsByDate [path to config JSON] [study ID] [date (YYYY-MM-DD)] " +
            "[debug/release]");
            return;
        }

        if ("debug".equals(args[3])) {
            System.out.println("Running in DEBUG mode");
            debug = true;
        } else if ("release".equals(args[3])) {
            System.out.println("Running in RELEASE mode");
            System.out.println("IMPORTANT: This will delete rows from DDB and files from S3");
            debug = false;
        } else {
            System.out.println("Must specify debug or release");
            return;
        }

        init(args[0]);
        execute(args[1], LocalDate.parse(args[2]));
        cleanup();

        System.out.println("Done");
    }

    public static void init(String configPath) throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));

        // init ddb
        String ddbPrefix = configNode.get("ddbBridgePrefix").textValue();
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        attachmentTable = ddbClient.getTable(ddbPrefix + "HealthDataAttachment");
        attachmentRecordIdIndex = attachmentTable.getIndex("recordId-index");
        healthCodeTable = ddbClient.getTable(ddbPrefix + "HealthCode");
        recordTable = ddbClient.getTable(ddbPrefix + "HealthDataRecord3");
        uploadTable = ddbClient.getTable(ddbPrefix + "Upload2");
        uploadDateIndex = uploadTable.getIndex("uploadDate-index");

        // init S3 client
        attachmentBucket = configNode.get("attachmentBucket").textValue();
        s3Client = new AmazonS3Client();
        uploadBucket = configNode.get("uploadBucket").textValue();
    }

    public static void cleanup() {
        ddbClient.shutdown();
        s3Client.shutdown();
    }

    public static void execute(String studyId, LocalDate date) {
        // find all uploads for this date
        Iterable<Item> uploadsForDateIter = uploadDateIndex.query("uploadDate", date.toString());
        for (Item oneUpload : uploadsForDateIter) {
            // sleep to avoid browning out DDB
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException ex) {
                System.err.println("Interrupted while sleeping: " + ex.getMessage());
                ex.printStackTrace();
            }

            String uploadId = oneUpload.getString("uploadId");
            try {
                // First step is to re-query the table to get all params.
                Item fullUpload = uploadTable.getItem("uploadId", uploadId);
                String healthCode = fullUpload.getString("healthCode");

                // Check healthCode table to verify what study this upload comes from.
                Item healthCodeToStudy = healthCodeTable.getItem("code", healthCode);
                String uploadStudyId = healthCodeToStudy.getString("studyIdentifier");
                if (!studyId.equals(uploadStudyId)) {
                    if (debug) {
                        System.out.println("Filtered out uploadId=" + uploadId + ": Upload not in study " + studyId +
                                ", instead in study " + uploadStudyId);
                        System.out.println();
                    }
                    continue;
                }

                String recordId = fullUpload.getString("recordId");
                System.out.println("Found qualifying upload with uploadId=" + uploadId + ", recordId=" + recordId);

                if (StringUtils.isNotBlank(recordId)) {
                    // Query attachments table by recordId
                    Iterable<Item> attachmentsForRecordIter = attachmentRecordIdIndex.query("recordId", recordId);
                    for (Item oneAttachment : attachmentsForRecordIter) {
                        String attachmentId = oneAttachment.getString("id");
                        System.out.println("Found attachment for uploadId=" + uploadId + ", recordId=" + recordId +
                                ", attachmentId=" + attachmentId);

                        if (!debug) {
                            // Delete from S3 first, then delete from attachments table. This way, if deleting from S3
                            // fails, we can find the record again through the attachments table.

                            // delete from S3
                            s3Client.deleteObject(attachmentBucket, attachmentId);

                            // delete from attachments table
                            attachmentTable.deleteItem("id", attachmentId);
                        }
                    }
                }

                if (!debug) {
                    // Delete the record before the upload. This way, we can find the record from the upload if
                    // deleting the record fails. Similarly for the upload in S3.

                    // delete record
                    if (StringUtils.isNotBlank(recordId)) {
                        recordTable.deleteItem("id", recordId);
                    }

                    // delete from S3
                    s3Client.deleteObject(uploadBucket, uploadId);

                    // delete upload
                    uploadTable.deleteItem("uploadId", uploadId);

                    System.out.println("Done deleting for uploadId=" + uploadId);
                }
            } catch (RuntimeException ex) {
                System.err.println("Error processing uploadId="  + uploadId + ": " + ex.getMessage());
                ex.printStackTrace();
            }

            // Newline for better logging.
            System.out.println();
        }
    }
}
