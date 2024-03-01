package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DownloadSqsMessages {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/649232250620/bridgeserver2-antivirus-prod-VirusScanDeadLetterQueue-HnnMlrhSqzwF";

    private static AmazonSQS sqs;
    private static PrintWriter fileWriter;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            logInfo("Usage: DownloadSqsMessages [path to config JSON] [path to output file]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        init(args[0], args[1]);

        // Execute.
        try {
            execute();
        } finally {
            // Close file writers.
            fileWriter.close();
            sqs.shutdown();
        }
    }

    private static void init(String configPath, String outputPath) throws IOException {
        // Init AWS SQS.
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));
        String awsKey = configNode.get("awsAccessKey").textValue();
        String awsSecretKey = configNode.get("awsSecretKey").textValue();

        AWSCredentials credentials = new BasicAWSCredentials(awsKey, awsSecretKey);
        AWSCredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);
        sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).build();

        // Init file writer.
        fileWriter = new PrintWriter(new FileWriter(outputPath), true);
    }

    private static void execute() {
        // Pre-make SQS request.
        ReceiveMessageRequest request = new ReceiveMessageRequest();
        request.setMaxNumberOfMessages(10);
        request.setQueueUrl(QUEUE_URL);
        request.setWaitTimeSeconds(20);

        int numMessages = 0;
        while (true) {
            // Without this sleep statement, really weird things happen when we Ctrl+C the process. (Not relevant for
            // production, but happens all the time for local testing.) Empirically, it takes up to 125ms for the JVM
            // to shut down cleanly.) Plus, it prevents us from polling the SQS queue too fast when there are a lot of
            // messages.
            //try {
            //    Thread.sleep(125);
            //} catch (InterruptedException ex) {
            //    logError("Interrupted while sleeping: " + ex.getMessage(), ex);
            //}

            try {
                // Poll SQS. According to the docs, this result.getMessages() will never be null.
                List<Message> messageList = sqs.receiveMessage(request).getMessages();
                if (messageList.isEmpty()) {
                    // We're done receiving messages. Quit.
                    break;
                }

                for (Message oneMessage : messageList) {
                    // Remove newlines to make the output file easier to read.
                    String body = oneMessage.getBody().replaceAll("\n", " ");

                    // Write message to file.
                    fileWriter.println(body);

                    // Delete message from SQS.
                    sqs.deleteMessage(QUEUE_URL, oneMessage.getReceiptHandle());

                    // Log progress.
                    numMessages++;
                    if (numMessages % 1000 == 0) {
                        logInfo("Processed " + numMessages + " messages");
                    }
                }
            } catch (Exception | Error e) {
                logError("Error processing message: " + e.getMessage(), e);
            }
        }
        logInfo("Processed a total of " + numMessages + " messages");
    }
}
