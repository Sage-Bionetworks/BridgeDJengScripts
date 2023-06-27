package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SortWorkerMessages {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static BufferedReader fileReader;
    private static final Map<String, PrintWriter> healthCodeWritersByStudy = new HashMap<>();
    private static PrintWriter miscMessageWriter;
    private static PrintWriter uploadIdWriter;
    private static String outputPrefix;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            logInfo("Usage: SortWorkerMessages [path to worker message file] [output file prefix]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        init(args[0], args[1]);

        // Execute.
        try {
            execute();
        } finally {
            // Close file readers and writers.
            fileReader.close();
            miscMessageWriter.close();
            uploadIdWriter.close();

            for (PrintWriter writer : healthCodeWritersByStudy.values()) {
                writer.close();
            }
        }

        // Need to force exit, because JavaSDK doesn't close itself.
        System.exit(0);
    }

    private static void init(String inputFilePath, String outputPrefix) throws IOException {
        // Set up readers and writers.
        fileReader = new BufferedReader(new FileReader(inputFilePath));
        miscMessageWriter = new PrintWriter(new FileWriter(outputPrefix + "misc"));
        uploadIdWriter = new PrintWriter(new FileWriter(outputPrefix + "uploads"));

        SortWorkerMessages.outputPrefix = outputPrefix;
    }

    private static void execute() throws IOException {
        // Read file line by line.
        String line;
        while ((line = fileReader.readLine()) != null) {
            try {
                // Parse JSON.
                JsonNode messageNode = JSON_MAPPER.readTree(line);
                String service = messageNode.get("service").textValue();
                JsonNode body = messageNode.get("body");

                switch (service) {
                    case "Exporter3Worker":
                        uploadIdWriter.println(body.get("recordId").textValue());
                        break;
                    case "Ex3ParticipantVersionWorker":
                        processParticipantMessage(body);
                        break;
                    default:
                        // Write the whole line to misc messages.
                        miscMessageWriter.println(line);
                        break;
                }
            } catch (Exception ex) {
                logError("Error parsing line: " + line, ex);
            }
        }
    }

    private static void processParticipantMessage(JsonNode body) throws IOException {
        String appId = body.get("appId").textValue();
        String healthCode = body.get("healthCode").textValue();

        PrintWriter writer = healthCodeWritersByStudy.get(appId);
        if (writer == null) {
            writer = new PrintWriter(new FileWriter(outputPrefix + "participants-" + appId));
            healthCodeWritersByStudy.put(appId, writer);
        }

        writer.println(healthCode);
    }
}
