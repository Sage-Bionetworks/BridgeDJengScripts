package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SortWorkerMessages {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static BufferedReader fileReader;
    private static PrintWriter healthCodeWriter;
    private static PrintWriter miscMessageWriter;
    private static PrintWriter uploadIdWriter;

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
            healthCodeWriter.close();
            miscMessageWriter.close();
            uploadIdWriter.close();
        }

        // Need to force exit, because JavaSDK doesn't close itself.
        System.exit(0);
    }

    private static void init(String inputFilePath, String outputPrefix) throws IOException {
        // Set up readers and writers.
        fileReader = new BufferedReader(new FileReader(inputFilePath));
        healthCodeWriter = new PrintWriter(new FileWriter(outputPrefix + "participants"));
        miscMessageWriter = new PrintWriter(new FileWriter(outputPrefix + "misc"));
        uploadIdWriter = new PrintWriter(new FileWriter(outputPrefix + "uploads"));
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
                        healthCodeWriter.println(body.get("healthCode").textValue());
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
}
