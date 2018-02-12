package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

import au.com.bytecode.opencsv.CSVReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

/**
 * <p>
 * Script to move FitBit heart rate data from a LargeText to a FileHandle.
 * </p>
 * <p>
 * Usage: Usage: FitBitBackfill [path to config JSON] [table ID]
 */
public class FitBitBackfill {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int REPORT_INTERVAL = 25;
    private static final Joiner COLUMN_JOINER = Joiner.on('\t').useForNull("");

    private static SynapseClient synapseClient;
    private static SynapseHelper synapseHelper;

    public static void main(String[] args) throws BridgeSynapseException, IOException, SynapseException {
        if (args.length != 2) {
            System.out.println("Usage: AddDataGroupsColumn [path to config JSON] [study ID]");
            return;
        }
        init(args[0]);
        execute(args[1]);
        cleanup();

        System.out.println("Done");
    }

    public static void init(String configPath) throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File(configPath));

        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);

        // init Synapse helper
        synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(synapseClient);
    }

    public static void cleanup() {

    }

    public static void execute(String tableId) throws BridgeSynapseException, IOException, SynapseException {
        // Make temp files.
        File tempDir = Files.createTempDir();
        logWithTimestamp("Making temp dir " + tempDir.getAbsolutePath());
        File oldCsvFile = new File(tempDir, "HeartRate.old.csv");
        File newCsvFile = new File(tempDir, "HeartRate.new.csv");

        // Download table.
        downloadTable(tableId, oldCsvFile);
        processTable(tempDir, oldCsvFile, newCsvFile);

        logWithTimestamp("Uploading to " + tableId);
        synapseHelper.uploadTsvFileToTable(tableId, newCsvFile);

        logWithTimestamp("Done");
    }

    private static void downloadTable(String tableId, File destinationFile) throws SynapseException {
        logWithTimestamp("Downloading from table " + tableId);

        // Query the table.
        String jobToken = synapseClient.downloadCsvFromTableAsyncStart("select * from " + tableId +
                " where \"dataset.old\" is not null", true, true, null, tableId);
        DownloadFromTableResult downloadResult = null;
        for (int i = 0; i < 6; i++) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            try {
                downloadResult = synapseClient.downloadCsvFromTableAsyncGet(jobToken, tableId);
            } catch (SynapseResultNotReadyException ex) {
                // squelch
            }
            if (downloadResult != null) {
                break;
            }
        }
        if (downloadResult == null) {
            throw new RuntimeException("Unable to query table");
        }

        // Download the result.
        synapseClient.downloadFromFileHandleTemporaryUrl(downloadResult.getResultsFileHandleId(), destinationFile);
    }

    private static void processTable(File tempDir, File oldCsvTable, File newCsvFile) throws IOException, SynapseException {
        logWithTimestamp("Starting to process table");

        try (CSVReader oldCsvReader = new CSVReader(Files.newReader(oldCsvTable, Charsets.UTF_8));
                PrintWriter newCsvWriter = new PrintWriter(Files.newWriter(newCsvFile, Charsets.UTF_8))) {
            // Find headers for dataset.old and dataset.
            String[] headers = oldCsvReader.readNext();
            Integer datasetOldIdx = null;
            Integer datasetNewIdx = null;
            int numColumns = headers.length;
            for (int i = 0; i < numColumns; i++) {
                if ("dataset".equals(headers[i])) {
                    datasetNewIdx = i;
                } else if ("dataset.old".equals(headers[i])) {
                    datasetOldIdx = i;
                }
            }
            if (datasetOldIdx == null) {
                throw new RuntimeException("Couldn't find \"dataset.old\" column");
            }
            if (datasetNewIdx == null) {
                throw new RuntimeException("Couldn't find \"dataset\" column");
            }

            // Copy over headers.
            newCsvWriter.println(COLUMN_JOINER.join(headers));

            // Process each row.
            Stopwatch stopwatch = Stopwatch.createStarted();
            int rowsProcessed = 0;
            String[] oldRow;
            while ((oldRow = oldCsvReader.readNext()) != null) {
                String[] newRow = new String[numColumns];

                // Write the old dataset value as a file.
                String datasetFilename = "dataset" + RandomStringUtils.randomAlphabetic(4);
                String datasetValue = oldRow[datasetOldIdx];
                File datasetFile = new File(tempDir, datasetFilename);
                try (Writer datasetWriter = Files.newWriter(datasetFile, Charsets.UTF_8)) {
                    datasetWriter.write(datasetValue);
                }

                // Upload the dataset value as a file handle.
                FileHandle fileHandle = synapseHelper.createFileHandleWithRetry(datasetFile);

                // Copy columns.
                for (int i = 0; i < numColumns; i++) {
                    if (i == datasetOldIdx) {
                        // blank out the old column
                        newRow[i] = null;
                    } else if (i == datasetNewIdx) {
                        // new column is a file handle ID
                        newRow[i] = fileHandle.getId();
                    } else {
                        // everything else is copied verbatim
                        newRow[i] = oldRow[i];
                    }
                }

                // Write row.
                newCsvWriter.println(COLUMN_JOINER.join(newRow));

                // Logging.
                rowsProcessed++;
                if (rowsProcessed % REPORT_INTERVAL == 0) {
                    logWithTimestamp("Processed " + rowsProcessed + " rows in " +
                            stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
                }
            }

            logWithTimestamp("Finished processing " + rowsProcessed + " rows in " +
                    stopwatch.elapsed(TimeUnit.SECONDS) + " seconds...");
        }
    }

    private static void logWithTimestamp(String msg) {
        System.out.println("[" + DateTime.now().toString() + "] " + msg);
    }
}
