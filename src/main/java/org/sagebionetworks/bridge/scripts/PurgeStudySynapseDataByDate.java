package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSelection;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

import org.sagebionetworks.bridge.synapse.SynapseTableIterator;

public class PurgeStudySynapseDataByDate {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int SLEEP_TIME_MILLIS = 1000;

    private static DynamoDB ddbClient;
    private static boolean debug;
    private static SynapseClient synapseClient;
    private static Table synapseMetaTablesDdbTable;
    private static Table synapseTablesDdbTable;

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage: PurgeStudySynapseDataByDate [path to config JSON] [study ID] " +
                    "[date (YYYY-MM-DD)] [debug/release]");
            return;
        }

        if ("debug".equals(args[3])) {
            System.out.println("Running in DEBUG mode");
            debug = true;
        } else if ("release".equals(args[3])) {
            System.out.println("Running in RELEASE mode");
            System.out.println("IMPORTANT: This will delete data from Synapse");
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
        String ddbPrefix = configNode.get("ddbPrefix").textValue();
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        synapseMetaTablesDdbTable = ddbClient.getTable(ddbPrefix + "SynapseMetaTables");
        synapseTablesDdbTable = ddbClient.getTable(ddbPrefix + "SynapseTables");

        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public static void cleanup() {
        ddbClient.shutdown();
    }

    public static void execute(String studyId, LocalDate date) {
        // iterate over all Synapse tables
        Iterable<Item> synapseMetaTablesDdbIter = synapseMetaTablesDdbTable.scan();
        for (Item oneSynapseMetaTable : synapseMetaTablesDdbIter) {
            String tableName = oneSynapseMetaTable.getString("tableName");
            String synapseTableId = oneSynapseMetaTable.getString("tableId");
            handleTable(studyId, date, tableName, synapseTableId);
            System.out.println();
        }

        Iterable<Item> synapseTablesDdbIter = synapseTablesDdbTable.scan();
        for (Item oneSynapseTable : synapseTablesDdbIter) {
            String schemaKey = oneSynapseTable.getString("schemaKey");
            String synapseTableId = oneSynapseTable.getString("tableId");
            handleTable(studyId, date, schemaKey, synapseTableId);
            System.out.println();
        }
    }

    public static void handleTable(String studyId, LocalDate date, String tableKey, String synapseTableId) {
        // sleep to avoid browning out DDB or Synapse
        try {
            Thread.sleep(SLEEP_TIME_MILLIS);
        } catch (InterruptedException ex) {
            System.err.println("Interrupted while sleeping: " + ex.getMessage());
            ex.printStackTrace();
        }

        if (!tableKey.startsWith(studyId)) {
            if (debug) {
                System.out.println("Skipping table " + tableKey);
            }
            return;
        }

        System.out.println("Processing table tableKey=" + tableKey + ", synapseTableId=" + synapseTableId);
        try {
            // get table from Synapse
            TableEntity synapseTable = synapseClient.getEntity(synapseTableId, TableEntity.class);
            if (synapseTable == null) {
                System.out.println("Couldn't find table tableKey=" + tableKey + ", synapseTableId=" + synapseTableId);
                return;
            }

            // query and iterate over all records in the given date
            SynapseTableIterator tableRowIter = new SynapseTableIterator(synapseClient, "SELECT * FROM " +
                    synapseTableId + " WHERE uploadDate = '" + date.toString() + "'", synapseTableId);

            // get headers
            List<SelectColumn> headerList = tableRowIter.getHeaders();
            int numCols = headerList.size();

            // check for recordId column and file handle columns
            Integer recordIdColIdx = null;
            List<Integer> fileHandleColIdxList = new ArrayList<>();
            for (int i = 0; i < numCols; i++) {
                SelectColumn oneHeader = headerList.get(i);

                if ("recordId".equals(oneHeader.getName())) {
                    recordIdColIdx = i;
                }

                if (oneHeader.getColumnType() == ColumnType.FILEHANDLEID) {
                    fileHandleColIdxList.add(i);
                }
            }

            // iterate over rows
            List<Long> rowIdsToDelete = new ArrayList<>();
            while (tableRowIter.hasNext()) {
                Row oneRow = tableRowIter.next();
                List<String> rowValueList = oneRow.getValues();
                String recordId = null;
                if (recordIdColIdx != null) {
                    recordId = rowValueList.get(recordIdColIdx);
                }
                rowIdsToDelete.add(oneRow.getRowId());
                System.out.println("Found record for table " + tableKey + ", recordId=" + recordId);

                try {
                    // find all file handle IDs
                    for (int oneFileHandleColIdx : fileHandleColIdxList) {
                        String fileHandleId = rowValueList.get(oneFileHandleColIdx);
                        if (StringUtils.isNotBlank(fileHandleId)) {
                            System.out.println("Found file handle for table " + tableKey + ", recordId=" + recordId +
                                    ", fileHandleId=" + fileHandleId);
                            if (!debug) {
                                synapseClient.deleteFileHandle(fileHandleId);
                            }
                        }
                    }
                } catch (SynapseException | RuntimeException ex) {
                    System.out.println("Error processing row, tableKey=" + tableKey + ", synapseTableId=" +
                            synapseTableId + ", recordId=" + recordId + ": " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            if (!rowIdsToDelete.isEmpty() && !debug) {
                // delete rows from Synapse table
                RowSelection rowSelectionToDelete = new RowSelection();
                rowSelectionToDelete.setEtag(tableRowIter.getEtag());
                rowSelectionToDelete.setRowIds(rowIdsToDelete);
                rowSelectionToDelete.setTableId(synapseTableId);
                synapseClient.deleteRowsFromTable(rowSelectionToDelete);

                System.out.println("Deleted rows from table tableKey=" + tableKey + ", synapseTableId=" +
                        synapseTableId + ", " + rowIdsToDelete.size() + " rows");
            }
        } catch (RuntimeException | SynapseException ex) {
            System.out.println("Error processing table " + tableKey + ": " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
