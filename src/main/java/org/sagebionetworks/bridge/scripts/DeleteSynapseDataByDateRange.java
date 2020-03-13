package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.EntityChildrenRequest;
import org.sagebionetworks.repo.model.EntityChildrenResponse;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.EntityType;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSelection;
import org.sagebionetworks.repo.model.table.SelectColumn;

import org.sagebionetworks.bridge.synapse.SynapseTableIterator;

public class DeleteSynapseDataByDateRange {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int SLEEP_TIME_MILLIS = 1000;

    private static boolean debug;
    private static SynapseClient synapseClient;

    public static void main(String[] args) throws IOException, SynapseException {
        if (args.length != 5) {
            logInfo("Usage: DeleteSynapseDataByDateRange [path to config JSON] [synapse project ID] " +
                    "[start date (YYYY-MM-DD)] [end date] [debug/release]");
            return;
        }

        if ("debug".equals(args[4])) {
            logInfo("Running in DEBUG mode");
            debug = true;
        } else if ("release".equals(args[4])) {
            logInfo("Running in RELEASE mode");
            logInfo("IMPORTANT: This will delete data from Synapse");
            debug = false;
        } else {
            logInfo("Must specify debug or release");
            return;
        }

        init(args[0]);
        execute(args[1], LocalDate.parse(args[2]), LocalDate.parse(args[3]));

        logInfo("Done");
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
    }

    public static void execute(String projectId, LocalDate startDate, LocalDate endDate) throws SynapseException {
        // Get all tables in Synapse project.
        EntityChildrenRequest entityChildrenRequest = new EntityChildrenRequest();
        entityChildrenRequest.setParentId(projectId);
        entityChildrenRequest.setIncludeTypes(ImmutableList.of(EntityType.table));
        EntityChildrenResponse entityChildrenResponse = synapseClient.getEntityChildren(entityChildrenRequest);

        boolean hasNext;
        do {
            List<EntityHeader> entityHeaderList = entityChildrenResponse.getPage();
            for (EntityHeader entityHeader : entityHeaderList) {
                try {
                    handleTable(entityHeader.getId(), startDate, endDate);
                } catch (Exception ex) {
                    logError("Exception handling table " + entityHeader.getId(), ex);
                }
            }

            // Fetch next page, if it exists.
            hasNext = entityChildrenResponse.getNextPageToken() != null;
            if (hasNext) {
                entityChildrenRequest.setNextPageToken(entityChildrenResponse.getNextPageToken());
                entityChildrenResponse = synapseClient.getEntityChildren(entityChildrenRequest);
            }
        } while (hasNext);
    }

    public static void handleTable(String synapseTableId, LocalDate startDate, LocalDate endDate)
            throws SynapseException {
        // Sleep to avoid getting throttled by Synapse.
        try {
            Thread.sleep(SLEEP_TIME_MILLIS);
        } catch (InterruptedException ex) {
            logError("Interrupted while sleeping: " + ex.getMessage(), ex);
        }

        logInfo("Processing table " + synapseTableId);

        // Check if the table has an uploadDate and a recordId column.
        List<ColumnModel> columnModelList = synapseClient.getColumnModelsForTableEntity(synapseTableId);
        Set<String> columnNameSet = columnModelList.stream().map(ColumnModel::getName).collect(Collectors.toSet());
        if (!columnNameSet.contains("uploadDate")) {
            logInfo("Table " + synapseTableId + " doesn't have a uploadDate column. Skipping.");
            return;
        }
        if (!columnNameSet.contains("recordId")) {
            logInfo("Table " + synapseTableId + " doesn't have a recordId column. Skipping.");
            return;
        }

        // Query and iterate over all records in the given date range.
        SynapseTableIterator tableRowIter = new SynapseTableIterator(synapseClient, "SELECT * FROM " +
                synapseTableId + " WHERE uploadDate >= '" + startDate.toString() + "' AND uploadDate <= '" +
                endDate.toString() + "'", synapseTableId);

        // Get headers.
        List<SelectColumn> headerList = tableRowIter.getHeaders();
        int numCols = headerList.size();

        // Check for recordId column.
        Integer recordIdColIdx = null;
        for (int i = 0; i < numCols; i++) {
            SelectColumn oneHeader = headerList.get(i);
            if ("recordId".equals(oneHeader.getName())) {
                recordIdColIdx = i;
            }
        }

        // Iterate over rows.
        List<Long> rowIdsToDelete = new ArrayList<>();
        while (tableRowIter.hasNext()) {
            Row row = tableRowIter.next();
            List<String> rowValueList = row.getValues();
            //noinspection ConstantConditions
            String recordId = rowValueList.get(recordIdColIdx);
            rowIdsToDelete.add(row.getRowId());
            logInfo("Found record for table " + synapseTableId + ", recordId=" + recordId);
        }

        if (!rowIdsToDelete.isEmpty() && !debug) {
            // Delete rows from Synapse table.
            RowSelection rowSelectionToDelete = new RowSelection();
            rowSelectionToDelete.setEtag(tableRowIter.getEtag());
            rowSelectionToDelete.setRowIds(rowIdsToDelete);
            rowSelectionToDelete.setTableId(synapseTableId);
            synapseClient.deleteRowsFromTable(rowSelectionToDelete);

            logInfo("Deleted rows from table " + synapseTableId + ", " + rowIdsToDelete.size() + " rows");
        }
    }
}
