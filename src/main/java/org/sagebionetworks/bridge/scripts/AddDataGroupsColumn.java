package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;

/**
 * <p>
 * Script to add dataGroups column to Bridge-EX Synapse tables. To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.AddDataGroupsColumn -Dexec.args=[args]"
 * </p>
 * <p>
 * Usage: AddDataGroupsColumn [path to config JSON] [study ID]
 * </p>
 */
public class AddDataGroupsColumn {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final ColumnModel DATA_GROUPS_COLUMN_MODEL;
    static {
        DATA_GROUPS_COLUMN_MODEL = new ColumnModel();
        DATA_GROUPS_COLUMN_MODEL.setName("dataGroups");
        DATA_GROUPS_COLUMN_MODEL.setColumnType(ColumnType.STRING);
        DATA_GROUPS_COLUMN_MODEL.setMaximumSize(100L);
    }

    private static DynamoDB ddbClient;
    private static SynapseClient synapseClient;
    private static Table synapseMetaTablesDdbTable;
    private static Table synapseTablesDdbTable;

    public static void main(String[] args) throws IOException, SynapseException {
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

    public static void execute(String studyId) throws SynapseException {
        // iterate over all Synapse tables
        Iterable<Item> synapseMetaTablesDdbIter = synapseMetaTablesDdbTable.scan();
        for (Item oneSynapseMetaTable : synapseMetaTablesDdbIter) {
            String tableName = oneSynapseMetaTable.getString("tableName");
            String synapseTableId = oneSynapseMetaTable.getString("tableId");
            handleTable(studyId, tableName, synapseTableId);
        }

        Iterable<Item> synapseTablesDdbIter = synapseTablesDdbTable.scan();
        for (Item oneSynapseTable : synapseTablesDdbIter) {
            String schemaKey = oneSynapseTable.getString("schemaKey");
            String synapseTableId = oneSynapseTable.getString("tableId");
            handleTable(studyId, schemaKey, synapseTableId);
        }
    }

    public static void handleTable(String studyId, String tableKey, String synapseTableId) throws SynapseException {
        if (!tableKey.startsWith(studyId)) {
            System.out.println("Skipping " + tableKey);
            return;
        }

        System.out.println("Processing " + tableKey);

        // Get the column models from Synapse.
        List<ColumnModel> columnModelList = synapseClient.getColumnModelsForTableEntity(synapseTableId);
        int externalIdColIdx = -1;
        for (int i = 0; i < columnModelList.size(); i++) {
            ColumnModel oneColumnModel = columnModelList.get(i);
            String colName = oneColumnModel.getName();
            if ("externalId".equals(colName)) {
                externalIdColIdx = i;
            } else if ("dataGroups".equals(colName)) {
                System.out.println("Table " + tableKey + " already has dataGroups column, skipping");
                return;
            }
        }

        // Post the column model to Synapse to get the column ID.
        ColumnModel createdColumnModel = synapseClient.createColumnModel(DATA_GROUPS_COLUMN_MODEL);
        String dataGroupsColId = createdColumnModel.getId();

        // Get the table entity from Synapse, which we'll modify.
        TableEntity table = synapseClient.getEntity(synapseTableId, TableEntity.class);
        List<String> colIdList = table.getColumnIds();
        if (externalIdColIdx >= 0) {
            // Put it after the externalId column.
            colIdList.add(externalIdColIdx + 1, dataGroupsColId);
        } else {
            // No externalId column. Just stick dataGroups at the end.
            colIdList.add(dataGroupsColId);
        }

        // Write the table back into Synapse.
        synapseClient.putEntity(table);
    }
}
