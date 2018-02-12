package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;

public class UpdateTableColumnTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String TABLE_ID = "syn5936633";

    private static SynapseClient synapseClient;

    public static void main(String[] args) throws IOException, SynapseException {
        init();
        execute();

        System.out.println("Done");
    }

    public static void init() throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File("/Users/dwaynejeng/scripts-config-local.json"));

        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public static void execute() throws SynapseException {
        // Create "asdf" column
        ColumnModel asdfColumnModel = new ColumnModel();
        asdfColumnModel.setName("asdf");
        asdfColumnModel.setColumnType(ColumnType.STRING);
        asdfColumnModel.setMaximumSize(24L);

        ColumnModel createdAsdfColumnModel = synapseClient.createColumnModel(asdfColumnModel);
        Map<String, ColumnModel> columnNamesToModels = new TreeMap<>();
        columnNamesToModels.put("asdf", createdAsdfColumnModel);

        // Get the column models from Synapse.
        List<ColumnModel> columnModelList = synapseClient.getColumnModelsForTableEntity(TABLE_ID);
        for (ColumnModel oneColumnModel : columnModelList) {
            columnNamesToModels.put(oneColumnModel.getName(), oneColumnModel);
        }

        // Get column ID list. We use TreeMap, so this will be in alphabetical order.
        List<String> colIdList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneColumnModel : columnNamesToModels.values()) {
            colIdList.add(oneColumnModel.getId());
        }

        // Get the table entity from Synapse, which we'll modify.
        TableEntity table = synapseClient.getEntity(TABLE_ID, TableEntity.class);
        table.setColumnIds(colIdList);
        synapseClient.putEntity(table);
    }
}
