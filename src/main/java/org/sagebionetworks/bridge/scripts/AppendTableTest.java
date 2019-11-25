package org.sagebionetworks.bridge.scripts;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Entity;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;

public class AppendTableTest {
    private static final String ENTITY_ID = "syn11956745";
    private static final String TABLE_ID = "syn11027205";

    public static void main(String[] args) throws InterruptedException, SynapseException {
        // args
        String user = args[0];
        String apiKey = args[1];

        // init SynapseClient
        SynapseClient client = new SynapseClientImpl();
        client.setUsername(user);
        client.setApiKey(apiKey);

        Entity entity = client.getEntityById(ENTITY_ID);
        System.out.println("Done!");

        //// Get column list from table, because we need column IDs. (Our test table only has one column.)
        //List<ColumnModel> columnModelList = client.getColumnModelsForTableEntity(TABLE_ID);
        //ColumnModel columnModel = columnModelList.get(0);
        //
        //// Make row headers
        //SelectColumn selectColumn = new SelectColumn();
        //selectColumn.setId(columnModel.getId());
        //
        //// make row
        //Row row = new Row();
        //row.setValues(ImmutableList.of("my-value-2"));
        //
        //// make rowset
        //RowSet rowSet = new RowSet();
        //rowSet.setTableId(TABLE_ID);
        //rowSet.setHeaders(ImmutableList.of(selectColumn));
        //rowSet.setRows(ImmutableList.of(row));
        //
        //// append
        //client.appendRowsToTable(rowSet, 30000, TABLE_ID);
    }
}
