package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.table.TableEntity;

/**
 * <p>
 * Backfill script to add BridgeStaff and BridgeAdmin to all Exporter-owned Synapse tables, so we don't have to log in
 * with Exporter account to do dev stuff.
 * </p>
 * <p>
 * To run, use
 * "mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.ExporterPermissionsBackfill -Dexec.args=[args]"
 * </p>
 */
public class ExporterPermissionsBackfill {
    static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static final Set<ACCESS_TYPE> ACCESS_TYPE_ADMIN = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD,
            ACCESS_TYPE.UPDATE, ACCESS_TYPE.DELETE, ACCESS_TYPE.CREATE, ACCESS_TYPE.CHANGE_PERMISSIONS,
            ACCESS_TYPE.CHANGE_SETTINGS, ACCESS_TYPE.MODERATE);
    static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);

    private final long bridgeAdminTeamId;
    private final long bridgeStaffTeamId;
    private final DynamoDB ddbClient;
    private final SynapseClient synapseClient;
    private final Table synapseMetaTablesDdbTable;
    private final Table synapseTablesDdbTable;

    // Rate limiter, used to limit the amount of traffic to Synapse. Synapse throttles at 10 requests per second.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: SmsRegistrationBackfill [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        ExporterPermissionsBackfill backfill = new ExporterPermissionsBackfill(configNode);

        // Execute.
        backfill.execute();

        // Cleanup.
        backfill.cleanup();
    }

    public ExporterPermissionsBackfill(JsonNode configNode) {
        // Init team IDs.
        bridgeAdminTeamId = configNode.get("bridgeAdminTeamId").longValue();
        bridgeStaffTeamId = configNode.get("bridgeStaffTeamId").longValue();

        // Init DDB.
        String ddbPrefix = configNode.get("ddbPrefix").textValue();
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        synapseMetaTablesDdbTable = ddbClient.getTable(ddbPrefix + "SynapseMetaTables");
        synapseTablesDdbTable = ddbClient.getTable(ddbPrefix + "SynapseTables");

        // init Synapse client
        String synapseUser = configNode.get("synapseUser").textValue();
        String synapseApiKey = configNode.get("synapseApiKey").textValue();
        synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(synapseUser);
        synapseClient.setApiKey(synapseApiKey);
    }

    public void execute() {
        logInfo("Starting backfill...");

        // Iterate over all Synapse tables.
        Iterable<Item> synapseMetaTablesDdbIter = synapseMetaTablesDdbTable.scan();
        for (Item oneSynapseMetaTable : synapseMetaTablesDdbIter) {
            String tableName = oneSynapseMetaTable.getString("tableName");
            String synapseTableId = oneSynapseMetaTable.getString("tableId");

            try {
                handleTable(tableName, synapseTableId);
            } catch (Exception ex) {
                logError("Error handling table " + tableName + ": " + ex.getMessage(), ex);
            }
        }

        Iterable<Item> synapseTablesDdbIter = synapseTablesDdbTable.scan();
        for (Item oneSynapseTable : synapseTablesDdbIter) {
            String schemaKey = oneSynapseTable.getString("schemaKey");
            String synapseTableId = oneSynapseTable.getString("tableId");

            try {
                handleTable(schemaKey, synapseTableId);
            } catch (Exception ex) {
                logError("Error handling table " + schemaKey + ": " + ex.getMessage(), ex);
            }
        }
    }

    private void handleTable(String tableKey, String synapseTableId) throws SynapseException {
        // Need 3 permits, 1 to check if the table exists, 1 to get existing ACls, 1 to update ACLs.
        rateLimiter.acquire(3);

        // Check if table exists.
        try {
            synapseClient.getEntity(synapseTableId, TableEntity.class);
        } catch (SynapseNotFoundException ex) {
            logInfo("Table " + tableKey + " (" + synapseTableId + ") doesn't exist, skipping...");
            return;
        }

        logInfo("Processing " + tableKey);

        // ResourceAccess is a mutable object, but the Synapse API takes them in a Set. This is a little weird.
        // IMPORTANT: Do not modify ResourceAccess objects after adding them to the set. This will break the set.
        AccessControlList acl = synapseClient.getACL(synapseTableId);
        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();

        // BridgeAdmin Team.
        ResourceAccess bridgeAdminAccess = new ResourceAccess();
        bridgeAdminAccess.setPrincipalId(bridgeAdminTeamId);
        bridgeAdminAccess.setAccessType(ACCESS_TYPE_ADMIN);
        resourceAccessSet.add(bridgeAdminAccess);

        // BridgeStaff Team.
        ResourceAccess bridgeStaffAccess = new ResourceAccess();
        bridgeStaffAccess.setPrincipalId(bridgeStaffTeamId);
        bridgeStaffAccess.setAccessType(ACCESS_TYPE_READ);
        resourceAccessSet.add(bridgeStaffAccess);

        synapseClient.updateACL(acl);
    }

    public void cleanup() {
        ddbClient.shutdown();
    }
}
