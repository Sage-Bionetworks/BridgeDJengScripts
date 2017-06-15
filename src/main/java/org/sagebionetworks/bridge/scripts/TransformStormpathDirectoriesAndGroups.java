package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

// Transforms the Stormpath export to an intermediate format, which makes it easier for our scripts to upload
// everything to MySQL.
//
// To execute:
// mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.scripts.TransformStormpathDirectoriesAndGroups -Dexec.args=[path to config file]
public class TransformStormpathDirectoriesAndGroups {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Pattern DIR_NAME_PATTERN = Pattern.compile("(?<studyId>[a-z0-9-]+)\\s+\\((?<env>\\w+)\\)");

    private static String stormpathExportRootPath;
    private static String outputDirPath;

    public static void main(String[] args) throws IOException {
        // input params
        if (args.length != 1){
            System.out.println("Usage: TransformStormpathDirectoriesAndGroups [config file]");
            return;
        }
        String configFilePath = args[0];

        // This is to make sure whatever script runs this Java application is properly capturing stdout and stderr and
        // sending them to the logs
        System.out.println("Verifying stdout...");
        System.err.println("Verifying stderr...");

        // init and execute
        init(configFilePath);
        transformDirectories();
        transformGroups();
    }

    private static void init(String configFilePath) throws IOException {
        System.out.println("Initializing...");

        // load config, which is a JSON file
        JsonNode configNode = JSON_MAPPER.readTree(new File(configFilePath));
        stormpathExportRootPath = configNode.get("stormpathExportRootPath").textValue();
        outputDirPath = configNode.get("outputDirPath").textValue();
    }

    private static void transformDirectories() throws IOException {
        // Read directories from Stormpath dump.
        ObjectNode directoriesById = JSON_MAPPER.createObjectNode();
        Iterator<Path> dirPathIter = Files.list(Paths.get(stormpathExportRootPath, "directories")).iterator();
        while (dirPathIter.hasNext()) {
            Path oneDirPath = dirPathIter.next();
            JsonNode inputDirNode = JSON_MAPPER.readTree(oneDirPath.toFile());
            String dirId = inputDirNode.get("id").textValue();
            String dirName = inputDirNode.get("name").textValue();

            // Parse director name.
            Matcher dirNameMatcher = DIR_NAME_PATTERN.matcher(dirName);
            if (dirNameMatcher.matches()) {
                String studyId = dirNameMatcher.group("studyId");
                String env = dirNameMatcher.group("env");

                ObjectNode outputDirNode = JSON_MAPPER.createObjectNode();
                outputDirNode.put("env", env);
                outputDirNode.put("studyId", studyId);
                directoriesById.set(dirId, outputDirNode);
            } else {
                System.err.println("Could not parse directory with name " + dirName);
            }
        }

        // Write to output file.
        JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValue(Paths.get(outputDirPath, "directoriesById.json")
                .toFile(), directoriesById);
    }

    private static void transformGroups() throws IOException {
        // Read groups from Stormpath dump. Remember, there are a lot of redundant groups, since groups are specific to
        // directories, while in Bridge, roles are global.
        Map<String, String> groupsById = new HashMap<>();
        Iterator<Path> groupPathIter = Files.list(Paths.get(stormpathExportRootPath, "groups")).iterator();
        while (groupPathIter.hasNext()) {
            Path oneGroupPath = groupPathIter.next();
            JsonNode inputGroupNode = JSON_MAPPER.readTree(oneGroupPath.toFile());
            String groupId = inputGroupNode.get("id").textValue();
            String groupName = inputGroupNode.get("name").textValue().toUpperCase();
            groupsById.put(groupId, groupName);
        }

        // Read group memberships from Stormpath dump. Some accounts have more than one group.
        Multimap<String, String> groupsByAccountId = HashMultimap.create();
        Iterator<Path> groupMembershipPathIter = Files.list(Paths.get(stormpathExportRootPath, "groupMemberships"))
                .iterator();
        while (groupMembershipPathIter.hasNext()) {
            Path oneGroupMembershipPath = groupMembershipPathIter.next();
            JsonNode inputGroupMembershipNode = JSON_MAPPER.readTree(oneGroupMembershipPath.toFile());
            String accountId = inputGroupMembershipNode.get("account").get("id").textValue();
            String groupId = inputGroupMembershipNode.get("group").get("id").textValue();
            groupsByAccountId.put(accountId, groupsById.get(groupId));
        }

        // Create output JSON.
        ObjectNode groupsByAccountIdNode = JSON_MAPPER.createObjectNode();
        for (Map.Entry<String, Collection<String>> groupsForOneAccountId : groupsByAccountId.asMap().entrySet()) {
            String accountId = groupsForOneAccountId.getKey();
            Collection<String> groupCol = groupsForOneAccountId.getValue();

            ArrayNode groupListNode = JSON_MAPPER.createArrayNode();
            groupCol.forEach(groupListNode::add);

            groupsByAccountIdNode.set(accountId, groupListNode);
        }

        // Write to output file.
        JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValue(Paths.get(outputDirPath, "groupsByAccountId.json")
                .toFile(), groupsByAccountIdNode);
    }
}
