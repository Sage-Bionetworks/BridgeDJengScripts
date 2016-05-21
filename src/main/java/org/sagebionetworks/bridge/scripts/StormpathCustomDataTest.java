package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.stormpath.sdk.account.Account;
import com.stormpath.sdk.account.Accounts;
import com.stormpath.sdk.api.ApiKey;
import com.stormpath.sdk.api.ApiKeys;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.client.Clients;
import com.stormpath.sdk.directory.CustomData;

public class StormpathCustomDataTest {
    private static final String ACCOUNT_HREF = "https://enterprise.stormpath.io/v1/accounts/2DkwVHJlpgXGEUFE63aS9G";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int NUM_ITERATIONS = 100;
    private static final Random RNG = new Random();

    private static Client stormpathClient;

    public static void main(String[] args) throws IOException {
        init();
        execute();

        System.out.println("Done");
    }

    private static void init() throws IOException {
        // load config JSON
        JsonNode configNode = JSON_MAPPER.readTree(new File("/Users/dwaynejeng/scripts-config-local.json"));
        String stormpathId = configNode.get("stormpath.id").textValue();
        String stormpathSecret = configNode.get("stormpath.secret").textValue();
        ApiKey apiKey = ApiKeys.builder().setId(stormpathId).setSecret(stormpathSecret).build();
        stormpathClient = Clients.builder().setApiKey(apiKey).setBaseUrl("https://enterprise.stormpath.io/v1").build();
    }

    private static void execute() {
        int numValid = 0;
        int numFailed = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            // sleep, so we don't brown out Stormpath
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            // Get account and write custom data.
            Account account = stormpathClient.getResource(ACCOUNT_HREF, Account.class,
                    Accounts.options().withCustomData());
            CustomData customData = account.getCustomData();
            customData.put("test-key", RNG.nextInt());

            // Copy custom data into a map, so we can do equality tests on it later.
            Map<String, Object> customDataMap = new HashMap<>(customData);

            customData.save();

            // Immediately get account back and compare custom data.
            Account returnedAccount = stormpathClient.getResource(ACCOUNT_HREF, Account.class,
                    Accounts.options().withCustomData());
            CustomData returnedCustomData = returnedAccount.getCustomData();
            Map<String, Object> returnedCustomDataMap = new HashMap<>(returnedCustomData);

            // Custom Data includes a key "modifiedAt", which is always different. Remove that from the map, so we
            // can validate in earnest.
            customDataMap.remove("modifiedAt");
            returnedCustomDataMap.remove("modifiedAt");

            // Compare and keep stats.
            boolean isValid = Objects.equals(customDataMap, returnedCustomDataMap);
            if (isValid) {
                numValid++;
            } else {
                numFailed++;
                describeFailure(customDataMap, returnedCustomDataMap);
            }
        }

        System.out.println("Finished " + NUM_ITERATIONS + " iterations in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds.");
        System.out.println("numValid: " + numValid);
        System.out.println("numFailed: " + numFailed);
    }

    private static void describeFailure(Map<String, Object> submittedCustomDataMap,
            Map<String, Object> returnedCustomDataMap) {
        // Union the keys, so we can compare maps on all keys.
        Set<String> allKeySet = Sets.union(submittedCustomDataMap.keySet(), returnedCustomDataMap.keySet());

        // Get the keys in order, so we can describe differences in a stable way.
        List<String> allKeyList = new ArrayList<>(allKeySet);
        Collections.sort(allKeyList);

        // Iterate the keys and log the differences.
        for (String oneKey : allKeyList) {
            Object submittedValue = submittedCustomDataMap.get(oneKey);
            Object returnedValue = returnedCustomDataMap.get(oneKey);
            if (!Objects.equals(submittedValue, returnedValue)) {
                System.out.println("For key " + oneKey + ", expected " + submittedValue + ", got " + returnedValue);
            }
        }
    }
}
