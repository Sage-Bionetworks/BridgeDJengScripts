package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ListPhoneNumbersOptedOutRequest;
import com.amazonaws.services.sns.model.ListPhoneNumbersOptedOutResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import org.sagebionetworks.bridge.helper.BridgeHelper;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.rest.model.SignIn;

@SuppressWarnings("UnstableApiUsage")
public class ExtractOptOuts {
    private static final String APP_ID = "sage-mpower-2";
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("ExtractOptOuts").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final BridgeHelper bridgeHelper;
    private final AmazonSNS sns;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: ExtractOptOuts [path to config JSON]");
            return;
        }

        logInfo("Initializing...");

        // Init Bridge Helper.
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        String workerAppId = configNode.get("workerAppId").textValue();
        String workerEmail = configNode.get("workerEmail").textValue();
        String workerPassword = configNode.get("workerPassword").textValue();

        SignIn workerSignIn = new SignIn().appId(workerAppId).email(workerEmail).password(workerPassword);
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO).withSignIn(workerSignIn)
                .build();
        BridgeHelper bridgeHelper = new BridgeHelper(clientManager);

        // Init AWS SNS.
        String awsKey = configNode.get("awsKey").textValue();
        String awsSecretKey = configNode.get("awsSecretKey").textValue();
        String awsSessionToken = configNode.get("awsSessionToken").textValue();

        AWSCredentials credentials = new BasicSessionCredentials(awsKey, awsSecretKey, awsSessionToken);
        AWSCredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);
        AmazonSNS sns = AmazonSNSClientBuilder.standard().withCredentials(credentialsProvider).build();

        // Execute.
        ExtractOptOuts extract = new ExtractOptOuts(bridgeHelper, sns);
        try {
            extract.execute();
        } finally {
            extract.cleanup();
            System.exit(0);
        }
    }

    public ExtractOptOuts(BridgeHelper bridgeHelper, AmazonSNS sns) {
        this.bridgeHelper = bridgeHelper;
        this.sns = sns;
    }

    public void execute() {
        logInfo("Starting extraction...");

        // Ask Bridge for all phone numbers in mPower 2.0.
        Iterator<AccountSummary> accountSummaryIter = bridgeHelper.getAccountSummariesForApp(APP_ID);
        Set<String> mPowerSet = Streams.stream(accountSummaryIter).map(AccountSummary::getPhone)
                .filter(Objects::nonNull).map(Phone::getNumber).collect(Collectors.toSet());
        logInfo("# phone numbers in mPower 2.0: " + mPowerSet.size());

        // Ask SNS for list of opt-outs.
        Set<String> optOutSet = new HashSet<>();
        String nextToken = null;
        do {
            ListPhoneNumbersOptedOutRequest listOptOutsRequest = new ListPhoneNumbersOptedOutRequest();
            listOptOutsRequest.setNextToken(nextToken);

            ListPhoneNumbersOptedOutResult listOptOutsResult = sns.listPhoneNumbersOptedOut(listOptOutsRequest);
            optOutSet.addAll(listOptOutsResult.getPhoneNumbers());
            nextToken = listOptOutsResult.getNextToken();
        } while (nextToken != null);
        logInfo("# opted out phone numbers: " + optOutSet.size());

        // Find the intersection. Sort the values for ease of display.
        Set<String> optOutMPowerSet = Sets.intersection(optOutSet, mPowerSet);
        List<String> optOutMPowerList = new ArrayList<>(optOutMPowerSet);
        Collections.sort(optOutMPowerList);
        logInfo("# opted out phone numbers in mPower 2.0: " + optOutMPowerList.size());
        for (String phoneNumber : optOutMPowerList) {
            System.out.println(phoneNumber);
        }
    }

    public void cleanup() {
        sns.shutdown();
    }
}
