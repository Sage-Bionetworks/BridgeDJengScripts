package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logError;
import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.util.concurrent.RateLimiter;

import org.sagebionetworks.bridge.helper.BridgeHelper;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.NotificationProtocol;
import org.sagebionetworks.bridge.rest.model.NotificationRegistration;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

public class SmsRegistrationBackfill {
    private static final ClientInfo CLIENT_INFO = new ClientInfo().appName("SmsRegistrationBackfill").appVersion(1);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // If there are a lot of users, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    private final BridgeHelper bridgeHelper;
    private final Multiset<String> metrics = TreeMultiset.create();
    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            logInfo("Usage: SmsRegistrationBackfill [path to config JSON]");
            return;
        }

        // Init.
        logInfo("Initializing...");
        JsonNode configNode = JSON_MAPPER.readTree(new File(args[0]));
        String researcherStudyId = configNode.get("researcherStudyId").textValue();
        String researcherEmail = configNode.get("researcherEmail").textValue();
        String researcherPassword = configNode.get("researcherPassword").textValue();

        SignIn researcherSignIn = new SignIn().study(researcherStudyId).email(researcherEmail)
                .password(researcherPassword);
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(CLIENT_INFO)
                .withSignIn(researcherSignIn).build();

        BridgeHelper bridgeHelper = new BridgeHelper(clientManager);
        SmsRegistrationBackfill backfill = new SmsRegistrationBackfill(bridgeHelper);

        // Execute.
        backfill.execute();
    }

    public SmsRegistrationBackfill(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    public void execute() {
        logInfo("Starting backfill...");

        Iterator<AccountSummary> accountSummaryIter = bridgeHelper.getAllAccountSummaries();

        int numUsers = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (accountSummaryIter.hasNext()) {
            // Rate limit
            perUserRateLimiter.acquire();

            // Process
            try {
                AccountSummary accountSummary = accountSummaryIter.next();

                try {
                    processAccount(accountSummary);
                } catch (Exception ex) {
                    logError("Error processing user ID " + accountSummary.getId() + ": " + ex.getMessage(), ex);
                }
            } catch (Exception ex) {
                logError("Error getting next user: " + ex.getMessage(), ex);
            }

            // Reporting.
            numUsers++;
            if (numUsers % REPORTING_INTERVAL == 0) {
                logInfo("Processing users in progress: " + numUsers + " users in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }
        logInfo("Finished processing " + numUsers + " users in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds");

        // Metrics.
        for (Multiset.Entry<String> metricsEntry : metrics.entrySet()) {
            logInfo(metricsEntry.getElement() + "=" + metricsEntry.getCount());
        }
    }

    private void processAccount(AccountSummary accountSummary) throws IOException {
        String userId = accountSummary.getId();

        // Filter out accounts that already have an SMS registration. This is the first filter, since (after
        // backfilling and implementing client-side registration), most of these will already have an SMS registration.
        List<NotificationRegistration> registrationList = bridgeHelper
                .getParticipantNotificationRegistrations(userId);
        for (NotificationRegistration registration : registrationList) {
            if (registration.getProtocol() == NotificationProtocol.SMS) {
                metrics.add("already_registered");
                return;
            }
        }

        // Next, filter out users who don't have a verified phone number. This filters out test users, admins and
        // developers, and users who downloaded the app but didn't finish registration.
        StudyParticipant participant = bridgeHelper.getParticipant(userId);
        if (participant.isPhoneVerified() != Boolean.TRUE) {
            metrics.add("phone_not_verified");
            return;
        }

        // Lastly, filter out users who aren't consented. This filters out users who started registration but for
        // whatever reason didn't submit consent.
        if (participant.isConsented() != Boolean.TRUE) {
            metrics.add("not_consented");
            return;
        }

        // We need to create SMS registration for this user.
        bridgeHelper.createSmsRegistration(userId);
        metrics.add("created_registration");
    }
}
