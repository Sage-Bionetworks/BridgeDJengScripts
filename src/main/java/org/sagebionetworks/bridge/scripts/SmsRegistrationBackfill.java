package org.sagebionetworks.bridge.scripts;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.sagebionetworks.bridge.helper.BridgeResearcherHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.NotificationProtocol;
import org.sagebionetworks.bridge.rest.model.NotificationRegistration;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

public class SmsRegistrationBackfill {
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");

    // If there are a lot of users, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    private final BridgeResearcherHelper bridgeResearcherHelper;
    private final Multiset<String> metrics = TreeMultiset.create();
    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    public SmsRegistrationBackfill(BridgeResearcherHelper bridgeResearcherHelper) {
        this.bridgeResearcherHelper = bridgeResearcherHelper;
    }

    public void execute() {
        logInfo("Starting backfill...");

        Iterator<AccountSummary> accountSummaryIter = bridgeResearcherHelper.getAllAccountSummaries();

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
        List<NotificationRegistration> registrationList = bridgeResearcherHelper
                .getParticipantNotificationRegistrations(userId);
        for (NotificationRegistration registration : registrationList) {
            if (registration.getProtocol() == NotificationProtocol.SMS) {
                metrics.add("already_registered");
                return;
            }
        }

        // Next, filter out users who don't have a verified phone number. This filters out test users, admins and
        // developers, and users who downloaded the app but didn't finish registration.
        StudyParticipant participant = bridgeResearcherHelper.getParticipant(userId);
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
        bridgeResearcherHelper.createSmsRegistration(userId);
        metrics.add("created_registration");
    }

    private static void logInfo(String msg) {
        System.out.print('[');
        System.out.print(DateTime.now(LOCAL_TIME_ZONE));
        System.out.print(']');
        System.out.println(msg);
    }

    private static void logError(String msg, Throwable ex) {
        System.err.print('[');
        System.err.print(DateTime.now(LOCAL_TIME_ZONE));
        System.err.print(']');
        System.err.println(msg);
        ex.printStackTrace();
    }
}
