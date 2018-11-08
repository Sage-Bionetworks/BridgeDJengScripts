package org.sagebionetworks.bridge.scripts;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.helper.BridgeHelper;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.model.NotificationProtocol;
import org.sagebionetworks.bridge.rest.model.NotificationRegistration;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

public class SmsRegistrationBackfillTest {
    private static TestUserHelper.TestUser researcher;

    private TestUserHelper.TestUser user;

    @BeforeClass
    public static void setup() throws Exception {
        researcher = TestUserHelper.createAndSignInUser(SmsRegistrationBackfillTest.class, false,
                Role.RESEARCHER);
    }

    @AfterMethod
    public void deleteUsers() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteResearcher() throws Exception {
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @Test
    public void noPhone() throws Exception {
        user = TestUserHelper.createAndSignInUser(this.getClass(), true);
        executeBackfill();
        verifyNoRegistrations(user);
    }

    @Test
    public void noConsent() throws Exception {
        user = makePhoneUser(false);
        executeBackfill();
        verifyNoRegistrations(user);
    }

    @Test
    public void notYetRegistered() throws Exception {
        user = makePhoneUser(true);
        executeBackfill();
        verifySmsRegistration(user);
    }

    @Test
    public void alreadyRegistered() throws Exception {
        // Make a phone user and register for SMS notifications.
        user = makePhoneUser(true);
        NotificationRegistration registration = new NotificationRegistration().protocol(NotificationProtocol.SMS)
                .endpoint(IntegTestUtils.PHONE.getNumber());
        user.getClient(ForConsentedUsersApi.class).createNotificationRegistration(registration).execute();

        // Execute backfill. We won't create a second registration.
        executeBackfill();
        verifySmsRegistration(user);
    }

    private static TestUserHelper.TestUser makePhoneUser(boolean consent) throws Exception {
        SignUp phoneSignUp = new SignUp().study(researcher.getStudyId()).consent(consent).phone(IntegTestUtils.PHONE);
        return new TestUserHelper.Builder(SmsRegistrationBackfillTest.class).withConsentUser(consent)
                .withSignUp(phoneSignUp).createAndSignInUser();
    }

    private static void executeBackfill() {
        // Wire up dependencies.
        BridgeHelper bridgeHelper = new BridgeHelper(researcher.getClientManager());
        SmsRegistrationBackfill backfill = new SmsRegistrationBackfill(bridgeHelper);
        backfill.execute();
    }

    private static void verifyNoRegistrations(TestUserHelper.TestUser user) throws Exception {
        List<NotificationRegistration> registrationList = researcher.getClient(ForResearchersApi.class)
                .getParticipantPushNotificationRegistrations(user.getUserId()).execute().body().getItems();
        assertTrue(registrationList.isEmpty());
    }

    private static void verifySmsRegistration(TestUserHelper.TestUser user) throws Exception {
        List<NotificationRegistration> registrationList = researcher.getClient(ForResearchersApi.class)
                .getParticipantPushNotificationRegistrations(user.getUserId()).execute().body().getItems();
        assertEquals(registrationList.size(), 1);

        NotificationRegistration registration = registrationList.get(0);
        assertEquals(registration.getProtocol(), NotificationProtocol.SMS);
        assertEquals(registration.getEndpoint(), IntegTestUtils.PHONE.getNumber());
    }
}
