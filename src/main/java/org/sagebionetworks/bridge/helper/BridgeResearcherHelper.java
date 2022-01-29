package org.sagebionetworks.bridge.helper;

import java.io.IOException;
import java.util.List;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.InternalApi;
import org.sagebionetworks.bridge.rest.model.NotificationRegistration;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

/** Abstracts away calls to Bridge and wraps the iterator classes. */
public class BridgeResearcherHelper {
    private final ClientManager clientManager;

    public BridgeResearcherHelper(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public void createSmsRegistration(String userId) throws IOException {
        clientManager.getClient(InternalApi.class).createSmsRegistration(userId).execute();
    }

    /** Gets a participant for the given user in the given study. */
    public StudyParticipant getParticipant(String userId) throws IOException {
        return clientManager.getClient(ForResearchersApi.class).getParticipantById(userId, true).execute()
                .body();
    }

    public List<NotificationRegistration> getParticipantNotificationRegistrations(String userId) throws IOException {
        return clientManager.getClient(ForResearchersApi.class).getParticipantPushNotificationRegistrations(userId)
                .execute().body().getItems();
    }
}
