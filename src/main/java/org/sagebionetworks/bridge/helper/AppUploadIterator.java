package org.sagebionetworks.bridge.helper;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
@SuppressWarnings("UnstableApiUsage")
public class AppUploadIterator  implements Iterator<Upload> {
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final double DEFAULT_RATE_LIMIT = 1.0;

    // Instance invariants
    private final ClientManager clientManager;
    private final String appId;
    private final DateTime startDateTime;
    private final DateTime endDateTime;
    private final int pageSize;
    private final RateLimiter rateLimiter;

    // Instance state tracking
    private UploadList uploadList;
    private int nextIndex;
    private String nextPageOffsetKey;

    /** Constructs an AppUploadIterator with the default page size and rate limit. */
    public AppUploadIterator(ClientManager clientManager, String appId, DateTime startDateTime, DateTime endDateTime) {
        this(clientManager, appId, startDateTime, endDateTime, DEFAULT_PAGE_SIZE, DEFAULT_RATE_LIMIT);
    }

    /**
     * Constructs an AppUploadIterator for the given Bridge client. This expects a worker account. This kicks off
     * requests to load the first page.
     */
    public AppUploadIterator(ClientManager clientManager, String appId, DateTime startDateTime, DateTime endDateTime,
            int pageSize, double rateLimit) {
        this.clientManager = clientManager;
        this.appId = appId;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.pageSize = pageSize;
        this.rateLimiter = RateLimiter.create(rateLimit);

        // Load first page.
        loadNextPage();
    }

    // Helper method to load the next page of uploads.
    private void loadNextPage() {
        // Rate limit.
        rateLimiter.acquire();

        // Call server for the next page.
        try {
            uploadList = clientManager.getClient(ForWorkersApi.class).getUploadsForApp(appId, startDateTime,
                    endDateTime, pageSize, nextPageOffsetKey).execute().body();
            nextPageOffsetKey = uploadList.getNextPageOffsetKey();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException("Error getting next page: " + ex.getMessage(), ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    // Helper method to determine if there are additional items in this page.
    private boolean hasNextItemInPage() {
        return nextIndex < uploadList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return uploadList.getNextPageOffsetKey() != null;
    }

    /** {@inheritDoc} */
    @Override
    public Upload next() {
        if (hasNextItemInPage()) {
            return getNextUpload();
        } else if (hasNextPage()) {
            loadNextPage();
            return getNextUpload();
        } else {
            throw new IllegalStateException("No more uploads left");
        }
    }

    // Helper method to get the next upload in the list.
    private Upload getNextUpload() {
        Upload upload = uploadList.getItems().get(nextIndex);
        nextIndex++;
        return upload;
    }
}
