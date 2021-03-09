package org.sagebionetworks.bridge.helper;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.util.concurrent.RateLimiter;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
@SuppressWarnings("UnstableApiUsage")
public class AccountSummaryIterator implements Iterator<AccountSummary> {
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final double DEFAULT_RATE_LIMIT = 1.0;

    // Instance invariants
    private final ClientManager clientManager;
    private final String appId;
    private final int pageSize;
    private final RateLimiter rateLimiter;

    // Instance state tracking
    private AccountSummaryList accountSummaryList;
    private int nextIndex;
    private int numAccounts = 0;

    /** Constructs an AccountSummaryIterator with the default page size and rate limit. */
    public AccountSummaryIterator(ClientManager clientManager, String appId) {
        this(clientManager, appId, DEFAULT_PAGE_SIZE, DEFAULT_RATE_LIMIT);
    }

    /**
     * Constructs an AccountSummaryIterator for the given Bridge client. This expects a worker account. This kicks off
     * requests to load the first page.
     */
    public AccountSummaryIterator(ClientManager clientManager, String appId, int pageSize, double rateLimit) {
        this.clientManager = clientManager;
        this.appId = appId;
        this.pageSize = pageSize;
        this.rateLimiter = RateLimiter.create(rateLimit);

        // Load first page.
        loadNextPage();
    }

    // Helper method to load the next page of users.
    private void loadNextPage() {
        // Rate limit.
        rateLimiter.acquire();

        // Call server for the next page.
        try {
            // The offset into the next page is equal to the number of accounts that we have seen.
            AccountSummarySearch search = new AccountSummarySearch().pageSize(pageSize).offsetBy(numAccounts);
            accountSummaryList = clientManager.getClient(ForWorkersApi.class)
                    .searchAccountSummariesForApp(appId, search).execute().body();
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
        return nextIndex < accountSummaryList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return numAccounts < accountSummaryList.getTotal();
    }

    /** {@inheritDoc} */
    @Override
    public AccountSummary next() {
        if (hasNextItemInPage()) {
            return getNextAccountSummary();
        } else if (hasNextPage()) {
            loadNextPage();
            return getNextAccountSummary();
        } else {
            throw new IllegalStateException("No more accounts left");
        }
    }

    // Helper method to get the next account in the list.
    private AccountSummary getNextAccountSummary() {
        AccountSummary accountSummary = accountSummaryList.getItems().get(nextIndex);
        nextIndex++;
        numAccounts++;
        return accountSummary;
    }
}
