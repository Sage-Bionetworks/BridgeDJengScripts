package org.sagebionetworks.bridge.synapse;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import org.apache.commons.lang.StringUtils;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.QueryNextPageToken;
import org.sagebionetworks.repo.model.table.QueryResult;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.SelectColumn;

/** Helper class to query Synapse tables and iterate over the results, abstracting away pagination. */
// This doesn't implement Iterator, since Iterator's methods can't throw checked exceptions.
public class SynapseTableIterator {
    private static final int ASYNC_QUERY_TIMEOUT_SECONDS = 300;

    // Constructor args.
    private final SynapseClient synapseClient;
    private final String synapseTableId;

    // Internal state tracking.
    private String asyncJobToken;
    private QueryResult curResult;
    private int curRowNumInPage = 0;
    private String etag;
    private boolean firstPage = true;
    private List<SelectColumn> headers;
    private Row nextRow;

    /**
     * Creates the Synapse table iterator with the specified args.
     *
     * @param synapseClient
     *         synapse client
     * @param sql
     *         SQL query to run, defaults to "SELECT * FROM [synapseTableId]"
     * @param synapseTableId
     *         synapse table ID to run the query against
     * @throws SynapseException
     *         if the synapse call fails
     */
    public SynapseTableIterator(SynapseClient synapseClient, String sql, String synapseTableId)
            throws SynapseException {
        if (StringUtils.isBlank(sql)) {
            sql = "SELECT * FROM " + synapseTableId;
        }

        this.synapseClient = synapseClient;
        this.synapseTableId = synapseTableId;
        this.asyncJobToken = queryTableAsyncStartWithRetry(sql);
    }

    /**
     * Returns true if the iterator has additional elements. Does not advance the iterator.
     *
     * @return true if the iterator has additional elements.
     * @throws SynapseException
     *         if the underlying Synapse call fails
     */
    // Checks if a row is available, and if so, loads it into nextRow.
    public boolean hasNext() throws SynapseException {
        if (nextRow != null) {
            // Next row is already loaded and ready.
            return true;
        }

        if (asyncJobToken != null) {
            // We have a page to fetch.
            fetchNextPage();

            // Once we've fetched the page, wipe the asyncJobToken so we don't try to fetch it again.
            asyncJobToken = null;

            // If we just fetched a page with no rows, then this means we've hit the end of the stream. Return false.
            if (curResult.getQueryResults().getRows().isEmpty()) {
                curResult = null;
                return false;
            }
        }

        if (curResult != null) {
            List<Row> rowList = curResult.getQueryResults().getRows();
            // This should be fine, since we always wipe the curResults when we get to the end of the page.
            nextRow = rowList.get(curRowNumInPage);

            // Advance the row num.
            curRowNumInPage++;
            if (curRowNumInPage == rowList.size()) {
                // If that was the last row and we have a next page token, go ahead and set up the
                // next page.
                QueryNextPageToken nextPageToken = curResult.getNextPageToken();
                if (nextPageToken != null) {
                    asyncJobToken = queryTableNextPageAsyncStartWithRetry(nextPageToken.getToken());
                }

                // Wipe out the curResult, since we're done with it. This allows the iterator to remember that we
                // need to fetch the next page. Also, the row num since we're in a new page.
                curResult = null;
                curRowNumInPage = 0;
            }

            return true;
        }

        // This is the end.
        return false;
    }

    /**
     * Returns the next element in the iterator. Throws a NoSuchElementException if no such element exists.
     *
     * @return the next element in the iterator
     * @throws NoSuchElementException
     *         if no such element exists
     * @throws SynapseException
     *         if an underlying Synapse call fails
     */
    // Call hasNext() first to check if we have a row or load the next row if needed.
    public Row next() throws NoSuchElementException, SynapseException {
        if (hasNext()) {
            // clear the nextRow, so hasNext() knows to load the next one
            Row retVal = nextRow;
            nextRow = null;
            return retVal;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the etag from the first page of results. This doesn't get updated with subsequent page fetches, so if
     * there's an intravening update, the etag will reflect that the query results are outdated.
     *
     * @return the etag from the query
     * @throws SynapseException
     *         if an underlying Synapse call fails
     */
    public String getEtag() throws SynapseException {
        if (etag == null) {
            // Force initialization by calling hasNext(), which fetches the page. This is safe, because hasNext() will
            // only fetch the page if it hasn't already been fetched.
            hasNext();
        }
        return etag;
    }

    /**
     * Returns the headers (selected columns) from the first page of results. This will remain the same for all
     * subsequent pages.
     *
     * @return list of headers (selected columns
     * @throws SynapseException
     *         if an underlying Synapse call fails
     */
    public List<SelectColumn> getHeaders() throws SynapseException {
        if (headers == null) {
            // Force initialization by calling hasNext(), which fetches the page. This is safe, because hasNext() will
            // only fetch the page if it hasn't already been fetched.
            hasNext();
        }
        return headers;
    }

    private void fetchNextPage() throws SynapseException {
        // poll asyncGet until success or timeout
        boolean success = false;
        for (int sec = 0; sec < ASYNC_QUERY_TIMEOUT_SECONDS; sec++) {
            // sleep for 1 sec
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // noop
            }

            // poll
            if (firstPage) {
                // This is the first page, so we call bundle get instead of next page get.
                QueryResultBundle resultBundle = queryTableAsyncGetWithRetry(asyncJobToken);
                if (resultBundle == null) {
                    // not ready, loop around again
                    continue;
                }
                curResult = resultBundle.getQueryResult();

                // fetch etag
                etag = curResult.getQueryResults().getEtag();
                headers = curResult.getQueryResults().getHeaders();

                // This is no longer the first page.
                firstPage = false;
            } else {
                // We're getting a next page.
                curResult = queryTableNextPageAsyncGetWithRetry(asyncJobToken);
                if (curResult == null) {
                    // not ready, loop around again
                    continue;
                }
            }

            // If we made it this far, we've successfully fetched the page.
            success = true;
            break;
        }

        if (!success) {
            throw new SynapseClientException("Timed out querying table " + synapseTableId);
        }
    }

    /**
     * Kicks off an async SQL query against the specified table. This uses jcabi-retry to retry the call on failure.
     *
     * @param sql
     *         SQL to run against table
     * @return the async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private String queryTableAsyncStartWithRetry(String sql) throws SynapseException {
        return synapseClient.queryTableEntityBundleAsyncStart(sql, null, null, true, SynapseClient.QUERY_PARTMASK,
                synapseTableId);
    }

    /**
     * Fetches the result of an async query, or returns null if the result is not ready. This uses jcabi-retry to
     * retry the call on failure.
     *
     * @param asyncJobToken
     *         async job token of the result to be fetched
     * @return the result of the async query, or null if the result is not ready
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private QueryResultBundle queryTableAsyncGetWithRetry(String asyncJobToken) throws SynapseException {
        try {
            return synapseClient.queryTableEntityBundleAsyncGet(asyncJobToken, synapseTableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /**
     * Asynchronously gets the next page of the query for the specified table. This uses jcabi-retry to retry the call
     * on failure.
     *
     * @param nextPageToken
     *         token used to fetch the next page
     * @return the async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private String queryTableNextPageAsyncStartWithRetry(String nextPageToken) throws SynapseException {
        return synapseClient.queryTableEntityNextPageAsyncStart(nextPageToken, synapseTableId);
    }

    /**
     * Fetches the result of an async query next page, or returns null if the result is not ready. This uses
     * jcabi-retry to retry the call on failure.
     *
     * @param asyncJobToken
     *         async job token of the result to be fetched
     * @return the result of the async query, or null if the result is not ready
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private QueryResult queryTableNextPageAsyncGetWithRetry(String asyncJobToken) throws SynapseException {
        try {
            return synapseClient.queryTableEntityNextPageAsyncGet(asyncJobToken, synapseTableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }
}
