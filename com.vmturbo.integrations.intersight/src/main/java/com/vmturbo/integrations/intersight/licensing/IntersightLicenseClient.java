package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.api.LicenseApi;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfoList;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.mediation.connector.intersight.IntersightDefaultQueryParameters;

/**
 * The IntersightLicenseClient manages requests made to the Intersight API.
 */
public class IntersightLicenseClient {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Set of retryable http codes. We'll retry on the following:
     * 429 (Too many requests): The Intersight API has quotas and may send this as a return code if
     * the quota is exceeded.
     * 502 (Bad Gateway): Potential request routing error.
     * 503 (Service Unavailable): Back-end may be temporarily unavaialble.
     * 504 (Gateway Timeout): We are not getting responses in time, which could be a temporary issue.
     */
    public static final Set<Integer> RETRYABLE_HTTP_CODES = ImmutableSet.of(429, 502, 503, 504);

    private final IntersightConnection connection;

    private final String defaultLicenseQueryFilter;

    /**
     * Construct an IntersightLicenseClient.
     * @param connection An intersight API connection to use.
     */
    public IntersightLicenseClient(@Nonnull final IntersightConnection connection, @Nullable String defaultLicenseQueryFilter) {
        this.connection = connection;
        this.defaultLicenseQueryFilter = defaultLicenseQueryFilter;
    }

    /**
     * Get an Intersight API Client to use for communicating with the Intersight back-end.
     *
     * @return an {@link ApiClient} that can be used for Intersight API calls.
     * @throws IOException if there is an error establishing the session
     */
    public ApiClient getApiClient() throws IOException {
        // The original intent of this method was to provide reuse of an ApiClient instance as long as we
        // could, since it may be a lot of unnecessary load for the IWO back end to have to keep
        // generating tokens.
        // But after reusing the instance for a few calls, the requests just start failing with NPE's.
        // There seems to be no explicit signal that the token has expired or anything to distinguish
        // token expirations / quotas from regular API errors. So we will just create a new one each
        // time and avoid this issue. We can come back and change this in the future if the
        // infrastructure provides a pattern that supports it.
        //
        logger.debug("Creating new Intersight API client.");
        return connection.getApiClient();
    }

    /**
     * Get the list of licenses that this account has from Intersight, using default parameters.
     */
    protected LicenseLicenseInfoList getIntersightLicenses() throws IOException, ApiException {
        return getIntersightLicenses(defaultLicenseQueryFilter);
    }

    /**
     * Get the list of licenses that this account has from Intersight using a specific filter.
     *
     * @param filter A filter to use when retreiving licenses. Can be null.
     * @return a {@link LicenseLicenseInfoList} containing the licenses available for the current
     * account.
     * @throws IOException if there is an error establishing the session
     * @throws ApiException if there are errors returned from the Intersight API
     */
    protected LicenseLicenseInfoList getIntersightLicenses(@Nullable String filter) throws IOException, ApiException {
        ApiClient apiClient = getApiClient();
        // get the Intersight license list
        LicenseApi licenseApi = new LicenseApi(apiClient);
        // the ApiClient seems to have a habit of throwing NPE's when there is no data returned
        // as part of a 204 response by the underlying API's. :( So we won't know if the NPE is
        // the result of an error or an empty response from the server.
        //
        // I'm seeing the NPE's come in context where the 204 doesn't make sense (i.e. the same
        // api returns data and then generates NPE's later) so I'm going to let the NPE propagate.
        if (filter != null) {
            logger.info("Fetching intersight licenses using filter: {}", filter);
        }
        LicenseLicenseInfoList licenseList = licenseApi.getLicenseLicenseInfoList(filter,
                IntersightDefaultQueryParameters.$orderby,
                IntersightDefaultQueryParameters.$top,
                IntersightDefaultQueryParameters.$skip,
                IntersightDefaultQueryParameters.$select,
                IntersightDefaultQueryParameters.$expand,
                IntersightDefaultQueryParameters.$apply,
                IntersightDefaultQueryParameters.$count,
                IntersightDefaultQueryParameters.$inlinecount,
                IntersightDefaultQueryParameters.$at,
                IntersightDefaultQueryParameters.$tags);
        return licenseList;
    }

    /**
     * Update a {@link LicenseLicenseInfo} instance in the intersight server.
     * @param moid the moid of the license info to update.
     * @param updatedLicenseInfo a LicenseLicenseInfo containing the new values.
     * @return LicenseLicenseInfo returned from the Intersight API.
     * @throws IOException if there is an error establishing the session
     * @throws ApiException if there are errors returned from the Intersight API
     */
    public LicenseLicenseInfo updateLicenseLicenseInfo(String moid, LicenseLicenseInfo updatedLicenseInfo) throws IOException, ApiException {
        ApiClient apiClient = getApiClient();
        LicenseApi licenseApi = new LicenseApi(apiClient);
        return licenseApi.updateLicenseLicenseInfo(moid, updatedLicenseInfo, null);
    }

    /**
     * Utility method for checking whether an exception can be considered retryable or not (from the
     * IntersightLicenseClient's perspective).
     *
     * @param e the Exception to check
     * @return true, if the exception seems to be of the retryable sort.
     */
    public static boolean isRetryable(Exception e) {
        if (e instanceof ApiException) {
            return RETRYABLE_HTTP_CODES.contains(((ApiException)e).getCode());
        }
        return false;
    }
}
