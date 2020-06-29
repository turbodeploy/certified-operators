package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.ApiResponse;
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

    private static final String INTERSIGHT_TRACEID =
            "x-starship-traceid";

    /**
     * Set of retryable http codes. We'll retry on the following:
     * 429 (Too many requests): The Intersight API has quotas and may send this as a return code if
     * the quota is exceeded.
     * 502 (Bad Gateway): Potential request routing error.
     * 503 (Service Unavailable): Back-end may be temporarily unavaialble.
     * 504 (Gateway Timeout): We are not getting responses in time, which could be a temporary issue.
     */
    public static final Set<Integer> RETRYABLE_HTTP_CODES = ImmutableSet.of(429, 502, 503, 504);
    public static final int HTTP_STATUS_NO_DATA = 204;
    public static final int HTTP_STATUS_OK = 200;

    private final IntersightConnection connection;

    // we are going to cache a reference to the ApiClient directly here, since the IntersightConnection
    // seems to create new tokens fairly often.
    private ApiClient apiClient;

    private final String defaultLicenseQueryFilter;

    /**
     * Construct an IntersightLicenseClient.
     * @param connection An intersight API connection to use.
     * @param defaultLicenseQueryFilter the filter expresssion to use when fetching licenses
     */
    public IntersightLicenseClient(@Nonnull final IntersightConnection connection,
                                   @Nullable String defaultLicenseQueryFilter) {
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
        apiClient = connection.getApiClient();
        return apiClient;
    }

    /**
     * Get the list of licenses that this account has from Intersight, using default parameters.
     *
     * @return a list of {@link LicenseLicenseInfo} containing the licenses available for the current
     * account.
     * @throws IOException if there is an error establishing the session
     * @throws ApiException if there are errors returned from the Intersight API
     */
    protected List<LicenseLicenseInfo> getIntersightLicenses() throws IOException, ApiException {
        return getIntersightLicenses(defaultLicenseQueryFilter);
    }

    /**
     * Get the list of licenses that this account has from Intersight using a specific filter.
     *
     * @param filter A filter to use when retreiving licenses. Can be null.
     * @return a list of {@link LicenseLicenseInfo} containing the licenses available for the current
     * account.
     * @throws IOException if there is an error establishing the session
     * @throws ApiException if there are errors returned from the Intersight API
     */
    protected List<LicenseLicenseInfo> getIntersightLicenses(@Nullable String filter) throws IOException, ApiException {
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
        try {
            ApiResponse<LicenseLicenseInfoList> response = licenseApi.getLicenseLicenseInfoListWithHttpInfo(filter,
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
            // the LicenseAPI is pretty clumsy with "no result" responses -- it may or may not return
            // an LicenseLicenseInfoList object in the reponse, and even then, the actual list reference
            // may be null. We're going to wrap all that here so the rest of our code doesn't have to
            // deal with it.
            if (response.getData() != null) {
                if (response.getData().getResults() != null) {
                    return response.getData().getResults();
                }
            }
            // log for posterity -- it's not an error, but we want to know why the empty collection
            // is getting returned instead of an actual response
            logger.info("getLicenseLicenseInfosList returned an empty response with status {}.", response.getStatusCode());
            return Collections.emptyList();
        } catch (ApiException e) {
            logger.error("Error Getting License using Intersight API. Query TraceId {} ",
                    e.getResponseHeaders() != null ?
                            e.getResponseHeaders().get(INTERSIGHT_TRACEID) : "Unknown");
            throw e;
        }
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
        ApiResponse<LicenseLicenseInfo> responseInfo = licenseApi.patchLicenseLicenseInfoWithHttpInfo(moid, updatedLicenseInfo, null);
        LicenseLicenseInfo licenseInfo = responseInfo.getData();
        logger.info("Response license: traceid {} moid {} license count {}}",
                IntersightLicenseUtils.getTraceIdHeader(responseInfo.getHeaders()),
                licenseInfo.getMoid(), licenseInfo.getLicenseCount());
        return licenseInfo;
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
