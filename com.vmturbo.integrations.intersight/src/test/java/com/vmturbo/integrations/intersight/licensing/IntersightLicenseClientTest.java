package com.vmturbo.integrations.intersight.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfoList;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;

/**
 * Tests involving the IntersightLicenseClient.
 */
public class IntersightLicenseClientTest {

    // We have access to two Intersight endpoints, one in our own lab and the other the Cisco
    // Intersight CICD test cloud (which requires being in the company network to access).
    // Pick one and fill in the cookie to use.
    //
    private static final String turboLabIntersight = "intersight.corp.vmturbo.com";
    private static final String turboLabClientId = "c39609c483122d0909959e6d8257c667ada01d5d0c6df24e08c071f8c0c611c0";
    private static final String turboLabClientSecret = "acc47d826d7721ccf85b319f0c6f66e4";
    private static final String cicdCloudIntersight = "cicdtest.starshipcloud.com";
    private static final int port = 443;
    private static final String CICD_CLOUD_CLIENT_ID = "0844282dbb9385ad692acffc6e2fe2fc8d321a80649f0eca564899eafcccfd02-5ecd67187564612d33eafca2";
    private static final String CICD_CLOUD_CLIENT_SECRET = "edbe8ea8afc0b4e8e21a761179a5fbe5";

    // Toggle the following between the cicdtest and the turbo lab instance
    private static final boolean useCicdtestConnection = true;
    private IntersightConnection connection = useCicdtestConnection ? new IntersightConnection(
            cicdCloudIntersight, port, CICD_CLOUD_CLIENT_ID, CICD_CLOUD_CLIENT_SECRET)
            : new IntersightConnection(turboLabIntersight, port, turboLabClientId, turboLabClientSecret);

    private IntersightLicenseClient intersightLicenseClient = new IntersightLicenseClient(connection, null);

    /**
     * Simple test to verify that the intersight license endpoint works. Note that since this does
     * make an actual RPC call, we may need to disable this test during normal builds since we cannot
     * guarantee 100% availability of the endpoint.
     *
     * @throws IOException if there is an error establishing the session
     * @throws ApiException if there are errors returned from the Intersight API
     */
    @Ignore // disabling by default since it requires access to an external resource
    @Test
    public void testGetIntersightLicenses() throws IOException, ApiException {
        LicenseLicenseInfoList response = intersightLicenseClient.getIntersightLicenses();
        List<com.cisco.intersight.client.model.LicenseLicenseInfo> licenses = response.getResults();
        Assert.assertNotNull(licenses);
        assertEquals(1, licenses.size());
        com.cisco.intersight.client.model.LicenseLicenseInfo license = licenses.get(0);
        // map it to a proxy license
        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(license);
        assertTrue(mappedLicense.hasExternalLicenseKey());
        assertEquals(IntersightProxyLicenseEdition.IWO_ESSENTIALS.name(), mappedLicense.getEdition());
    }


    /**
     * Test the utility function for creating LicenseLicenseInfo. It's here instead of
     * IntersightLicenseUtilsTest because we're relying on the JSON parser from the license client.
     */
    @Ignore
    @Test
    public void testCreateIwoLicenseLicenseInfo() throws IOException {
        LicenseLicenseInfo licenseInfo = IntersightLicenseUtils
                .createIwoLicense(intersightLicenseClient.getApiClient().getJSON(), "1",
                        LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.GRACEEXPIRED);
        assertEquals("1", licenseInfo.getMoid());
        assertEquals(LicenseStateEnum.GRACEEXPIRED, licenseInfo.getLicenseState());
        assertEquals(LicenseTypeEnum.ESSENTIAL, licenseInfo.getLicenseState());
    }

    /**
     * Test the retryable check on the Api exceptions / http status codes we want to retry and not
     * retry on.
     */
    @Test
    public void testIsRetryableExceptionApiExceptions() {

        // if a quota is exceeded, we may get a 429 back. We'll consider this retryable, but we
        // should make sure to be extending the back off time.
        assertTrue(IntersightLicenseClient.isRetryable(new ApiException(429, "Too many requests")));

        // we'll retry on gateway timeout (504) and service unavailable (503) as well.
        assertTrue(IntersightLicenseClient.isRetryable(new ApiException(503, "Service unavailable")));
        assertTrue(IntersightLicenseClient.isRetryable(new ApiException(504, "Gateway Timeout")));

        // other http codes, like 400, are non-retryable.
        assertFalse(IntersightLicenseClient.isRetryable(new ApiException(400, "Bad Request")));
    }

    /**
     * Test the retryable check on other exception types.
     */
    @Test
    public void testIsRetryableExceptionNonRetraybleExceptions() {
        // we are not going to consider IOException (which is a checked exception that can be thrown
        // when trying to set up a session) as retryable.
        assertFalse(IntersightLicenseClient.isRetryable(new IOException()));

        // we also won't try on other non-ApiException types
        assertFalse(IntersightLicenseClient.isRetryable(new Exception()));
    }

}
