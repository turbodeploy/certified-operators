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

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;

/**
 * Tests involving the IntersightLicenseClient.
 */
public class IntersightLicenseClientTest {

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
