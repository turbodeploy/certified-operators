package com.vmturbo.api.component.external.api.health;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.component.external.api.logging.GlobalExceptionHandler;
import com.vmturbo.api.component.external.api.service.LicenseService;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;

/**
 * Verify {@ApiComponentRestartHelper}.
 */
public class ApiComponentRestartHelperTest {

    ApiComponentRestartHelper apiComponentRestartHelper;
    GlobalExceptionHandler handler;
    LicenseService service;

    /**
     * Before.
     */
    @Before
    public void setup() {
        handler = mock(GlobalExceptionHandler.class);
        service = mock(LicenseService.class);
        apiComponentRestartHelper = new ApiComponentRestartHelper(handler, service, 6);
    }

    /**
     * If there is grpc unknown status error, it will report unhealthy.
     *
     * @throws UnauthorizedObjectException if missing permission.
     */
    @Test
    public void testIsHealthyWhenGrpcUnknownStatusError() throws UnauthorizedObjectException {
        when(handler.wasGrpcUnknownStatusError()).thenReturn(true);
        when(service.getLicensesSummary()).thenThrow(new StatusRuntimeException(Status.UNKNOWN));
        assertFalse("It should be unhealthy", apiComponentRestartHelper.isHealthy());
        assertEquals(-1L, apiComponentRestartHelper.getLastSuccessTime());
    }

    /**
     * if there is no grpc unknown status error, it will report healthy.
     *
     * @throws UnauthorizedObjectException if missing permission.
     */
    @Test
    public void testIsHealthyWithoutGrpcUnknownStatusError() throws UnauthorizedObjectException {
        when(handler.wasGrpcUnknownStatusError()).thenReturn(false);
        when(service.getLicensesSummary()).thenReturn(new LicenseApiDTO());
        assertTrue("It should be healthy", apiComponentRestartHelper.isHealthy());
        assertEquals(-1L, apiComponentRestartHelper.getLastSuccessTime());
    }

    /**
     * If there is grpc unknown status error, but it's recovered, it will report unhealthy.
     *
     * @throws UnauthorizedObjectException if missing permission.
     */
    @Test
    public void testIsHealthyWhenGrpcUnknownStatusErrorAndRecovered()
            throws UnauthorizedObjectException {
        when(handler.wasGrpcUnknownStatusError()).thenReturn(true);
        when(service.getLicensesSummary()).thenReturn(new LicenseApiDTO());
        assertTrue("It should be healthy", apiComponentRestartHelper.isHealthy());
        // last success time should be set
        assertNotEquals(-1L, apiComponentRestartHelper.getLastSuccessTime());
    }
}