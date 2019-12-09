package com.vmturbo.api.component.external.api.interceptor;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;

/**
 * Unit tests for {@link DevFreemiumInterceptor}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DevFreemiumInterceptorTest {
    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private LicenseCheckClient licenseCheckClient;

    private DevFreemiumInterceptor devFreemiumInterceptor;

    /**
     * Test set up.
     */
    @Before
    public void setUp() {
        devFreemiumInterceptor = new DevFreemiumInterceptor(licenseCheckClient);
        when(request.getMethod()).thenReturn("POST");
        when(request.getPathInfo()).thenReturn("/actions/**");
    }

    /**
     * Test API call when license is developer freemium.
     *
     * @throws Exception exception to be thrown.
     */
    @Test
    public void testDevFreemium() throws Exception {
        when(licenseCheckClient.isDevFreemium()).thenReturn(true);
        boolean result = devFreemiumInterceptor.preHandle(request, response, null);
        assertFalse(result);
    }
}
