package com.vmturbo.api.component.external.api.interceptor;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.prometheus.client.CollectorRegistry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.servlet.HandlerMapping;

import com.vmturbo.api.serviceinterfaces.IAppVersionInfo;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricDefinitions;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricUtilities;

/**
 * Test the {@link TelemetryInterceptor} which intercepts requests and gathers metadata about them.
 */
@RunWith(MockitoJUnitRunner.class)
public class TelemetryInterceptorTest {

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private IAppVersionInfo appVersionInfo;

    private TelemetryInterceptor telemetryInterceptor;

    /**
     * Set up the test.
     *
     * @throws Exception if an unexpected exception occurs.
     */
    @Before
    public void setUp() throws Exception {
        telemetryInterceptor = new TelemetryInterceptor(appVersionInfo);
    }

    /**
     * Test that the telemetry interceptor is reporting metadata like the URI.
     *
     * <p>Note that this test relies on the state of static singleton classes. There is currently
     * no method available to recreate all the static initialization that is done in classes like
     * {@link TelemetryMetricDefinitions}. Therefore it's important not to wipe out the state of
     * the associated classes before, during or after tests run.</p>
     *
     * <p>For example, this test has failed in the past when other tests were manipulating
     * (in this case clearing) the registry. The following line of code was the culprit:
     * CollectorRegistry.defaultRegistry.clear();</p>
     *
     * @throws Exception if an unexpected exception occurs.
     */
    @Test
    public void testAfterCompletion() throws Exception {
        // prepare
        when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
                .thenReturn("/login");
        when(request.getAttribute("latencyTimer"))
                .thenReturn("invalidTimer");
        when(request.getMethod()).thenReturn("GET");

        when(response.getStatus()).thenReturn(200);

        when(appVersionInfo.getVersion()).thenReturn("1.2.3");
        when(appVersionInfo.getBuildTime()).thenReturn("");

        // act
        telemetryInterceptor.preHandle(request, response, null);
        telemetryInterceptor.afterCompletion(request, response, null, null);

        final String telemetryOutput =
                TelemetryMetricUtilities.format004(CollectorRegistry.defaultRegistry);

        // assert
        assertThat(telemetryOutput, containsString(
                "turbo_api_calls_total{method=\"GET\",uri=\"/login\",from_browser=\"false\",failed=\"false\","));
    }

    /**
     * Test that the telemetry interceptor is reporting metadata like the URI.
     *
     * <p>Note that this test relies on the state of static singleton classes. There is currently
     * no method available to recreate all the static initialization that is done in classes like
     * {@link TelemetryMetricDefinitions}. Therefore it's important not to wipe out the state of
     * the associated classes before, during or after tests run.</p>
     *
     * <p>For example, this test has failed in the past when other tests were manipulating
     * (in this case clearing) the registry. The following line of code was the culprit:
     * CollectorRegistry.defaultRegistry.clear();</p>
     *
     * @throws Exception if an unexpected exception occurs.
     */
    @Test
    public void testAfterUnknown() throws Exception {
        // prepare
        when(request.getAttribute("latencyTimer")).thenReturn("invalidTimer");
        when(request.getMethod()).thenReturn("GET");

        // Test for an unknown URI.
        when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
                .thenReturn(null);

        when(response.getStatus()).thenReturn(200);

        when(appVersionInfo.getVersion()).thenReturn("1.2.3");
        when(appVersionInfo.getBuildTime()).thenReturn("");

        telemetryInterceptor.preHandle(request, response, null);
        telemetryInterceptor.afterCompletion(request, response, null, null);

        // act
        final String unknownTelemetryOutput =
                TelemetryMetricUtilities.format004(CollectorRegistry.defaultRegistry);

        // assert
        assertThat(unknownTelemetryOutput, containsString(
                "turbo_api_calls_total{method=\"GET\",uri=\"UNKNOWN\",from_browser=\"false\",failed=\"false\","));
    }

}