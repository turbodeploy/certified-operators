package com.vmturbo.api.component.external.api.interceptor;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.ModelAndView;

import com.vmturbo.api.serviceinterfaces.IAppVersionInfo;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricDefinitions;

/**
 * This interceptor is used to intercept every request, it contains three methods:
 * "preHandle": Called before the handler execution (before methods in every controller)
 * "postHandle": Called after the handler execution, result can be changed here before rendering to client
 * "afterCompletion": Called after the complete request has finished.
 *
 * <p>Note: Request mapping for this interceptor is detailed in
 * com.vmturbo/BlazeDS/WEB-INF/rest-dispatcher-servlet.xml</p>
 *
 * <p>This interceptor instruments each request to track information about:
 *  1. The number of times a given endpoint is called.
 *  2. How long it takes to service requests.</p>
 */
@Component
public class TelemetryInterceptor implements HandlerInterceptor {

    private static final Logger logger = LogManager.getLogger();
    private static final String LATENCY_TIMER_ATTRIBUTE = "latencyTimer";
    private static final String UNKNOWN_URI = "UNKNOWN";

    private final IAppVersionInfo appVersionInfo;

    /**
     * Create a TelemetryInterceptor.
     *
     * @param appVersionInfo describes the software we are running
     */
    @Autowired
    public TelemetryInterceptor(IAppVersionInfo appVersionInfo) {
        this.appVersionInfo = appVersionInfo;
    }

    /**
     * Instrument incoming requests.
     * See documentation of overridden method for further information.
     *
     * <p>1. Increment the call count for the associated endpoint.
     * 2. Add an attribute to the request containing a timer that can be used to track how
     *    long it takes to service the request.</p>
     *
     * @param httpServletRequest The request to instrument.
     * @param httpServletResponse The response object (unused).
     * @param o Unused.
     * @return true (continue the response chain).
     * @throws Exception If something goes wrong.
     */
    @Override
    public boolean preHandle(HttpServletRequest httpServletRequest,
            HttpServletResponse httpServletResponse, Object o) throws Exception {
        final String uriForTelemetry = getTelemetryUri(httpServletRequest);
        String turboVersion = appVersionInfo.getVersion();
        String turboRevision = appVersionInfo.getBuildTime();

        // Insert an attribute into the request containing a timer that can be used
        // to track how long it took to service the request.
        httpServletRequest.setAttribute(LATENCY_TIMER_ATTRIBUTE, TelemetryMetricDefinitions
                .apiLatencyTimer(httpServletRequest.getMethod(), uriForTelemetry, turboVersion, turboRevision));

        return true;
    }

    @Override
    public void postHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
            Object o, ModelAndView modelAndView) throws Exception {
        // Nothing to do
    }

    /**
     * Increments the count for this request. Uses the timer attribute on the request object to
     * track how long it took to service the request. Also it finds out whether the request was
     * successful or not.
     *
     * @param httpServletRequest The request being instrumented.
     * @param httpServletResponse The response object (unused)
     * @param o Unused
     * @throws Exception If something goes wrong.
     */
    @Override
    public void afterCompletion(HttpServletRequest httpServletRequest,
            HttpServletResponse httpServletResponse, Object o,
            Exception e) throws Exception {
        final int statusCode = httpServletResponse.getStatus();
        boolean failed = false;
        if (statusCode >= 400) {
            failed = true;
        }

        String turboVersion = appVersionInfo.getVersion();
        String turboRevision = appVersionInfo.getBuildTime();

        final String uriForTelemetry = getTelemetryUri(httpServletRequest);
        TelemetryMetricDefinitions.incrementApiCallCount(httpServletRequest.getMethod(),
                uriForTelemetry, isFromBrowser(httpServletRequest), failed, turboVersion, turboRevision);

        // Retrieve the timer.
        final Object latencyTimerObject = httpServletRequest.getAttribute(LATENCY_TIMER_ATTRIBUTE);
        if (latencyTimerObject instanceof DataMetricTimer) {
            // Use the timer to track how long it took to service the request.
            double timeTaken = ((DataMetricTimer)latencyTimerObject).observe();
            logger.trace("{} request for uri {} took {} seconds.", httpServletRequest.getMethod(),
                    getTelemetryUri(httpServletRequest), timeTaken);
        } else {
            logger.warn("Unable to find latency timer for {}", httpServletRequest.getRequestURI());
        }
    }

    /**
     * Examine the request user-agent to determine if the request was made from the browser.
     * This can be used to differentiate requests originating through automated systems and
     * those coming directly from the browser.
     * All the major browsers in their user agent string contain "Mozilla". So in order to establish
     * that the incoming request originated from a browser we just check if the request's user agent
     * string contains "Mozilla".
     * Ref : https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
     *
     * @param request The request object containing user-agent information.
     * @return True if the request originated from a browser, false if not.
     */
    @SuppressWarnings("WeakerAccess")
    @VisibleForTesting
    boolean isFromBrowser(@Nonnull final HttpServletRequest request) {
        final String userAgent = request.getHeader("User-Agent");
        return userAgent != null && userAgent.startsWith("Mozilla");
    }

    /**
     * Get the URI to use for telemetry. For URI's with template parameters
     * (ie "/foo/{id}", we want the template URI prior to substitution so that we have a single
     * metric series for "/foo/{id}" and not a separate metric series for every foo object
     * ever requested.
     *
     * <p>In the case where we cannot retrieve the attribute for the template URI from the request
     * object, fall back to returning the URI after substitution.
     *
     * @param request The request object containing URI information.
     * @return The template URI prior to variable substitution, or, if unavailable, the
     * request URI.
     */
    @SuppressWarnings("WeakerAccess")
    @VisibleForTesting
    String getTelemetryUri(@Nonnull final HttpServletRequest request) {
        Object uriTemplatePattern =
                request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        if (uriTemplatePattern instanceof String) {
            return (String)uriTemplatePattern;
        } else {
            return UNKNOWN_URI;
        }
    }
}