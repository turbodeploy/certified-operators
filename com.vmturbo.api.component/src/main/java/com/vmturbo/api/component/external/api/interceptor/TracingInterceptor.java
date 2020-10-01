package com.vmturbo.api.component.external.api.interceptor;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.opentracing.Tracer;

import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.ModelAndView;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.tracing.TracingManager;

/**
 * Interceptor for the external API REST calls.
 *
 * TODO (roman, May 28 2019): We should expand the tracing interceptor configuration to allow
 * dynamically specifying which routes to record.
 */
public class TracingInterceptor implements HandlerInterceptor {

    private static final String TRACING_SCOPE_NAME = "tracingScope";

    private static final String QUERY_STR_TAG = "query_string";

    private static final String TRACE_HEADER_NAME = "X-Trace";

    @Override
    public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler) throws Exception {
        final boolean forceTrace = Optional.ofNullable(request.getHeader(TRACE_HEADER_NAME))
                .map(Boolean::valueOf)
                .orElse(false);
        final Tracer tracer;
        if (forceTrace) {
            tracer = TracingManager.alwaysOnTracer();
        } else {
            tracer = Tracing.tracer();
        }
        final String opName = getOpName(request);
        TracingScope scope = Tracing.trace(opName, tracer);
        if (request.getQueryString() != null) {
            scope.tag(QUERY_STR_TAG, request.getQueryString());
        }
        request.setAttribute(TRACING_SCOPE_NAME, scope);
        return true;
    }

    @Override
    public void postHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final Exception ex) throws Exception {
        final Object scope = request.getAttribute(TRACING_SCOPE_NAME);
        if (scope instanceof TracingScope) {
            ((TracingScope)scope).close();
        }
    }

    /**
     * Extract the name of the operation for tracing from an HTTP request.
     *
     * @param request The {@link HttpServletRequest}.
     * @return The name to use for the operation.
     */
    @Nonnull
    private String getOpName(@Nonnull final HttpServletRequest request) {
        final Object uriTemplatePattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        final String uri = uriTemplatePattern instanceof String ?
            (String)uriTemplatePattern : request.getRequestURI();
        final String method = request.getMethod();
        return method + " " + uri;
    }
}
