package com.vmturbo.api.component.external.api.interceptor;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.ModelAndView;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;

import com.vmturbo.components.api.tracing.Tracing;

/**
 * Interceptor for the external API REST calls.
 *
 * TODO (roman, May 28 2019): We should expand the tracing interceptor configuration to allow
 * dynamically specifying which routes to record.
 */
public class TracingInterceptor implements HandlerInterceptor {

    private static final String TRACING_SCOPE_NAME = "tracingScope";

    private static final String TRACING_SPAN_NAME = "tracingSpan";

    private static final String QUERY_STR_TAG = "query_string";

    @Override
    public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler) throws Exception {
        final Tracer tracer = Tracing.tracer();
        final String opName = getOpName(request);
        final Span span = tracer.buildSpan(opName).start();
        if (request.getQueryString() != null) {
            span.setTag(QUERY_STR_TAG, request.getQueryString());
        }
        final Scope scope = tracer.scopeManager().activate(span);
        request.setAttribute(TRACING_SCOPE_NAME, scope);
        request.setAttribute(TRACING_SPAN_NAME, span);
        return true;
    }

    @Override
    public void postHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final Exception ex) throws Exception {
        final Object scope = request.getAttribute(TRACING_SCOPE_NAME);
        if (scope instanceof Scope) {
            ((Scope)scope).close();
        }

        final Object span = request.getAttribute(TRACING_SPAN_NAME);
        if (span instanceof Span) {
            ((Span)span).finish();
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
