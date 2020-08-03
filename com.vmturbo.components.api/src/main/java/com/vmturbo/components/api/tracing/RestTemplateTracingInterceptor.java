package com.vmturbo.components.api.tracing;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestTemplate;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

/**
 * A simple interceptor that can be added to a {@link RestTemplate} to add a REST call made with
 * the template to an open-tracing {@link Tracer}.
 */
public class RestTemplateTracingInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(final HttpRequest request,
                                        final byte[] body,
                                        final ClientHttpRequestExecution execution) throws IOException {
        final Tracer tracer = GlobalTracer.get();
        final String opName = getOpName(request);
        final Span activeSpan = tracer.activeSpan();
        final Span operationSpan;
        if (activeSpan == null) {
            operationSpan = tracer.buildSpan(opName).start();
        } else {
            operationSpan = tracer.buildSpan(opName).asChildOf(activeSpan).start();
        }

        try {
            return execution.execute(request, body);
        } finally {
            operationSpan.finish();
        }
    }

    @Nonnull
    private String getOpName(@Nonnull final HttpRequest request) {
        final String uriOp = request.getURI().toString();
        final String method = request.getMethod().name();
        return method + " " + uriOp;
    }
}
