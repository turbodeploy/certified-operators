package com.vmturbo.components.common.diagnostics;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit test for {@link PrometheusDiagnosticsProvider}.
 */
public class PrometheusDiagnosticsProviderTest {
    /**
     * Test writing Prometheus metrics and reading them back.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWritePrometheusMetrics() throws DiagnosticsException {
        // Clear the registry so that only the metrics registered by this test are written
        // by the diagnostics dump.
        CollectorRegistry.defaultRegistry.clear();

        // Basic JVM metrics don't create a histogram or a summary, so add two of those.
        final Histogram histogram =
                Histogram.build().name("testHist").help("The histogram").register();
        histogram.observe(10);

        final Summary summary = Summary.build().name("testSummary").help("The summary").register();
        summary.observe(10);
        final PrometheusDiagnosticsProvider provider =
                new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        provider.collectDiags(appender);
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender).appendString(captor.capture());

        final String metrics = captor.getValue();
        assertThat(metrics, containsString("testHist"));
        assertThat(metrics, containsString("testSummary"));
    }
}
