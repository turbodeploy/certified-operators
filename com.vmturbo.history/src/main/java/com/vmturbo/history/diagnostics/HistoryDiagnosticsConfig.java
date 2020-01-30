package com.vmturbo.history.diagnostics;

import java.time.Clock;
import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsController;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.history.db.HistoryDbConfig;

/**
 * Configuration for custom diagnostics for the history component.
 */
@Configuration
@Import({HistoryDbConfig.class})
public class HistoryDiagnosticsConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

    /**
     * Aggregation metadata diagnostics provider.
     *
     * @return aggregation metadata diagnostics provider
     */
    @Bean
    public AggregationMetadataDiagnostics aggregationMetadataDiagnostics() {
        return new AggregationMetadataDiagnostics(Clock.systemUTC(), historyDbConfig.historyDbIO());
    }

    /**
     * Aggregation performance diagnostics.
     *
     * @return aggregation performance diagnostics provider
     */
    @Bean
    public AggregationPerformanceDiagnostics aggregationPerformanceDiagnostics() {
        return new AggregationPerformanceDiagnostics(Clock.systemUTC(), historyDbConfig.historyDbIO());
    }

    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    @Bean
    public DiagnosticsHandler historyDiagnostics() {
        return new DiagnosticsHandler(
                Arrays.asList(aggregationMetadataDiagnostics(), aggregationPerformanceDiagnostics(),
                        prometheusDiagnisticsProvider()));
    }

    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsController(historyDiagnostics());
    }
}
