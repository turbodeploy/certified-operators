package com.vmturbo.history.diagnostics;

import java.sql.SQLException;
import java.time.Clock;
import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsController;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.history.db.DbAccessConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for custom diagnostics for the history component.
 */
@Configuration
@Import({HistoryDbConfig.class})
public class HistoryDiagnosticsConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    /**
     * Aggregation metadata diagnostics provider.
     *
     * @return aggregation metadata diagnostics provider
     */
    @Bean
    public AggregationMetadataDiagnostics aggregationMetadataDiagnostics() {
        try {
            return new AggregationMetadataDiagnostics(Clock.systemUTC(), dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create aggregationMetadataDiagnostics bean",
                    e);
        }
    }

    /**
     * Aggregation performance diagnostics.
     *
     * @return aggregation performance diagnostics provider
     */
    @Bean
    public AggregationPerformanceDiagnostics aggregationPerformanceDiagnostics() {
        try {
            return new AggregationPerformanceDiagnostics(Clock.systemUTC(), dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException(
                    "Failed to create aggregationPerformanceDiagnostics bean", e);
        }
    }

    /**
     * Create {@link PrometheusDiagnosticsProvider } instance.
     *
     * @return PrometheusDiagnosticsProvider
     */
    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    /**
     * Create {@link DiagnosticsHandler} for history.
     * @return DiagnosticsHandler
     */
    @Bean
    public DiagnosticsHandler historyDiagnostics() {
        return new DiagnosticsHandler(
                Arrays.asList(aggregationMetadataDiagnostics(), aggregationPerformanceDiagnostics(),
                        prometheusDiagnisticsProvider()));
    }

    /**
     * Create {@link DiagnosticsController} for history.
     * @return DiagnosticsController
     */
    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsController(historyDiagnostics());
    }
}
