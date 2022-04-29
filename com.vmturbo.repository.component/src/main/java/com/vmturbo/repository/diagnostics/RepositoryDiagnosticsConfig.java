package com.vmturbo.repository.diagnostics;

import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.repository.RepositoryComponentConfig;

/**
 * Spring configuration of beans related to diagnostics operations.
 */
@Configuration
@Import({RepositoryComponentConfig.class})
public class RepositoryDiagnosticsConfig {

    @Autowired
    private RepositoryComponentConfig repositoryComponentConfig;

    /**
     * Bean to operate with source live topology.
     *
     * @return source live topology diagnostics
     */
    @Bean
    public TopologyStoreSourceDiagnostics topologyStoreSourceDiagnostics() {
        return new TopologyStoreSourceDiagnostics(repositoryComponentConfig.liveTopologyStore());
    }

    /**
     * Bean to operate with projected live topology.
     *
     * @return projected live topology diagnostics
     */
    @Bean
    public TopologyStoreProjectedDiagnostics topologyStoreProjectedDiagnostics() {
        return new TopologyStoreProjectedDiagnostics(repositoryComponentConfig.liveTopologyStore());
    }

    /**
     * Prometheus diagnostics provider.
     *
     * @return prometheus diagnostics provider
     */
    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    /**
     * Repository diagnostics handler.
     *
     * @return diagnostics handler
     */
    @Bean
    public DiagnosticsHandlerImportable repositoryDiagnosticsHandler() {
        return new DiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                Arrays.asList(repositoryComponentConfig.topologyManager(),
                        topologyStoreSourceDiagnostics(), topologyStoreProjectedDiagnostics(),
                        prometheusDiagnisticsProvider()));
    }

    /**
     * Zip file factory to read diags from ZIP.
     *
     * @return zip file factory
     */
    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    /**
     * REST controller to handle diagnostics requests.
     *
     * @return dagnostics controller
     */
    @Bean
    public DiagnosticsControllerImportable repositoryDiagnosticController() {
        return new DiagnosticsControllerImportable(repositoryDiagnosticsHandler());
    }
}
