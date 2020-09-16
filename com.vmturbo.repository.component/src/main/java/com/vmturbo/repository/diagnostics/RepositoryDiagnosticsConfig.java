package com.vmturbo.repository.diagnostics;

import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.api.ComponentRestTemplate;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.repository.RepositoryComponentConfig;
import com.vmturbo.repository.RepositoryProperties;

/**
 * Spring configuration of beans related to diagnostics operations.
 */
@Configuration
@Import({RepositoryProperties.class, RepositoryComponentConfig.class})
public class RepositoryDiagnosticsConfig {

    @Value("${arangoDumpRestorePort:8599}")
    private int arangoDumpRestorePort;

    @Autowired
    private RepositoryProperties repositoryProperties;
    @Autowired
    private RepositoryComponentConfig repositoryComponentConfig;

    /**
     * Arango DB dump.
     *
     * @return Aranto dump.
     */
    @Bean
    public ArangoDump arangoDump() {
        final String dumpEndPoint =
                String.format("http://%s:%d/dump/", repositoryProperties.getArangodb().getHost(),
                        arangoDumpRestorePort);

        return new ArangoDump.Builder().endpoint(dumpEndPoint)
                .outputDir(repositoryProperties.getArangodb().getArangoDumpOutputDir())
                .build();
    }

    /**
     * Arango DB restore.
     *
     * @return Arango restore
     */
    @Bean
    public ArangoRestore arangoRestore() {
        final String restoreEndpoint =
                String.format("http://%s:%d/restore/", repositoryProperties.getArangodb().getHost(),
                        arangoDumpRestorePort);

        return new ArangoRestore.Builder().endpoint(restoreEndpoint)
                .baseDir(repositoryProperties.getArangodb().getArangoRestoreBaseDir())
                .inputDir(repositoryProperties.getArangodb().getArangoRestoreInputDir())
                .build();
    }

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
     * Bean to operate with supply chain diagnostics.
     *
     * @return supply chain diagnostics bean
     */
    @Bean
    public RealtimeSupplyChainDiagnostics supplyChainDiagnostics() {
        return new RealtimeSupplyChainDiagnostics(repositoryComponentConfig.topologyManager(),
                repositoryComponentConfig.globalSupplyChainManager(),
                repositoryComponentConfig.arangoDBExecutor());
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
                Arrays.asList(repositoryComponentConfig.topologyManager(), supplyChainDiagnostics(),
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
     * REST template to communicate with Arango DB.
     *
     * @return REST template
     */
    @Bean
    public RestTemplate restTemplate() {
        return ComponentRestTemplate.create();
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
