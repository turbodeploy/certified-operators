package com.vmturbo.topology.processor.diagnostics;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.proactivesupport.DataCollectorFramework;
import com.vmturbo.proactivesupport.bridge.TCPAggregatorBridge;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.ldcf.DataMetricTopology;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the Topology package in TopologyProcessor.
 */
@Configuration
@Import({TargetConfig.class, SchedulerConfig.class, EntityConfig.class, GroupConfig.class,
    TemplateConfig.class, IdentityProviderConfig.class, ProbeConfig.class, CloudCostConfig.class,
    TopologyConfig.class, HistoryAggregationConfig.class})
public class TopologyProcessorDiagnosticsConfig {
    /**
     * The urgent collection interval setting.
     * The default is 10 minutes.
     */
    @Value("${collectionIntervalUrgentSec}")
    private long collectionIntervalUrgent;

    /**
     * The offline collection interval setting.
     * The default is 3 days.
     */
    @Value("${collectionIntervalOfflineSec}")
    private long collectionIntervalOffline;

    /**
     * The TCP/IP bridge receiver host.
     */
    @Value("${bridgeHost}")
    private String bridgeHost;

    /**
     * The TCP/IP bridge receiver port.
     */
    @Value("${bridgePort}")
    private int bridgePort;

    /**
     * The anonymized flag.
     */
    @Value("${anonymized:true}")
    private boolean anonymized;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private CloudCostConfig cloudCostConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private HistoryAggregationConfig historyAggregationConfig;

    @Autowired
    private OperationConfig operationConfig;

    /**
     * The hardLock key.
     */
    private static final String TELEMETRY_LOCKED = "hardlock";

    /**
     * The enabled key.
     */
    private static final String TELEMETRY_ENABLED = "enabled";

    @Bean
    public TopologyProcessorDiagnosticsHandler diagsHandler() {
        final Map<String, BinaryDiagsRestorable> fixedFilenameBinaryDiagnosticParts =
                        historyAggregationConfig.statefulEditors().stream().collect(Collectors
                                        .toMap(item -> item.getFileName()
                                                                        + DiagsZipReader.BINARY_DIAGS_SUFFIX,
                                                        Function.identity()));
        return new TopologyProcessorDiagnosticsHandler(targetConfig.targetStore(),
            targetConfig.persistentIdentityStore(),
            schedulerConfig.scheduler(),
            entityConfig.entityStore(),
            probeConfig.probeStore(),
            groupConfig.discoveredGroupUploader(),
            templateConfig.discoveredTemplatesUploader(),
            identityProviderConfig.identityProvider(),
            cloudCostConfig.discoveredCloudCostUploader(),
            cloudCostConfig.priceTableUploader(),
            topologyConfig.pipelineExecutorService(),
            fixedFilenameBinaryDiagnosticParts,
            operationConfig.binaryDiscoveryDumper());
    }

    @Bean
    public TCPAggregatorBridge tcpAggegatorReceiverBridge() throws IOException {
        return new TCPAggregatorBridge(bridgeHost, bridgePort);
    }

    @Bean
    public DataCollectorFramework dataCollectorFramework() throws IOException {
        DataCollectorFramework.instance().setKeyValueCollector(
                () -> {
                    // In case we are hard locked, disallow collection.
                    String locked = kvConfig.keyValueStoreTelemetry().get(TELEMETRY_LOCKED, "false");
                    if (Boolean.parseBoolean(locked)) {
                        return false;
                    }
                    // Check the regular "enabled" flag.
                    String enabled = kvConfig.keyValueStoreTelemetry().get(TELEMETRY_ENABLED, "true");
                    return Boolean.parseBoolean(enabled);
                });
        DataCollectorFramework.instance().setAggregatorBridge(tcpAggegatorReceiverBridge());
        DataCollectorFramework.instance().registerMetric(new DataMetricTopology(diagsHandler(), anonymized));
        DataCollectorFramework.instance().start(
                TimeUnit.SECONDS.toMillis(collectionIntervalUrgent),
                TimeUnit.SECONDS.toMillis(collectionIntervalOffline));
        return DataCollectorFramework.instance();
    }
}
