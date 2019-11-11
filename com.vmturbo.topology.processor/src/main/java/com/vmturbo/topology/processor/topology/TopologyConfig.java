package com.vmturbo.topology.processor.topology;

import java.util.concurrent.Executors;

import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.topology.processor.ncm.MatrixConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;
import com.vmturbo.topology.processor.history.HistoryAggregationConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.reservation.ReservationConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidationConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;
import com.vmturbo.topology.processor.workflow.WorkflowConfig;

/**
 * Configuration for the Topology package in TopologyProcessor.
 */
@Configuration
@Import({
    TopologyProcessorApiConfig.class,
    EntityConfig.class,
    SupplyChainValidationConfig.class,
    IdentityProviderConfig.class,
    GroupConfig.class,
    StitchingConfig.class,
    PlanConfig.class,
    RepositoryConfig.class,
    TemplateConfig.class,
    ClockConfig.class,
    ReservationConfig.class,
    ProbeConfig.class,
    TargetConfig.class,
    ControllableConfig.class,
    WorkflowConfig.class,
    HistoryClientConfig.class,
    CloudCostConfig.class,
    MatrixConfig.class,
    HistoryAggregationConfig.class
})
public class TopologyConfig {

    @Autowired
    private TopologyProcessorApiConfig apiConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private SupplyChainValidationConfig supplyChainValidationConfig;

    @Autowired
    private ControllableConfig controllableConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private CloudCostConfig cloudCostConfig;

    @Autowired
    private SQLDatabaseConfig sqlDatabaseConfig;

    @Autowired
    private MatrixConfig matrixConfig;

    @Autowired
    private HistoryAggregationConfig historyAggregationConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public TopologyHandler topologyHandler() {
        return new TopologyHandler(realtimeTopologyContextId(),
                topologyPipelineFactory(),
                identityProviderConfig.identityProvider(),
                entityConfig.entityStore(),
                probeConfig.probeStore(),
                targetConfig.targetStore(),
                clockConfig.clock());
    }

    @Bean
    public TopologyEditor topologyEditor() {
        return new TopologyEditor(identityProviderConfig.identityProvider(),
                templateConfig.templateConverterFactory(),
                // we don't use groupResolver cache here because we want
                // up-to-date results.
                groupConfig.groupServiceBlockingStub());
    }

    @Bean
    public PlanTopologyScopeEditor planTopologyScopeEditor() {
        return new PlanTopologyScopeEditor(groupConfig.groupServiceBlockingStub());
    }

    @Bean
    public DemandOverriddenCommodityEditor dmandOverriddenCommodityEditor() {
        return new DemandOverriddenCommodityEditor(groupConfig.groupServiceBlockingStub());
    }

    @Bean
    public DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner() {
        return new DiscoveredSettingPolicyScanner(probeConfig.probeStore(), targetConfig.targetStore());
    }

    @Bean
    public StitchingGroupFixer stitchingGroupFixer() {
        return new StitchingGroupFixer();
    }

    @Bean
    public EnvironmentTypeInjector environmentTypeInjector() {
        return new EnvironmentTypeInjector(targetConfig.targetStore());
    }

    @Bean MatrixInterface matrixInterface() {
        return matrixConfig.matrixInterface();
    }

    @Bean
    public TopologyPipelineFactory topologyPipelineFactory() {
        return new TopologyPipelineFactory(apiConfig.topologyProcessorNotificationSender(),
                groupConfig.policyManager(),
                stitchingConfig.stitchingManager(),
                planConfig.discoveredTemplatesUploader(),
                groupConfig.discoveredGroupUploader(),
                workflowConfig.discoveredWorkflowUploader(),
                cloudCostConfig.discoveredCloudCostUploader(),
                groupConfig.settingsManager(),
                groupConfig.entitySettingsApplicator(),
                environmentTypeInjector(),
                topologyEditor(),
                repositoryConfig.repository(),
                groupConfig.searchResolver(),
                groupConfig.groupServiceBlockingStub(),
                reservationConfig.reservationManager(),
                discoveredSettingPolicyScanner(),
                stitchingGroupFixer(),
                entityConfig.entityValidator(),
                supplyChainValidationConfig.supplyChainValidator(),
                groupConfig.discoveredClusterConstraintCache(),
                applicationCommodityKeyChanger(),
                controllableConfig.controllableManager(),
                commoditiesEditor(),
                planTopologyScopeEditor(),
                historicalEditor(),
                matrixInterface(),
                probeActionCapabilitiesApplicatorEditor(),
                historyAggregationConfig.historyAggregationStage(),
                dmandOverriddenCommodityEditor()
        );
    }

    /**
     * Used to identify the topology context of the real-time topology sent to the market component.
     *
     * @return the real-time topology context identifier.
     */
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }

    @Bean
    public CommoditiesEditor commoditiesEditor() {
        return  new CommoditiesEditor(historyClient());
    }

    @Bean
    public ApplicationCommodityKeyChanger applicationCommodityKeyChanger() {
        return new ApplicationCommodityKeyChanger();
    }

    @Bean
    public HistoricalUtilizationDatabase historicalUtilizationDatabase() {
        return new HistoricalUtilizationDatabase(sqlDatabaseConfig.dsl());
    }

    @Bean
    public HistoricalEditor historicalEditor() {
        return new HistoricalEditor(historicalUtilizationDatabase(), Executors.newSingleThreadExecutor());
    }

    @Bean
    public ProbeActionCapabilitiesApplicatorEditor probeActionCapabilitiesApplicatorEditor() {
        return new ProbeActionCapabilitiesApplicatorEditor(targetConfig.targetStore());
    }
}
