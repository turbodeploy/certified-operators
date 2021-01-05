package com.vmturbo.topology.processor.topology;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.actions.ActionsConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;
import com.vmturbo.topology.processor.history.HistoryAggregationConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.ncm.MatrixConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.reservation.ReservationConfig;
import com.vmturbo.topology.processor.rpc.TopologyProcessorRpcConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidationConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.pipeline.LivePipelineFactory;
import com.vmturbo.topology.processor.topology.pipeline.PlanPipelineFactory;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
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
    OperationConfig.class,
    MatrixConfig.class,
    HistoryAggregationConfig.class,
    LicenseCheckClientConfig.class,
    TopologyProcessorDBConfig.class,
    ConsistentScalingConfig.class,
    ActionsConfig.class
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
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Autowired
    private MatrixConfig matrixConfig;

    @Autowired
    private HistoryAggregationConfig historyAggregationConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Autowired
    private ConsistentScalingConfig consistentScalingConfig;

    @Autowired
    private ActionsConfig actionsConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Autowired
    private TopologyProcessorRpcConfig topologyProcessorRpcConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${waitForBroadcastTimeoutMin:60}")
    private long waitForBroadcastTimeoutMin;

    @Value("${concurrentPlanPipelinesAllowed:1}")
    private int concurrentPlanPipelinesAllowed;

    @Value("${maxQueuedPipelinesAllowed:1000}")
    private int maxQueuedPlanPipelinesAllowed;

    @Value("${useReservationPipeline:true}")
    private boolean useReservationPipeline;

    @Value("${supplyChainValidationFrequency:40}")
    private int supplyChainValidationFrequency;

    /**
     * How long we will wait to successfully discover targets at startup before allowing broadcasts.
     */
    @Value("${startupDiscovery.maxDiscoveryWaitMins:360}")
    private long startupDiscoveryMaxDiscoveryWaitMinutes;

    @Bean
    public TopologyHandler topologyHandler() {
        return new TopologyHandler(realtimeTopologyContextId(),
            pipelineExecutorService(),
            identityProviderConfig.identityProvider(),
            probeConfig.probeStore(),
            targetConfig.targetStore(),
            clockConfig.clock(),
            waitForBroadcastTimeoutMin,
            TimeUnit.MINUTES);
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
    public EnvironmentTypeInjector environmentTypeInjector() {
        return new EnvironmentTypeInjector(targetConfig.targetStore());
    }

    @Bean MatrixInterface matrixInterface() {
        return matrixConfig.matrixInterface();
    }

    /**
     * Helper for cloud migration stage.
     *
     * @return Newly created single helper instance per plan pipeline.
     */
    @Bean
    public CloudMigrationPlanHelper cloudMigrationPlanHelper() {
        return new CloudMigrationPlanHelper(
                groupConfig.groupServiceBlockingStub(),
                historyClient());
    }

    /**
     * A bean configuration to instantiate a live pipeline factory.
     *
     * @return A {@link LivePipelineFactory} instance.
     */
    @Bean
    public LivePipelineFactory livePipelineFactory() {
        return new LivePipelineFactory(apiConfig.topologyProcessorNotificationSender(),
                groupConfig.policyManager(),
                stitchingConfig.stitchingManager(),
                templateConfig.discoveredTemplatesUploader(),
                groupConfig.discoveredGroupUploader(),
                workflowConfig.discoveredWorkflowUploader(),
                cloudCostConfig.discoveredCloudCostUploader(),
                groupConfig.settingsManager(),
                groupConfig.entitySettingsApplicator(),
                environmentTypeInjector(),
                groupConfig.searchResolver(),
                groupConfig.groupServiceBlockingStub(),
                reservationConfig.reservationManager(),
                discoveredSettingPolicyScanner(),
                entityConfig.entityValidator(),
                supplyChainValidationConfig.supplyChainValidator(),
                groupConfig.discoveredClusterConstraintCache(),
                applicationCommodityKeyChanger(),
                controllableConfig.controllableManager(),
                historicalEditor(),
                matrixInterface(),
                actionsConfig.cachedTopology(),
                probeActionCapabilitiesApplicatorEditor(),
                historyAggregationConfig.historyAggregationStage(),
                licenseCheckClientConfig.licenseCheckClient(),
                consistentScalingConfig.consistentScalingManager(),
                actionsConfig.actionConstraintsUploader(),
                actionsConfig.actionMergeSpecsUploader(),
                requestCommodityThresholdsInjector(),
                ephemeralEntityEditor(),
                ReservationServiceGrpc.newStub(planClientConfig.planOrchestratorChannel()),
                topologyProcessorRpcConfig.groupResolverSearchFilterResolver(),
                targetConfig.groupScopeResolver(),
                supplyChainValidationFrequency
        );
    }

    /**
     * A bean configuration to instantiate a plan pipeline factory.
     *
     * @return A {@link PlanPipelineFactory} instance.
     */
    @Bean
    public PlanPipelineFactory planPipelineFactory() {
        return new PlanPipelineFactory(apiConfig.topologyProcessorNotificationSender(),
                groupConfig.policyManager(),
                stitchingConfig.stitchingManager(),
                groupConfig.settingsManager(),
                groupConfig.entitySettingsApplicator(),
                environmentTypeInjector(),
                topologyEditor(),
                repositoryConfig.repository(),
                groupConfig.searchResolver(),
                groupConfig.groupServiceBlockingStub(),
                reservationConfig.reservationManager(),
                entityConfig.entityValidator(),
                groupConfig.discoveredClusterConstraintCache(),
                applicationCommodityKeyChanger(),
                commoditiesEditor(),
                planTopologyScopeEditor(),
                probeActionCapabilitiesApplicatorEditor(),
                historicalEditor(),
                matrixInterface(),
                actionsConfig.cachedTopology(),
                historyAggregationConfig.historyAggregationStage(),
                dmandOverriddenCommodityEditor(),
                consistentScalingConfig.consistentScalingManager(),
                requestCommodityThresholdsInjector(),
                ephemeralEntityEditor(),
                topologyProcessorRpcConfig.groupResolverSearchFilterResolver(),
                cloudMigrationPlanHelper()
        );
    }

    /**
     * The entrance point for triggering broadcasts.
     *
     * @return The {@link TopologyPipelineExecutorService}.
     */
    @Bean
    public TopologyPipelineExecutorService pipelineExecutorService() {
        return new TopologyPipelineExecutorService(concurrentPlanPipelinesAllowed,
            maxQueuedPlanPipelinesAllowed,
            livePipelineFactory(),
            planPipelineFactory(),
            entityConfig.entityStore(),
            apiConfig.topologyProcessorNotificationSender(),
            targetConfig.targetStore(),
            clockConfig.clock(),
            startupDiscoveryMaxDiscoveryWaitMinutes,
            TimeUnit.MINUTES);
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
        return new HistoricalUtilizationDatabase(topologyProcessorDBConfig.dsl());
    }

    @Bean
    public HistoricalEditor historicalEditor() {
        return new HistoricalEditor(historicalUtilizationDatabase(), Executors.newSingleThreadExecutor());
    }

    @Bean
    public RequestAndLimitCommodityThresholdsInjector requestCommodityThresholdsInjector() {
        return new RequestAndLimitCommodityThresholdsInjector();
    }

    @Bean
    public EphemeralEntityEditor ephemeralEntityEditor() {
        return new EphemeralEntityEditor();
    }

    @Bean
    public ProbeActionCapabilitiesApplicatorEditor probeActionCapabilitiesApplicatorEditor() {
        return new ProbeActionCapabilitiesApplicatorEditor(targetConfig.targetStore());
    }
}
