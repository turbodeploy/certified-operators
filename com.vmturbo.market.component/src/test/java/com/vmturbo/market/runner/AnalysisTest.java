package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Removed;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCleaner;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.diagnostics.DiagsFileSystem;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reservations.InitialPlacementHandler;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisEngine;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisEngine.NamespaceQuotaAnalysisFactory;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisResult;
import com.vmturbo.market.runner.reconfigure.ExternalReconfigureActionEngine;
import com.vmturbo.market.runner.wasted.applicationservice.WastedApplicationServiceAnalysisEngine;
import com.vmturbo.market.runner.wasted.applicationservice.WastedApplicationServiceResults;
import com.vmturbo.market.runner.wasted.files.WastedFilesAnalysisEngine;
import com.vmturbo.market.runner.wasted.files.WastedFilesResults;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.market.topology.conversions.cloud.JournalActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.JournalActionSavingsCalculatorFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link Analysis}.
 */
public class AnalysisTest {

    private long topologyContextId = 1111;
    private long topologyId = 2222;
    private static final float DEFAULT_RATE_OF_RESIZE = 2.0f;
    private TopologyType topologyType = TopologyType.PLAN;
    private AnalysisRICoverageListener listener;

    private static final long PM1_ID = 1L;
    private static final long PM2_ID = 2L;
    private static final long VM_ID = 3L;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setTopologyType(topologyType)
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS)
            .addAnalysisType(AnalysisType.WASTED_FILES)
            .addAnalysisType(AnalysisType.WASTED_APP_SERVICE_PLANS)
            .build();

    private TopologyEntityCloudTopology cloudTopology;
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    private GroupServiceBlockingStub groupServiceClient;

    private static final Instant START_INSTANT = Instant.EPOCH.plus(90, ChronoUnit.MINUTES);
    private static final Instant END_INSTANT = Instant.EPOCH.plus(100, ChronoUnit.MINUTES);

    private static final float QUOTE_FACTOR = 0.77f;
    private static final float MOVE_COST_FACTOR = 0.05f;
    private static final int LICENSE_PRICE_WEIGHT_SCALE = 3;
    private static final long VOLUME_ID_DELETE_ACTION = 1111L;
    private static final long APP_SERVICE_PLAN_ID_DELETE_ACTION = 3333L;
    private final Clock mockClock = mock(Clock.class);

    private final Action wastedFileAction = Action.newBuilder()
        .setInfo(ActionInfo.newBuilder()
                    .setDelete(Delete.newBuilder()
                            .setTarget(ActionEntity.newBuilder().setId(VOLUME_ID_DELETE_ACTION)
                                    .setType(EntityType.VIRTUAL_VOLUME_VALUE))))
        .setExplanation(Explanation.newBuilder()
            .setDelete(DeleteExplanation.getDefaultInstance()))
        .setDeprecatedImportance(0.0d)
        .setId(1234L).build();

    // Note: Azure App Service Plans will migrate from App Components to VMSPEC entity types in the future.
    private final Action wastedAppServicePlanAction = Action.newBuilder()
            .setInfo(ActionInfo.newBuilder()
                    .setDelete(Delete.newBuilder()
                            .setTarget(ActionEntity.newBuilder().setId(APP_SERVICE_PLAN_ID_DELETE_ACTION)
                                    .setType(EntityType.APPLICATION_COMPONENT_VALUE))))
            .setExplanation(Explanation.newBuilder()
                    .setDelete(DeleteExplanation.getDefaultInstance())).setSavingsPerHour(
                    CurrencyAmount.newBuilder().setAmount(3.50d))
            .setDeprecatedImportance(0.0d)
            .setId(4343L).build();

    private final Action namespaceResizeAction = Action.newBuilder()
        .setInfo(ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(2222)
                    .setType(EntityType.NAMESPACE_VALUE))))
        .setExplanation(Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0)
                .setDeprecatedEndUtilization(0)))
        .setDeprecatedImportance(0)
        .setId(2345L)
        .build();

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
            mock(BuyRIImpactAnalysisFactory.class);
    private BuyRIImpactAnalysis buyRIImpactAnalysis = mock(BuyRIImpactAnalysis.class);

    private InitialPlacementHandler initialPlacementHandler = mock(InitialPlacementHandler.class);

    private JournalActionSavingsCalculatorFactory actionSavingsCalculatorFactory =
            mock(JournalActionSavingsCalculatorFactory.class);

    private ConsistentScalingHelper csm = mock(ConsistentScalingHelper.class);
    private ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory
            = mock(ReversibilitySettingFetcherFactory.class);
    private ExecutorService threadPool = Executors.newSingleThreadExecutor();
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
                     testSettingPolicyService);

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
        when(mockClock.instant())
                .thenReturn(START_INSTANT)
                .thenReturn(END_INSTANT);
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        listener = mock(AnalysisRICoverageListener.class);
        cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(csm);
        when(buyRIImpactAnalysisFactory.createAnalysis(eq(topologyInfo), any(), any(), any()))
                .thenReturn(buyRIImpactAnalysis);

        // setup default action savings
        final JournalActionSavingsCalculator savingsCalculator = mock(JournalActionSavingsCalculator.class);
        when(actionSavingsCalculatorFactory.newCalculator(anyMap(), any(), any(), anyMap(), anyMap(), anyMap(), anyMap()))
                .thenReturn(savingsCalculator);
        when(savingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.NO_SAVINGS_USD);
        InitialPlacementFinder placementFinder = mock(InitialPlacementFinder.class);
        when(initialPlacementHandler.getPlacementFinder()).thenReturn(placementFinder);
        when(placementFinder.shouldConstructEconomyCache()).thenReturn(false);
    }

    /**
     * Convenience method to get an Analysis based on an analysisConfig, a set of
     * TopologyEntityDTOs, and TopologyInfo.
     *
     * @param analysisConfig configuration for the Analysis.
     * @param topologySet Set of TopologyEntityDTOs for the Analysis.
     * @param topoInfo TopologyInfo related to the Analysis.
     * @return Analysis
     */
    private Analysis getAnalysis(AnalysisConfig analysisConfig, Set<TopologyEntityDTO> topologySet,
                                 TopologyInfo topoInfo, Optional<FakeEntityCreator> fakeEntityCreator) {
        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
        when(priceTableFactory.newPriceTable(any(), eq(CloudCostData.empty()))).thenReturn(mock(
                CloudRateExtractor.class));
        when(cloudCostCalculatorFactory.newCalculator(topoInfo, cloudTopology)).thenReturn(cloudCostCalculator);
        final long vmOid = 123L;
        final TopologyEntityDTO cloudVm = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD).setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        when(cloudTopology.getEntities()).thenReturn(ImmutableMap.of(vmOid, cloudVm));
        when(cloudTopologyFactory.newCloudTopology(anyLong(), any())).thenReturn(cloudTopology);
        when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(cloudTopology);
        final WastedFilesAnalysisEngine wastedFilesAnalysisEngine =
            mock(WastedFilesAnalysisEngine.class);
        final WastedApplicationServiceAnalysisEngine wastedApplicationServiceAnalysisEngine =
                mock(WastedApplicationServiceAnalysisEngine.class);
        final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
                mock(BuyRIImpactAnalysisFactory.class);
        // Mocks for wasted entity results (volumes & application services (for now azure app service plans))
        final WastedFilesResults wastedFilesAnalysis = mock(WastedFilesResults.class);
        when(wastedFilesAnalysisEngine.analyze(any(), any(), any(), any()))
                .thenReturn(wastedFilesAnalysis);
        when(wastedFilesAnalysis.getAllActions())
                .thenReturn(Collections.singletonList(wastedFileAction));
        when(wastedFilesAnalysis.getEntityIds())
                .thenReturn(ImmutableSet.of(wastedFileAction.getInfo().getDelete().getTarget().getId()));
        when(wastedFilesAnalysis.getMbReleasedOnProvider(anyLong())).thenReturn(OptionalLong.empty());
        final WastedApplicationServiceResults wastedApplicationServiceResults = mock(WastedApplicationServiceResults.class);
        when(wastedApplicationServiceAnalysisEngine.analyze(any(), any(), any(), any()))
                .thenReturn(wastedApplicationServiceResults);
        when(wastedApplicationServiceResults.getAllActions())
                .thenReturn(Collections.singletonList(wastedAppServicePlanAction));
        when(wastedApplicationServiceResults.getEntityIds())
                .thenReturn(ImmutableSet.of(wastedAppServicePlanAction.getInfo().getDelete().getTarget().getId()));

        final NamespaceQuotaAnalysisFactory nsQuotaAnalysisFactory =
            mock(NamespaceQuotaAnalysisFactory.class);
        final NamespaceQuotaAnalysisEngine nsQuotaAnalysisEngine = mock(NamespaceQuotaAnalysisEngine.class);
        when(nsQuotaAnalysisFactory.newNamespaceQuotaAnalysisEngine(any()))
            .thenReturn(nsQuotaAnalysisEngine);
        final NamespaceQuotaAnalysisResult nsQuotaAnalysisResult = mock(NamespaceQuotaAnalysisResult.class);
        when(nsQuotaAnalysisEngine.execute(any(), any(), any()))
            .thenReturn(nsQuotaAnalysisResult);
        when(nsQuotaAnalysisResult.getNamespaceQuotaResizeActions())
                .thenReturn(Collections.singletonList(namespaceResizeAction));
        final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService = mock(MigratedWorkloadCloudCommitmentAnalysisService.class);
        doNothing().when(migratedWorkloadCloudCommitmentAnalysisService).startAnalysis(anyLong(), any(), anyList());

        final ExternalReconfigureActionEngine externalReconfigureActionEngine = mock(
                ExternalReconfigureActionEngine.class);
        final AnalysisDiagnosticsCollectorFactory analysisCollectorFactory =
                mock(AnalysisDiagnosticsCollectorFactory.class);
        when(analysisCollectorFactory.newDiagsCollector(any(), any())).thenReturn(Optional.empty());
        GroupMemberRetriever groupMemberRetriever = new GroupMemberRetriever(groupServiceClient);
        return new Analysis(topoInfo, topologySet, groupMemberRetriever, mockClock, analysisConfig,
                cloudTopologyFactory, cloudCostCalculatorFactory, priceTableFactory,
                wastedFilesAnalysisEngine, buyRIImpactAnalysisFactory, nsQuotaAnalysisFactory,
                tierExcluderFactory, listener, consistentScalingHelperFactory,
                initialPlacementHandler, reversibilitySettingFetcherFactory,
                migratedWorkloadCloudCommitmentAnalysisService, new CommodityIdUpdater(),
                actionSavingsCalculatorFactory, externalReconfigureActionEngine,
                new AnalysisDiagnosticsCleaner(10, 10, new DiagsFileSystem()),
                analysisCollectorFactory, wastedApplicationServiceAnalysisEngine,
                fakeEntityCreator.isPresent() ? fakeEntityCreator.get()
                        : new FakeEntityCreator(groupMemberRetriever));
    }

    /**
     * Convenience method to get an Analysis based on an analysisConfig, a set of
     * TopologyEntityDTOs, and TopologyInfo.
     *
     * @param analysisConfig configuration for the Analysis.
     * @param topologySet Set of TopologyEntityDTOs for the Analysis.
     * @param topoInfo TopologyInfo related to the Analysis.
     * @return Analysis
     */
    private Analysis getAnalysis(AnalysisConfig analysisConfig, Set<TopologyEntityDTO> topologySet,
                                 TopologyInfo topoInfo) {
        return getAnalysis(analysisConfig, topologySet, topoInfo, Optional.empty());
    }


    /**
     * Convenience method to get an Analysis based on an analysisConfig and a set of
     * TopologyEntityDTOs.
     *
     * @param analysisConfig configuration for the analysis
     * @param topologySet set of TopologyEntityDTOs representing the topology
     * @return Analysis
     */
    private Analysis getAnalysis(AnalysisConfig analysisConfig, Set<TopologyEntityDTO> topologySet) {
        return getAnalysis(analysisConfig, topologySet, topologyInfo);
    }

    private Analysis getAnalysis(AnalysisConfig analysisConfig) {
        return getAnalysis(analysisConfig, Collections.emptySet());
    }

        /**
         * Test the {@link Analysis} constructor.
         */
    @Test
    public void testConstructor() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                    SuspensionsThrottlingConfig.DEFAULT,
                    Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();

        final Analysis analysis = getAnalysis(analysisConfig);

        assertEquals(topologyContextId, analysis.getContextId());
        assertEquals(topologyId, analysis.getTopologyId());
        assertEquals(analysisConfig, analysis.getConfig());
        assertEquals(Collections.emptyMap(), analysis.getTopology());
        assertEquals(Instant.EPOCH, analysis.getStartTime());
        assertEquals(Instant.EPOCH, analysis.getCompletionTime());
    }

    /**
     * Test the {@link Analysis#execute} method.
     */
    @Test
    public void testExecute() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
            SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
            .setIncludeVDC(true)
            .build();

        final Analysis analysis = getAnalysis(analysisConfig);
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(analysis.getState(), AnalysisState.SUCCEEDED);
        assertEquals(START_INSTANT, analysis.getStartTime());
        assertEquals(END_INSTANT, analysis.getCompletionTime());

        assertTrue(analysis.getActionPlan().isPresent());
        assertTrue(analysis.getProjectedTopology().isPresent());
        assertTrue(analysis.getActionPlan().get().getActionList().contains(wastedFileAction));
        assertTrue(analysis.getActionPlan().get().getActionList().contains(wastedAppServicePlanAction));
    }

    /**
     * Test execution of Analysis where TopologyInfo indicates not to run wastedFilesAnalysis.
     */
    @Test
    public void testExecuteNoWastedFiles() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
            SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
            .setIncludeVDC(true)
            .build();

        // create TopologyInfo that does not include wasted files analysis
        final TopologyInfo topoInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setTopologyType(topologyType)
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .build();

        final Analysis analysis = getAnalysis(analysisConfig, Collections.emptySet(), topoInfo);
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(analysis.getState(), AnalysisState.SUCCEEDED);
        assertEquals(START_INSTANT, analysis.getStartTime());
        assertEquals(END_INSTANT, analysis.getCompletionTime());

        assertTrue(analysis.getActionPlan().isPresent());
        assertTrue(analysis.getProjectedTopology().isPresent());
        assertFalse(analysis.getActionPlan().get().getActionList().contains(wastedFileAction));
    }

    /**
     * Test that Virtual volume with a corresponding Delete action is marked appropriately in the
     * projected entities list.
     */
    @Test
    public void testProjectedVolumeEntities() {
        // 2 Volumes in the topology, one with Delete action and another without
        final long volumeWithoutAction = 99999L;
        final TopologyEntityDTO deleteActionVolume = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setOid(VOLUME_ID_DELETE_ACTION)
                .build();
        final TopologyEntityDTO noDeleteActionVolume = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setOid(volumeWithoutAction)
                .build();
        final Set<TopologyEntityDTO> topologySet = ImmutableSet.of(deleteActionVolume,
                noDeleteActionVolume);

        // On Analysis execution, projected entities are populated
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR,
                MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();
        when(cloudTopology.getAllEntitiesOfType(any())).thenReturn(ImmutableList
                .of(deleteActionVolume, noDeleteActionVolume));
        final Analysis analysis = getAnalysis(analysisConfig, topologySet);
        analysis.execute();

        // Assert that only 1 volume exists in the projected entities list, the one with no
        // corresponding Delete volume action
        Assert.assertTrue(analysis.getProjectedTopology().isPresent());
        final Map<Long, ProjectedTopologyEntity> projectedEntities =
                analysis.getProjectedTopology().get();
        Assert.assertFalse(projectedEntities.isEmpty());
        Assert.assertEquals(2, projectedEntities.size());
        Assert.assertEquals(noDeleteActionVolume, projectedEntities.get(noDeleteActionVolume.getOid()).getEntity());
        Assert.assertFalse(projectedEntities.get(noDeleteActionVolume.getOid()).getDeleted());

        Assert.assertEquals(deleteActionVolume, projectedEntities.get(deleteActionVolume.getOid()).getEntity());
        Assert.assertTrue(projectedEntities.get(deleteActionVolume.getOid()).getDeleted());
    }

    /**
     * Test that Azure App Service Plans (ASPs) with a corresponding to Delete action is marked appropriately in the
     * projected entities list. This existing functionality may be extended to support more application services from
     * other cloud providers (examples of similar services: AWS ElasticBeanstalk, GCP App Engine, DO App Platform, etc).
     */
    @Test
    public void testProjectedWastedAppServicePlanEntities() {
        // 2 App Service Plans in the topology, one with Delete action and another without
        final long aspWithoutAction = 99999L;
        // Note: Azure App Service Plans will migrate from App Components to VMSPEC entity types in the future.
        final TopologyEntityDTO deleteActionASP = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setOid(APP_SERVICE_PLAN_ID_DELETE_ACTION)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                        .setCommodityType(CommodityType.newBuilder()
                                                .setType(
                                                        CommodityDTO.CommodityType.LICENSE_ACCESS.getNumber())
                                                .setKey("Linux_AppServicePlan")))).putEntityPropertyMap(
                        "PLAN_SIZE", "P2v3")
                .build();
        final TopologyEntityDTO noDeleteActionASP = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setOid(aspWithoutAction)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                        .setCommodityType(CommodityType.newBuilder()
                                                .setType(
                                                        CommodityDTO.CommodityType.LICENSE_ACCESS.getNumber())
                                                .setKey("Linux_AppServicePlan")))).putEntityPropertyMap(
                        "PLAN_SIZE", "P2v3")
                .build();
        final Set<TopologyEntityDTO> topologySet = ImmutableSet.of(deleteActionASP,
                noDeleteActionASP);

        // On Analysis execution, projected entities are populated
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR,
                        MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
                        Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();
        when(cloudTopology.getAllEntitiesOfType(any())).thenReturn(ImmutableList
                .of(deleteActionASP, noDeleteActionASP));
        final Analysis analysis = getAnalysis(analysisConfig, topologySet);
        analysis.execute();

        // Assert that only 1 ASP exists in the projected entities list, the one with no delete action
        Assert.assertTrue(analysis.getProjectedTopology().isPresent());
        final Map<Long, ProjectedTopologyEntity> projectedEntities =
                analysis.getProjectedTopology().get();
        Assert.assertFalse(projectedEntities.isEmpty());
        Assert.assertEquals(2, projectedEntities.size());
        Assert.assertEquals(noDeleteActionASP, projectedEntities.get(noDeleteActionASP.getOid()).getEntity());
        Assert.assertFalse(projectedEntities.get(noDeleteActionASP.getOid()).getDeleted());

        Assert.assertEquals(deleteActionASP, projectedEntities.get(deleteActionASP.getOid()).getEntity());
        Assert.assertTrue(projectedEntities.get(deleteActionASP.getOid()).getDeleted());
    }

    /**
     * A buyer that buys a negative amount (and therefore causes a failure).
     * @return a buyer
     */
    private TopologyEntityDTO buyer() {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(1000)
            .setOid(7)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(10)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder().setType(1).build())
                    .setUsed(-1)))
            .build(); // buyer
    }

    /**
     * Test that invoking {@link Analysis#execute} multiple times throws an exception.
     */
    @Test
    public void testTwoExecutes() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();

        final Analysis analysis  = getAnalysis(analysisConfig);
        boolean first = analysis.execute();
        boolean second = analysis.execute();
        assertTrue(first);
        assertFalse(second);
    }

    @Test
    public void testActionPlanTimestamps() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();

        final Analysis analysis  = getAnalysis(analysisConfig);

        analysis.execute();
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        assertEquals(actionPlan.getAnalysisStartTimestamp(), START_INSTANT.toEpochMilli());
        assertEquals(actionPlan.getAnalysisCompleteTimestamp(), END_INSTANT.toEpochMilli());
    }

    @Test
    public void testCreateFakeEntityForSuspensionThrottling() {
        // create a topology with two storage entities in the same storage cluster but have different
        // segmentation policy, make sure a fake VM entity will be constructed with that storage cluster comm
        // bought.
        TopologyDTO.CommoditySoldDTO storageClusterCommSold =
                        TopologyDTO.CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
                                        .setKey("StorageCluster::dummy")
                                        .build())
                                .build();
        TopologyDTO.CommoditySoldDTO segmentCommCommSold1 =
                        TopologyDTO.CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                                        .setKey("Segmentation1")
                                        .build())
                                .build();
        TopologyDTO.CommoditySoldDTO segmentCommCommSold2 =
                        TopologyDTO.CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                                        .setKey("Segmentation2")
                                        .build())
                                .build();
        final CommodityBoughtDTO topologyStAmtBought = CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                                        .build())
                                .build();
        TopologyDTO.TopologyEntityDTO dsEntity1 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .setOid(10000L)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditySoldList(storageClusterCommSold)
                .addCommoditySoldList(segmentCommCommSold1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(topologyStAmtBought)
                        .setProviderEntityType(EntityType.DISK_ARRAY_VALUE))
                .putEntityPropertyMap("dummy", "dummy")
                .build();
        TopologyDTO.TopologyEntityDTO dsEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.STORAGE_VALUE)
                        .setOid(20000L)
                        .setEntityState(EntityState.POWERED_ON)
                        .addCommoditySoldList(storageClusterCommSold)
                        .addCommoditySoldList(segmentCommCommSold2)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .addCommodityBought(topologyStAmtBought)
                                .setProviderEntityType(EntityType.DISK_ARRAY_VALUE))
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        Set<TopologyEntityDTO> topologySet = new HashSet<>();
        topologySet.add(dsEntity1);
        topologySet.add(dsEntity2);
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                    SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE,
                    false)
                .setIncludeVDC(false)
                .setRightsizeLowerWatermark(0.3f)
                .setRightsizeUpperWatermark(0.8f)
                .build();
        FakeEntityCreator fakeEntityCreator = spy(new FakeEntityCreator(mock(GroupMemberRetriever.class)));
        Analysis analysis = Mockito.spy(getAnalysis(analysisConfig, topologySet));

        Mockito.doReturn(topologySet).when(fakeEntityCreator)
                .getEntityDTOsInCluster(eq(GroupType.STORAGE_CLUSTER), eq(Maps.newHashMap()));
        Mockito.doReturn(new HashSet<>()).when(fakeEntityCreator)
        .getEntityDTOsInCluster(eq(GroupType.COMPUTE_HOST_CLUSTER), eq(Maps.newHashMap()));
        Map<Long, TopologyEntityDTO> fakeEntity = fakeEntityCreator.createFakeTopologyEntityDTOs(Maps.newHashMap(), true, false);
        assertTrue(fakeEntity.size() == 1);
        TopologyEntityDTO fakeEntityDTO = fakeEntity.values().iterator().next();
        CommodityType type = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
                .setKey("StorageCluster::dummy")
                .build();
        assertEquals(type, fakeEntityDTO.getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBought(0).getCommodityType());
    }

    /**
     * Make sure fake entities creation is invloked only for REALTIME and not for PLAN topology type.
     * @throws ExecutionException if exception is encountered by the task.
     * @throws InterruptedException if interruption is encountered on the task.
     */
    @Test
    public void testCreateFakeEntityForSuspensionThrottlingForRealtimeAndPlan()
            throws ExecutionException, InterruptedException {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                SuspensionsThrottlingConfig.CLUSTER, Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE,
                false)
                .setIncludeVDC(false)
                .setRightsizeLowerWatermark(0.3f)
                .setRightsizeUpperWatermark(0.8f)
                .build();

        TopologyInfo realtimeTopologyType = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
                .build();
        FakeEntityCreator fakeEntityCreator = mock(FakeEntityCreator.class);
        Analysis analysis = Mockito.spy(getAnalysis(analysisConfig, Collections.emptySet(), realtimeTopologyType, Optional.of(fakeEntityCreator)));
        when(cloudTopology.getEntities()).thenReturn(ImmutableMap.of());
        analysis.execute();

        // Fake Entities creation should be called once since its REALTIME.
        Mockito.verify(fakeEntityCreator, times(1)).createFakeTopologyEntityDTOs(ImmutableMap.of(), true, false);

        //Change Analysis object to plan
        TopologyInfo planTopologyType = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN)
                .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
                .build();
        reset(fakeEntityCreator);
        analysis = Mockito.spy(getAnalysis(analysisConfig, Collections.emptySet(), planTopologyType, Optional.of(fakeEntityCreator)));
        when(cloudTopology.getEntities()).thenReturn(ImmutableMap.of());

        analysis.execute();

        // Fake Entities creation should not be called since its a PLAN.
        Mockito.verify(fakeEntityCreator, times(0)).createFakeTopologyEntityDTOs(ImmutableMap.of(), true, false);
    }

    /**
     * If cost notification status is not success then Analysis execution should return false.
     *
     * @throws ExecutionException if exception is encountered by the task.
     * @throws InterruptedException if interruption is encountered on the task.
     */
    @Test
    public void testAnalysisFalseOnFailNotification() throws ExecutionException,
            InterruptedException {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR,
                MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();

        final Analysis analysis = getAnalysis(analysisConfig, Collections.emptySet(),
                TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME)
                        .build());
        final Future<CostNotification> costNotificationFuture = mock(Future.class);
        final CostNotification costNotification = CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder().setStatus(Status.FAIL).build())
                .build();
        when(costNotificationFuture.get()).thenReturn(costNotification);
        when(listener.receiveCostNotification(analysis)).thenReturn(costNotificationFuture);
        Assert.assertFalse(analysis.execute());
    }

    /**
     * If cost notification status is success then Analysis should execution return true.
     *
     * @throws ExecutionException if exception is encountered by the task.
     * @throws InterruptedException if interruption is encountered on the task.
     */
    @Test
    public void testAnalysisTrueOnSuccessNotification() throws ExecutionException,
            InterruptedException {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR,
                MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();
        final Analysis analysis = getAnalysis(analysisConfig, Collections.emptySet(),
                TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME)
                        .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
                        .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS)
                        .build());
        final Future<CostNotification> costNotificationFuture = mock(Future.class);
        final CostNotification costNotification = CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder().setStatus(Status.SUCCESS).build())
                .build();
        when(costNotificationFuture.get()).thenReturn(costNotification);
        when(listener.receiveCostNotification(analysis)).thenReturn(costNotificationFuture);
        Assert.assertTrue(analysis.execute());
    }

    /**
     *  Two VMs in the topology, one with Removed set and other not.
     *  Removed entity should not be in projected topology.
     */
    @Test
    public void testEntitiesRemoved() {
        // 2 VMs in the topology, one with Removed set and other not
        final TopologyEntityDTO removedVM = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.UNKNOWN)
                .setEdit(Edit.newBuilder()
                        .setRemoved(Removed.newBuilder().setPlanId(topologyId).build())
                        .build())
                .setOid(1L)
                .build();
        final TopologyEntityDTO vmInScope = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setOid(2L)
                .build();
        final Set<TopologyEntityDTO> topologySet = ImmutableSet.of(removedVM,
                vmInScope);

        // On Analysis execution, projected entities are populated
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR,
                MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
                Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
                .setIncludeVDC(true)
                .build();

        when(cloudTopology.getAllEntitiesOfType(any())).thenReturn(ImmutableList
                .of());

        when(csm.getScalingGroupId(any())).thenReturn(Optional.empty());

        final Analysis analysis = getAnalysis(analysisConfig, topologySet, topologyInfo);
        analysis.execute();

        // Assert that only 1 VM exists in the projected entities list.
        Assert.assertTrue(analysis.getProjectedTopology().isPresent());
        final Collection<ProjectedTopologyEntity> projectedEntities =
                analysis.getProjectedTopology().get().values();
        Assert.assertEquals(1, projectedEntities.size());
        Assert.assertEquals(vmInScope.getOid(), projectedEntities.iterator().next().getEntity().getOid());
    }

    /**
     * Test that when the hosts are over-provisioned, no move actions are generated for the VM.
     */
    @Test
    public void testNoMoveActionWithOverProvisionedHosts() {
        when(csm.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(csm.getScalingGroupUsage(any())).thenReturn(Optional.empty());

        // disable unquoted commodities
        final AnalysisConfig analysisConfig = createAnalysisConfigWithOverprovisioningSetting(false, false);

        Set<TopologyEntityDTO> topologySet = createTopologyWithOverProvisionedHosts();

        final Analysis analysis = getAnalysis(analysisConfig, topologySet, topologyInfo);
        analysis.execute();

        // assert that no move actions are generated, because both PMs have over-provisioned CPU.
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        assertFalse(actionPlan.getActionList().stream()
                .anyMatch(action -> action.getInfo().hasMove()));
    }

    /**
     * Test that when the hosts are over-provisioned, move actions are generated for the VM when OP is enabled.
     */
    @Test
    public void testMoveActionWithOverProvisionedHostsAndEnableOP() {
        when(csm.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(csm.getScalingGroupUsage(any())).thenReturn(Optional.empty());

        // disable unquoted commodities
        final AnalysisConfig analysisConfig = createAnalysisConfigWithOverprovisioningSetting(false, true);

        Set<TopologyEntityDTO> topologySet = createTopologyWithOverProvisionedHosts();

        final Analysis analysis = getAnalysis(analysisConfig, topologySet, topologyInfo);
        analysis.execute();

        // assert that no move actions are generated, because both PMs have over-provisioned CPU.
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        assertTrue(actionPlan.getActionList().stream()
                .anyMatch(action -> action.getInfo().hasMove()));
    }

    /**
     * Test that when the hosts are very over-provisioned (200%), move actions are NOT generated for the VM
     * even when OP is enabled.
     */
    @Test
    public void testNoMoveActionWithVeryOverProvisionedHostsAndEnableOP() {
        when(csm.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(csm.getScalingGroupUsage(any())).thenReturn(Optional.empty());

        // disable unquoted commodities
        final AnalysisConfig analysisConfig = createAnalysisConfigWithOverprovisioningSetting(false, true);

        Set<TopologyEntityDTO> topologySet = createTopologyWithOverProvisionedHosts(200);

        final Analysis analysis = getAnalysis(analysisConfig, topologySet, topologyInfo);
        analysis.execute();

        // assert that no move actions are generated, because both PMs have over-provisioned CPU.
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        assertFalse(actionPlan.getActionList().stream()
                .anyMatch(action -> action.getInfo().hasMove()));
    }

    /**
     * Test that when the hosts are over-provisioned and the AllowUnlimitedHostOverprovisioning is not set,
     * a move action is still generated for the VM based on the actual CPU and MEM commodities.
     */
    @Test
    public void testMoveActionWithOverProvisionedHosts() {
        when(csm.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(csm.getScalingGroupUsage(any())).thenReturn(Optional.empty());

        // enable unquoted commodities
        final AnalysisConfig analysisConfig = createAnalysisConfigWithOverprovisioningSetting(true, false);

        Set<TopologyEntityDTO> topologySet = createTopologyWithOverProvisionedHosts();

        final Analysis analysis = getAnalysis(analysisConfig, topologySet, topologyInfo);
        analysis.execute();

        // assert that move VM action from pm1 to pm2 is generated, even though both PMs
        // have over-provisioned CPU.
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        Optional<Action> moveAction = actionPlan.getActionList().stream()
                .filter(action -> action.getInfo().hasMove())
                .findAny();
        assertTrue(moveAction.isPresent());
        assertEquals(VM_ID, moveAction.get().getInfo().getMove()
                .getTarget().getId());
        assertEquals(PM1_ID, moveAction.get().getInfo().getMove()
                .getChanges(0).getSource().getId());
        assertEquals(PM2_ID, moveAction.get().getInfo().getMove()
                .getChanges(0).getDestination().getId());

    }

    /**
     * Test {@link Analysis#execute} where corresponding namespace resize action will be generated.
     */
    @Test
    public void testExecuteWithNamespaceQuotaResizing() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
            SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap(), false, LICENSE_PRICE_WEIGHT_SCALE, false)
            .setIncludeVDC(true)
            .build();
        final Analysis analysis = getAnalysis(analysisConfig);
        analysis.execute();
        assertTrue(analysis.getActionPlan().isPresent());
        assertTrue(analysis.getActionPlan().get().getActionList().contains(namespaceResizeAction));
    }

    /**
     * Test MarketAnalysisUtils.isCTAvailableForVM with
     * 1. a compute tier connected with AZs and a VM connected with one of those AZs.
     * 2. a compute tier connected with AZs and a VM is not connected with any of them.
     */
    @Test
    public void testIsCTAvailableForVM() {
        final long zone1Id = 12345L;
        final long zone2Id = 22345L;
        final long vmOid = 100001L;
        TopologyEntityDTO ct1 = TopologyEntityDTO.newBuilder().setOid(98765L)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectedEntityId(zone1Id).build())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectedEntityId(zone2Id).build()).build();
        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(vmOid).addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectedEntityId(zone1Id).build()).build();

        assertTrue(MarketAnalysisUtils.isCTAvailableForVM(ct1, vm));

        // AWS/Azure compute tiers have region as connected entities.
        final long zone3Id = 44444L;
        final long zone4Id = 55555L;
        TopologyEntityDTO ct2 = TopologyEntityDTO.newBuilder().setOid(88765L)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectedEntityId(zone3Id).build())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectedEntityId(zone4Id).build()).build();
        assertFalse(MarketAnalysisUtils.isCTAvailableForVM(ct2, vm));

    }

    /**
     * Create an{@link AnalysisConfig} with {@link GlobalSettingSpecs} setting.
     *
     * @param settingValue the setting value.
     * @return the analysis config.
     */
    private AnalysisConfig createAnalysisConfigWithOverprovisioningSetting(boolean settingValue, boolean enableOP) {
        return AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                SuspensionsThrottlingConfig.DEFAULT,
                ImmutableMap.of(GlobalSettingSpecs.AllowUnlimitedHostOverprovisioning.getSettingName(),
                        Setting.newBuilder()
                                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                        .setValue(settingValue)
                                        .build())
                                .build()), false, LICENSE_PRICE_WEIGHT_SCALE, enableOP)
                .setIncludeVDC(settingValue)
                .build();
    }

    /**
     * Create a topology with 2 PMs with over-provisioned CPU and one VM.
     *
     * @return a set of the created entities.
     */
    private Set<TopologyEntityDTO> createTopologyWithOverProvisionedHosts() {
        return createTopologyWithOverProvisionedHosts(100);
    }

    /**
     * Create a topology with 2 PMs with over-provisioned CPU and one VM.
     *
     * @return a set of the created entities.
     */
    private Set<TopologyEntityDTO> createTopologyWithOverProvisionedHosts(double cpuProvUsed) {
        TopologyDTO.TopologyEntityDTO pm1 = createCpuProvisionedPM(PM1_ID, 10, cpuProvUsed);
        TopologyDTO.TopologyEntityDTO pm2 = createCpuProvisionedPM(PM2_ID, 2, cpuProvUsed);

        // create a VM that is currently hosted on pm1.
        TopologyDTO.TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_ID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PM1_ID)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommodityBought(createCommodityBoughtDTO(
                                CommodityDTO.CommodityType.CPU_VALUE, 5))
                        .addCommodityBought(createCommodityBoughtDTO(
                                CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE, 5))
                        .addCommodityBought(createCommodityBoughtDTO(
                                CommodityDTO.CommodityType.MEM_VALUE, 2))
                        .addCommodityBought(createCommodityBoughtDTO(
                                CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE, 5))
                        .build())
                .build();

        return ImmutableSet.of(pm1, pm2, vm);
    }

    /**
     * Create a PM with over-provisioned CPU.
     *
     * @param oid the oid of the PM.
     * @param usedCPU the used CPU value.
     * @param usedCPUProv the used CPU Provisioned.
     * @return the PM entity.
     */
    private TopologyDTO.TopologyEntityDTO createCpuProvisionedPM(long oid, double usedCPU, double usedCPUProv) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(oid)
                .addCommoditySoldList(createCommoditySoldDTO(
                        CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE, usedCPUProv, 100))
                .addCommoditySoldList(createCommoditySoldDTO(
                        CommodityDTO.CommodityType.CPU_VALUE, usedCPU, 10))
                .addCommoditySoldList(createCommoditySoldDTO(
                        CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE, 50, 100))
                .addCommoditySoldList(createCommoditySoldDTO(
                        CommodityDTO.CommodityType.MEM_VALUE, 5, 10))
                .build();
    }

    /**
     * Create a {@link CommoditySoldDTO}.
     *
     * @param commodityType the commodity type
     * @param used the used value
     * @param capacity the capacity value
     * @return sold commodity
     */
    private CommoditySoldDTO createCommoditySoldDTO(int commodityType, double used, double capacity) {
        return CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType)
                        .build())
                .setUsed(used)
                .setCapacity(capacity)
                .build();
    }

    /**
     * Create a {@link CommodityBoughtDTO}.
     *
     * @param commodityType the commodity type
     * @param used the used value
     * @return bought commodity
     */
    private CommodityBoughtDTO createCommodityBoughtDTO(int commodityType, double used) {
        return CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType)
                        .build())
                .setUsed(used)
                .build();
    }

}
