package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

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
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit tests for {@link Analysis}.
 */
public class AnalysisTest {

    private long topologyContextId = 1111;
    private long topologyId = 2222;
    private static final float DEFAULT_RATE_OF_RESIZE = 2.0f;
    private TopologyType topologyType = TopologyType.PLAN;
    private AnalysisRICoverageListener listener;


    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setTopologyType(topologyType)
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS)
            .addAnalysisType(AnalysisType.WASTED_FILES)
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
    private static final long VOLUME_ID_DELETE_ACTION = 1111L;
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

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
            mock(BuyRIImpactAnalysisFactory.class);
    private BuyRIImpactAnalysis buyRIImpactAnalysis = mock(BuyRIImpactAnalysis.class);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
                     testSettingPolicyService);

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
            .thenReturn(mock(ConsistentScalingHelper.class));
        when(buyRIImpactAnalysisFactory.createAnalysis(eq(topologyInfo), any(), any(), any()))
                .thenReturn(buyRIImpactAnalysis);
    }

    private Map<String, Setting> getRateOfResizeSettingMap(float resizeValue) {
        return ImmutableMap.of(GlobalSettingSpecs.RateOfResize.getSettingName(), Setting.newBuilder()
            .setSettingSpecName(GlobalSettingSpecs.RateOfResize.getSettingName())
            .setNumericSettingValue(SettingDTOUtil.createNumericSettingValue(resizeValue))
            .build());
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
        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
        when(priceTableFactory.newPriceTable(any(), eq(CloudCostData.empty()))).thenReturn(mock(MarketPriceTable.class));
        when(cloudCostCalculatorFactory.newCalculator(topoInfo, cloudTopology)).thenReturn(cloudCostCalculator);
        final long vmOid = 123L;
        final TopologyEntityDTO cloudVm = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD).setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        when(cloudTopology.getEntities()).thenReturn(ImmutableMap.of(vmOid, cloudVm));
        when(cloudTopologyFactory.newCloudTopology(anyLong(), any())).thenReturn(cloudTopology);
        when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(cloudTopology);
        final WastedFilesAnalysisFactory wastedFilesAnalysisFactory =
            mock(WastedFilesAnalysisFactory.class);
        final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
                mock(BuyRIImpactAnalysisFactory.class);
        final WastedFilesAnalysis wastedFilesAnalysis = mock(WastedFilesAnalysis.class);
        when(wastedFilesAnalysisFactory.newWastedFilesAnalysis(any(),any(), any(), any(), any()))
                .thenReturn(wastedFilesAnalysis);
        when(wastedFilesAnalysis.getState()).thenReturn(AnalysisState.SUCCEEDED);
        when(wastedFilesAnalysis.getActions())
                .thenReturn(Collections.singletonList(wastedFileAction));
        return new Analysis(topoInfo, topologySet,
            new GroupMemberRetriever(groupServiceClient), mockClock, analysisConfig,
            cloudTopologyFactory, cloudCostCalculatorFactory, priceTableFactory,
            wastedFilesAnalysisFactory, buyRIImpactAnalysisFactory, tierExcluderFactory,
                listener, consistentScalingHelperFactory);
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
                    getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
            getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
    }

    /**
     * Test execution of Analysis where TopologyInfo indicates not to run wastedFilesAnalysis.
     */
    @Test
    public void testExecuteNoWastedFiles() {
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
            SuspensionsThrottlingConfig.DEFAULT,
            getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
     * Test the {@link Analysis#execute} method for a failed run.
     */
    @Test
    public void testFailedAnalysis() {
        Set<TopologyEntityDTO> set = Sets.newHashSet(buyer());
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(QUOTE_FACTOR, MOVE_COST_FACTOR,
                    SuspensionsThrottlingConfig.DEFAULT,
                    // RateOfResize negative to throw exception
                    getRateOfResizeSettingMap(-1))
                .setIncludeVDC(true)
                .build();

        final Analysis analysis  = getAnalysis(analysisConfig);
        analysis.execute();
        assertFalse(analysis.isDone());
        assertSame(AnalysisState.FAILED, analysis.getState());
        assertNotNull(analysis.getErrorMsg());

        assertFalse(analysis.getActionPlan().isPresent());
        assertFalse(analysis.getProjectedTopology().isPresent());
    }

    /**
     * Test that Virtual volume with a corresponding Delete action is not present in the
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
                getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
                .setIncludeVDC(true)
                .build();
        when(cloudTopology.getAllEntitiesOfType(any())).thenReturn(ImmutableList
                .of(deleteActionVolume, noDeleteActionVolume));
        final Analysis analysis = getAnalysis(analysisConfig, topologySet);
        analysis.execute();

        // Assert that only 1 volume exists in the projected entities list, the one with no
        // corresponding Delete volume action
        Assert.assertTrue(analysis.getProjectedTopology().isPresent());
        final Collection<ProjectedTopologyEntity> projectedEntities =
                analysis.getProjectedTopology().get();
        Assert.assertFalse(projectedEntities.isEmpty());
        Assert.assertEquals(1, projectedEntities.size());
        Assert.assertEquals(noDeleteActionVolume, projectedEntities.iterator().next().getEntity());
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
                getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
                getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
                    SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap())
                .setIncludeVDC(false)
                .setRightsizeLowerWatermark(0.3f)
                .setRightsizeUpperWatermark(0.8f)
                .build();

        Analysis analysis = Mockito.spy(getAnalysis(analysisConfig, topologySet));

        Mockito.doReturn(topologySet).when(analysis)
                .getEntityDTOsInCluster(eq(GroupType.STORAGE_CLUSTER));
        Mockito.doReturn(new HashSet<>()).when(analysis)
        .getEntityDTOsInCluster(eq(GroupType.COMPUTE_HOST_CLUSTER));
        Map<Long, TopologyEntityDTO> fakeEntity = analysis.createFakeTopologyEntityDTOsForSuspensionThrottling();
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
                getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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
                getRateOfResizeSettingMap(DEFAULT_RATE_OF_RESIZE))
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

}
