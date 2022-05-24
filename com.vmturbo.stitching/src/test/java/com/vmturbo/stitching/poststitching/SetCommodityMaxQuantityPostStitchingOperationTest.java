package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.stats.Stats.CommodityMaxValue;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests for SetCommodityMaxQuantityPostStitchingOperation.
 */
public class SetCommodityMaxQuantityPostStitchingOperationTest {

    private static final CommodityTypeView VCPU_NO_KEY = new CommodityTypeImpl().setType(CommodityType.VCPU_VALUE);
    private static final CommodityTypeView VCPU_EMPTY_KEY = new CommodityTypeImpl().setType(CommodityType.VCPU_VALUE).setKey("");
    private static final long maxValuesBackgroundLoadFreqMins = 1;
    private static final double DELTA = 1e-5;

    private final StatsMoles.StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsMoles.StatsHistoryServiceMole());
    private SetCommodityMaxQuantityPostStitchingOperation setMaxOperation;
    private ScheduledExecutorService mockExecutorService;
    private IStitchingJournal<TopologyEntity> journal;

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceMole);

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsHistoryServiceBlockingStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        mockExecutorService = mock(ScheduledExecutorService.class);
        com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig config =
                new com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig(
                        statsHistoryServiceBlockingStub, maxValuesBackgroundLoadFreqMins, 3);
        setMaxOperation = new SetCommodityMaxQuantityPostStitchingOperation(config);
        setMaxOperation.setBackgroundStatsLoadingExecutor(mockExecutorService);
        journal = (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);
        SettingProto.Setting setting = SettingProto.Setting.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                .setNumericSettingValue(SettingProto.NumericSettingValue.newBuilder().setValue(60).build())
                .build();
        when(statsHistoryServiceMole.getStatsDataRetentionSettings(any())).thenReturn(Collections.singletonList(setting));
    }

    /**
     * Test that we only schedule the background task on the first broadcast, and not after that.
     */
    @Test
    public void testOnlyFirstBroadcastSchedulesBackgroundTask() {
        List<TopologyEntity> entities = new ArrayList<>();
        broadcast(entities);
        verify(mockExecutorService, times(1)).scheduleWithFixedDelay(any(), eq(0L), eq(maxValuesBackgroundLoadFreqMins), eq(TimeUnit.MINUTES));
        broadcast(entities);
        verifyNoMoreInteractions(mockExecutorService);
    }

    /**
     * Test that max is updated when the used changes with each broadcast.
     */
    @Test
    public void testMaxChangesWithBroadcasts() {
        //broadcast 1
        TopologyEntity vm1 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        TopologyEntity cont1 = topologyEntity(EntityType.CONTAINER_VALUE, 101L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 300)));
        List<TopologyEntity> entities = ImmutableList.of(vm1, cont1);
        broadcast(entities);
        assertEquals(100, vm1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);

        //broadcast 2
        vm1.getTopologyEntityImpl().getCommoditySoldListImpl(0).setUsed(99);
        cont1.getTopologyEntityImpl().getCommoditySoldListImpl(0).setUsed(299);
        broadcast(entities);
        // remains unchanged because the new used of 99 and 299 are less than current max
        assertEquals(100, vm1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);

        //broadcast 3
        vm1.getTopologyEntityImpl().getCommoditySoldListImpl(0).setUsed(101);
        cont1.getTopologyEntityImpl().getCommoditySoldListImpl(0).setUsed(301);
        broadcast(entities);
        // changes because the new used of 101 and 301 are greater than current max
        assertEquals(101, vm1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(301, cont1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
    }

    /**
     * Test background task.
     * 1. Do a broadcast to set the oids. The resizable flags should be false after the broadcast since max query did not finish.
     * 2. Run the background task to fetch max from grpc call.
     * 3. Do next broadcast - this should reflect the new max brought from the task. The resizable flags should be
     * true after the broadcast since max query has finished.
     */
    @Test
    public void testBackgroundTask() {
        // First do a broadcast so that we have oids which we want to query for
        TopologyEntity vm1 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        TopologyEntity vm2 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 2L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        TopologyEntity cont1 = topologyEntity(EntityType.CONTAINER_VALUE, 101L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 300)));
        List<TopologyEntity> entities = ImmutableList.of(vm1, vm2, cont1);
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(100, vm1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(100, vm2.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale()
                || vm2.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm2.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale()
                || cont1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || cont1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Setup the response from GRPC call
        List<EntityCommoditiesMaxValues> response = ImmutableList.of(
                entityCommoditiesMaxValues(1L, commodityMaxValue(VCPU_EMPTY_KEY.toProto(), 101)),
                entityCommoditiesMaxValues(2L, commodityMaxValue(VCPU_EMPTY_KEY.toProto(), 102)));
        when(statsHistoryServiceMole.getEntityCommoditiesMaxValues(any())).thenReturn(response);

        // Run the background task. This will fetch new higher max values for VMs.
        // Wait for task to complete.
        SetCommodityMaxQuantityPostStitchingOperation.LoadMaxValuesFromDBTask dbTask = setMaxOperation.new LoadMaxValuesFromDBTask();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(dbTask, 0, TimeUnit.MINUTES);
        awaitTerminationAfterShutdown(executor);

        // Capture the actual request made during GRPC call
        final ArgumentCaptor<GetEntityCommoditiesMaxValuesRequest> requestCaptor =
                ArgumentCaptor.forClass(GetEntityCommoditiesMaxValuesRequest.class);
        verify(statsHistoryServiceMole).getEntityCommoditiesMaxValues(requestCaptor.capture());
        verify(statsHistoryServiceMole, times(1)).getEntityCommoditiesMaxValues(any());
        GetEntityCommoditiesMaxValuesRequest actualRequest = requestCaptor.getValue();
        assertEquals(ImmutableSet.of(1L, 2L), new HashSet<>(actualRequest.getUuidsList()));

        // Do second broadcast to set the new max
        // Start with analysis flags as true
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(101, vm1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(102, vm2.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityImpl().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(true, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                && vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale()
                && vm2.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                && vm2.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale()
                && cont1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                && cont1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());
    }

    /**
     * Do 4 broadcasts. Don't run the background task.
     * For 3 broadcasts, the resizable flags should be false. For the 4th broadcast, the resizable flags should be
     * true even if background task did not complete.
     */
    @Test
    public void testMultipleBroadcastsWithoutMaxQueryCompletion() {
        // First do a broadcast so that we have oids which we want to query for
        TopologyEntity vm1 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        List<TopologyEntity> entities = ImmutableList.of(vm1);
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do second broadcast. Start with analysis flags as true
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do third broadcast. Start with analysis flags as true
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do fourth broadcast. Start with analysis flags as true. This time, the analysis flags will remain true.
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(true, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                && vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());
    }

    /**
     * Set DISABLE_MAX_QUERY to true.
     * Stop the background task from running, and InitialMaxQueryCompleted &
     * MaxValuesBackgroundTaskScheduled from being set to true.
     * Do 4 broadcasts. For 3 broadcasts, the resizable flags should be false.
     * For the 4th broadcast, the resizable flags should be
     * true even if background task did not complete.
     * InitialMaxQueryCompleted & MaxValuesBackgroundTaskScheduled should still be false.
     */
    @Test
    public void testMultipleBroadcastsWithDisableMaxQueryEnabled() {
        featureFlagTestRule.enable(FeatureFlags.DISABLE_MAX_QUERY);

        // First do a broadcast so that we have oids which we want to query for
        TopologyEntity vm1 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        List<TopologyEntity> entities = ImmutableList.of(vm1);
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do second broadcast. Start with analysis flags as true
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do third broadcast. Start with analysis flags as true
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(false, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                || vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        // Do fourth broadcast. Start with analysis flags as true. This time, the analysis flags will remain true.
        setAnalysisFlags(entities);
        broadcast(entities);
        assertEquals(true, vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForResizeDown()
                && vm1.getTopologyEntityImpl().getAnalysisSettings().getIsEligibleForScale());

        assertFalse(setMaxOperation.getInitialMaxQueryCompleted());
        assertFalse(setMaxOperation.getMaxValuesBackgroundTaskScheduled());
    }

    /**
     * Test that SetCommodityMaxQuantityPostStitchingOperation is performed after
     * SetResizeDownAnalysisSettingPostStitchingOperation. If someone changes the order in the future, this test will fail
     * and catch this.
     */
    @Test
    public void testOrderOfOperations() {
        final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceClient =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        PostStitchingOperationLibrary postStitchingOperationLibrary =
                new PostStitchingOperationLibrary(
                        new com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig(
                                statsServiceClient, 30, 0), //meaningless values
                        Mockito.mock(DiskCapacityCalculator.class), mock(CpuCapacityStore.class),  Mockito.mock(Clock.class), 0,
                        mock(SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache.class));
        List<PostStitchingOperation> postStitchingOperations = postStitchingOperationLibrary.getPostStitchingOperations();
        List<String> operations = postStitchingOperations.stream().map(op -> op.getClass().getSimpleName()).collect(Collectors.toList());
        int operation1Index = operations.indexOf(SetResizeDownAnalysisSettingPostStitchingOperation.class.getSimpleName());
        int operation2Index = operations.indexOf(SetCommodityMaxQuantityPostStitchingOperation.class.getSimpleName());
        assertTrue(operation1Index < operation2Index);
    }

    private void broadcast(List<TopologyEntity> entities) {
        EntityChangesBuilder<TopologyEntity> resultBuilder = new PostStitchingTestUtilities.UnitTestResultBuilder();
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
    }

    private void setAnalysisFlags(List<TopologyEntity> entities) {
        for (TopologyEntity entity : entities) {
            entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().setIsEligibleForResizeDown(true);
            entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().setIsEligibleForScale(true);
        }
    }

    private void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private EntityCommoditiesMaxValues entityCommoditiesMaxValues(long oid, CommodityMaxValue... commodityMaxValues) {
        return EntityCommoditiesMaxValues.newBuilder()
                .setOid(oid).addAllCommodityMaxValues(Arrays.asList(commodityMaxValues)).build();
    }

    private CommodityMaxValue commodityMaxValue(TopologyDTO.CommodityType commType, double max) {
        return CommodityMaxValue.newBuilder().setCommodityType(commType).setMaxValue(max).build();
    }

    private TopologyEntity topologyEntity(int entityType, long oid, List<CommoditySoldView> commsSold) {
        return TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commsSold))
                .build();
    }

    private CommoditySoldView commoditySoldDTO(CommodityTypeView commodityType, double used) {
        return new CommoditySoldImpl().setCommodityType(commodityType).setUsed(used);
    }
}
