package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.stats.Stats.CommodityMaxValue;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;

/**
 * Tests for SetCommodityMaxQuantityPostStitchingOperation.
 */
public class SetCommodityMaxQuantityPostStitchingOperationTest {

    private static final TopologyDTO.CommodityType VCPU_NO_KEY = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE).build();
    private static final TopologyDTO.CommodityType VCPU_EMPTY_KEY = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE).setKey("").build();
    private static final long maxValuesBackgroundLoadFreqMins = 1;
    private static final double DELTA = 1e-5;

    private final StatsMoles.StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsMoles.StatsHistoryServiceMole());
    private SetCommodityMaxQuantityPostStitchingOperation setMaxOperation;
    private TopologicalChangelog.EntityChangesBuilder<TopologyEntity> resultBuilder;
    private ScheduledExecutorService mockExecutorService;
    private IStitchingJournal<TopologyEntity> journal;

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceMole);

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
                        statsHistoryServiceBlockingStub, maxValuesBackgroundLoadFreqMins);
        setMaxOperation = new SetCommodityMaxQuantityPostStitchingOperation(config);
        setMaxOperation.setBackgroundStatsLoadingExecutor(mockExecutorService);
        resultBuilder = new PostStitchingTestUtilities.UnitTestResultBuilder();
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
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        verify(mockExecutorService, times(1)).scheduleWithFixedDelay(any(), eq(0L), eq(maxValuesBackgroundLoadFreqMins), eq(TimeUnit.MINUTES));
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
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
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(100, vm1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);

        //broadcast 2
        vm1.getTopologyEntityDtoBuilder().getCommoditySoldListBuilder(0).setUsed(99);
        cont1.getTopologyEntityDtoBuilder().getCommoditySoldListBuilder(0).setUsed(299);
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        // remains unchanged because the new used of 99 and 299 are less than current max
        assertEquals(100, vm1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);

        //broadcast 3
        vm1.getTopologyEntityDtoBuilder().getCommoditySoldListBuilder(0).setUsed(101);
        cont1.getTopologyEntityDtoBuilder().getCommoditySoldListBuilder(0).setUsed(301);
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        // changes because the new used of 101 and 301 are greater than current max
        assertEquals(101, vm1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(301, cont1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
    }

    /**
     * Test background task.
     * 1. Do a broadcast to set the oids.
     * 2. Run the background task to fetch max from grpc call.
     * 3. Do next broadcast - this should reflect the new max brought from the task.
     */
    @Test
    public void testBackgroundTask() {
        // First do a broadcast so that we have oids which we want to query for
        TopologyEntity vm1 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        TopologyEntity vm2 = topologyEntity(EntityType.VIRTUAL_MACHINE_VALUE, 2L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 100)));
        TopologyEntity cont1 = topologyEntity(EntityType.CONTAINER_VALUE, 101L, ImmutableList.of(commoditySoldDTO(VCPU_NO_KEY, 300)));
        List<TopologyEntity> entities = ImmutableList.of(vm1, vm2, cont1);
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(100, vm1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(100, vm2.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);

        // Setup the response from GRPC call
        List<EntityCommoditiesMaxValues> response = ImmutableList.of(
                entityCommoditiesMaxValues(1L, commodityMaxValue(VCPU_EMPTY_KEY, 101)),
                entityCommoditiesMaxValues(2L, commodityMaxValue(VCPU_EMPTY_KEY, 102)));
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
        setMaxOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(101, vm1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(102, vm2.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
        assertEquals(300, cont1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getMaxQuantity(), DELTA);
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

    private TopologyEntity topologyEntity(int entityType, long oid, List<CommoditySoldDTO> commsSold) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commsSold))
                .build();
    }

    private CommoditySoldDTO commoditySoldDTO(TopologyDTO.CommodityType commodityType, double used) {
        return CommoditySoldDTO.newBuilder().setCommodityType(commodityType).setUsed(used).build();
    }
}
