package com.vmturbo.topology.processor.topology;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.createGraph;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;

/**
 * Test editing of historical values of commodities.
 */
public class HistoricalEditorTest {

    private static final double FLOATING_POINT_DELTA = 0.01;

    private HistoricalUtilizationDatabase historicalUtilizationDatabase =
        mock(HistoricalUtilizationDatabase.class);

    private ExecutorService executorService = mock(ExecutorService.class);

    private HistoricalEditor historicalEditor =
        new HistoricalEditor(historicalUtilizationDatabase, executorService);

    /**
     * Apply historical editor. historicalUsed and historicalPeak are updated.
     */
    @Test
    public void testApplyCommodityEdits() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build();

        TopologyGraph<TopologyEntity> graph = createGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(10,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(10, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        graph = createGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);
        // Second time apply historicalEditor. Match the new commodity with the previous one.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }

    /**
     * Apply historical editor. historicalUsed and historicalPeak are updated.
     */
    @Test
    public void testApplyCommodityEditsRealtimeThenPlan() {
        TopologyInfo realTimeInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build();

        TopologyGraph<TopologyEntity> graph = createGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(10,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(10, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Recreate Graph for plan
        graph = createGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);

        TopologyInfo planTopologyInfo = TopologyInfo.newBuilder().setPlanInfo(
                PlanTopologyInfo.newBuilder().setPlanProjectType(PlanProjectType.USER)).build();

        // Second time apply historicalEditor : with Plan. Map initialized with above call and
        // only entity values are updated but map values remain same as before.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), planTopologyInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Third time apply historicalEditor with realtime : values will be calculated to same
        // (because underlying map was not updated in previous cycle).
        // In current cycle map values should change because replace will be successful.
        graph = createGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);

        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Fourth time apply historicalEditor with realtime : values will be updated for weighted avg of previous 3 times
        // (because map was in previous cycle).
        // In current cycle map values should change because replace will be successful.
        graph = createGraph(CommodityDTO.CommodityType.MEM, 40, 400, 30, 300);

        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(30,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(300,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 15 + 0.5 * 30 = 22.5
        Assert.assertEquals(22.5,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 101 + 0.01 * 300 = 102.99
        Assert.assertEquals(102.99,
                vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(40, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(400, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 15 + 0.5 * 40 = 27.5
        Assert.assertEquals(27.5, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 101 + 0.01 * 400 = 103.99
        Assert.assertEquals(103.99, pm.getTopologyEntityDtoBuilder()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }

    /**
     * Don't allow historical editor to update commodities for headroom plan.
     */
    @Test
    public void testApplyCommodityEditsForClusterHeadroomPlan() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(
            PlanTopologyInfo.newBuilder().setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM)).build();

        testApplyCommodityEditsNoUpdate(Collections.emptyList(), topologyInfo);
    }

    /**
     * Don't allow historical editor to update commodities for historicalBaseline plan.
     */
    @Test
    public void testApplyCommodityEditsForHistoricalBaselinePlan() {
        List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder().setHistoricalBaseline(
                HistoricalBaseline.newBuilder().setBaselineDate(1)))
            .build());

        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(
            PlanTopologyInfo.newBuilder().setPlanType(PlanProjectType.USER.name())).build();

        testApplyCommodityEditsNoUpdate(changes, topologyInfo);
    }

    /**
     * Test {@link HistoricalEditor#copyHistoricalValuesToClonedEntities(TopologyGraph, List)}.
     */
    @Test
    public void testCopyHistoricalValuesToClonedEntities() {
        final ImmutableList.Builder<CommoditySoldDTO> commSoldList = ImmutableList.builder();
        commSoldList.add(CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VMEM.getNumber()).setKey("").build())
            .setHistoricalUsed(HistoricalValues.newBuilder().setMaxQuantity(1).setHistUtilization(2)
                .addTimeSlot(3).setPercentile(4))
            .setHistoricalPeak(HistoricalValues.newBuilder().setMaxQuantity(5).setHistUtilization(6)
                .addTimeSlot(7).setPercentile(8))
            .build());
        commSoldList.add(CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU.getNumber()).setKey("").build())
            .setHistoricalUsed(HistoricalValues.newBuilder().setMaxQuantity(10).setHistUtilization(20)
                .addTimeSlot(30).setPercentile(40))
            .setHistoricalPeak(HistoricalValues.newBuilder().setMaxQuantity(50).setHistUtilization(60)
                .addTimeSlot(70).setPercentile(80))
            .build());

        final ImmutableList.Builder<CommoditiesBoughtFromProvider> commBoughtList = ImmutableList.builder();
        commBoughtList.add(CommoditiesBoughtFromProvider.newBuilder().setProviderId(1000L)
            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                TopologyDTO.CommodityType.newBuilder().setType(CommodityType.MEM.getNumber()).setKey("").build())
                .setHistoricalUsed(HistoricalValues.newBuilder().setMaxQuantity(-1).setHistUtilization(-2)
                    .addTimeSlot(-3).setPercentile(-4))
                .setHistoricalPeak(HistoricalValues.newBuilder().setMaxQuantity(-5).setHistUtilization(-6)
                    .addTimeSlot(-7).setPercentile(-8)))
            .build());
        commBoughtList.add(CommoditiesBoughtFromProvider.newBuilder().setProviderId(1001L)
            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                TopologyDTO.CommodityType.newBuilder().setType(CommodityType.STORAGE_AMOUNT.getNumber()).setKey("").build())
                .setHistoricalUsed(HistoricalValues.newBuilder().setMaxQuantity(-10).setHistUtilization(-20)
                    .addTimeSlot(-30).setPercentile(-40))
                .setHistoricalPeak(HistoricalValues.newBuilder().setMaxQuantity(-50).setHistUtilization(-60)
                    .addTimeSlot(-70).setPercentile(-80)))
            .build());

        final TopologyEntity.Builder origin = buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues(100L,
            HistoricalValues.newBuilder().setMaxQuantity(1).setHistUtilization(2).addTimeSlot(3).setPercentile(4).build(),
            HistoricalValues.newBuilder().setMaxQuantity(5).setHistUtilization(6).addTimeSlot(7).setPercentile(8).build(),
            HistoricalValues.newBuilder().setMaxQuantity(10).setHistUtilization(20).addTimeSlot(30).setPercentile(40).build(),
            HistoricalValues.newBuilder().setMaxQuantity(50).setHistUtilization(60).addTimeSlot(70).setPercentile(80).build(),
            101L, HistoricalValues.newBuilder().setMaxQuantity(-1).setHistUtilization(-2).addTimeSlot(-3).setPercentile(-4).build(),
            HistoricalValues.newBuilder().setMaxQuantity(-5).setHistUtilization(-6).addTimeSlot(-7).setPercentile(-8).build(),
            102L, HistoricalValues.newBuilder().setMaxQuantity(-10).setHistUtilization(-20).addTimeSlot(-30).setPercentile(-40).build(),
            HistoricalValues.newBuilder().setMaxQuantity(-50).setHistUtilization(-60).addTimeSlot(-70).setPercentile(-80).build());

        final TopologyEntity.Builder clone = buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues(200L,
            HistoricalValues.getDefaultInstance(), HistoricalValues.getDefaultInstance(),
            HistoricalValues.getDefaultInstance(), HistoricalValues.getDefaultInstance(),
            -1L, HistoricalValues.getDefaultInstance(), HistoricalValues.getDefaultInstance(),
            -2L, HistoricalValues.getDefaultInstance(), HistoricalValues.getDefaultInstance())
            .setClonedFromEntityOid(100L);
        final Map<Long, Long> oldProvidersMap = ImmutableMap.of(-1L, 101L, -2L, 102L);
        clone.getEntityBuilder().putEntityPropertyMap("oldProviders", new Gson().toJson(oldProvidersMap));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
            ImmutableMap.of(origin.getOid(), origin, clone.getOid(), clone));

        historicalEditor.copyHistoricalValuesToClonedEntities(graph,
            ImmutableList.of(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.getDefaultInstance())
                .build()));

        for (int i = 0; i <= 1; i++) {
            final CommoditySoldDTO.Builder originalCommoditySold =
                origin.getEntityBuilder().getCommoditySoldListBuilder(i);
            final CommoditySoldDTO.Builder clonedCommoditySold =
                origin.getEntityBuilder().getCommoditySoldListBuilder(i);
            checkHistoricalValues(originalCommoditySold.getHistoricalUsedBuilder(),
                clonedCommoditySold.getHistoricalUsedBuilder());
            checkHistoricalValues(originalCommoditySold.getHistoricalPeakBuilder(),
                clonedCommoditySold.getHistoricalPeakBuilder());
        }

        final Map<Long, CommoditiesBoughtFromProvider.Builder> originalProviderOidToCommBought =
            origin.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().stream()
                .collect(Collectors.toMap(CommoditiesBoughtFromProvider.Builder::getProviderId, Function.identity()));
        final Map<Long, CommoditiesBoughtFromProvider.Builder> clonedProviderOidToCommBought =
            clone.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().stream()
                .collect(Collectors.toMap(CommoditiesBoughtFromProvider.Builder::getProviderId, Function.identity()));

        for (Entry<Long, Long> entry : oldProvidersMap.entrySet()) {
            final CommodityBoughtDTO.Builder originalCommodityBought =
                originalProviderOidToCommBought.get(entry.getValue()).getCommodityBoughtBuilder(0);
            final CommodityBoughtDTO.Builder clonedCommodityBought =
                clonedProviderOidToCommBought.get(entry.getKey()).getCommodityBoughtBuilder(0);
            checkHistoricalValues(originalCommodityBought.getHistoricalUsedBuilder(),
                clonedCommodityBought.getHistoricalUsedBuilder());
            checkHistoricalValues(originalCommodityBought.getHistoricalPeakBuilder(),
                clonedCommodityBought.getHistoricalPeakBuilder());
        }
    }

    /**
     * Check historical values.
     *
     * @param origin original historical values
     * @param clone cloned historical values
     */
    private void checkHistoricalValues(final HistoricalValues.Builder origin,
                                       final HistoricalValues.Builder clone) {
        Assert.assertEquals(origin.getMaxQuantity(), clone.getMaxQuantity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(origin.getHistUtilization(), clone.getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(origin.getTimeSlotList(), clone.getTimeSlotList());
        Assert.assertEquals(origin.getPercentile(), clone.getPercentile(), FLOATING_POINT_DELTA);
    }

    /**
     * Don't allow historical editor to update commodities.
     *
     * @param changes to iterate over and find relevant changes (e.g baseline change)
     * @param topologyInfo to identify if it is a cluster headroom plan
     */
    private void testApplyCommodityEditsNoUpdate(@Nonnull final List<ScenarioChange> changes,
                                                 @Nonnull final TopologyInfo topologyInfo) {
        TopologyGraph<TopologyEntity> graph = createGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        historicalEditor.applyCommodityEdits(graph, changes, topologyInfo);

        graph = createGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);
        historicalEditor.applyCommodityEdits(graph, changes, topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // All values are not changed.
        Assert.assertEquals(20,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        Assert.assertEquals(0,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        Assert.assertEquals(20, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }
}
