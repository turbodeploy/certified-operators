package com.vmturbo.topology.processor.topology;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.createTopologyGraph;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.historical.HistoricalCommodityInfo;
import com.vmturbo.topology.processor.historical.HistoricalServiceEntityInfo;
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
        TopologyDTO.TopologyInfo topologyInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME).build();

        TopologyGraph<TopologyEntity> graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(10,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(10, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);
        // Second time apply historicalEditor. Match the new commodity with the previous one.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }

    /**
     * Apply historical editor with 0 initial values for real time.
     */
    @Test
    public void testApplyCommodityEditsWith0InitialValuesForRealTime() {
        testApplyCommodityEditsWith0InitialValues(TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME).build());
    }

    /**
     * Apply historical editor with 0 initial values for plan.
     */
    @Test
    public void testApplyCommodityEditsWith0InitialValuesForPlan() {
        testApplyCommodityEditsWith0InitialValues(TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN).build());
    }

    /**
     * Test when commodity bought and commodity sold has used and peak set versus unset.
     */
    @Test
    public void testCommoditySoldAndBoughtWithUsedUnset() {
        HistoricalCommodityInfo commSold1 = new HistoricalCommodityInfo();
        commSold1.setCommodityTypeAndKey(new TopologyPOJO.CommodityTypeImpl().setType(CommodityType.CPU_PROVISIONED_VALUE));
        commSold1.setHistoricalUsed(-1.0f);
        commSold1.setHistoricalPeak(-1.0f);
        commSold1.setSourceId(-1);
        commSold1.setMatched(false);
        commSold1.setUpdated(false);
        HistoricalCommodityInfo commSold2 = new HistoricalCommodityInfo();
        commSold2.setCommodityTypeAndKey(new TopologyPOJO.CommodityTypeImpl().setType(CommodityType.CPU_VALUE));
        commSold2.setHistoricalUsed(-1.0f);
        commSold2.setHistoricalPeak(-1.0f);
        commSold2.setSourceId(-1);
        commSold2.setMatched(false);
        commSold2.setUpdated(false);
        HistoricalServiceEntityInfo hisSE = new HistoricalServiceEntityInfo();
        hisSE.addHistoricalCommoditySold(commSold2);
        HistoricalCommodityInfo commBought1 = new HistoricalCommodityInfo();
        commBought1.setCommodityTypeAndKey(new TopologyPOJO.CommodityTypeImpl().setType(CommodityType.POWER_VALUE));
        commBought1.setHistoricalUsed(-1.0f);
        commBought1.setHistoricalPeak(-1.0f);
        commBought1.setSourceId(-1);
        commSold1.setMatched(false);
        commSold1.setUpdated(false);
        hisSE.addHistoricalCommodityBought(commBought1);
        HistoricalCommodityInfo commBought2 = new HistoricalCommodityInfo();
        commBought2.setCommodityTypeAndKey(new TopologyPOJO.CommodityTypeImpl().setType(CommodityType.SPACE_VALUE));
        commBought2.setHistoricalUsed(-1.0f);
        commBought2.setHistoricalPeak(-1.0f);
        commBought2.setSourceId(-1);
        commSold2.setMatched(false);
        commSold2.setUpdated(false);
        hisSE.addHistoricalCommodityBought(commBought2);

        long entityOid = 1000L;
        historicalEditor.historicalInfo.put(entityOid, hisSE);

        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        // Two commodities sold are created. The cpu provisioned comm has no used and peak value,
        // while the cpu has used and peak value set.
        final ImmutableList.Builder<CommoditySoldImpl> commSoldList = ImmutableList.builder();
        CommoditySoldImpl commoditySoldBuilder1 = new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityType.CPU_PROVISIONED_VALUE))
                .setActive(true);
        CommoditySoldImpl commoditySoldBuilder2 = new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(CommodityType.CPU_VALUE))
                .setActive(true)
                .setUsed(10)
                .setPeak(10);
        commSoldList.add(commoditySoldBuilder1);
        commSoldList.add(commoditySoldBuilder2);
        // Two commodities bought are created. The space comm has no used and peak value,
        // while the power has used and peak value set.
        CommoditiesBoughtFromProviderImpl commBoughtPrd = new CommoditiesBoughtFromProviderImpl()
                .addCommodityBought(new CommodityBoughtImpl().setUsed(20).setPeak(20).setActive(true)
                        .setCommodityType(new CommodityTypeImpl().setType(CommodityType.POWER_VALUE)))
                .addCommodityBought(new CommodityBoughtImpl().setActive(true)
                        .setCommodityType(new CommodityTypeImpl().setType(CommodityType.SPACE_VALUE)));
        TopologyEntity.Builder entity = TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl()
                .setOid(entityOid)
                .addCommoditiesBoughtFromProviders(commBoughtPrd)
                .addAllCommoditySoldList(commSoldList.build())
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE));

        topology.put(entityOid, entity);

        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // Run historical editor smoothing logic, the cpu and power are expected to have historical
        // utilization values while the cpu provisioned and space are expected to have no historical
        // utilization.
        historicalEditor.applyCommodityEdits(graph, new ArrayList<>(), TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME).build());
        Optional<CommoditySoldView> commSoldOpt1 = entity.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.CPU_PROVISIONED_VALUE).findFirst();
        Assert.assertTrue(commSoldOpt1.isPresent() && !commSoldOpt1.get().hasHistoricalUsed()
                && !commSoldOpt1.get().hasHistoricalPeak());
        Optional<CommoditySoldView> commSoldOpt2 = entity.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.CPU_VALUE).findFirst();
        Assert.assertTrue(commSoldOpt2.isPresent() && commSoldOpt2.get().hasHistoricalUsed()
                && commSoldOpt2.get().hasHistoricalPeak());
        Assert.assertEquals(10, commSoldOpt2.get().getHistoricalUsed().getHistUtilization(), 0);
        Assert.assertEquals(10, commSoldOpt2.get().getHistoricalPeak().getHistUtilization(), 0);
        Optional<CommodityBoughtView> commBoughtOpt1 = entity.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(c -> c.getCommodityBoughtList()).flatMap(List::stream)
                .filter(c -> c.getCommodityType().getType() == CommodityType.POWER_VALUE)
                .findFirst();
        Assert.assertTrue(commBoughtOpt1.isPresent() && commBoughtOpt1.get().hasHistoricalUsed()
                && commBoughtOpt1.get().hasHistoricalPeak());
        Assert.assertEquals(20, commBoughtOpt1.get().getHistoricalUsed().getHistUtilization(), 0);
        Assert.assertEquals(20, commBoughtOpt1.get().getHistoricalPeak().getHistUtilization(), 0);
        Optional<CommodityBoughtView> commBoughtOpt2 = entity.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(c -> c.getCommodityBoughtList()).flatMap(List::stream)
                .filter(c -> c.getCommodityType().getType() == CommodityType.SPACE_VALUE)
                .findFirst();
        Assert.assertTrue(commBoughtOpt2.isPresent() && !commBoughtOpt2.get().hasHistoricalUsed()
                && !commBoughtOpt2.get().hasHistoricalPeak());
    }

    /**
     * Apply historical editor. historicalUsed and historicalPeak are updated.
     */
    @Test
    public void testApplyCommodityEditsRealtimeThenPlan() {
        TopologyDTO.TopologyInfo  realTimeInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME).build();

        TopologyGraph<TopologyEntity> graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(10,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(10, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(100, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // historicalUsed = used
        Assert.assertEquals(10, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(100, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Recreate Graph for plan
        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);

        TopologyDTO.TopologyInfo  planTopologyInfo = TopologyDTO.TopologyInfo.newBuilder().setPlanInfo(
                TopologyDTO.PlanTopologyInfo.newBuilder().setPlanProjectType(PlanProjectType.USER)).build();

        // Second time apply historicalEditor : with Plan. Map initialized with above call and
        // only entity values are updated but map values remain same as before.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), planTopologyInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Third time apply historicalEditor with realtime : values will be calculated to same
        // (because underlying map was not updated in previous cycle).
        // In current cycle map values should change because replace will be successful.
        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);

        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(20,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(20, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(200, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 10 + 0.5 * 20 = 15
        Assert.assertEquals(15, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 100 + 0.01 * 200 = 101
        Assert.assertEquals(101, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // Fourth time apply historicalEditor with realtime : values will be updated for weighted avg of previous 3 times
        // (because map was in previous cycle).
        // In current cycle map values should change because replace will be successful.
        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 40, 400, 30, 300);

        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), realTimeInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        // used doesn't change
        Assert.assertEquals(30,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(300,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 15 + 0.5 * 30 = 22.5
        Assert.assertEquals(22.5,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
                FLOATING_POINT_DELTA);
        // 0.99 * 101 + 0.01 * 300 = 102.99
        Assert.assertEquals(102.99,
                vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                        .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
                FLOATING_POINT_DELTA);

        // Check PM values
        // used doesn't change
        Assert.assertEquals(40, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        // peak doesn't change
        Assert.assertEquals(400, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        // 0.5 * 15 + 0.5 * 40 = 27.5
        Assert.assertEquals(27.5, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        // 0.99 * 101 + 0.01 * 400 = 103.99
        Assert.assertEquals(103.99, pm.getTopologyEntityImpl()
                .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }

    /**
     * Don't allow historical editor to update commodities for headroom plan.
     */
    @Test
    public void testApplyCommodityEditsForClusterHeadroomPlan() {
        TopologyDTO.TopologyInfo  topologyInfo = TopologyDTO.TopologyInfo.newBuilder().setPlanInfo(
            TopologyDTO.PlanTopologyInfo.newBuilder().setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM)).build();

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

        TopologyDTO.TopologyInfo topologyInfo = TopologyDTO.TopologyInfo.newBuilder().setPlanInfo(
                TopologyDTO.PlanTopologyInfo.newBuilder().setPlanType(PlanProjectType.USER.name())).build();

        testApplyCommodityEditsNoUpdate(changes, topologyInfo);
    }

    /**
     * Test historical value removal of entities which no longer exists.
     */
    @Test
    public void testHistoricalInfoEntityDeletion() {
        // The first broadcast cycle historical values cached for pm oid 1L, vm oid 2L
        TopologyGraph<TopologyEntity> graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        historicalEditor.applyCommodityEdits(graph, new ArrayList<>(), TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyContextId(777777).build());
        // Suppose the pm with oid 1L failed to be discovered 5 times, and vm with oid 2L failed to
        // be discovered 3 times
        historicalEditor.historicalInfo.getOidToMissingConsecutiveCycleCounts().put(1L, 5);
        historicalEditor.historicalInfo.getOidToMissingConsecutiveCycleCounts().put(2L, 3);
        // This broadcast cycle no thing discovered
        historicalEditor.applyCommodityEdits(TopologyEntityTopologyGraphCreator.newGraph(new HashMap<>()),
                new ArrayList<>(), TopologyDTO.TopologyInfo.newBuilder().setTopologyContextId(777777).build());
        // Pm with oid 1L failed to be discovered 6 times, it should be deleted from historicalEditor cache
        Assert.assertFalse(historicalEditor.historicalInfo.getOidToHistoricalSEInfo().containsKey(1L));
        Assert.assertFalse(historicalEditor.historicalInfo.getOidToMissingConsecutiveCycleCounts()
                .containsKey(1L));
        // Vm with oid 2L failed to be discovered 4 times, it should still exists in historicalEditor cache
        Assert.assertTrue(historicalEditor.historicalInfo.getOidToHistoricalSEInfo().containsKey(2L));
        Assert.assertTrue(historicalEditor.historicalInfo.getOidToMissingConsecutiveCycleCounts().get(2L) == 4);
    }

    /**
     * Test {@link HistoricalEditor#copyHistoricalValuesToClonedEntities(TopologyGraph, List)}.
     */
    @Test
    public void testCopyHistoricalValuesToClonedEntities() {
        final ImmutableList.Builder<CommoditySoldImpl> commSoldList = ImmutableList.builder();
        commSoldList.add(new CommoditySoldImpl().setCommodityType(
            new CommodityTypeImpl().setType(CommodityType.VMEM.getNumber()).setKey(""))
            .setHistoricalUsed(new HistoricalValuesImpl().setMaxQuantity(1).setHistUtilization(2)
                .addTimeSlot(3.0).setPercentile(4))
            .setHistoricalPeak(new HistoricalValuesImpl().setMaxQuantity(5).setHistUtilization(6)
                .addTimeSlot(7.0).setPercentile(8)));
        commSoldList.add(new CommoditySoldImpl().setCommodityType(
            new CommodityTypeImpl().setType(CommodityType.VCPU.getNumber()).setKey(""))
            .setHistoricalUsed(new HistoricalValuesImpl().setMaxQuantity(10).setHistUtilization(20)
                .addTimeSlot(30.0).setPercentile(40))
            .setHistoricalPeak(new HistoricalValuesImpl().setMaxQuantity(50).setHistUtilization(60)
                .addTimeSlot(70.0).setPercentile(80)));

        final ImmutableList.Builder<CommoditiesBoughtFromProviderImpl> commBoughtList = ImmutableList.builder();
        commBoughtList.add(new CommoditiesBoughtFromProviderImpl().setProviderId(1000L)
            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityType.MEM.getNumber()).setKey(""))
                .setHistoricalUsed(new HistoricalValuesImpl().setMaxQuantity(-1).setHistUtilization(-2)
                    .addTimeSlot(-3.0).setPercentile(-4))
                .setHistoricalPeak(new HistoricalValuesImpl().setMaxQuantity(-5).setHistUtilization(-6)
                    .addTimeSlot(-7.0).setPercentile(-8))));
        commBoughtList.add(new CommoditiesBoughtFromProviderImpl().setProviderId(1001L)
            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT.getNumber()).setKey(""))
                .setHistoricalUsed(new HistoricalValuesImpl().setMaxQuantity(-10).setHistUtilization(-20)
                    .addTimeSlot(-30.0).setPercentile(-40))
                .setHistoricalPeak(new HistoricalValuesImpl().setMaxQuantity(-50).setHistUtilization(-60)
                    .addTimeSlot(-70.0).setPercentile(-80))));

        final TopologyEntity.Builder origin = buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues(100L,
                new HistoricalValuesImpl().setMaxQuantity(1).setHistUtilization(2).addTimeSlot(3.0).setPercentile(4),
                new HistoricalValuesImpl().setMaxQuantity(5).setHistUtilization(6).addTimeSlot(7.0).setPercentile(8),
                new HistoricalValuesImpl().setMaxQuantity(10).setHistUtilization(20).addTimeSlot(30.0).setPercentile(40),
                new HistoricalValuesImpl().setMaxQuantity(50).setHistUtilization(60).addTimeSlot(70.0).setPercentile(80),
            101L, new HistoricalValuesImpl().setMaxQuantity(-1).setHistUtilization(-2).addTimeSlot(-3.0).setPercentile(-4),
                new HistoricalValuesImpl().setMaxQuantity(-5).setHistUtilization(-6).addTimeSlot(-7.0).setPercentile(-8),
            102L, new HistoricalValuesImpl().setMaxQuantity(-10).setHistUtilization(-20).addTimeSlot(-30.0).setPercentile(-40),
                new HistoricalValuesImpl().setMaxQuantity(-50).setHistUtilization(-60).addTimeSlot(-70.0).setPercentile(-80));

        final TopologyEntity.Builder clone = buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues(200L,
            HistoricalValuesView.getDefaultInstance(), HistoricalValuesView.getDefaultInstance(),
                HistoricalValuesView.getDefaultInstance(), HistoricalValuesView.getDefaultInstance(),
            -1L, HistoricalValuesView.getDefaultInstance(), HistoricalValuesView.getDefaultInstance(),
            -2L, HistoricalValuesView.getDefaultInstance(), HistoricalValuesView.getDefaultInstance())
            .setClonedFromEntity(origin.getTopologyEntityImpl());
        final Map<Long, Long> oldProvidersMap = ImmutableMap.of(-1L, 101L, -2L, 102L);
        clone.getTopologyEntityImpl().putEntityPropertyMap(TopologyDTOUtil.OLD_PROVIDERS,
            new Gson().toJson(oldProvidersMap));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
            ImmutableMap.of(origin.getOid(), origin, clone.getOid(), clone));

        historicalEditor.copyHistoricalValuesToClonedEntities(graph,
            ImmutableList.of(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.getDefaultInstance())
                .build()));

        for (int i = 0; i <= 1; i++) {
            final CommoditySoldImpl originalCommoditySold =
                origin.getTopologyEntityImpl().getCommoditySoldListImpl(i);
            final CommoditySoldImpl clonedCommoditySold =
                origin.getTopologyEntityImpl().getCommoditySoldListImpl(i);
            checkHistoricalValues(originalCommoditySold.getHistoricalUsed(),
                clonedCommoditySold.getHistoricalUsed());
            checkHistoricalValues(originalCommoditySold.getHistoricalPeak(),
                clonedCommoditySold.getHistoricalPeak());
        }

        final Map<Long, CommoditiesBoughtFromProviderImpl> originalProviderOidToCommBought =
            origin.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersImplList().stream()
                .collect(Collectors.toMap(CommoditiesBoughtFromProviderImpl::getProviderId, Function.identity()));
        final Map<Long, CommoditiesBoughtFromProviderImpl> clonedProviderOidToCommBought =
            clone.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersImplList().stream()
                .collect(Collectors.toMap(CommoditiesBoughtFromProviderImpl::getProviderId, Function.identity()));

        for (Entry<Long, Long> entry : oldProvidersMap.entrySet()) {
            final CommodityBoughtImpl originalCommodityBought =
                originalProviderOidToCommBought.get(entry.getValue()).getCommodityBoughtImpl(0);
            final CommodityBoughtImpl clonedCommodityBought =
                clonedProviderOidToCommBought.get(entry.getKey()).getCommodityBoughtImpl(0);
            checkHistoricalValues(originalCommodityBought.getHistoricalUsed(),
                clonedCommodityBought.getHistoricalUsed());
            checkHistoricalValues(originalCommodityBought.getHistoricalPeak(),
                clonedCommodityBought.getHistoricalPeak());
        }
    }

    /**
     * Check historical values.
     *
     * @param origin original historical values
     * @param clone cloned historical values
     */
    private void checkHistoricalValues(final HistoricalValuesView origin,
                                       final HistoricalValuesView clone) {
        Assert.assertEquals(origin.getHistUtilization(), clone.getHistUtilization(), FLOATING_POINT_DELTA);
    }

    /**
     * Don't allow historical editor to update commodities.
     *
     * @param changes to iterate over and find relevant changes (e.g baseline change)
     * @param topologyInfo to identify if it is a cluster headroom plan
     */
    private void testApplyCommodityEditsNoUpdate(@Nonnull final List<ScenarioChange> changes,
                                                 @Nonnull final TopologyDTO.TopologyInfo  topologyInfo) {
        TopologyGraph<TopologyEntity> graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        historicalEditor.applyCommodityEdits(graph, changes, topologyInfo);

        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);
        historicalEditor.applyCommodityEdits(graph, changes, topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // All values are not changed.
        Assert.assertEquals(20,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getPeak(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        Assert.assertEquals(0,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        Assert.assertEquals(20, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }

    private void testApplyCommodityEditsWith0InitialValues(TopologyDTO.TopologyInfo topologyInfo) {
        // 0 used/peak values during first broadcast
        TopologyGraph<TopologyEntity> graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 0, 0, 0, 0);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

        // Check VM values
        Assert.assertEquals(0,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(0,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        Assert.assertEquals(0, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);

        // correct used/peak values during second broadcast
        graph = createTopologyGraph(CommodityDTO.CommodityType.MEM, 20, 200, 20, 200);
        // Second time apply historicalEditor. Match the new commodity with the previous one.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        pm = graph.getEntity(1L).get();
        vm = graph.getEntity(2L).get();

        // Check VM values
        Assert.assertEquals(20,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getHistUtilization(),
            FLOATING_POINT_DELTA);
        // historicalPeak = peak
        Assert.assertEquals(200,
            vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getHistUtilization(),
            FLOATING_POINT_DELTA);

        // Check PM values
        Assert.assertEquals(20, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200, pm.getTopologyEntityImpl()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
    }
}
