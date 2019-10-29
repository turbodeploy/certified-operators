package com.vmturbo.topology.processor.topology;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.createGraph;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;

/**
 * Test editing of historical values of commodities.
 */
public class HistoricalEditorTest {

    private static final double FLOATING_POINT_DELTA = 1e-7;

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
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(
            PlanTopologyInfo.newBuilder().setPlanType(PlanProjectType.USER.name())).build();

        TopologyGraph<TopologyEntity> graph = createGraph(CommodityDTO.CommodityType.MEM, 10, 100, 10, 100);
        // First time apply historicalEditor.
        historicalEditor.applyCommodityEdits(graph, Collections.emptyList(), topologyInfo);

        TopologyEntity pm = graph.getEntity(1L).get();
        TopologyEntity vm = graph.getEntity(2L).get();

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
        Assert.assertEquals(0,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalUsed().getSystemLoad(),
            FLOATING_POINT_DELTA);
        Assert.assertEquals(0,
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().get(0)
                .getCommodityBoughtList().get(0).getHistoricalPeak().getSystemLoad(),
            FLOATING_POINT_DELTA);

        Assert.assertEquals(20, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(200, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getPeak(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalPeak().getHistUtilization(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalUsed().getSystemLoad(), FLOATING_POINT_DELTA);
        Assert.assertEquals(0, pm.getTopologyEntityDtoBuilder()
            .getCommoditySoldList(0).getHistoricalPeak().getSystemLoad(), FLOATING_POINT_DELTA);
    }
}
