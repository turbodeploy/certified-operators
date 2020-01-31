package com.vmturbo.topology.processor.history;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

/**
 * Unit tests for HistoryAggregator.
 */
public class HistoryAggregatorTest extends BaseGraphRelatedTest {
    private static CommodityType CT1 = CommodityType.newBuilder().setType(1).build();
    private static CommodityType CT2 = CommodityType.newBuilder().setType(2).build();
    private static CommodityType CT3 = CommodityType.newBuilder().setType(3).setKey("qqq").build();
    private static double DELTA = 0.00001;

    /**
     * Test that no aggregation is made when pipeline context is not applicable.
     * @throws PipelineStageException when failed
     */
    @Test
    public void testContextNotApplicable() throws PipelineStageException {
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        TopologyGraph<TopologyEntity> graph = mockGraph(Collections.emptySet());
        HistoryAggregator stage = new HistoryAggregator(executor, Collections
                        .singleton(new TestHistoricalEditor(graph, false, Collections.emptySet(),
                                                            Collections.emptySet())));
        stage.applyCommodityEdits(new GraphWithSettings(graph, Collections.emptyMap(),
                                                        Collections.emptyMap()),
                                  null, null, null);

        Mockito.verify(executor, Mockito.never()).submit(Mockito.any(Callable.class));
    }

    /**
     * Test that no aggregation is made when graph has no applicable entities.
     * @throws PipelineStageException when failed
     */
    @Test
    public void testEntityTypeNotApplicable() throws PipelineStageException {
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        int entityType1 = 1;
        int entityType2 = 2;
        TopologyEntity entity = mockEntity(entityType2, 1L, CT1, 1, 0D, null, null, null, null,
                        true);
        TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(entity));
        HistoryAggregator stage = new HistoryAggregator(executor, Collections
                        .singleton(new TestHistoricalEditor(graph, true, Collections.singleton(entityType1),
                                                            Collections.singleton(CT1))));
        stage.applyCommodityEdits(new GraphWithSettings(graph, Collections.emptyMap(),
                                                        Collections.emptyMap()),
                                  null, null, null);

        Mockito.verify(executor, Mockito.never()).submit(Mockito.any(Callable.class));
    }

    /**
     * Test that no aggregation is made when graph entities have no applicable commodities.
     * @throws PipelineStageException when failed
     */
    @Test
    public void testCommodityTypeNotApplicable() throws PipelineStageException {
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        int entityType1 = 1;
        TopologyEntity entity = mockEntity(entityType1, 1L, CT1, 1D, 0D, 2L, CT3, 0D, null, true);
        TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(entity));
        HistoryAggregator stage = new HistoryAggregator(executor, Collections
                        .singleton(new TestHistoricalEditor(graph, true, Collections.singleton(entityType1),
                                                            Collections.singleton(CT2))));
        stage.applyCommodityEdits(new GraphWithSettings(graph, Collections.emptyMap(),
                                                        Collections.emptyMap()),
                                  null, null, null);

        Mockito.verify(executor, Mockito.never()).submit(Mockito.any(Callable.class));
    }

    /**
     * Test that aggregation is made on sold and bought commodities.
     * @throws PipelineStageException when failed
     */
    @Test
    public void testCommodityTypesApplicable() throws PipelineStageException {
        ExecutorService executor = Mockito.spy(Executors.newCachedThreadPool());
        try {
            int entityType1 = 1;
            TopologyEntity entity = mockEntity(entityType1, 1L, CT1, 1D, 0D, 2L, CT3, 0D, null,
                            true);
            TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(entity));
            HistoryAggregator stage = new HistoryAggregator(executor, Collections
                            .singleton(new TestHistoricalEditor(graph, true, Collections.singleton(entityType1),
                                                                ImmutableSet.of(CT1, CT2, CT3))));
            stage.applyCommodityEdits(new GraphWithSettings(graph, Collections.emptyMap(),
                                                            Collections.emptyMap()),
                                      null, null, null);

            Mockito.verify(executor, Mockito.times(2)).submit(Mockito.any(Callable.class));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Test that aggregation modifies the values.
     * @throws PipelineStageException when failed
     */
    @Test
    public void testAggregationResults() throws PipelineStageException {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            int entityType1 = 1;
            int entityType2 = 2;
            long oid1 = 12L;
            long oid2 = 123L;
            double usedSold1 = 10D;
            double usedSold2 = 22D;
            double usedBought = 20D;
            TopologyEntity entity1 = mockEntity(entityType1, oid1, CT1, 100D, usedSold1, null, null, null, null,
                            true);
            TopologyEntity entity2 = mockEntity(entityType2, oid2, CT3, 100D, usedSold2, oid1, CT2, usedBought, null,
                            true);
            TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(entity1, entity2));
            HistoryAggregator stage = new HistoryAggregator(executor, Collections
                            .singleton(new TestHistoricalEditor(graph, true,
                                                                ImmutableSet.of(entityType1, entityType2),
                                                                ImmutableSet.of(CT1, CT2, CT3))));
            stage.applyCommodityEdits(new GraphWithSettings(graph, Collections.emptyMap(),
                                                            Collections.emptyMap()),
                                      null, null, null);

            // hist utilization should now be used / 2 for all passed commodities

            TopologyEntityDTO dto1 = entity1.getTopologyEntityDtoBuilder().build();
            Assert.assertEquals(1, dto1.getCommoditySoldListCount());
            Assert.assertTrue(dto1.getCommoditySoldList(0).hasHistoricalUsed());
            Assert.assertTrue(dto1.getCommoditySoldList(0).getHistoricalUsed().hasHistUtilization());
            Assert.assertEquals(usedSold1 / 2, dto1.getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), DELTA);

            TopologyEntityDTO dto2 = entity2.getTopologyEntityDtoBuilder().build();
            Assert.assertEquals(1, dto2.getCommoditySoldListCount());
            Assert.assertTrue(dto2.getCommoditySoldList(0).hasHistoricalUsed());
            Assert.assertTrue(dto2.getCommoditySoldList(0).getHistoricalUsed().hasHistUtilization());
            Assert.assertEquals(usedSold2 / 2, dto2.getCommoditySoldList(0).getHistoricalUsed().getHistUtilization(), DELTA);
            Assert.assertEquals(1, dto2.getCommoditiesBoughtFromProvidersCount());
            List<CommodityBoughtDTO> bought = dto2.getCommoditiesBoughtFromProviders(0).getCommodityBoughtList();
            Assert.assertEquals(1, bought.size());
            Assert.assertTrue(bought.get(0).hasHistoricalUsed());
            Assert.assertTrue(bought.get(0).getHistoricalUsed().hasHistUtilization());
            Assert.assertEquals(usedBought / 2, bought.get(0).getHistoricalUsed().getHistUtilization(), DELTA);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Test implementation of a historical type editor.
     * Which accepts by-entity and commodity types that it modifies.
     * And then creates a single initialization task and a single calculation task that divides usage by 2
     * and sets into historical utilization.
     */
    private static class TestHistoricalEditor implements IHistoricalEditor<HistoricalEditorConfig> {
        private final TopologyGraph<TopologyEntity> graph;
        private final boolean applicable;
        private final Set<Integer> applicableEntityTypes;
        private final Set<TopologyDTO.CommodityType> applicableCommTypes;

        TestHistoricalEditor(@Nonnull TopologyGraph<TopologyEntity> graph, boolean applicable,
                             @Nonnull Set<Integer> applicableEntityTypes,
                             @Nonnull Set<TopologyDTO.CommodityType> applicableCommTypes) {
            this.graph = graph;
            this.applicable = applicable;
            this.applicableEntityTypes = applicableEntityTypes;
            this.applicableCommTypes = applicableCommTypes;
        }

        @Override
        public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                    PlanScope scope) {
            return applicable;
        }

        @Override
        public boolean isEntityApplicable(TopologyEntity entity) {
            return applicableEntityTypes.contains(entity.getEntityType());
        }

        @Override
        public boolean isCommodityApplicable(TopologyEntity entity,
                                             TopologyDTO.CommoditySoldDTO.Builder commSold) {
            return applicableCommTypes.contains(commSold.getCommodityType());
        }

        @Override
        public boolean
               isCommodityApplicable(TopologyEntity entity,
                                     TopologyDTO.CommodityBoughtDTO.Builder commBought) {
            return applicableCommTypes.contains(commBought.getCommodityType());
        }

        @Override
        public List<? extends Callable<List<EntityCommodityFieldReference>>>
               createPreparationTasks(List<EntityCommodityReference> commodityRefs) {
            // simulate loading 'used' for all passed commodities
            return Collections.singletonList(new Callable<List<EntityCommodityFieldReference>>() {
                @Override
                public List<EntityCommodityFieldReference> call() throws Exception {
                    return commodityRefs
                                    .stream()
                                    .map(cref -> cref.getProviderOid() == null
                                        ? new EntityCommodityFieldReference(cref.getEntityOid(),
                                                                            cref.getCommodityType(),
                                                                            CommodityField.USED)
                                        : new EntityCommodityFieldReference(cref.getEntityOid(),
                                                                            cref.getCommodityType(),
                                                                            cref.getProviderOid(),
                                                                            CommodityField.USED))
                                    .collect(Collectors.toList());
                }});
        }

        @Override
        public List<? extends Callable<List<Void>>>
               createCalculationTasks(List<EntityCommodityReference> commodityFieldRefs) {
            return Collections.singletonList(new Callable<List<Void>>() {
                @Override
                public List<Void> call() throws Exception {
                    // divide running used by 2 and set into hist util
                    commodityFieldRefs.stream().forEach(fieldRef -> {
                        TopologyEntity entity = graph.getEntity(fieldRef.getEntityOid()).get();
                        if (fieldRef.getProviderOid() == null) {
                            CommoditySoldDTO.Builder commSold =
                                          CommodityFieldAccessor.SOLD_BUILDER_EXTRACTOR
                                                          .apply(fieldRef,
                                                                 entity.getTopologyEntityDtoBuilder());
                            commSold.getHistoricalUsedBuilder().setHistUtilization(commSold.getUsed() / 2);
                        } else {
                            CommodityBoughtDTO.Builder commBought =
                                          CommodityFieldAccessor.BOUGHT_BUILDER_EXTRACTOR
                                                          .apply(fieldRef,
                                                                 entity.getTopologyEntityDtoBuilder());
                            commBought.getHistoricalUsedBuilder().setHistUtilization(commBought.getUsed() / 2);
                        }
                    });
                    return Collections.emptyList();
                }});
        }

        @Override
        public boolean isMandatory() {
            return true;
        }

        @Override
        public void initContext(GraphWithSettings graph, ICommodityFieldAccessor accessor,
                                List<EntityCommodityReference> eligibleComms, boolean isPlan) {
        }

        @Override
        public void completeBroadcast() throws HistoryCalculationException {
        }
    }
}
