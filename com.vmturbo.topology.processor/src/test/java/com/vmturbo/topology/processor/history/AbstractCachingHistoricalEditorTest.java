package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Unit tests for AbstractCachingHistoricalEditor.
 */
public class AbstractCachingHistoricalEditorTest {
    private static final float DB_VALUE = 10f;
    private static final float ABOVE_DB_VALUE = 15f;
    private static double DELTA = 0.00001;
    private static final CachingHistoricalEditorConfig CONFIG1 = new CachingHistoricalEditorConfig(2, 3);
    private static final CachingHistoricalEditorConfig CONFIG2 = new CachingHistoricalEditorConfig(10, 10);

    private EntityCommodityReference cref1;
    private EntityCommodityReference cref2;
    private EntityCommodityReference cref3;
    private EntityCommodityReference cref4;
    private EntityCommodityReference cref5;
    private List<EntityCommodityReference> allComms;
    private Set<EntityCommodityReference> seenCommRefs;
    private Map<EntityCommodityReference, TopologyEntity> entity2commref = new HashMap<>();

    @Before
    public void setUp() {
        cref1 = createCommRef(1l, 5);
        cref2 = createCommRef(2l, 6);
        cref3 = createCommRef(3l, 7);
        cref4 = createCommRef(4l, ABOVE_DB_VALUE);
        cref5 = createCommRef(5l, 9);
        allComms = ImmutableList.of(cref1, cref2, cref3, cref4, cref5);
        seenCommRefs = new HashSet<>();
        entity2commref = new HashMap<>();
    }

    /**
     * Test the partitioning for preparation tasks creation.
     */
    @Test
    public void testCreatePreparationTasksChunking() {
        IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG1);
        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(allComms);
        Assert.assertEquals(3, tasks.size());
        editor = new TestCachingEditor(CONFIG2);
        tasks = editor.createPreparationTasks(allComms);
        Assert.assertEquals(1, tasks.size());
    }

    /**
     * Test the partitioning for calculation tasks creation.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testCreateCalculationTasksChunking() throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG1);
            // when nothing in the cache yet, no tasks
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(allComms);
            Assert.assertEquals(0, calcTasks.size());
            List<? extends Callable<List<EntityCommodityFieldReference>>> loadTasks = editor
                            .createPreparationTasks(allComms);
            Assert.assertEquals(3, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now the requested calculation tasks should be created and partitioned
            calcTasks = editor.createCalculationTasks(allComms);
            Assert.assertEquals(2, calcTasks.size());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Test the 2nd invocation of loading tasks with same commodities set.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testCreatePreparationTasksAllCached() throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG2);
            List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                            .createPreparationTasks(allComms);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            // all commodities must be loaded and next invocation should issue no tasks
            tasks = editor.createPreparationTasks(allComms);
            Assert.assertEquals(0, tasks.size());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Test the 2nd invocation of loading tasks with different commodities set.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testExecutePreparationTasksPartialCached() throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG2);
            List<EntityCommodityReference> comms1 = ImmutableList.of(cref1, cref2, cref3);
            List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor.createPreparationTasks(comms1);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            Assert.assertEquals(new HashSet<>(comms1), seenCommRefs);

            List<EntityCommodityReference> comms2 = ImmutableList.of(cref2, cref3, cref4, cref5);
            tasks = editor.createPreparationTasks(comms2);
            Assert.assertEquals(1, tasks.size());
            seenCommRefs.clear();
            executor.invokeAll(tasks);
            // this time only those that are in comms2 but not in comms1 should be loaded
            Assert.assertEquals(Sets.difference(new HashSet<>(comms2), new HashSet<>(comms1)), seenCommRefs);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Test the aggregation tasks invocation.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testExecuteAggregationTasks() throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG2);
            List<? extends Callable<List<EntityCommodityFieldReference>>> loadTasks = editor
                            .createPreparationTasks(allComms);
            Assert.assertEquals(1, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now internal cache should contain all commodities values initialized
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(allComms);
            Assert.assertEquals(1, calcTasks.size());
            executor.invokeAll(calcTasks);

            Assert.assertEquals(DB_VALUE, getSoldBuilder(cref1).getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, getSoldBuilder(cref2).getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, getSoldBuilder(cref3).getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(ABOVE_DB_VALUE, getSoldBuilder(cref4).getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, getSoldBuilder(cref5).getHistoricalUsed().getMaxQuantity(), DELTA);
        } finally {
            executor.shutdownNow();
        }
    }

    private CommoditySoldDTO.Builder getSoldBuilder(EntityCommodityReference cref) {
        return Optional.ofNullable(entity2commref.get(cref))
                        .map(e -> e.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                                        .get(0))
                        .orElseThrow(() -> new IllegalStateException("Unexpected test setup"));
    }

    private EntityCommodityReference createCommRef(long oid, double used) {
        CommodityType ct = CommodityType.newBuilder().setType(1).build();
        TopologyEntity entity = Mockito.mock(TopologyEntity.class);
        Mockito.when(entity.getOid()).thenReturn(oid);
        Mockito.when(entity.getEntityType()).thenReturn(1);
        TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder();
        entityBuilder.setOid(oid).setEntityType(1);
        entityBuilder.addCommoditySoldListBuilder().setUsed(used);
        Mockito.when(entity.getTopologyEntityDtoBuilder()).thenReturn(entityBuilder);
        EntityCommodityReference commref = new EntityCommodityReference(entity.getOid(), ct, null);
        entity2commref.put(commref, entity);
        return commref;
    }

    /**
     * Dummy implementation for caching historical editor.
     */
    private class TestCachingEditor extends
                    AbstractCachingHistoricalEditor<TestHistoryCommodityData,
                                                    TestLoadingTask,
                                                    CachingHistoricalEditorConfig,
                                                    Float,
                                                    StatsHistoryServiceBlockingStub> {

        public TestCachingEditor(CachingHistoricalEditorConfig config) {
            super(config, null, TestLoadingTask::new, TestHistoryCommodityData::new);
        }

        @Override
        public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                    PlanScope scope) {
            return true;
        }

        @Override
        public boolean isEntityApplicable(TopologyEntity entity) {
            return true;
        }

        @Override
        public boolean isCommodityApplicable(TopologyEntity entity, CommoditySoldDTO.Builder commSold) {
            return true;
        }

        @Override
        public boolean
               isCommodityApplicable(TopologyEntity entity, CommodityBoughtDTO.Builder commBought) {
            return true;
        }

        @Override
        public boolean isMandatory() {
            return true;
        }
    }

    /**
     * Test implementation for per-commodity history data.
     * Set maximum of pre-loaded and current values.
     */
    private class TestHistoryCommodityData
                    implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float> {
        private Float value;

        @Override
        public void aggregate(@Nonnull EntityCommodityFieldReference field,
                              @Nonnull CachingHistoricalEditorConfig config,
                              @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
            seenCommRefs.add(field);
            CommoditySoldDTO.Builder builder = getSoldBuilder(field);
            double current = builder.getUsed();
            builder.getHistoricalUsedBuilder()
                            .setMaxQuantity(value == null ? current : Math.max(current, value));
        }

        @Override
        public void init(EntityCommodityFieldReference field,
                         Float dbValue, CachingHistoricalEditorConfig config,
                         ICommodityFieldAccessor commodityFieldsAccessor) {
            value = dbValue;
        }
    }

    /**
     * Test implementation for data loading task.
     * Loads fixed used value from db for all passed commodities.
     */
    private class TestLoadingTask implements IHistoryLoadingTask<CachingHistoricalEditorConfig, Float> {

        public TestLoadingTask(StatsHistoryServiceBlockingStub statsHistoryClient) {}

        @Override
        public Map<EntityCommodityFieldReference, Float>
               load(Collection<EntityCommodityReference> commodities, CachingHistoricalEditorConfig config)
                               throws HistoryCalculationException {
            seenCommRefs.addAll(commodities);
            return commodities.stream()
                            .map(comm -> new EntityCommodityFieldReference(comm,
                                                                           CommodityField.USED))
                            .collect(Collectors.toMap(comm -> comm, comm -> DB_VALUE));
        }
    }

}
