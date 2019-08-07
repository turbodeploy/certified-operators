package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
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

    private EntityCommodityReferenceWithBuilder CREF1;
    private EntityCommodityReferenceWithBuilder CREF2;
    private EntityCommodityReferenceWithBuilder CREF3;
    private EntityCommodityReferenceWithBuilder CREF4;
    private EntityCommodityReferenceWithBuilder CREF5;
    private List<EntityCommodityReferenceWithBuilder> ALL_COMMS;
    private Set<EntityCommodityReferenceWithBuilder> seenCommRefs;

    @Before
    public void setUp() {
        CREF1 = createCommRef(1l, 5);
        CREF2 = createCommRef(2l, 6);
        CREF3 = createCommRef(3l, 7);
        CREF4 = createCommRef(4l, ABOVE_DB_VALUE);
        CREF5 = createCommRef(5l, 9);
        ALL_COMMS = ImmutableList.of(CREF1, CREF2, CREF3, CREF4, CREF5);
        seenCommRefs = new HashSet<>();
    }

    /**
     * Test the partitioning for preparation tasks creation.
     */
    @Test
    public void testCreatePreparationTasksChunking() {
        IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG1);
        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(ALL_COMMS);
        Assert.assertEquals(3, tasks.size());
        editor = new TestCachingEditor(CONFIG2);
        tasks = editor.createPreparationTasks(ALL_COMMS);
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
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(ALL_COMMS);
            Assert.assertEquals(0, calcTasks.size());
            List<? extends Callable<List<EntityCommodityFieldReference>>> loadTasks = editor
                            .createPreparationTasks(ALL_COMMS);
            Assert.assertEquals(3, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now the requested calculation tasks should be created and partitioned
            calcTasks = editor.createCalculationTasks(ALL_COMMS);
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
                            .createPreparationTasks(ALL_COMMS);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            // all commodities must be loaded and next invocation should issue no tasks
            tasks = editor.createPreparationTasks(ALL_COMMS);
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
            List<EntityCommodityReferenceWithBuilder> comms1 = ImmutableList.of(CREF1, CREF2, CREF3);
            List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor.createPreparationTasks(comms1);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            Assert.assertEquals(new HashSet<>(comms1), seenCommRefs);

            List<EntityCommodityReferenceWithBuilder> comms2 = ImmutableList.of(CREF2, CREF3, CREF4, CREF5);
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
                            .createPreparationTasks(ALL_COMMS);
            Assert.assertEquals(1, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now internal cache should contain all commodities values initialized
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(ALL_COMMS);
            Assert.assertEquals(1, calcTasks.size());
            executor.invokeAll(calcTasks);

            Assert.assertEquals(DB_VALUE, CREF1.getSoldBuilder().build().getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, CREF2.getSoldBuilder().build().getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, CREF3.getSoldBuilder().build().getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(ABOVE_DB_VALUE, CREF4.getSoldBuilder().build().getHistoricalUsed().getMaxQuantity(), DELTA);
            Assert.assertEquals(DB_VALUE, CREF5.getSoldBuilder().build().getHistoricalUsed().getMaxQuantity(), DELTA);
        } finally {
            executor.shutdownNow();
        }
    }

    private static EntityCommodityReferenceWithBuilder createCommRef(long oid, double used) {
        CommodityType ct = CommodityType.newBuilder().setType(1).build();
        return new EntityCommodityReferenceWithBuilder(oid, ct, CommoditySoldDTO.newBuilder()
                        .setCommodityType(ct).setUsed(used));
    }

    /**
     * Dummy implementation for caching historical editor.
     */
    private class TestCachingEditor extends
                    AbstractCachingHistoricalEditor<TestHistoryCommodityData, TestLoadingTask, CachingHistoricalEditorConfig, Float> {

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
        public void aggregate(EntityCommodityFieldReference field,
                              CachingHistoricalEditorConfig config) {
            seenCommRefs.add(field);
            Float current = (float)field.getSoldBuilder().getUsed();
            field.getHistoricalUsedBuilder()
                            .setMaxQuantity(value == null ? current : Math.max(current, value));
        }

        @Override
        public void init(Float dbValue, CachingHistoricalEditorConfig config) {
            value = dbValue;
        }
    }

    /**
     * Test implementation for data loading task.
     * Loads fixed used value from db for all passed commodities.
     */
    private class TestLoadingTask implements IHistoryLoadingTask<Float> {

        public TestLoadingTask(StatsHistoryServiceBlockingStub statsHistoryClient) {}

        @Override
        public Map<EntityCommodityFieldReference, Float>
               load(Collection<EntityCommodityReferenceWithBuilder> commodities)
                               throws HistoryCalculationException {
            seenCommRefs.addAll(commodities);
            return commodities.stream()
                            .map(comm -> new EntityCommodityFieldReference(comm,
                                                                           CommodityField.USED))
                            .collect(Collectors.toMap(comm -> comm, comm -> DB_VALUE));
        }
    }

}
