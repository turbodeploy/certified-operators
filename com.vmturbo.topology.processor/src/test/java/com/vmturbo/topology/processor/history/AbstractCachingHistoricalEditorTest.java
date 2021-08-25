package com.vmturbo.topology.processor.history;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

/**
 * Unit tests for AbstractCachingHistoricalEditor.
 */
public class AbstractCachingHistoricalEditorTest {
    private static final float DB_VALUE = 10f;
    private static final float ABOVE_DB_VALUE = 15f;
    private static final double DELTA = 0.00001;
    private static final long TOPOLOGY_ID = 777777L;
    private static final KVConfig KV_CONFIG = Mockito.mock(KVConfig.class);
    private static final CachingHistoricalEditorConfig CONFIG_1 = new CachingHistoricalEditorConfig(2, 3,
            TOPOLOGY_ID, Clock.systemUTC(), KV_CONFIG) {
        @Override
        protected String getDiagnosticsEnabledPropertyName() {
            return null;
        }
    };
    private static final CachingHistoricalEditorConfig CONFIG_2 = new CachingHistoricalEditorConfig(10, 10,
            TOPOLOGY_ID, Clock.systemUTC(), KV_CONFIG) {
        @Override
        protected String getDiagnosticsEnabledPropertyName() {
            return null;
        }
    };
    private static final HistoryAggregationContext CONTEXT = new HistoryAggregationContext(
                    TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build(),
                    Mockito.mock(GraphWithSettings.class), false);

    private EntityCommodityReference cref1;
    private EntityCommodityReference cref2;
    private EntityCommodityReference cref3;
    private EntityCommodityReference cref4;
    private EntityCommodityReference cref5;
    private List<EntityCommodityReference> allComms;
    private Set<EntityCommodityReference> seenCommRefs;
    private Map<EntityCommodityReference, TopologyEntity> entity2commref = new HashMap<>();
    private final StatsHistoryServiceMole statsHistoryServiceMole = Mockito.spy( new StatsHistoryServiceMole());

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceMole);
    private final BiFunction<StatsHistoryServiceBlockingStub, Pair<Long, Long>, TestLoadingTask>
            testLoadingTaskCreator = (statsHistoryClient, range) -> new TestLoadingTask();
    private final Supplier<TestHistoryCommodityData> testHistoryCommodityDataSupplier = TestHistoryCommodityData::new;
    private StatsHistoryServiceBlockingStub statsHistoryServiceStub;

    /**
     * Set up test environment.
     *
     * @throws IOException if failed to start grpc server
     */
    @Before
    public void setUp() throws IOException {
        entity2commref = new HashMap<>();
        cref1 = createCommRef(1L, 5, 1);
        cref2 = createCommRef(2L, 6, 1);
        cref3 = createCommRef(3L, 7, 1);
        cref4 = createCommRef(4L, ABOVE_DB_VALUE, 2);
        cref5 = createCommRef(5L, 9, 2);
        allComms = ImmutableList.of(cref1, cref2, cref3, cref4, cref5);
        seenCommRefs = new HashSet<>();
        statsHistoryServiceStub = StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Test the partitioning for preparation tasks creation.
     */
    @Test
    public void testCreatePreparationTasksChunking() {
        IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG_1,
                statsHistoryServiceStub, testLoadingTaskCreator, testHistoryCommodityDataSupplier);
        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(CONTEXT, allComms);
        Assert.assertEquals(3, tasks.size());
        editor = new TestCachingEditor(CONFIG_2, statsHistoryServiceStub, testLoadingTaskCreator, testHistoryCommodityDataSupplier);
        tasks = editor.createPreparationTasks(CONTEXT, allComms);
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
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(
                    CONFIG_1,
                    statsHistoryServiceStub, testLoadingTaskCreator, testHistoryCommodityDataSupplier);
            // when nothing in the cache yet, no tasks
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(CONTEXT, allComms);
            Assert.assertEquals(0, calcTasks.size());
            List<? extends Callable<List<EntityCommodityFieldReference>>> loadTasks = editor
                            .createPreparationTasks(CONTEXT, allComms);
            Assert.assertEquals(3, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now the requested calculation tasks should be created and partitioned
            calcTasks = editor.createCalculationTasks(CONTEXT, allComms);
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
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(CONFIG_2, statsHistoryServiceStub,
                    testLoadingTaskCreator, testHistoryCommodityDataSupplier);
            List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                            .createPreparationTasks(CONTEXT, allComms);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            // all commodities must be loaded and next invocation should issue no tasks
            tasks = editor.createPreparationTasks(CONTEXT, allComms);
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
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(
                    CONFIG_2,
                    statsHistoryServiceStub, testLoadingTaskCreator,
                    testHistoryCommodityDataSupplier);
            List<EntityCommodityReference> comms1 = ImmutableList.of(cref1, cref2, cref3);
            List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor.createPreparationTasks(CONTEXT,
                                                                                                                comms1);
            Assert.assertEquals(1, tasks.size());
            executor.invokeAll(tasks);
            Assert.assertEquals(new HashSet<>(comms1), seenCommRefs);

            List<EntityCommodityReference> comms2 = ImmutableList.of(cref2, cref3, cref4, cref5);
            tasks = editor.createPreparationTasks(CONTEXT, comms2);
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
            IHistoricalEditor<CachingHistoricalEditorConfig> editor = new TestCachingEditor(
                    CONFIG_2,
                    statsHistoryServiceStub, testLoadingTaskCreator,
                    testHistoryCommodityDataSupplier);
            List<? extends Callable<List<EntityCommodityFieldReference>>> loadTasks = editor
                            .createPreparationTasks(CONTEXT, allComms);
            Assert.assertEquals(1, loadTasks.size());
            executor.invokeAll(loadTasks);
            // now internal cache should contain all commodities values initialized
            List<? extends Callable<List<Void>>> calcTasks = editor.createCalculationTasks(CONTEXT, allComms);
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

    private EntityCommodityFieldReference createCommRef(long oid, double used, int entityType) {
        CommodityType ct = CommodityType.newBuilder().setType(1).build();
        TopologyEntity entity = Mockito.mock(TopologyEntity.class);
        Mockito.when(entity.getOid()).thenReturn(oid);
        Mockito.when(entity.getEntityType()).thenReturn(entityType);
        TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder();
        entityBuilder.setOid(oid).setEntityType(entityType);
        entityBuilder.addCommoditySoldListBuilder().setUsed(used);
        Mockito.when(entity.getTopologyEntityDtoBuilder()).thenReturn(entityBuilder);
        EntityCommodityFieldReference commref = new EntityCommodityFieldReference(entity.getOid(),
                ct, null, CommodityField.USED);
        entity2commref.put(commref, entity);
        return commref;
    }

    /**
     * Dummy implementation for caching historical editor.
     */
    private class TestCachingEditor extends
                    AbstractCachingHistoricalEditor<TestHistoryCommodityData, TestLoadingTask,
                                                    CachingHistoricalEditorConfig,
                                                    Float,
                                                    StatsHistoryServiceBlockingStub,
                                                    Void> {

        private TestCachingEditor(CachingHistoricalEditorConfig config,
                final StatsHistoryServiceBlockingStub statsHistoryServiceStub,
                final BiFunction<StatsHistoryServiceBlockingStub, Pair<Long, Long>, TestLoadingTask> loadingTaskCreator,
                final Supplier<TestHistoryCommodityData> historyCommodityDataSupplier) {
            super(config, statsHistoryServiceStub, loadingTaskCreator,
                    historyCommodityDataSupplier);
        }

        @Override
        public boolean isApplicable(@Nullable List<ScenarioChange> changes,
                @Nonnull TopologyInfo topologyInfo, @Nullable PlanScope scope) {
            return true;
        }

        @Override
        public boolean isEntityApplicable(TopologyEntity entity) {
            return true;
        }

        @Override
        public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                @Nonnull Builder commSold, @Nullable TopologyInfo topoInfo) {
            return true;
        }

        @Override
        public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                @Nonnull CommodityBoughtDTO.Builder commBought, int providerType) {
            return true;
        }

        @Override
        public boolean isMandatory() {
            return true;
        }

        @Override
        protected void exportState(@Nonnull OutputStream appender)
                        throws DiagnosticsException, IOException {
        }

        @Override
        protected void restoreState(@Nonnull byte[] bytes) {
        }
    }

    /**
     * Test implementation for per-commodity history data.
     * Set maximum of pre-loaded and current values.
     */
    private class TestHistoryCommodityData
                    implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float, Void> {
        private Float value;

        @Override
        public void aggregate(@Nonnull EntityCommodityFieldReference field,
                              @Nonnull CachingHistoricalEditorConfig config,
                              @Nonnull HistoryAggregationContext context) {
            seenCommRefs.add(field);
            CommoditySoldDTO.Builder builder = getSoldBuilder(field);
            double current = builder.getUsed();
            builder.getHistoricalUsedBuilder()
                            .setMaxQuantity(value == null ? current : Math.max(current, value));
        }

        @Override
        public void init(@Nonnull EntityCommodityFieldReference field, @Nullable Float dbValue,
                @Nonnull CachingHistoricalEditorConfig config,
                @Nonnull HistoryAggregationContext context) {
            value = dbValue;
        }

        @Override
        public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                        @Nonnull HistoryAggregationContext context,
                        @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig) {
            return false;
        }
    }

    /**
     * Test implementation for data loading task.
     * Loads fixed used value from db for all passed commodities.
     */
    private class TestLoadingTask implements IHistoryLoadingTask<CachingHistoricalEditorConfig, Float> {

        private TestLoadingTask() {}

        @Override
        @Nonnull
        public Map<EntityCommodityFieldReference, Float> load(
                @Nonnull Collection<EntityCommodityReference> commodities,
                @Nonnull CachingHistoricalEditorConfig config, final LongSet oidsToUse) {
            seenCommRefs.addAll(commodities);
            return commodities.stream()
                            .map(comm -> new EntityCommodityFieldReference(comm,
                                                                           CommodityField.USED))
                            .collect(Collectors.toMap(comm -> comm, comm -> DB_VALUE));
        }
    }

}
