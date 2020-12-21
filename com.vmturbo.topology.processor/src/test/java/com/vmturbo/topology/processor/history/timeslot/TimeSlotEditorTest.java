package com.vmturbo.topology.processor.history.timeslot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.setting.DailyObservationWindowsCount;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit tests for TimeSlotEditor.
 */
public class TimeSlotEditorTest extends BaseGraphRelatedTest {

    private static final long OID1 = 12;
    private static final long OID2 = 15;
    private static final long OID3 = 16;
    private static final long PERIOD1 = 100;
    private static final long PERIOD2 = 200;
    private static final CommodityType CT = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();
    private static final double SOLD_CAPACITY = 10;
    private static final DailyObservationWindowsCount SLOTS = DailyObservationWindowsCount.FOUR;
    private static final Pair<Long, Long> DEFAULT_RANGE = Pair.create(null, null);
    private ExecutorService backgroundLoadingPool;
    private TimeslotHistoricalEditorConfig config;
    private TopologyInfo topologyInfo;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        backgroundLoadingPool = Executors.newCachedThreadPool();
        config = createConfig(100, Clock.systemUTC());
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(777777L).build();
    }

    private static TimeslotHistoricalEditorConfig createConfig(int backgroundLoadThreshold,
                    Clock clock) {
        return new TimeslotHistoricalEditorConfig(1, 1, 777777L, backgroundLoadThreshold, 1, 1, 1,
                        clock, null);
    }

    /**
     * Test the preparation tasks creation.
     * That tasks are created for uninitialized commodities for different observation windows.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCreatePreparationTasks() throws HistoryCalculationException, InterruptedException {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();

        EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);
        EntityCommodityReference ref3 = new EntityCommodityReference(OID3, CT, null);

        TimeslotEditorCacheAccess editor = new TimeslotEditorCacheAccess(config, null,
                        backgroundLoadingPool, TimeSlotLoadingTask::new);
        List<EntityCommodityReference> comms = ImmutableList.of(ref1, ref2, ref3);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        editor.initContext(context, comms);
        // as if already got data for oid2
        editor.getCache().put(new EntityCommodityFieldReference(ref2, CommodityField.USED),
                              new TimeSlotCommodityData());

        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(context, comms);

        // should have 2 tasks - for ref1 and ref3
        Assert.assertEquals(2, tasks.size());
    }

    /**
     * Checks the case when background loading failed.
     *
     * @throws InterruptedException in case thread was interrupted
     * @throws HistoryCalculationException in case time slot calculation process failed
     */
    @Test
    public void checkBackgroundLoadingFailed()
                    throws InterruptedException, HistoryCalculationException {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();

        config = createConfig(1, Clock.systemUTC());

        final List<EntityCommodityReference> comms = createCommodityReferences();
        final CountDownLatch latch = new CountDownLatch(comms.size());
        final TimeslotEditorCacheAccess editor = getEditor(latch);
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        editor.initContext(context, comms);
        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(context, comms);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(0, editor.createCalculationTasks(context, comms).size());
        latch.await();
        backgroundLoadingPool.shutdown();
        backgroundLoadingPool.awaitTermination(1, TimeUnit.DAYS);
        // Next broadcast
        editor.initContext(context, comms);
        Assert.assertEquals(0, editor.createPreparationTasks(context, comms).size());
        Assert.assertEquals(0, editor.createCalculationTasks(context, comms).size());
    }

    /**
     * Checks the case when background loading failed, but it will be enabled again after TP was
     * working for more than max observation period value.
     *
     * @throws InterruptedException in case thread was interrupted
     * @throws HistoryCalculationException in case time slot calculation process
     *                 failed
     */
    @Test
    public void checkBackgroundLoadingFailedAnalysisEnabledAfterObservationPeriod()
                    throws InterruptedException, HistoryCalculationException {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();

        final Clock clock = Mockito.mock(Clock.class);

        config = createConfig(1, clock);

        final EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        final EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);
        final EntityCommodityReference ref3 = new EntityCommodityReference(OID3, CT, null);
        final List<EntityCommodityReference> comms = ImmutableList.of(ref1, ref2, ref3);
        Mockito.when(clock.millis()).thenReturn(0L);
        final CountDownLatch latch = new CountDownLatch(comms.size());
        final TimeslotEditorCacheAccess editor = getEditor(latch);
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        editor.initContext(context, comms);
        final List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(context, comms);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(0, editor.createCalculationTasks(context, comms).size());
        latch.await();
        backgroundLoadingPool.shutdown();
        backgroundLoadingPool.awaitTermination(1, TimeUnit.DAYS);
        // Next broadcast
        editor.initContext(context, comms);
        Assert.assertEquals(0, editor.createPreparationTasks(context, comms).size());
        Assert.assertEquals(0, editor.createCalculationTasks(context, comms).size());
        /*
         Max observation period days passed
         Cache initialized with runtime values.
         */
        Stream.of(ref1, ref2).forEach(ref -> editor.getCache()
                        .put(new EntityCommodityFieldReference(ref, CommodityField.USED),
                                        new TimeSlotCommodityData()));
        final long afterMaxPeriodDaysPassed = Duration.ofDays(PERIOD2).toMillis();
        Mockito.when(clock.millis()).thenReturn(afterMaxPeriodDaysPassed + 100);
        Assert.assertEquals(1, editor.createPreparationTasks(context, comms).size());
        editor.getCache().put(new EntityCommodityFieldReference(ref3, CommodityField.USED),
                        new TimeSlotCommodityData());
        Assert.assertEquals(3, editor.createCalculationTasks(context, comms).size());
    }

    /**
     * Checks the case when background loading completed successfully on second broadcast.
     *
     * @throws InterruptedException in case thread was interrupted
     * @throws HistoryCalculationException in case time slot calculation process failed
     */
    @Test
    public void checkBackgroundLoadingCompletedInTwoBroadcasts()
                    throws InterruptedException, HistoryCalculationException {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();

        config = createConfig(1, Clock.systemUTC());

        final List<EntityCommodityReference> comms = createCommodityReferences();
        final CountDownLatch latch = new CountDownLatch(comms.size());
        final CountDownLatch discoveryLatch = new CountDownLatch(1);
        final TimeslotEditorCacheAccess editor =
                        new TimeslotEditorCacheAccess(config, null, backgroundLoadingPool,
                                        (client, range) -> new TestTimeSlotLoadingTask(client,
                                                        latch, discoveryLatch, DEFAULT_RANGE));
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        editor.initContext(context, comms);
        discoveryLatch.countDown();
        final List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(context, comms);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(0, editor.createCalculationTasks(context, comms).size());
        latch.await();
        backgroundLoadingPool.shutdown();
        backgroundLoadingPool.awaitTermination(1, TimeUnit.DAYS);
        // Next broadcast
        editor.initContext(context, comms);
        comms.forEach(ref -> editor.getCache()
                        .put(new EntityCommodityFieldReference(ref, CommodityField.USED),
                                        new TimeSlotCommodityData()));
        Assert.assertThat(editor.createPreparationTasks(context, comms).size(), CoreMatchers.is(0));
        Assert.assertThat(editor.createCalculationTasks(context, comms).size(), CoreMatchers.is(3));
    }

    /**
     * Checks that maintenance is working as expected.
     *
     * @throws InterruptedException in case thread was interrupted
     * @throws HistoryCalculationException in case time slot calculation process
     *                 failed
     */
    @Test
    public void checkMaintenance() throws InterruptedException, HistoryCalculationException {
        final Clock clock = Mockito.mock(Clock.class);
        config = createConfig(10, clock);
        final EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        final EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);
        final EntityCommodityReference ref3 = new EntityCommodityReference(OID3, CT, null);
        final List<EntityCommodityReference> comms = ImmutableList.of(ref1, ref2, ref3);
        final List<Pair<Long, StatRecord>> oldRecords = Collections.singletonList(Pair.create(100L,
                        StatRecord.newBuilder().setUsed(StatsAccumulator.singleStatValue(50F))
                                        .setCapacity(StatsAccumulator.singleStatValue(100F))
                                        .build()));
        final Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> maintenaceData =
                        ImmutableMap.of(new EntityCommodityFieldReference(ref1,
                                        CommodityField.USED), oldRecords);
        final TimeslotEditorCacheAccess editor =
                        new TimeslotEditorCacheAccess(config, null, backgroundLoadingPool,
                                        (client, range) -> new TestTimeSlotLoadingTask(client,
                                                        maintenaceData, DEFAULT_RANGE));
        final HistoryAggregationContext firstBroadcastContext = createContext(
                        ImmutableMap.of(ref1, Pair.create(70F, 100F), ref2, Pair.create(80F, 200F),
                                        ref3, Pair.create(30F, 100F))).getFirst();
        editor.initContext(firstBroadcastContext, comms);
        for (EntityCommodityReference ref : comms) {
            final EntityCommodityFieldReference fieldRef =
                            new EntityCommodityFieldReference(ref, CommodityField.USED);
            final TimeSlotCommodityData data = new TimeSlotCommodityData();
            data.init(fieldRef, null, config, firstBroadcastContext);
            editor.getCache().put(fieldRef, data);
        }
        Mockito.doAnswer((invocation) -> Duration.ofHours(1).toMillis()).when(clock)
                        .millis();
        doCalculations(editor, firstBroadcastContext);
        final HistoryAggregationContext secondBroadcastContext = createContext(ImmutableMap
                        .of(ref1, Pair.create(10F, 100F), ref2, Pair.create(195F, 200F), ref3,
                                        Pair.create(95F, 100F))).getFirst();
        Mockito.doAnswer((invocation) -> Duration.ofHours(1).toMillis() * 3)
                        .when(clock).millis();
        doCalculations(editor, secondBroadcastContext);
        Mockito.doAnswer((invocation) -> System.currentTimeMillis()).when(clock).millis();
        editor.completeBroadcast(secondBroadcastContext);
        final Pair<HistoryAggregationContext, GraphWithSettings> contextToGraph2 =
                        createContext(ImmutableMap.of(ref1, Pair.create(10F, 100F), ref2,
                                        Pair.create(195F, 200F), ref3, Pair.create(95F, 100F)));
        doCalculations(editor, contextToGraph2.getFirst());
        Assert.assertEquals(getTimeSlots(ref1, contextToGraph2.getSecond()).get(0), 10F, 0.0001);
    }

    /**
     * Checks write and read capability for timeslot data.
     *
     * @throws InterruptedException in case thread was interrupted.
     * @throws HistoryCalculationException in case time slot calculation process failed.
     * @throws IOException in case of an error during writing or reading.
     * @throws DiagnosticsException in case of error during diagnostics export/import.
     */
    @Test
    public void testWriterReaderTimeslotData() throws InterruptedException, HistoryCalculationException, IOException,
            DiagnosticsException {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();
        config = createConfig(1, Clock.systemUTC());
        final List<EntityCommodityReference> comms = createCommodityReferences();
        final CountDownLatch latch = new CountDownLatch(comms.size());
        final TimeslotEditorCacheAccess editor = getEditor(latch);
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                graphWithSettings, false);
        editor.initContext(context, comms);
        final List<EntityCommodityReference> entityCommodityFieldReferences = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            entityCommodityFieldReferences.add(new EntityCommodityFieldReference(i, CT, null));
        }
        final Map<EntityCommodityFieldReference, TimeSlotCommodityData> cache = editor.getCache();
        for (EntityCommodityReference ref : entityCommodityFieldReferences) {
            final EntityCommodityFieldReference fieldRef = new EntityCommodityFieldReference(ref,
                    CommodityField.USED);
            final TimeSlotCommodityData data = new TimeSlotCommodityData();
            cache.put(fieldRef, data);
        }
        final TimeslotEditorCacheAccess timeslotEditorCacheAccess = getEditor(
                new CountDownLatch(0));
        final long timestamp;
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            editor.exportState(output);
            timestamp = editor.getStartTimestamp();
            timeslotEditorCacheAccess.restoreState(output.toByteArray());
        }
        Assert.assertEquals(timestamp, timeslotEditorCacheAccess.getStartTimestamp());
        for (Entry<EntityCommodityFieldReference, TimeSlotCommodityData> entityCommodityReferenceTimeSlotCommodityData : timeslotEditorCacheAccess
                .getCache()
                .entrySet()) {
            final TimeSlotCommodityData timeSlotCommodityData = cache.get(
                    entityCommodityReferenceTimeSlotCommodityData.getKey());
            Assert.assertNotNull(timeSlotCommodityData);
            Assert.assertEquals(timeSlotCommodityData,
                    entityCommodityReferenceTimeSlotCommodityData.getValue());
        }
    }

    @Nonnull
    private TimeslotEditorCacheAccess getEditor(@Nonnull CountDownLatch latch) {
        return new TimeslotEditorCacheAccess(config, null, backgroundLoadingPool,
                (client, range) -> new TestTimeSlotLoadingTask(client, latch, DEFAULT_RANGE));
    }

    private void doCalculations(TimeslotEditorCacheAccess editor,
                    HistoryAggregationContext context) {
        for (Entry<EntityCommodityFieldReference, TimeSlotCommodityData> refToData : editor
                        .getCache().entrySet()) {
            refToData.getValue().aggregate(refToData.getKey(), config, context);
        }
    }

    private static List<Double> getTimeSlots(@Nonnull EntityCommodityReference reference,
                    @Nonnull GraphWithSettings graph) {
        return graph.getTopologyGraph().getEntity(reference.getEntityOid()).get()
                        .getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                        .filter(cs -> reference.getCommodityType().equals(cs.getCommodityType()))
                        .findFirst().get().getHistoricalUsed().getTimeSlotList();
    }

    private Pair<HistoryAggregationContext, GraphWithSettings> createContext(
                    Map<EntityCommodityReference, Pair<Float, Float>> initialRefToStats) {
        final GraphWithSettings graphWithSettings = createGraphWithSettings();
        populateStatsToEntities(graphWithSettings, initialRefToStats);
        return Pair.create(new HistoryAggregationContext(topologyInfo, graphWithSettings, false),
                        graphWithSettings);
    }

    private static void populateStatsToEntities(GraphWithSettings graphWithSettings,
                    Map<EntityCommodityReference, Pair<Float, Float>> initialRefToStats) {
        initialRefToStats.forEach((ref, stats) -> {
            final TopologyEntityDTO.Builder entityBuilder =
                            graphWithSettings.getTopologyGraph().getEntity(ref.getEntityOid()).get()
                                            .getTopologyEntityDtoBuilder();
            final Long providerOid = ref.getProviderOid();
            if (providerOid != null) {
                final CommoditiesBoughtFromProvider.Builder boughtFromProviderBuilder =
                                CommoditiesBoughtFromProvider.newBuilder()
                                                .setProviderId(providerOid).setProviderEntityType(
                                                EntityType.DESKTOP_POOL_VALUE);
                boughtFromProviderBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(ref.getCommodityType()).setUsed(stats.getFirst())
                                .build());
                entityBuilder.getCommoditiesBoughtFromProvidersList()
                                .add(boughtFromProviderBuilder.build());
            } else {
                entityBuilder.addAllCommoditySoldList(
                                Collections.singleton(CommoditySoldDTO.newBuilder()
                                                .setCommodityType(ref.getCommodityType())
                                                .setUsed(stats.getFirst())
                                                .setCapacity(stats.getSecond()).build()));
            }
        });
    }

    private static List<EntityCommodityReference> createCommodityReferences() {
        final EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        final EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);
        final EntityCommodityReference ref3 = new EntityCommodityReference(OID3, CT, null);
        return ImmutableList.of(ref1, ref2, ref3);
    }

    private static GraphWithSettings createGraphWithSettings() {
        final Map<Long, Builder> topologyBuilderMap = new HashMap<>();
        final Map<Long, EntitySettings> entitySettings = new HashMap<>();

        addEntityWithSetting(OID1, EntityType.BUSINESS_USER_VALUE,
                        EntitySettingSpecs.MaxObservationPeriodDesktopPool, PERIOD1,
                        topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID2, EntityType.BUSINESS_USER_VALUE,
                        EntitySettingSpecs.MaxObservationPeriodDesktopPool, PERIOD1,
                        topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID3, EntityType.BUSINESS_USER_VALUE,
                        EntitySettingSpecs.MaxObservationPeriodDesktopPool, PERIOD2,
                        topologyBuilderMap, entitySettings);
        return new GraphWithSettings(
                        TopologyEntityTopologyGraphCreator.newGraph(topologyBuilderMap),
                        entitySettings, Collections.emptyMap());
    }

    /**
     * Access to the editor cached data.
     */
    private static class TimeslotEditorCacheAccess extends TimeSlotEditor {
        /**
         * Construct the instance.
         *
         * @param config configuration parameters
         * @param statsHistoryClient remote persistence
         * @param backgroundLoadingPool pool for background loading tasks.
         * @param loadingTaskSupplier supplier for loading task.
         */
        private TimeslotEditorCacheAccess(TimeslotHistoricalEditorConfig config,
                        StatsHistoryServiceBlockingStub statsHistoryClient,
                        ExecutorService backgroundLoadingPool,
                        BiFunction<StatsHistoryServiceBlockingStub, Pair<Long, Long>, TimeSlotLoadingTask> loadingTaskSupplier) {
            super(config, statsHistoryClient, backgroundLoadingPool, loadingTaskSupplier);
        }

        @Override
        public Map<EntityCommodityFieldReference, TimeSlotCommodityData> getCache() {
            return super.getCache();
        }

    }

    /**
     * {@link TimeSlotLoadingTask} implementation dedicated for tests.
     */
    private static class TestTimeSlotLoadingTask extends TimeSlotLoadingTask {
        private final CountDownLatch taskCompletionLatch;
        private final CountDownLatch broadcastLatch;
        private final Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> maintenaceData;

        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> maintenaceData, @Nonnull Pair<Long, Long> range) {
            this(client, null, null, maintenaceData, range);
        }

        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        CountDownLatch taskCompletionLatch, CountDownLatch broadcastLatch,
                        Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> maintenaceData,
                        @Nonnull Pair<Long, Long> range) {
            super(client, range);
            this.taskCompletionLatch = taskCompletionLatch;
            this.broadcastLatch = broadcastLatch;
            this.maintenaceData = maintenaceData;
        }

        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        CountDownLatch taskCompletionLatch, CountDownLatch broadcastLatch,
                        @Nonnull Pair<Long, Long> range) {
            this(client, taskCompletionLatch, broadcastLatch, Collections.emptyMap(), range);
        }

        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        CountDownLatch taskCompletionLatch, @Nonnull Pair<Long, Long> range) {
            this(client, taskCompletionLatch, null, Collections.emptyMap(), range);
        }

        @Nonnull
        @Override
        public Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> load(
                        @Nonnull Collection<EntityCommodityReference> commodities,
                        @Nonnull TimeslotHistoricalEditorConfig config)
                        throws HistoryCalculationException {
            try {
                if (!maintenaceData.isEmpty()) {
                    return maintenaceData;
                }
                if (broadcastLatch != null) {
                    broadcastLatch.await();
                    return Collections.emptyMap();
                }
                return super.load(commodities, config);
            } catch (InterruptedException e) {
                throw new HistoryCalculationException("Test has been interrupted", e);
            } finally {
                if (taskCompletionLatch != null) {
                    taskCompletionLatch.countDown();
                }
            }
        }
    }
}
