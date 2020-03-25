package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.DailyObservationWindowsCount;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
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
    private ExecutorService backgroundLoadingPool;
    private TopologyProcessorNotificationSender notificationSender;
    private TimeslotHistoricalEditorConfig config;
    private TopologyInfo topologyInfo;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        backgroundLoadingPool = Executors.newCachedThreadPool();
        notificationSender = Mockito.mock(TopologyProcessorNotificationSender.class);
        config = createConfig(100, Clock.systemUTC());
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(777777L).build();
    }

    private static TimeslotHistoricalEditorConfig createConfig(int backgroundLoadThreshold,
                    Clock clock) {
        return new TimeslotHistoricalEditorConfig(1, 1, backgroundLoadThreshold, 1, 1, 1, clock);
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
                        backgroundLoadingPool, notificationSender, TimeSlotLoadingTask::new);
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
     * Test the sold commodity historical value update when broadcast is completed.
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSoldCommodityUpdateOnCompleteBroadcst()
            throws InterruptedException, HistoryCalculationException {
        final TopologyEntity dp1 = mockEntity(EntityType.DESKTOP_POOL_VALUE, OID1,
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build(),
                SOLD_CAPACITY, 0d, null, null, null, null, true);

        final TopologyEntity dp2 = mockEntity(EntityType.DESKTOP_POOL_VALUE, OID2,
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.POOL_MEM_VALUE).build(),
                SOLD_CAPACITY, 0d, null, null, null, null, true);

        final TopologyEntity dp3 = mockEntity(EntityType.DESKTOP_POOL_VALUE, OID3,
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.POOL_STORAGE_VALUE).build(),
                SOLD_CAPACITY, 0d, null, null, null, null, true);

        Map<Long, Builder> topologyBuilderMap = new HashMap<>();
        Map<Long, EntitySettings> entitySettings = new HashMap<>();

        final TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(dp1, dp2, dp3));
        addEntityWithSetting(OID1,
                EntityType.DESKTOP_POOL_VALUE,
                EntitySettingSpecs.DailyObservationWindowDesktopPool,
                SLOTS.ordinal(),
                topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID2,
                EntityType.DESKTOP_POOL_VALUE,
                EntitySettingSpecs.DailyObservationWindowDesktopPool,
                SLOTS.ordinal(),
                topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID3,
                EntityType.DESKTOP_POOL_VALUE,
                EntitySettingSpecs.DailyObservationWindowDesktopPool,
                SLOTS.ordinal(),
                topologyBuilderMap, entitySettings);
        GraphWithSettings graphWithSettings = new GraphWithSettings(graph,
                entitySettings,
                Collections.emptyMap());

        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        TimeslotEditorCacheAccess editor = new TimeslotEditorCacheAccess(config, null,
                        backgroundLoadingPool, notificationSender);
        editor.completeBroadcast(context);

        // verify used
        Assert.assertNotNull(dp1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed());
        Assert.assertEquals(SLOTS.ordinal(), dp1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getTimeSlotCount());
        Assert.assertNotNull(dp2.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed());
        Assert.assertEquals(SLOTS.ordinal(), dp2.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getTimeSlotCount());
        Assert.assertNotNull(dp3.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed());
        Assert.assertEquals(SLOTS.ordinal(), dp3.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getHistoricalUsed().getTimeSlotCount());

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
        final TimeslotEditorCacheAccess editor =
                        new TimeslotEditorCacheAccess(config, null, backgroundLoadingPool,
                                        notificationSender,
                                        (client) -> new TestTimeSlotLoadingTask(client, latch));
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
        final TimeslotEditorCacheAccess editor =
                        new TimeslotEditorCacheAccess(config, null, backgroundLoadingPool,
                                        notificationSender,
                                        (client) -> new TestTimeSlotLoadingTask(client, latch));
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
                                        notificationSender,
                                        (client) -> new TestTimeSlotLoadingTask(client, latch,
                                                        discoveryLatch));
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
         * @param notificationSender sends notification about failures.
         * @param loadingTaskSupplier supplier for loading task.
         */
        private TimeslotEditorCacheAccess(TimeslotHistoricalEditorConfig config,
                        StatsHistoryServiceBlockingStub statsHistoryClient,
                        ExecutorService backgroundLoadingPool,
                        TopologyProcessorNotificationSender notificationSender,
                        Function<StatsHistoryServiceBlockingStub, TimeSlotLoadingTask> loadingTaskSupplier) {
            super(config, statsHistoryClient, backgroundLoadingPool, loadingTaskSupplier);
        }

        /**
         * Construct the instance.
         *
         * @param config configuration parameters
         * @param statsHistoryClient remote persistence
         * @param backgroundLoadingPool pool for background loading tasks.
         * @param notificationSender sends notification about failures.
         */
        private TimeslotEditorCacheAccess(TimeslotHistoricalEditorConfig config,
                        StatsHistoryServiceBlockingStub statsHistoryClient,
                        ExecutorService backgroundLoadingPool,
                        TopologyProcessorNotificationSender notificationSender) {
            this(config, statsHistoryClient, backgroundLoadingPool, notificationSender,
                            TimeSlotLoadingTask::new);
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


        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        CountDownLatch taskCompletionLatch, CountDownLatch broadcastLatch) {
            super(client);
            this.taskCompletionLatch = taskCompletionLatch;
            this.broadcastLatch = broadcastLatch;
        }

        private TestTimeSlotLoadingTask(StatsHistoryServiceBlockingStub client,
                        CountDownLatch taskCompletionLatch) {
            this(client, taskCompletionLatch, null);

        }

        @Nonnull
        @Override
        public Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> load(
                        @Nonnull Collection<EntityCommodityReference> commodities,
                        @Nonnull TimeslotHistoricalEditorConfig config)
                        throws HistoryCalculationException {
            try {
                if (broadcastLatch != null) {
                    broadcastLatch.await();
                    return Collections.emptyMap();
                }
                return super.load(commodities, config);
            } catch (InterruptedException e) {
                throw new HistoryCalculationException("Test has been interrupted", e);
            } finally {
                taskCompletionLatch.countDown();
            }
        }
    }
}
