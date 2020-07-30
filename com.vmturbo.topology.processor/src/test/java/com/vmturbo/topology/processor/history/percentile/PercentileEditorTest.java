package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor.CacheBackup;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit test for {@link PercentileEditor}.
 */
public class PercentileEditorTest extends BaseGraphRelatedTest {
    private static final long TIMESTAMP_AUG_28_2019_12_00 = 1566993600000L;
    private static final long TIMESTAMP_AUG_29_2019_00_00 = 1567036800000L;
    private static final long TIMESTAMP_AUG_29_2019_06_00 = 1567058400000L;
    private static final long TIMESTAMP_AUG_29_2019_12_00 = 1567080000000L;
    private static final long TIMESTAMP_AUG_30_2019_00_00 = 1567123200000L;
    private static final long TIMESTAMP_AUG_30_2019_12_00 = 1567166400000L;
    private static final long TIMESTAMP_AUG_31_2019_00_00 = 1567209600000L;
    private static final long TIMESTAMP_AUG_31_2019_12_00 = 1567252800000L;
    private static final long TIMESTAMP_INIT_START_SEP_1_2019 = 1567296000000L;

    private static final int MAINTENANCE_WINDOW_HOURS = 12;
    private static final long MAINTENANCE_WINDOW_MS = TimeUnit.HOURS.toMillis(MAINTENANCE_WINDOW_HOURS);
    private static final String PERCENTILE_BUCKETS_SPEC = "0,1,5,99,100";
    private static final KVConfig KV_CONFIG = createKvConfig();
    private static final PercentileHistoricalEditorConfig PERCENTILE_HISTORICAL_EDITOR_CONFIG =
                    new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 777777L, 10,
                                    100,
                                    ImmutableMap.of(CommodityType.VCPU, PERCENTILE_BUCKETS_SPEC,
                                                    CommodityType.IMAGE_CPU,
                                                    PERCENTILE_BUCKETS_SPEC), KV_CONFIG,
                                    Clock.systemUTC());

    private static final long VIRTUAL_MACHINE_OID = 1;
    private static final long VIRTUAL_MACHINE_OID_2 = 2;
    private static final long BUSINESS_USER_OID = 10;
    private static final long BUSINESS_USER_OID_2 = 11;
    private static final long DATABASE_SERVER_OID = 100;
    private static final long DATABASE_OID = 500;
    private static final long CONTAINER_OID = 1000;
    private static final long CONTAINER_POD_OID = 2000;
    private static final long DESKTOP_POOL_PROVIDER_OID = 3;
    private static final long PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 2;
    private static final long NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 4;
    private static final long PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD = 4;
    private static final long NEW_BUSINESS_USER_OBSERVATION_PERIOD = 2;
    private static final float CAPACITY = 1000F;
    private static final String DESKTOP_POOL_COMMODITY_KEY = "DesktopPool::key";
    private static final EntityCommodityFieldReference VCPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(VIRTUAL_MACHINE_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .build(), CommodityField.USED);
    private static final EntityCommodityFieldReference IMAGE_CPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(BUSINESS_USER_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .setKey(DESKTOP_POOL_COMMODITY_KEY)
                            .build(), DESKTOP_POOL_PROVIDER_OID, CommodityField.USED);

    private final Clock clock = Mockito.mock(Clock.class);
    private Map<Long, Builder> topologyBuilderMap;
    private Map<Long, EntitySettings> entitySettings;
    private GraphWithSettings graphWithSettings;
    private PercentileEditorCacheAccess percentileEditor;
    private List<PercentilePersistenceTask> percentilePersistenceTasks;
    private TopologyInfo topologyInfo;

    /**
     * Set up the test.
     */
    @Before
    public void setUp() {
        setUpTopology();
        percentilePersistenceTasks = new ArrayList<>();
        percentileEditor =
                        new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG, null,
                                        clock, (service, range) -> {
                            final PercentileTaskStub result =
                                            Mockito.spy(new PercentileTaskStub(service, range));
                            percentilePersistenceTasks.add(result);
                            return result;
                        });
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(77777L).build();
    }

    private void setUpTopology() {
        topologyBuilderMap = new HashMap<>();
        entitySettings = new HashMap<>();
        addEntityWithSetting(VIRTUAL_MACHINE_OID,
                             EntityType.VIRTUAL_MACHINE_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodVirtualMachine,
                             PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                             topologyBuilderMap, entitySettings);
        addEntityWithSetting(BUSINESS_USER_OID,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodBusinessUser,
                             PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD,
                             topologyBuilderMap, entitySettings);
        graphWithSettings = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                                                                  .newGraph(topologyBuilderMap),
                                                  entitySettings,
                                                  Collections.emptyMap());
    }

    /**
     * Test the generic applicability of percentile calculation.
     */
    @Test
    public void testIsApplicable() {
        Assert.assertTrue(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder().build(), null));

        // Percentile should be set for custom plan project is created by the user
        Assert.assertTrue(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder()
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanProjectType(PlanProjectType.USER))
                        .build(), null));

        // Percentile should not be set for cluster headroom plans
        Assert.assertFalse(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder()
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                        .build(), null));

        Assert.assertTrue(percentileEditor.isApplicable(
                Collections.singletonList(ScenarioChange.newBuilder().build()),
                TopologyInfo.newBuilder().build(), null));

        // Percentile should not be set for baseline
        Assert.assertFalse(percentileEditor.isApplicable(Collections.singletonList(
                ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder()
                                .setHistoricalBaseline(
                                        HistoricalBaseline.newBuilder().setBaselineDate(0).build()))
                        .build()), TopologyInfo.newBuilder().build(), null));
    }

    /**
     * Test the applicability of percentile calculation at the entity level.
     */
    @Test
    public void testIsEntityApplicable() {
        // Percentile should not be set for database server entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOid(DATABASE_SERVER_OID)
                        .setEntityType(EntityType.DATABASE_SERVER_VALUE)).build()));

        // Percentile should not be set for database entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(DATABASE_OID)
                .setEntityType(EntityType.DATABASE_VALUE)).build()));

        // Percentile should not be set for container entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(CONTAINER_OID)
                .setEntityType(EntityType.CONTAINER_VALUE)).build()));

        // Percentile should not be set for container pod entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(CONTAINER_POD_OID)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)).build()));

        // Percentile should be set for virtual machine entities
        Assert.assertTrue(percentileEditor.isEntityApplicable(
                topologyBuilderMap.get(VIRTUAL_MACHINE_OID).build()));

        // Percentile should be set for business user entities
        Assert.assertTrue(percentileEditor.isEntityApplicable(
            topologyBuilderMap.get(BUSINESS_USER_OID).build()));
    }

    /**
     * Test the initial data loading.
     * That requests to load full and latest window blobs are made and results are accumulated.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testLoadData() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db.
        percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                        false), Collections.emptyList());
        // LATEST(1, 2, 3, 4, 5) + TOTAL(41, 42, 43, 44, 45) = FULL(42, 44, 46, 48, 50)
        Assert.assertEquals(Arrays.asList(42, 44, 46, 48, 50),
                percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet(), true)
                        .build()
                        .getUtilizationList());
        // LATEST(46, 47, 48, 49, 50) + TOTAL(86, 87, 88, 89, 90) = FULL(132, 134, 136, 138, 140)
        Assert.assertEquals(Arrays.asList(132, 134, 136, 138, 140),
                percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet(), true)
                        .build()
                        .getUtilizationList());
    }

    /**
     * Test that {@link PercentileEditor} correctly handles case when observation period of entity
     * changed. Particularly checks that in case observation window changed before next maintenance
     * operation will happen than oldest day will not be subtracted from total utilization points,
     * i.e. total blob from cache will be sent to database without any changes.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChangedSaveTotal()
                    throws InterruptedException, HistoryCalculationException {
        final List<Integer> buUtilizations = Arrays.asList(214, 218, 222, 226, 230);
        final List<Integer> vmUtilizations = Arrays.asList(148, 156, 164, 172, 180);
        checkObservationWindowChanged(TIMESTAMP_INIT_START_SEP_1_2019 + 2000,
                                      TIMESTAMP_INIT_START_SEP_1_2019,
                                      Arrays.asList(vmUtilizations, buUtilizations), buUtilizations,
                                      vmUtilizations,
                                      true, 13, 12);
    }

    /**
     * Test that {@link PercentileEditor} correctly handles case when observation period of entity
     * changed. Particularly checks that in case observation window changed after timestamp when next maintenance
     * operation was scheduled than oldest day will be subtracted from total utilization points,
     * i.e. 'normal' maintenance workflow will be passed.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChangedSaveTotalWithRemoval()
            throws InterruptedException, HistoryCalculationException {
        final long nextScheduledCheckpointMs =
                TIMESTAMP_INIT_START_SEP_1_2019 + TimeUnit.HOURS.toMillis(
                        PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours());
        checkObservationWindowChanged(nextScheduledCheckpointMs + 2000, nextScheduledCheckpointMs,
                                      Arrays.asList(Arrays.asList(112, 119, 126, 133, 140),
                                      Arrays.asList(153, 156, 159, 162, 165)),
                                      Arrays.asList(214, 218, 222, 226, 230),
                                      Arrays.asList(148, 156, 164, 172, 180),
                                      false, 10, 13);
    }

    private void checkObservationWindowChanged(long broadcastTimeAfterWindowChanged,
                                               long periodMsForTotalBlob,
                                               List<List<Integer>> expectedTotalUtilizations,
                                               List<Integer> buUtilizations,
                                               List<Integer> vmUtilizations,
                                               boolean enforceMaintenanceIsExpected,
                                               int dayPersistenceIdx,
                                               int totalPersistenceIdx)
                    throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        final HistoryAggregationContext firstContext =
                        new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        // First initializing history from db.
        percentileEditor.initContext(firstContext, Collections.emptyList());
        // Necessary to set last checkpoint timestamp.
        percentileEditor.completeBroadcast(firstContext);

        // Change observation periods.
        entitySettings.put(VIRTUAL_MACHINE_OID,
                createEntitySetting(VIRTUAL_MACHINE_OID,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                        EntitySettingSpecs.MaxObservationPeriodVirtualMachine));
        entitySettings.put(BUSINESS_USER_OID,
                createEntitySetting(BUSINESS_USER_OID,
                        NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                        EntitySettingSpecs.MaxObservationPeriodBusinessUser));

        // Check observation periods changed.
        final HistoryAggregationContext secondContext =
                        new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        percentileEditor.initContext(secondContext,
                        Collections.emptyList());
        // Check full utilization count array for virtual machine VCPU commodity.
        // 36 37 38 39 40 [x] 28 Aug 2019 12:00:00 GMT.
        // 31 32 33 34 35 [x] 29 Aug 2019 00:00:00 GMT.
        // 26 27 28 29 30 [x] 29 Aug 2019 12:00:00 GMT.
        // 21 22 23 24 25 [x] 30 Aug 2019 00:00:00 GMT.
        // 16 14 18 19 20 [x] 30 Aug 2019 12:00:00 GMT.
        // 11 12 13 14 15 [x] 31 Aug 2019 00:00:00 GMT.
        //  6  7  8  9 10 [x] 31 Aug 2019 12:00:00 GMT.
        //  1  2  3  4  5 [x]  1 Sep 2019 00:00:00 GMT.
        // -----------------------------------------
        // 148, 156, 164, 172, 180 - full for 4 days of observation.
        checkEntityUtilization(vmUtilizations, VCPU_COMMODITY_REFERENCE,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD);
        // Check full utilization count array for business user ImageCPU commodity.
        // 81 82 83 84 85 [ ] 28 Aug 2019 12:00:00 GMT.
        // 76 77 78 79 80 [ ] 29 Aug 2019 00:00:00 GMT.
        // 71 72 73 74 75 [ ] 29 Aug 2019 12:00:00 GMT.
        // 66 67 68 69 70 [ ] 30 Aug 2019 00:00:00 GMT.
        // 61 62 63 64 65 [x] 30 Aug 2019 12:00:00 GMT.
        // 56 57 58 59 60 [x] 31 Aug 2019 00:00:00 GMT.
        // 51 52 53 54 55 [x] 31 Aug 2019 12:00:00 GMT.
        // 46 47 48 49 50 [x]  1 Sep 2019 00:00:00 GMT.
        // -----------------------------------------
        // 214 218 222 226 230 - full for 2 days of observation.
        checkEntityUtilization(buUtilizations, IMAGE_CPU_COMMODITY_REFERENCE,
                        NEW_BUSINESS_USER_OBSERVATION_PERIOD);
        // Check that maintenance will be called
        Mockito.when(clock.millis()).thenReturn(broadcastTimeAfterWindowChanged);
        final CacheBackup cacheBackup = Mockito.mock(CacheBackup.class);
        final PercentileEditorCacheAccess spiedEditor = Mockito.spy(percentileEditor);
        Mockito.when(spiedEditor.createCacheBackup()).thenReturn(cacheBackup);
        spiedEditor.completeBroadcast(secondContext);
        checkMaintenance(periodMsForTotalBlob, expectedTotalUtilizations,
            enforceMaintenanceIsExpected, dayPersistenceIdx, totalPersistenceIdx);
        Mockito.verify(cacheBackup, Mockito.times(1)).keepCacheOnClose();
    }

    /**
     * Tests the case were the capacity for percentile data changes and we the lookback period
     * changes. We need to ensure that we used new capacity rather than outdated capacity in DB.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void checkObservationPeriodMaintenanceWindowChanged()
            throws InterruptedException, HistoryCalculationException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        final HistoryAggregationContext firstContext =
            new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        // First initializing history from db.
        percentileEditor.initContext(firstContext, Collections.emptyList());
        // Necessary to set last checkpoint timestamp.
        percentileEditor.completeBroadcast(firstContext);

        // Change the capacity
        UtilizationCountStore store =
            percentileEditor.getCache().get(VCPU_COMMODITY_REFERENCE).getUtilizationCountStore();
        store.addPoints(Collections.singletonList(20d), CAPACITY * 2,
            TIMESTAMP_INIT_START_SEP_1_2019);

        // Change observation periods.
        entitySettings.put(VIRTUAL_MACHINE_OID,
            createEntitySetting(VIRTUAL_MACHINE_OID,
                NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                EntitySettingSpecs.MaxObservationPeriodVirtualMachine));

        // Check observation periods changed.
        final HistoryAggregationContext secondContext =
            new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        percentileEditor.initContext(secondContext,
            Collections.emptyList());

        // Ensure the capacity it the updated one
        Assert.assertThat(store.getLatestCountsRecord().getCapacity(), Matchers.is(CAPACITY * 2));
        PercentileRecord.Builder fullRecord = store.checkpoint(Collections.emptyList(), false);
        Assert.assertThat(fullRecord.getCapacity(), Matchers.is(CAPACITY * 2));
    }

    private void checkMaintenance(long periodMsForTotalBlob,
                                  List<List<Integer>> expectedTotalUtilizations,
                                  boolean enforceMaintenanceIsExpected, int dayPersistenceIdx,
                                  int totalPersistenceIdx)
                    throws HistoryCalculationException, InterruptedException {
        // Verify total persistence has been called
        final ArgumentCaptor<PercentileCounts> percentileCountsCaptor =
                        ArgumentCaptor.forClass(PercentileCounts.class);
        final ArgumentCaptor<Long> periodCaptor = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(percentilePersistenceTasks.get(totalPersistenceIdx), Mockito.atLeastOnce())
                .save(percentileCountsCaptor.capture(), periodCaptor.capture(), Mockito.any());
         final List<PercentileCounts> capturedCounts = percentileCountsCaptor.getAllValues();
        final List<Long> capturedPeriods = periodCaptor.getAllValues();
        final PercentileCounts counts = capturedCounts.get(0);
        final List<PercentileRecord> percentileRecords = counts.getPercentileRecordsList();
        final Set<Long> periods =
                        percentileRecords.stream().filter(Objects::nonNull)
                                        .map(r -> (long)r.getPeriod())
                                        .collect(Collectors.toSet());
        Assert.assertThat(capturedPeriods.get(0),
                        CoreMatchers.is(periodMsForTotalBlob));
        Assert.assertThat(percentileRecords.stream()
                                        .sorted(Comparator.comparingLong(PercentileRecord::getEntityOid))
                                        .map(PercentileRecord::getUtilizationList).collect(Collectors.toList()),
                        CoreMatchers.is(expectedTotalUtilizations));
        Assert.assertThat(periods.size(), CoreMatchers.is(2));
        Assert.assertThat(periods, Matchers.containsInAnyOrder(NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD));

        Mockito.verify(percentilePersistenceTasks.get(dayPersistenceIdx), Mockito.atLeastOnce())
            .save(percentileCountsCaptor.capture(), periodCaptor.capture(), Mockito.any());
        checkMaintenance(enforceMaintenanceIsExpected, periodCaptor, capturedCounts.get(1));
    }

    private static void checkMaintenance(boolean enforceMaintenanceIsExpected,
                    ArgumentCaptor<Long> periodCaptor, PercentileCounts maintenanceCurrentDay) {
        final List<PercentileRecord> currentDayRecords =
                        maintenanceCurrentDay.getPercentileRecordsList();
        final Set<Long> currentDayPeriods = currentDayRecords.stream().filter(Objects::nonNull)
                        .map(r -> (long)r.getPeriod()).collect(Collectors.toSet());
        Assert.assertThat(currentDayPeriods.size(), CoreMatchers.is(1));
        Assert.assertThat(currentDayPeriods, Matchers.containsInAnyOrder(1L));
        Assert.assertThat(currentDayRecords.stream().map(PercentileRecord::getUtilizationList)
                                        .flatMap(Collection::stream).collect(Collectors.toSet()),
                        CoreMatchers.is(Collections.singleton(0)));

        if (enforceMaintenanceIsExpected) {
            final int indexOfYesterdaySave = 1;
            final long period = periodCaptor.getAllValues().get(indexOfYesterdaySave);
            Assert.assertEquals(period, MAINTENANCE_WINDOW_MS);
        }
    }

    private void checkEntityUtilization(List<Integer> utilizations,
                    EntityCommodityFieldReference commodityReference, long newObservationPeriod)
                    throws HistoryCalculationException {
        final PercentileRecord percentileRecord = percentileEditor.getCacheEntry(commodityReference)
                        .getUtilizationCountStore().checkpoint(Collections.emptySet(), true).build();
        Assert.assertEquals(utilizations, percentileRecord.getUtilizationList());
        Assert.assertEquals(newObservationPeriod, percentileRecord.getPeriod());
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast()} fails if
     * object is not initialized.
     *
     * @throws HistoryCalculationException always
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastFailsIfObjectIsNotInitialized()
                    throws HistoryCalculationException, InterruptedException {
        final ArgumentCaptor<Long> periodCaptor = ArgumentCaptor.forClass(Long.class);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.completeBroadcast(context);
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1))
                        .save(Mockito.any(), periodCaptor.capture(), Mockito.any());
        Assert.assertThat(periodCaptor.getValue(), CoreMatchers.is(MAINTENANCE_WINDOW_MS));
    }

    /**
     * Tests that {@link PercentileEditor.CacheBackup} doesn't restore cache after call of
     * {@link PercentileEditor.CacheBackup#keepCacheOnClose()}.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCacheBackupSuccessCase()
                    throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        final EntityCommodityFieldReference mockReference =
                        Mockito.mock(EntityCommodityFieldReference.class);
        try (PercentileEditor.CacheBackup cacheBackup = new PercentileEditor.CacheBackup(percentileEditor.getCache())) {
            percentileEditor.getCache().put(mockReference,
                                            Mockito.mock(PercentileCommodityData.class));
            cacheBackup.keepCacheOnClose();
        }

        Assert.assertTrue(percentileEditor.getCache().containsKey(mockReference));
    }

    /**
     * Tests that {@link PercentileEditor.CacheBackup} restores cache after call of
     * {@link PercentileEditor.CacheBackup#close()}. Also checks that two subsequent calls
     * of {@link PercentileEditor.CacheBackup#close()} doesn't emit a cache backup.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCacheBackupFailureCase() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        final Map<EntityCommodityFieldReference, PercentileCommodityData> originalCache =
                        percentileEditor.getCache();
        final PercentileEditor.CacheBackup cacheBackup =
                        new PercentileEditor.CacheBackup(originalCache);
        final EntityCommodityFieldReference mock =
                        Mockito.mock(EntityCommodityFieldReference.class);
        originalCache.put(mock, Mockito.mock(PercentileCommodityData.class));
        cacheBackup.close();
        Assert.assertFalse(originalCache.containsKey(mock));

        // Check that double close don't override original cache
        originalCache.put(mock, Mockito.mock(PercentileCommodityData.class));
        cacheBackup.close();
        Assert.assertTrue(originalCache.containsKey(mock));
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast()} works correctly
     * when half of maintenance windows passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastTooSoon() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_06_00);

        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // We should update LATEST with the latest values we have.
        final ArgumentCaptor<PercentileCounts> latestCaptor =
                        ArgumentCaptor.forClass(PercentileCounts.class);
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(latestCaptor.capture(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
        final PercentileCounts latest = latestCaptor.getValue();
        Assert.assertEquals(2, latest.getPercentileRecordsList().size());

        Assert.assertThat(percentilePersistenceTasks.size(), CoreMatchers.is(1));

        // We don't update TOTAL because there is not right time now.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.never()).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_29_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Maintenance shouldn't load any data.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.never())
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast()} works correctly
     * when one maintenance windows passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastOneMaintenanceWindow() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_12_00);

        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // Save current LATEST day percentiles with one write.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We do exactly one TOTAL write to DB when one maintenance windows passed since last checkpoint.
        Mockito.verify(percentilePersistenceTasks.get(3), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_29_2019_12_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We load exactly two times in maintenance because we have two different periods in graph.
        Mockito.verify(percentilePersistenceTasks.get(1), Mockito.times(1))
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
        Mockito.verify(percentilePersistenceTasks.get(2), Mockito.times(1))
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast()} works correctly
     * when several maintenance windows passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastTwoMaintenanceWindows() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_30_2019_00_00);

        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // Update LATEST day once.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Normally we should do exactly one TOTAL write even if two maintenance windows passed since last checkpoint.
        Mockito.verify(percentilePersistenceTasks.get(5), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_30_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Two periods * two maintenance window = exactly four invocations.
        for (int invocation = 1; invocation < 5; invocation++) {
            Mockito.verify(percentilePersistenceTasks.get(invocation), Mockito.times(1))
                            .load(Mockito.any(),
                                            Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
        }
    }

    /**
     * Checks that exporting and loading of percentile diagnostics is working as expected.
     *
     * @throws DiagnosticsException in case of error during diagnostics
     *                 export/import
     * @throws InterruptedException in case test has been interrupted.
     * @throws HistoryCalculationException in case context initialization failed.
     * @throws IOException diagnostics collection could not write state into stream.
     */
    @Test
    public void checkRestoreDiags()
                    throws DiagnosticsException, InterruptedException, HistoryCalculationException,
                    IOException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db.
        percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                        false), Collections.emptyList());
        createKvConfig();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            percentileEditor.collectDiags(output);
            percentileEditor.restoreDiags(output.toByteArray());
        }
        // LATEST(1, 2, 3, 4, 5) + TOTAL(42, 44, 46, 48, 50) = FULL(43, 46, 49, 52, 55)
        Assert.assertEquals(Arrays.asList(43, 46, 49, 52, 55),
                        percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                                        .getUtilizationCountStore()
                                        .checkpoint(Collections.emptySet(), true)
                                        .build()
                                        .getUtilizationList());
        // LATEST(46, 47, 48, 49, 50) + TOTAL(132, 134, 136, 138, 140) = FULL(178, 181, 184, 187, 190)
        Assert.assertEquals(Arrays.asList(178, 181, 184, 187, 190),
                        percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                                        .getUtilizationCountStore()
                                        .checkpoint(Collections.emptySet(), true)
                                        .build()
                                        .getUtilizationList());
    }

    private static KVConfig createKvConfig() {
        final KVConfig result = Mockito.mock(KVConfig.class);
        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);
        Mockito.when(result.keyValueStore()).thenReturn(kvStore);
        Mockito.when(kvStore.get(Mockito.any())).thenAnswer(invocation -> {
            final String parameter = invocation.getArgumentAt(0, String.class);
            if (parameter.equals(
                            CachingHistoricalEditorConfig.STORE_CACHE_TO_DIAGNOSTICS_PROPERTY)) {
                return Optional.of("true");
            }
            return Optional.empty();
        });
        return result;
    }

    /**
     * Tests the case where the entity in the plan has been cloned from another entity. It
     * ensures that the percentile has been set for the entity.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testPercentileEditorPlanContext() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            // ARRANGE
            Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
            // First initializing history from db.
            percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                false), Collections.emptyList());
            // in our setup the entity with VIRTUAL_MACHINE_OID is a clone of a virtual machine
            // with live entity OID of VIRTUAL_MACHINE_OID_2
            final Map<Long, TopologyEntity.Builder> topologyMap = createTopologyMap();
            final TopologyInfo planTopologyInfo =
                TopologyInfo.newBuilder().setTopologyId(71111L).build();
            final GraphWithSettings graph = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                .newGraph(topologyMap), entitySettings, Collections.emptyMap());
            final HistoryAggregationContext context = new HistoryAggregationContext(planTopologyInfo,
                graph, true);

            List<EntityCommodityReference> commodities = Arrays.asList(
                new EntityCommodityReference(VIRTUAL_MACHINE_OID_2,
                    TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE).build(),
                    null),
                new EntityCommodityFieldReference(BUSINESS_USER_OID_2,
                    TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY)
                        .build(), DESKTOP_POOL_PROVIDER_OID, CommodityField.USED)
            );

            // ACT
            percentileEditor.createPreparationTasks(context, commodities);

            List<? extends Callable<List<Void>>> callableList =
                percentileEditor.createCalculationTasks(context, commodities);

            List<Future<List<Void>>> futureList = executor.invokeAll(callableList);
            for (Future<List<Void>> future : futureList) {
                future.get();
            }

            // ASSERT
            Assert.assertTrue(topologyMap.get(VIRTUAL_MACHINE_OID_2)
                .getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().hasPercentile());
            Assert.assertThat(topologyMap.get(VIRTUAL_MACHINE_OID_2)
                .getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().getPercentile(),
                Matchers.closeTo(1.0, 0.001));
            Assert.assertTrue(topologyMap.get(BUSINESS_USER_OID_2).getEntityBuilder()
                .getCommoditiesBoughtFromProvidersBuilder(0)
                .getCommodityBought(0)
                .getHistoricalUsed()
                .hasPercentile());
            Assert.assertThat(topologyMap.get(BUSINESS_USER_OID_2).getEntityBuilder()
                    .getCommoditiesBoughtFromProvidersBuilder(0)
                    .getCommodityBought(0)
                    .getHistoricalUsed()
                    .getPercentile(),
                Matchers.closeTo(1.0, 0.001));
        } finally {
            executor.shutdownNow();
        }
    }

    private HashMap<Long, Builder> createTopologyMap() {
        HashMap<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(VIRTUAL_MACHINE_OID_2, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(VIRTUAL_MACHINE_OID_2)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(
                    TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE))
                        .setCapacity(CAPACITY))
            )
                .setClonedFromEntity(TopologyEntityDTO.newBuilder().setOid(VIRTUAL_MACHINE_OID))
        );

        topologyMap.put(BUSINESS_USER_OID_2, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(BUSINESS_USER_OID_2)
                .setEntityType(EntityType.BUSINESS_USER_VALUE)
                .addCommoditiesBoughtFromProviders(TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(DESKTOP_POOL_PROVIDER_OID)
                    .addCommodityBought(TopologyDTO.CommodityBoughtDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY))))
            )
                .setClonedFromEntity(TopologyEntityDTO.newBuilder().setOid(BUSINESS_USER_OID))
        );

        topologyMap.put(DESKTOP_POOL_PROVIDER_OID, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(DESKTOP_POOL_PROVIDER_OID)
                .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                .addCommoditySoldList(
                    TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY))
                        .setCapacity(CAPACITY))
            )
        );
        return topologyMap;
    }

    /**
     * Access to the percentile editor cached data.
     */
    private static class PercentileEditorCacheAccess extends PercentileEditor {
        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                StatsHistoryServiceStub statsHistoryClient, Clock clock,
                BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, PercentilePersistenceTask> taskCreator) {
            super(config, statsHistoryClient, clock, taskCreator);
        }

        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                                    StatsHistoryServiceStub statsHistoryClient, Clock clock) {
            this(config, statsHistoryClient, clock, PercentileTaskStub::new);
        }

        PercentileCommodityData getCacheEntry(EntityCommodityFieldReference field) {
            return getCache().get(field);
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileCommodityData> getCache() {
            return super.getCache();
        }
    }

    /**
     * Helper for load/save operations without use to GRPC.
     */
    private static class PercentileTaskStub extends PercentilePersistenceTask {
        // Entity OID -> timestamp -> utilization counts array.
        private static final Map<Long, Map<Long, List<Integer>>> DEFAULT_UTILISATION;
        private static final List<Long> KNOWN_TIMESTAMPS =
                        Arrays.asList(TIMESTAMP_INIT_START_SEP_1_2019, TIMESTAMP_AUG_31_2019_12_00,
                                        TIMESTAMP_AUG_31_2019_00_00, TIMESTAMP_AUG_30_2019_12_00,
                                        TIMESTAMP_AUG_30_2019_00_00, TIMESTAMP_AUG_29_2019_12_00,
                                        TIMESTAMP_AUG_29_2019_00_00, TIMESTAMP_AUG_28_2019_12_00,
                                        PercentilePersistenceTask.TOTAL_TIMESTAMP);
        private final Map<Long, Map<Long, List<Integer>>> currentUtilization;
        private Map<EntityCommodityFieldReference, PercentileRecord> resultForLoad;

        static {
            // Virtual Machine - VCPU
            // 41 42 43 44 45 - TOTAL
            // 36 37 38 39 40 - 28 Aug 2019 12:00:00 GMT.
            // 31 32 33 34 35 - 29 Aug 2019 00:00:00 GMT.
            // 26 27 28 29 30 - 29 Aug 2019 12:00:00 GMT.
            // 21 22 23 24 25 - 30 Aug 2019 00:00:00 GMT.
            // 16 14 18 19 20 - 30 Aug 2019 12:00:00 GMT.
            // 11 12 13 14 15 - 31 Aug 2019 00:00:00 GMT.
            //  6  7  8  9 10 - 31 Aug 2019 12:00:00 GMT.
            //  1  2  3  4  5 -  1 Sep 2019 00:00:00 GMT. - LATEST

            // Business User - ImageCPU
            // 86 87 88 89 90 - TOTAL
            // 81 82 83 84 85 - 28 Aug 2019 12:00:00 GMT.
            // 76 77 78 79 80 - 29 Aug 2019 00:00:00 GMT.
            // 71 72 73 74 75 - 29 Aug 2019 12:00:00 GMT.
            // 66 67 68 69 70 - 30 Aug 2019 00:00:00 GMT.
            // 61 62 63 64 65 - 30 Aug 2019 12:00:00 GMT.
            // 56 57 58 59 60 - 31 Aug 2019 00:00:00 GMT.
            // 51 52 53 54 55 - 31 Aug 2019 12:00:00 GMT.
            // 46 47 48 49 50 -  1 Sep 2019 00:00:00 GMT. - LATEST

            DEFAULT_UTILISATION = new HashMap<>();
            int i = 0;
            final List<Long> entities = Arrays.asList(VIRTUAL_MACHINE_OID, BUSINESS_USER_OID);
            for (long entityOid : entities) {
                final Map<Long, List<Integer>> timestampToUtilization = new HashMap<>();
                DEFAULT_UTILISATION.put(entityOid, timestampToUtilization);
                for (long timestamp : KNOWN_TIMESTAMPS) {
                    timestampToUtilization.put(timestamp, Arrays.asList(++i, ++i, ++i, ++i, ++i));
                }
            }
        }

        PercentileTaskStub(StatsHistoryServiceStub unused, Pair<Long, Long> range) {
            super(unused, range);
            currentUtilization = new HashMap<>(DEFAULT_UTILISATION);
            PercentileRecord virtualMachinePercentileRecord = PercentileRecord.newBuilder()
                            .setEntityOid(VIRTUAL_MACHINE_OID)
                            .setCommodityType(VCPU_COMMODITY_REFERENCE.getCommodityType().getType())
                            .setCapacity(CAPACITY)
                            .setPeriod((int)PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD)
                            .build();

            PercentileRecord businessUserPercentileRecord = PercentileRecord.newBuilder()
                            .setEntityOid(BUSINESS_USER_OID)
                            .setCommodityType(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getType())
                            .setProviderOid(IMAGE_CPU_COMMODITY_REFERENCE.getProviderOid())
                            .setKey(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getKey())
                            .setCapacity(CAPACITY)
                            .setProviderOid(DESKTOP_POOL_PROVIDER_OID)
                            .setPeriod((int)PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD)
                            .build();

            resultForLoad = new HashMap<>();
            resultForLoad.put(new EntityCommodityFieldReference(VCPU_COMMODITY_REFERENCE,
                                                                CommodityField.USED),
                              virtualMachinePercentileRecord);
            resultForLoad.put(new EntityCommodityFieldReference(IMAGE_CPU_COMMODITY_REFERENCE, CommodityField.USED),
                              businessUserPercentileRecord);
        }

        public Map<EntityCommodityFieldReference, PercentileRecord> getResultForLoad() {
            return resultForLoad;
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileRecord> load(
                        @Nonnull Collection<EntityCommodityReference> commodities,
                        @Nonnull PercentileHistoricalEditorConfig config) {
            // Current implementation doesn't use this parameter.
            // If this assertion fails then most likely the implementation changed and
            // you should add more unit tests for that.
            Assert.assertTrue(commodities.isEmpty());

            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> entry : resultForLoad
                            .entrySet()) {
                EntityCommodityFieldReference entityCommodityFieldReference = entry.getKey();
                PercentileRecord percentileRecord = entry.getValue();
                final long startTimestamp = KNOWN_TIMESTAMPS.contains(getStartTimestamp()) ?
                                getStartTimestamp() :
                                PercentilePersistenceTask.TOTAL_TIMESTAMP;
                final List<Integer> utilizations =
                                currentUtilization.get(entityCommodityFieldReference.getEntityOid())
                                                .get(startTimestamp);

                if (utilizations == null) {
                    continue;
                }
                entry.setValue(percentileRecord.toBuilder().clearUtilization().addAllUtilization(
                                utilizations).build());
            }
            return Collections.unmodifiableMap(resultForLoad);
        }

        @Override
        public void save(@Nonnull PercentileCounts counts,
                         long periodMs,
                         @Nonnull PercentileHistoricalEditorConfig config) {
            // UNUSED
        }
    }
}
