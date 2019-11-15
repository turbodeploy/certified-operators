package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Units;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit test for {@link PercentileEditor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StatsHistoryServiceStub.class})
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
    private static final String PERCENTILE_BUCKETS_SPEC = "0,1,5,99,100";
    private static final PercentileHistoricalEditorConfig PERCENTILE_HISTORICAL_EDITOR_CONFIG =
            new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 10, 100,
                    ImmutableMap.of(CommodityType.VCPU, PERCENTILE_BUCKETS_SPEC,
                            CommodityType.IMAGE_CPU, PERCENTILE_BUCKETS_SPEC), null);

    private static final long VIRTUAL_MACHINE_OID = 1;
    private static final long BUSINESS_USER_OID = 10;
    private static final long DESKTOP_POOL_PROVIDER_OID = 3;
    private static final long DEFAULT_SETTING_POLICY_ID = 1;
    private static final long SETTING_POLICY_ID = 1;
    private static final long PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 2;
    private static final long NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 4;
    private static final long PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD = 4;
    private static final long NEW_BUSINESS_USER_OBSERVATION_PERIOD = 2;
    private static final float CAPACITY = 1000F;
    private static final EntityCommodityFieldReference VCPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(VIRTUAL_MACHINE_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .build(), CommodityField.USED);
    private static final EntityCommodityFieldReference IMAGE_CPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(BUSINESS_USER_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .setKey("DesktopPool::key")
                            .build(), DESKTOP_POOL_PROVIDER_OID, CommodityField.USED);

    private final StatsHistoryServiceStub statsHistoryServiceStub =
            PowerMockito.mock(StatsHistoryServiceStub.class);
    private final Clock clock = Mockito.mock(Clock.class);
    private ICommodityFieldAccessor commodityFieldAccessor;
    private Map<Long, Builder> topologyBuilderMap;
    private Map<Long, EntitySettings> entitySettings;
    private GraphWithSettings graphWithSettings;
    private PercentileEditorCacheAccess percentileEditor;

    /**
     * Set up the test.
     */
    @Before
    public void setUp() {
        setUpTopology();
        percentileEditor = new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG,
                                                           statsHistoryServiceStub, clock);
    }

    private void setUpTopology() {
        topologyBuilderMap = new HashMap<>();
        entitySettings = new HashMap<>();
        addEntityWithSetting(VIRTUAL_MACHINE_OID,
                             EntityType.VIRTUAL_MACHINE_VALUE,
                             EntitySettingSpecs.PercentileObservationPeriodVirtualMachine,
                             PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD);
        addEntityWithSetting(BUSINESS_USER_OID,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.PercentileObservationPeriodBusinessUser,
                             PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD);
        graphWithSettings = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                                                                  .newGraph(topologyBuilderMap),
                                                  entitySettings,
                                                  Collections.emptyMap());
        commodityFieldAccessor = new CommodityFieldAccessor(graphWithSettings.getTopologyGraph());
    }

    private void addEntityWithSetting(long entityOid, int entityType,
            EntitySettingSpecs entitySettingSpecs, long value) {
        topologyBuilderMap.put(entityOid, TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder().setOid(entityOid).setEntityType(entityType)));
        entitySettings.put(entityOid,
                createPercentileObservationWindowSetting(entityOid, value, entitySettingSpecs));
    }

    /**
     * Test the generic applicability of percentile calculation.
     */
    @Test
    public void testIsApplicable() {
        PercentileEditor editor = new PercentileEditor(PERCENTILE_HISTORICAL_EDITOR_CONFIG, null, null);
        Assert.assertTrue(editor.isApplicable(Collections.emptyList(),
                                              TopologyInfo.newBuilder().build(),
                                              null));
        Assert.assertTrue(editor.isApplicable(Collections.emptyList(),
                                              TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo
                                                      .newBuilder()
                                                      .setPlanProjectType(PlanProjectType.USER))
                                                      .build(),
                                              null));
        Assert.assertFalse(editor.isApplicable(Collections.emptyList(),
                                              TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo
                                                      .newBuilder()
                                                      .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                                                      .build(),
                                              null));
        Assert.assertTrue(editor.isApplicable(Collections.singletonList(ScenarioChange.newBuilder().build()),
                                              TopologyInfo.newBuilder().build(),
                                              null));
        Assert.assertFalse(editor.isApplicable(Collections.singletonList(ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder()
                                        .setHistoricalBaseline(HistoricalBaseline.newBuilder()
                                                       .setBaselineDate(0).build()))
                        .build()),
                                               TopologyInfo.newBuilder().build(),
                                               null));
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
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
        // LATEST(1, 2, 3, 4, 5) + TOTAL(41, 42, 43, 44, 45) = FULL(42, 44, 46, 48, 50)
        Assert.assertEquals(Arrays.asList(42, 44, 46, 48, 50),
                percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet())
                        .build()
                        .getUtilizationList());
        // LATEST(46, 47, 48, 49, 50) + TOTAL(86, 87, 88, 89, 90) = FULL(132, 134, 136, 138, 140)
        Assert.assertEquals(Arrays.asList(132, 134, 136, 138, 140),
                percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet())
                        .build()
                        .getUtilizationList());
    }

    /**
     * Test that {@link PercentileEditor#initContext}
     * correctly handles case when observation period of entity changed.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChanged()
            throws InterruptedException, HistoryCalculationException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db.
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
        // Necessary to set last checkpoint timestamp.
        percentileEditor.completeBroadcast();

        // Change observation periods.
        entitySettings.put(VIRTUAL_MACHINE_OID,
                createPercentileObservationWindowSetting(VIRTUAL_MACHINE_OID,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                        EntitySettingSpecs.PercentileObservationPeriodVirtualMachine));
        entitySettings.put(BUSINESS_USER_OID,
                createPercentileObservationWindowSetting(BUSINESS_USER_OID,
                        NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                        EntitySettingSpecs.PercentileObservationPeriodBusinessUser));

        // Check observation periods changed.
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);

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
        final PercentileRecord vCpuPercentileRecord =
                percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet())
                        .build();
        Assert.assertEquals(Arrays.asList(148, 156, 164, 172, 180),
                vCpuPercentileRecord.getUtilizationList());
        Assert.assertEquals(NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                vCpuPercentileRecord.getPeriod());

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
        final PercentileRecord imageCpuPercentileRecord =
                percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet())
                        .build();
        Assert.assertEquals(Arrays.asList(214, 218, 222, 226, 230),
                imageCpuPercentileRecord.getUtilizationList());
        Assert.assertEquals(NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                imageCpuPercentileRecord.getPeriod());
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast()} fails if
     * object is not initialized.
     *
     * @throws HistoryCalculationException always
     * @throws InterruptedException when something goes wrong
     */
    @Test(expected = HistoryCalculationException.class)
    public void testCompleteBroadcastFailsIfObjectIsNotInitialized()
                    throws HistoryCalculationException, InterruptedException {
        percentileEditor.completeBroadcast();
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
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
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
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
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
        final PercentileTaskStub stub = Mockito.spy(new PercentileTaskStub(statsHistoryServiceStub));
        percentileEditor = new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG,
                                                           statsHistoryServiceStub, clock,
                                                           (service) -> stub);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_06_00);
        Mockito.reset(stub);

        percentileEditor.completeBroadcast();

        // We should update LATEST with the latest values we have.
        final ArgumentCaptor<PercentileCounts> latestCaptor =
                        ArgumentCaptor.forClass(PercentileCounts.class);
        Mockito.verify(stub, Mockito.times(1)).save(latestCaptor.capture(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
        final PercentileCounts latest = latestCaptor.getValue();
        Assert.assertEquals(2, latest.getPercentileRecordsList().size());

        // We don't update TOTAL because there is not right time now.
        Mockito.verify(stub, Mockito.never()).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_29_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Maintenance shouldn't load any data.
        Mockito.verify(stub, Mockito.never())
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
        final PercentileTaskStub stub = Mockito.spy(new PercentileTaskStub(statsHistoryServiceStub));
        percentileEditor = new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG,
                                                           statsHistoryServiceStub, clock,
                                                           (service) -> stub);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_12_00);

        Mockito.reset(stub);
        percentileEditor.completeBroadcast();

        // Save current LATEST day percentiles with one write.
        Mockito.verify(stub, Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We do exactly one TOTAL write to DB when one maintenance windows passed since last checkpoint.
        Mockito.verify(stub, Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_29_2019_12_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We load exactly two times in maintenance because we have two different periods in graph.
        Mockito.verify(stub, Mockito.times(2)).load(Mockito.any(),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
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
        final PercentileTaskStub stub = Mockito.spy(new PercentileTaskStub(statsHistoryServiceStub));
        percentileEditor = new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG,
                                                           statsHistoryServiceStub, clock,
                                                           (service) -> stub);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor, false);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_30_2019_00_00);

        Mockito.reset(stub);
        percentileEditor.completeBroadcast();

        // Update LATEST day once.
        Mockito.verify(stub, Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Normally we should do exactly one TOTAL write even if two maintenance windows passed since last checkpoint.
        Mockito.verify(stub, Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_30_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Two periods * two maintenance window = exactly four invocations.
        Mockito.verify(stub, Mockito.times(4)).load(Mockito.any(),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
    }

    private static EntitySettings createPercentileObservationWindowSetting(long entityOid,
            long value, EntitySettingSpecs entitySettingSpecs) {
        return EntitySettings.newBuilder()
                .setEntityOid(entityOid)
                .setDefaultSettingPolicyId(DEFAULT_SETTING_POLICY_ID)
                .addUserSettings(SettingToPolicyId.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setSettingSpecName(entitySettingSpecs.getSettingName())
                                .setNumericSettingValue(
                                        NumericSettingValue.newBuilder().setValue(value))
                                .build())
                        .addSettingPolicyId(SETTING_POLICY_ID)
                        .build())
                .build();
    }

    /**
     * Access to the percentile editor cached data.
     */
    private static class PercentileEditorCacheAccess extends PercentileEditor {
        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                StatsHistoryServiceStub statsHistoryClient, Clock clock,
                Function<StatsHistoryServiceStub, PercentilePersistenceTask> taskCreator) {
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
            final List<Long> timestamps =
                            Arrays.asList(TIMESTAMP_INIT_START_SEP_1_2019, TIMESTAMP_AUG_31_2019_12_00,
                                          TIMESTAMP_AUG_31_2019_00_00, TIMESTAMP_AUG_30_2019_12_00,
                                          TIMESTAMP_AUG_30_2019_00_00, TIMESTAMP_AUG_29_2019_12_00,
                                          TIMESTAMP_AUG_29_2019_00_00, TIMESTAMP_AUG_28_2019_12_00,
                                          PercentilePersistenceTask.TOTAL_TIMESTAMP);
            int i = 0;
            final List<Long> entities = Arrays.asList(VIRTUAL_MACHINE_OID, BUSINESS_USER_OID);
            for (long entityOid : entities) {
                final Map<Long, List<Integer>> timestampToUtilization = new HashMap<>();
                DEFAULT_UTILISATION.put(entityOid, timestampToUtilization);
                for (long timestamp : timestamps) {
                    timestampToUtilization.put(timestamp, Arrays.asList(++i, ++i, ++i, ++i, ++i));
                }
            }
        }

        PercentileTaskStub(StatsHistoryServiceStub unused) {
            super(unused);
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
                final List<Integer> utilizations =
                                currentUtilization.get(entityCommodityFieldReference.getEntityOid())
                                                .get(getStartTimestamp());
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
