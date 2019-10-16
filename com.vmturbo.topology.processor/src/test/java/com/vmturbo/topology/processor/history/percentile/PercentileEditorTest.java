package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private static final long TIMESTAMP_TOTAL = 0L;
    private static final long TIMESTAMP_AUG_28_2019_12_00 = 1566993600000L;
    private static final long TIMESTAMP_AUG_29_2019_00_00 = 1567036800000L;
    private static final long TIMESTAMP_AUG_29_2019_12_00 = 1567080000000L;
    private static final long TIMESTAMP_AUG_30_2019_00_00 = 1567123200000L;
    private static final long TIMESTAMP_AUG_30_2019_12_00 = 1567166400000L;
    private static final long TIMESTAMP_AUG_31_2019_00_00 = 1567209600000L;
    private static final long TIMESTAMP_AUG_31_2019_12_00 = 1567252800000L;
    private static final long TIMESTAMP_INIT_START_SEP_1_2019 = 1567296000000L;

    // Entity OID -> timestamp -> utilization counts array.
    private static final Map<Long, Map<Long, List<Integer>>> UTILIZATIONS;

    private static final int MAINTENANCE_WINDOW_HOURS = 12;
    private static final String PERCENTILE_BUCKETS_SPEC = "0,1,5,99,100";
    private static final PercentileHistoricalEditorConfig PERCENTILE_HISTORICAL_EDITOR_CONFIG =
            new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 10, 100,
                    ImmutableMap.of(CommodityType.VCPU, PERCENTILE_BUCKETS_SPEC,
                            CommodityType.IMAGE_CPU, PERCENTILE_BUCKETS_SPEC));

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

        UTILIZATIONS = new HashMap<>();
        final List<Long> timestamps =
                Arrays.asList(TIMESTAMP_INIT_START_SEP_1_2019, TIMESTAMP_AUG_31_2019_12_00,
                        TIMESTAMP_AUG_31_2019_00_00, TIMESTAMP_AUG_30_2019_12_00,
                        TIMESTAMP_AUG_30_2019_00_00, TIMESTAMP_AUG_29_2019_12_00,
                        TIMESTAMP_AUG_29_2019_00_00, TIMESTAMP_AUG_28_2019_12_00, TIMESTAMP_TOTAL);
        int i = 0;
        final List<Long> entities = Arrays.asList(VIRTUAL_MACHINE_OID, BUSINESS_USER_OID);
        for (long entityOid : entities) {
            final Map<Long, List<Integer>> timestampToUtilization = new HashMap<>();
            UTILIZATIONS.put(entityOid, timestampToUtilization);
            for (long timestamp : timestamps) {
                timestampToUtilization.put(timestamp, Arrays.asList(++i, ++i, ++i, ++i, ++i));
            }
        }
    }

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
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        setUpStatsHistoryService();
        setUpTopology();
        percentileEditor = new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG,
                statsHistoryServiceStub, clock);
    }

    private void setUpTopology() {
        topologyBuilderMap = new HashMap<>();
        entitySettings = new HashMap<>();
        addEntityWithSetting(VIRTUAL_MACHINE_OID, EntityType.VIRTUAL_MACHINE_VALUE,
                EntitySettingSpecs.PercentileObservationPeriodVirtualMachine,
                PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD);
        addEntityWithSetting(BUSINESS_USER_OID, EntityType.BUSINESS_USER_VALUE,
                EntitySettingSpecs.PercentileObservationPeriodBusinessUser,
                PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD);
        graphWithSettings = new GraphWithSettings(
                TopologyEntityTopologyGraphCreator.newGraph(topologyBuilderMap), entitySettings,
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

    private void setUpStatsHistoryService() {
        Mockito.when(statsHistoryServiceStub.setPercentileCounts(Mockito.any()))
                .thenAnswer((invocation -> {
                    final StreamObserver<SetPercentileCountsResponse> streamObserver =
                            invocation.getArgumentAt(0, StreamObserver.class);
                    final CallStreamObserver<PercentileChunk> mock =
                            Mockito.mock(CallStreamObserver.class);
                    Mockito.doAnswer(i -> {
                        streamObserver.onNext(SetPercentileCountsResponse.newBuilder().build());
                        streamObserver.onCompleted();
                        return null;
                    }).when(mock).onCompleted();
                    Mockito.when(mock.isReady()).thenReturn(true);
                    return mock;
                }));

        Mockito.doAnswer(new GetPercentileCountsAnswer())
                .when(statsHistoryServiceStub)
                .getPercentileCounts(Mockito.any(), Mockito.any());
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
        // First initializing history from db.
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor);
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
     * Test for {@link PercentileEditor#checkObservationPeriodsChanged}.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChanged()
            throws InterruptedException, HistoryCalculationException {
        // First initializing history from db.
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor);
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
        percentileEditor.initContext(graphWithSettings, commodityFieldAccessor);

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
     * To answer when the stubbed {@link StatsHistoryServiceStub#getPercentileCounts} method is
     * called.
     */
    private static class GetPercentileCountsAnswer implements Answer<Object> {

        @Override
        public Object answer(InvocationOnMock invocation) {
            final GetPercentileCountsRequest getPercentileCountsRequest =
                    invocation.getArgumentAt(0, GetPercentileCountsRequest.class);

            final StreamObserver<PercentileChunk> streamObserver =
                    invocation.getArgumentAt(1, StreamObserver.class);

            PercentileRecord virtualMachinePercentileRecord = PercentileRecord.newBuilder()
                    .setEntityOid(VIRTUAL_MACHINE_OID)
                    .setCommodityType(VCPU_COMMODITY_REFERENCE.getCommodityType().getType())
                    .addAllUtilization(UTILIZATIONS.get(VIRTUAL_MACHINE_OID)
                            .get(getPercentileCountsRequest.getStartTimestamp()))
                    .setCapacity(CAPACITY)
                    .setPeriod((int)PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD)
                    .build();

            PercentileRecord businessUserPercentileRecord = PercentileRecord.newBuilder()
                    .setEntityOid(BUSINESS_USER_OID)
                    .setCommodityType(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getType())
                    .setProviderOid(IMAGE_CPU_COMMODITY_REFERENCE.getProviderOid())
                    .setKey(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getKey())
                    .addAllUtilization(UTILIZATIONS.get(BUSINESS_USER_OID)
                            .get(getPercentileCountsRequest.getStartTimestamp()))
                    .setCapacity(CAPACITY)
                    .setProviderOid(DESKTOP_POOL_PROVIDER_OID)
                    .setPeriod((int)PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD)
                    .build();

            final byte[] content = PercentileCounts.newBuilder()
                    .addPercentileRecords(virtualMachinePercentileRecord)
                    .addPercentileRecords(businessUserPercentileRecord)
                    .build()
                    .toByteArray();

            streamObserver.onNext(PercentileChunk.newBuilder()
                    .setPeriod(MAINTENANCE_WINDOW_HOURS)
                    .setStartTimestamp(getPercentileCountsRequest.getStartTimestamp())
                    .setContent(ByteString.copyFrom(content))
                    .build());

            streamObserver.onCompleted();
            return null;
        }
    }

    /**
     * Access to the percentile editor cached data.
     */
    private static class PercentileEditorCacheAccess extends PercentileEditor {
        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                StatsHistoryServiceStub statsHistoryClient, Clock clock) {
            super(config, statsHistoryClient, clock);
        }

        PercentileCommodityData getCacheEntry(EntityCommodityFieldReference field) {
            return getCache().get(field);
        }
    }
}
