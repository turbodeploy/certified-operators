package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for PercentileCommodityData.
 */
public class PercentileCommodityDataTest extends BaseGraphRelatedTest {
    private static final double DELTA = 0.001;
    private static final CommodityType commType = CommodityType.newBuilder().setType(1).build();
    private static final EntityCommodityFieldReference field =
                    new EntityCommodityFieldReference(1, commType, CommodityField.USED);
    private static final int UNAVAILABLE_DATA_PERIOD = 30;
    private PercentileHistoricalEditorConfig config;
    private HistoryAggregationContext context;

    private Clock clock;
    private TopologyInfo topologyInfo;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(0L));
        config = new PercentileHistoricalEditorConfig(1, 24, 777777L, 10, 100,
                        Collections.emptyMap(), null, clock);
        context = Mockito.mock(HistoryAggregationContext.class);
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(77777L).build();
    }

    /**
     * Test that init method stores passed db values.
     *
     * @throws HistoryCalculationException when initialization fails
     */
    @Test
    public void testInit() throws HistoryCalculationException {
        float cap = 100f;
        int used = 70;
        TopologyEntity entity = mockEntity(1, 1, commType, cap, used, null, null, null, null, true);
        ICommodityFieldAccessor accessor = Mockito.spy(new CommodityFieldAccessor(
                        mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(field);
        Mockito.when(context.getAccessor()).thenReturn(accessor);
        PercentileRecord.Builder dbValue =
                        PercentileRecord.newBuilder().setCapacity(cap).setEntityOid(entity.getOid())
                                        .setCommodityType(commType.getType())
                                        .setPeriod(UNAVAILABLE_DATA_PERIOD);
        for (int i = 0; i <= 100; ++i) {
            dbValue.addUtilization(i == used ? 1 : 0);
        }

        PercentileCommodityData pcd = new PercentileCommodityData();
        pcd.init(field, dbValue.build(), config, context);

        Assert.assertEquals(0, pcd.getUtilizationCountStore().getPercentile(0));
        Assert.assertEquals(used, pcd.getUtilizationCountStore().getPercentile(100));
        PercentileRecord record = pcd.getUtilizationCountStore().getLatestCountsRecord().build();
        Assert.assertEquals(101, record.getUtilizationCount());
        Assert.assertEquals(1, record.getUtilization(used));
    }

    /**
     * Test that aggregate method accounts for passed running values from DTOs.
     */
    @Test
    public void testAggregate() {
        float cap = 100f;
        double used1 = 76d;
        double used2 = 99d;
        double used3 = 12d;
        double realTime = 80d;
        TopologyEntity entity =
                        mockEntity(1, 1, commType, cap, used1, null, null, null, null, true);
        ICommodityFieldAccessor accessor =
                        createAccessor(cap, used2, used3, realTime, entity, 1000);
        Mockito.when(context.getAccessor()).thenReturn(accessor);

        PercentileCommodityData pcd = new PercentileCommodityData();
        pcd.init(field, null, config, context);
        pcd.aggregate(field, config, context);
        CommoditySoldDTO.Builder commSold =
                        entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                                        .get(0);
        Assert.assertTrue(commSold.hasHistoricalUsed());
        Assert.assertEquals(used3 / cap, commSold.getHistoricalUsed().getPercentile(), DELTA);
    }

    /**
     * Checks that min observation period setting is working as expected.
     */
    @Test
    public void testMinObservationPeriod() {
        final long currentTime = Duration.ofMinutes(31).toMillis();
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(currentTime));
        final PercentileCommodityData pcd = new PercentileCommodityData();
        final ICommodityFieldAccessor accessor = createAccessor(currentTime);
        Mockito.when(context.getAccessor()).thenReturn(accessor);
        pcd.init(field, null, config, context);
        // First time until startTimestamp initialized
        aggregate(false, currentTime, pcd, 1);
        // Start timestamp initialized, but there is no enough history data
        aggregate(false, currentTime, pcd, 1);
        // Start timestamp initialized, setting changed, data enough
        aggregate(true, currentTime, pcd, 0);
        // Start timestamp initialized, data enough, but we did not get data from probe for more than 30 minutes
        final long nextCollectAfterMissing = Duration.ofMinutes(93).toMillis();
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(nextCollectAfterMissing));
        aggregate(false, nextCollectAfterMissing, pcd, 1);
    }

    private void aggregate(boolean expectedResizable, long currentTime, PercentileCommodityData pcd,
                    int minObservationPeriodValue) {
        final TopologyEntity entity =
                        mockEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1, commType, 100f, 76d, null,
                                        null, null, null, true);
        final Setting minObservationPeriodSetting = Setting.newBuilder().setSettingSpecName(
                        EntitySettingSpecs.MinObservationPeriodVirtualMachine.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                        .setValue(minObservationPeriodValue).build()).build();
        final EntitySettings settings =
                        EntitySettings.newBuilder().setEntityOid(1).setDefaultSettingPolicyId(3)
                                        .addUserSettings(SettingToPolicyId.newBuilder()
                                                        .setSetting(minObservationPeriodSetting)
                                                        .build()).build();
        Long2ObjectMap<TopologyEntity.Builder> topologyMap = new Long2ObjectOpenHashMap<>();
        topologyMap.put(1L, TopologyEntity.newBuilder(entity.getTopologyEntityDtoBuilder()));
        HistoryAggregationContext ctx = Mockito.spy(new HistoryAggregationContext(topologyInfo,
                new GraphWithSettings(new TopologyGraphCreator<>(topologyMap).build(),
                        Collections.singletonMap(1L, settings), Collections.emptyMap()), false));
        final ICommodityFieldAccessor accessor = createAccessor(100f, 99d, 12d, 80d, entity, currentTime);
        Mockito.doReturn(accessor).when(ctx).getAccessor();
        pcd.aggregate(field, config, ctx);
        final CommoditySoldDTO.Builder commSold =
                        entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                                        .get(0);
        Assert.assertThat(commSold.getIsResizeable(), CoreMatchers.is(expectedResizable));
        Assert.assertNotNull(entity.getTopologyEntityDtoBuilder().getAnalysisSettings());
        entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().forEach(
            commBoughtGrouping -> Assert.assertThat(commBoughtGrouping.getScalable(),
                CoreMatchers.is(expectedResizable)));
    }

    private static ICommodityFieldAccessor createAccessor(long currentTime) {
        final TopologyEntity entity =
                        mockEntity(EntityType.VIRTUAL_MACHINE_VALUE, 1, commType, 100f, 76d, null,
                                        null, null, null, true);
        return createAccessor(100f, 99d, 12d, 80d, entity, currentTime);
    }

    private static ICommodityFieldAccessor createAccessor(float cap, double used2, double used3,
                    double realTime, TopologyEntity entity, long lastPointTimestamp) {
        ICommodityFieldAccessor accessor = Mockito.spy(new CommodityFieldAccessor(
                        mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(field);
        Mockito.doReturn(realTime).when(accessor).getRealTimeValue(field);
        UtilizationData data =
                        UtilizationData.newBuilder().setLastPointTimestampMs(lastPointTimestamp)
                                        .setIntervalMs(10).addPoint(used2 / cap * 100)
                                        .addPoint(used3 / cap * 100).build();
        Mockito.doReturn(data).when(accessor).getUtilizationData(Mockito.any());
        return accessor;
    }
}
