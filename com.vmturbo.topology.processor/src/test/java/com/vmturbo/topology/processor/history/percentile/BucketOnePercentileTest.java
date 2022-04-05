package com.vmturbo.topology.processor.history.percentile;

import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.UtilizationDataImpl;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;

/**
 * Unit tests for PercentileCommodityData.
 */
@RunWith(Parameterized.class)
public class BucketOnePercentileTest extends BaseGraphRelatedTest {
    /**
     * Test parameters.
     */
    @Parameters(name = "{index}: Test with plan = {0}, migrationPlan = {1}, expecting percentile = {2}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                {false, false, true},
                {true, false, true},
                {true, true, false}};
        return Arrays.asList(data);
    }

    private boolean isPlan;
    private boolean isMigrationPlan;
    private boolean expectingPercentile;

    /**
     * Tests to verify skipping 1% percentile values when running migration plans.
     *
     * @param isPlan if true, running a plan
     * @param isMigrationPlan if true, running a migration plan
     * @param expectingPercentile if true, expecting a percentile value of 0.01.
     */
    public BucketOnePercentileTest(boolean isPlan, boolean isMigrationPlan,
            boolean expectingPercentile) {
        this.isPlan = isPlan;
        this.isMigrationPlan = isMigrationPlan;
        this.expectingPercentile = expectingPercentile;
    }

    private static final double EPSILON = 1E-5;
    private static final CommodityTypeImpl commType = new CommodityTypeImpl().setType(1);
    private static final EntityCommodityFieldReference field = new EntityCommodityFieldReference(1,
            commType, CommodityField.USED);
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
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(0L));
        config = new PercentileHistoricalEditorConfig(1, 24, 777777L, 10, 100, Collections.emptyMap(), null, clock);
        context = Mockito.mock(HistoryAggregationContext.class);
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(77777L).build();
    }

    /**
     * Verify that percentile value 1 is returned for realtime and optimize plan topologies.
     */
    @Test
    public void testBucketOnePercentile() {
        when(context.isPlan()).thenReturn(isPlan);
        when(context.isMigrationPlan()).thenReturn(isMigrationPlan);
        CommoditySoldView commSold = runBucketTestScenario(0.5d, 0.25d);

        Assert.assertEquals(expectingPercentile, commSold.hasHistoricalUsed());
        if (expectingPercentile) {
            HistoricalValuesView historicalUsed = commSold.getHistoricalUsed();
            Assert.assertNotNull(historicalUsed);
            Assert.assertTrue(historicalUsed.hasPercentile());
            Assert.assertEquals(historicalUsed.getPercentile(), 0.01d, EPSILON);
        }
    }

    /**
     * Verify that percentiles other than 1 are returned regardless of plan settings.
     */
    @Test
    public void testBucketNotOnePercentile() {
        when(context.isPlan()).thenReturn(isPlan);
        when(context.isMigrationPlan()).thenReturn(isMigrationPlan);
        CommoditySoldView commSold = runBucketTestScenario(1.5d, 2d);

        Assert.assertTrue(commSold.hasHistoricalUsed());
        HistoricalValuesView historicalUsed = commSold.getHistoricalUsed();
        Assert.assertNotNull(historicalUsed);
        Assert.assertTrue(historicalUsed.hasPercentile());
        Assert.assertEquals(historicalUsed.getPercentile(), 0.02d, EPSILON);
    }

    private CommoditySoldView runBucketTestScenario(double used2, double used3) {
        float cap = 100f;
        double used1 = 76d;
        double realTime = 80d;
        TopologyEntity entity = mockEntity(1, 1, commType, cap, used1, null, null, null, null, true);
        ICommodityFieldAccessor accessor = createAccessor(cap, realTime, entity, 1000, used2, used3);
        when(context.getAccessor()).thenReturn(accessor);

        PercentileCommodityData pcd = new PercentileCommodityData();
        pcd.init(field, null, config, context);
        if (context.isPlan()) {
            // If we are running a plan, we need to run the aggregation once in realtime so that
            // the percentile data is calculated.
            when(context.isPlan()).thenReturn(false);
            pcd.aggregate(field, config, context);
            when(context.isPlan()).thenReturn(true);
        }
        pcd.aggregate(field, config, context);
        return entity.getTopologyEntityImpl().getCommoditySoldListList().get(0);
    }

    private static ICommodityFieldAccessor createAccessor(float cap,
            double realTime, TopologyEntity entity, long lastPointTimestamp,
            double... usedValues) {
        ICommodityFieldAccessor accessor = Mockito.spy(new CommodityFieldAccessor(
                mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(field);
        Mockito.doReturn(realTime).when(accessor).getRealTimeValue(field);
        UtilizationDataImpl data =
                new UtilizationDataImpl()
                        .setLastPointTimestampMs(lastPointTimestamp)
                        .setIntervalMs(10);
        Arrays.stream(usedValues).forEach(uv -> data.addPoint(uv / cap * 100));
        Mockito.doReturn(data).when(accessor).getUtilizationData(Mockito.any());
        return accessor;
    }
}
