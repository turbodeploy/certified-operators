package com.vmturbo.topology.processor.history.movingstats;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCapacityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;
import com.vmturbo.topology.processor.history.movingstats.MovingStatisticsSamplingConfiguration.ThrottlingSamplerConfiguration;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Tests for {@link VcpuThrottlingSampler}.
 */
public class VcpuThrottlingSamplerTest {
    private static final long OID = 12345L;

    final TopologyEntityDTO.Builder containerSpec = TopologyEntityDTO.newBuilder()
        .setOid(OID)
        .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
            .setLastUpdatedTime(System.currentTimeMillis())))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE).build())
            .setCapacity(100.0)
            .setUsed(50.0)
            .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE).build())
            .setCapacity(100.0)
            .setUsed(50.0)
            .build());
    final TopologyEntity.Builder topologyEntity = TopologyEntity.newBuilder(containerSpec);
    final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
        ImmutableMap.of(OID, topologyEntity));

    // We created the containerspec with VCPU at index 0 in the commoditySold list
    private static final int VCPU_COMMODITY_INDEX = 0;
    // We created the containerspec with Throttling at index 1 in the commoditySold list
    private static final int THROTTLING_COMMODITY_INDEX = 1;

    private static final long FAST_HALFLIFE_HOURS = 12;
    private static final long SLOW_HALFLIFE_HOURS = 72;
    private static final long ESTABLISH_INITIAL_VALUE_HOURS = 5_000;

    private static final double SMALL_DELTA = 0.000001;
    private static Duration retentionPeriod = Duration.ofDays(30);

    private static final ThrottlingSamplerConfiguration SAMPLER_CONFIGURATION =
        new ThrottlingSamplerConfiguration(Duration.ofHours(FAST_HALFLIFE_HOURS),
            Duration.ofHours(SLOW_HALFLIFE_HOURS),
            retentionPeriod,
            2.0,
            3.5);

    private final ICommodityFieldAccessor commodityFieldAccessor = new CommodityFieldAccessor(graph);

    private static final EntityCommodityFieldReference VCPU_FIELD =
        new EntityCommodityFieldReference(OID, CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_VALUE).build(), CommodityField.USED);
    private static final EntityCommodityFieldReference THROTTLING_FIELD =
        new EntityCommodityFieldReference(OID, CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE).build(), CommodityField.USED);
    private static final List<EntityCommodityFieldReference> PARTNER_FIELDS =
        Collections.singletonList(THROTTLING_FIELD);

    private static final EntityCommodityFieldReference TEST_FIELD =
        new EntityCommodityFieldReference(74033736057638L, CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_VALUE).build(), CommodityField.USED);

    private static final String PROTO_DEFINITION = ""
        + "  entity_oid: 74033736057638\n"
        + "  throttling_record {\n"
        + "    capacity_records {\n"
        + "      vcpu_capacity: 275\n"
        + "      last_sample_timestamp: 1627832864308\n"
        + "      sample_count: 298\n"
        + "      throttling_max_sample: 0\n"
        + "      throttling_fast_moving_average: 0\n"
        + "      throttling_fast_moving_variance: 0\n"
        + "      throttling_slow_moving_average: 0\n"
        + "      throttling_slow_moving_variance: 0\n"
        + "    }\n"
        + "    capacity_records {\n"
        + "      vcpu_capacity: 255\n"
        + "      last_sample_timestamp: 1628536302628\n"
        + "      sample_count: 25\n"
        + "      throttling_max_sample: 52.785515320334262\n"
        + "      throttling_fast_moving_average: 1.7545257185759844\n"
        + "      throttling_fast_moving_variance: 51.308091189861351\n"
        + "      throttling_slow_moving_average: 0.68414809803867938\n"
        + "      throttling_slow_moving_variance: 4.800934588446534\n"
        + "    }\n"
        + "    capacity_records {\n"
        + "      vcpu_capacity: 555\n"
        + "      last_sample_timestamp: 1628551897638\n"
        + "      sample_count: 58\n"
        + "      throttling_max_sample: 0\n"
        + "      throttling_fast_moving_average: 0\n"
        + "      throttling_fast_moving_variance: 0\n"
        + "      throttling_slow_moving_average: 0\n"
        + "      throttling_slow_moving_variance: 0\n"
        + "    }\n"
        + "    capacity_records {\n"
        + "      vcpu_capacity: 335\n"
        + "      last_sample_timestamp: 1628427098617\n"
        + "      sample_count: 24\n"
        + "      throttling_max_sample: 54.545454545454547\n"
        + "      throttling_fast_moving_average: 2.1103754604214484\n"
        + "      throttling_fast_moving_variance: 53.54823703214791\n"
        + "      throttling_slow_moving_average: 1.2854708143527569\n"
        + "      throttling_slow_moving_variance: 4.5666648737851165\n"
        + "    }\n"
        + "    active_vcpu_capacity: 555\n"
        + "  }\n"
        + "";

    /**
     * Test that when there's a spike from low values to high values, the fast halflife
     * moves the moving average up appropriately over time.
     */
    @Test
    public void testFastHalflife() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        elapseHours(0, 0,
            ESTABLISH_INITIAL_VALUE_HOURS, sampler);

        elapseHours(50.0, 100.0, FAST_HALFLIFE_HOURS, sampler);
        assertHistoricalThrottling(25.0, sampler, SMALL_DELTA);
        assertNull(sampler.meanPlusSigma(VCPU_FIELD, 0));
    }

    /**
     * Test that when there's a drop from high values to low values, the slow halflife
     * has a sufficiently long memory for the past high values.
     */
    @Test
    public void testSlowHalflife() {
        // Set them both to fast because the max of
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        elapseHours(50.0, 100.0,
            ESTABLISH_INITIAL_VALUE_HOURS, sampler);

        elapseHours(0, 0, SLOW_HALFLIFE_HOURS, sampler);
        assertHistoricalThrottling(25.0, sampler, SMALL_DELTA);
    }

    /**
     * Test that when we use uneven timesteps we get a reasonable approximation of the right
     * answer. We won't be exactly correct. Note that we can't achieve
     * as high accuracy with a variable timestep as we can with a fixed timestep but we
     * need to support variable timestep because discovery doesn't always happen with exactly
     * the same delta between updates.
     */
    @Test
    public void testUnevenTimestep() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        elapseHours(50.0, 100.0, ESTABLISH_INITIAL_VALUE_HOURS, sampler);
        final Random r = new Random(0);

        for (int i = 0; i < SLOW_HALFLIFE_HOURS; i++) {
            elapseHoursByIncrement(0, 0, 1,
                (long)(0.5 + 0.5 * r.nextDouble() * TimeUnit.MINUTES.toMillis(10)), sampler);
        }

        assertHistoricalThrottling(25.0, sampler, 0.5);
    }

    /**
     * Test that the value we send to analysis does not exceed the maximum value seen
     * at the capacity.
     */
    @Test
    public void testCapByMax() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        elapseHours(0, 0,
            ESTABLISH_INITIAL_VALUE_HOURS, sampler);

        elapseHours(50.0, 100.0, FAST_HALFLIFE_HOURS, sampler);
        // At this point mean+2*sigma should be 75.0 for Historical Throttling but we should
        // cap the value for analysis at 50.0 because that's the highest value that's
        // actually been received.
        assertEquals(50.0, sampler.meanPlusSigma(THROTTLING_FIELD, 2.0), SMALL_DELTA);
    }

    /**
     * Test that when all samples are above target, we pick the one with the highest VCPU capacity.
     */
    @Test
    public void testNoLowerBoundWhenAllSamplesAboveTarget() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        assertNull(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0));

        setThrottlingAtCapacity(7.0, 100.0, sampler);
        assertThat(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0),
            greaterThan(100.0));

        setThrottlingAtCapacity(7.5, 90.0, sampler);
        assertThat(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0),
            greaterThan(100.0));

        setThrottlingAtCapacity(6.0, 200.0, sampler);
        assertThat(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0),
            greaterThan(200.0));
    }

    /**
     * Test that the lower bound is not computed when all values are above the target.
     */
    @Test
    public void testNoLowerBoundWhenAllSamplesBelowTarget() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        assertNull(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0));

        setThrottlingAtCapacity(2.0, 100.0, sampler);
        assertNull(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0));

        setThrottlingAtCapacity(3.0, 90.0, sampler);
        assertNull(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0));
    }

    /**
     * Test that the lower bound is correctly computed when the target is between different capacity samples.
     */
    @Test
    public void testLowerBoundAroundTarget() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        assertNull(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0));

        setThrottlingAtCapacity(6.0, 200.0, sampler);
        assertThat(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0), greaterThan(200.0));

        setThrottlingAtCapacity(3.0, 500.0, sampler);
        assertEquals(300.0, sampler.getMinThreshold(VCPU_FIELD, 0, 5.0), SMALL_DELTA);

        // Since this value has not tight to the target compared to the prior capacity values,
        // it should not affect the lower bound interpolation
        setThrottlingAtCapacity(0.0, 1000.0, sampler);
        assertEquals(300.0, sampler.getMinThreshold(VCPU_FIELD, 0, 5.0), SMALL_DELTA);
    }

    /**
     * Test interpolation at large spread picks a value closer to the capacity that is closer to the target.
     * throttling value.
     */
    @Test
    public void testLowerBoundComputationSpreadValues() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        setThrottlingAtCapacity(100.0, 100.0, sampler);
        setThrottlingAtCapacity(0, 1000.0, sampler);

        assertEquals(955.0, sampler.getMinThreshold(VCPU_FIELD, 0, 5.0), SMALL_DELTA);
    }

    /**
     * Usually we expect a higher VCPU value to produce a lower throttling value. However, when
     * that is not the case we should pick the higher VCPU value with the higher throttling value
     * that is over the target threshold instead of interpolating.
     */
    @Test
    public void testLowerBoundBackward() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        setThrottlingAtCapacity(20, 100.0, sampler);
        setThrottlingAtCapacity(0, 50.0, sampler);

        assertThat(sampler.getMinThreshold(VCPU_FIELD, 0, 5.0),
            greaterThan(100.0));
    }

    /**
     * Test capacity lower bound interpolation.
     */
    @Test
    public void testCapacityLowerBound() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);

        containerSpec.getCommoditySoldListBuilderList().get(VCPU_COMMODITY_INDEX)
            .setCapacity(100.0);
        elapseHours(50.0, 100.0, FAST_HALFLIFE_HOURS, sampler);

        containerSpec.getCommoditySoldListBuilderList().get(VCPU_COMMODITY_INDEX)
            .setCapacity(500.0);
        elapseHours(0.0, 150.0, FAST_HALFLIFE_HOURS, sampler);

        Double vcpuMinThreshold = sampler.getMinThreshold(VCPU_FIELD, 0, 5.0);
        assertThat(vcpuMinThreshold, greaterThan(455.0));

        // We should now reuse the data from the original capacity
        containerSpec.getCommoditySoldListBuilderList().get(VCPU_COMMODITY_INDEX)
            .setCapacity(100.0);
        elapseHours(20.0, 100.0, FAST_HALFLIFE_HOURS, sampler);

        // Since we decreased throttling at the lower capacity, the lower bound should decrease
        vcpuMinThreshold = sampler.getMinThreshold(VCPU_FIELD, 0, 5.0);
        assertThat(vcpuMinThreshold, lessThan(455.0));
    }

    /**
     * Test clearing expired data clears data outside of retention period.
     */
    @Test
    public void testClearExpiredData() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);

        updateContainerSpecTimestamp(Duration.ofHours(10).toMillis());
        setThrottlingAtCapacity(6.0, 200.0, sampler);

        updateContainerSpecTimestamp(Duration.ofHours(10).toMillis());
        setThrottlingAtCapacity(6.0, 300.0, sampler);

        assertEquals(2, sampler.getCapacityStatCount());

        final long updateTime = getContainerspecTimestamp() + retentionPeriod.toMillis() + 100L;
        assertTrue(sampler.cleanExpiredData(updateTime, SAMPLER_CONFIGURATION));
        assertEquals(1, sampler.getCapacityStatCount());
    }

    /**
     * Test clearing expired data will not clear active data even if it is expired.
     */
    @Test
    public void testClearExpiredDataDoesNotClearActive() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);

        updateContainerSpecTimestamp(Duration.ofHours(10).toMillis());
        setThrottlingAtCapacity(6.0, 200.0, sampler);

        final long updateTime = getContainerspecTimestamp() + retentionPeriod.toMillis() + 100L;
        assertEquals(1, sampler.getCapacityStatCount());
        assertFalse(sampler.cleanExpiredData(updateTime, SAMPLER_CONFIGURATION));
        assertEquals(1, sampler.getCapacityStatCount());
    }

    /**
     * Test that clearing expired data on an uninitialized sampler does not throw an exception.
     */
    @Test
    public void testClearExpiredDataOnUninitialized() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        assertFalse(sampler.cleanExpiredData(0, SAMPLER_CONFIGURATION));
    }

    /**
     * Test serialization writes data correctly.
     */
    @Test
    public void testSerialize() {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);

        updateContainerSpecTimestamp(Duration.ofHours(10).toMillis());
        setThrottlingAtCapacity(6.0, 200.0, sampler);
        setThrottlingAtCapacity(5.0, 200.0, sampler);

        updateContainerSpecTimestamp(Duration.ofHours(10).toMillis());
        setThrottlingAtCapacity(6.0, 300.0, sampler);
        setThrottlingAtCapacity(5.0, 300.0, sampler);

        final MovingStatisticsRecord.Builder record = sampler.serialize();
        final ThrottlingMovingStatisticsRecord throttlingRecord = record.getThrottlingRecord();
        assertEquals(2, throttlingRecord.getCapacityRecordsCount());
        assertEquals(300.0, throttlingRecord.getActiveVcpuCapacity(), 0);
        assertEquals(6.0, throttlingRecord.getCapacityRecords(0).getThrottlingMaxSample(), 0);
        assertEquals(2, throttlingRecord.getCapacityRecords(0).getSampleCount(), 0);
        assertEquals(6.0, throttlingRecord.getCapacityRecords(0).getThrottlingFastMovingAverage(), 1.0);
        assertNotEquals(0, throttlingRecord.getCapacityRecords(0).getThrottlingFastMovingVariance());
        assertEquals(6.0, throttlingRecord.getCapacityRecords(0).getThrottlingSlowMovingAverage(), 1.0);
        assertNotEquals(0, throttlingRecord.getCapacityRecords(0).getThrottlingSlowMovingVariance());
        assertNotEquals(0, throttlingRecord.getCapacityRecords(0).getLastSampleTimestamp());
    }

    /**
     * Test that deserialization restores data correctly.
     *
     * @throws InvalidHistoryDataException on exception.
     */
    @Test
    public void testDeserialize() throws InvalidHistoryDataException {
        final MovingStatisticsRecord.Builder record = MovingStatisticsRecord.newBuilder()
            .setEntityOid(VCPU_FIELD.getEntityOid())
            .setThrottlingRecord(ThrottlingMovingStatisticsRecord.newBuilder()
                .addCapacityRecords(ThrottlingCapacityMovingStatistics.newBuilder()
                    .setVcpuCapacity(200.0)
                    .setSampleCount(2)
                    .setLastSampleTimestamp(1234L)
                    .setThrottlingMaxSample(100.0)
                    .setThrottlingFastMovingAverage(10.0)
                    .setThrottlingFastMovingVariance(2.0)
                    .setThrottlingSlowMovingAverage(12.0)
                    .setThrottlingSlowMovingVariance(1.0))
                .addCapacityRecords(ThrottlingCapacityMovingStatistics.newBuilder()
                    .setVcpuCapacity(300.0)
                    .setSampleCount(2)
                    .setLastSampleTimestamp(1234L)
                    .setThrottlingMaxSample(100.0)
                    .setThrottlingFastMovingAverage(10.0)
                    .setThrottlingFastMovingVariance(2.0)
                    .setThrottlingSlowMovingAverage(12.0)
                    .setThrottlingSlowMovingVariance(1.0))
                .setActiveVcpuCapacity(200.0)
                .build());

        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(VCPU_FIELD);
        sampler.deserialize(record.build());
        assertEquals(record.build(), sampler.serialize().build());
    }

    /**
     * Test restoring from a String proto definition and generating the correct min threshold from a complex
     * history.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testRestoreFromProtoDefinition() throws Exception {
        final VcpuThrottlingSampler sampler = new VcpuThrottlingSampler(TEST_FIELD);
        final MovingStatisticsRecord.Builder record = MovingStatisticsRecord.newBuilder();
        TextFormat.getParser().merge(PROTO_DEFINITION, record);

        sampler.deserialize(record.build());
        final Double minThreshold = sampler.getMinThreshold(TEST_FIELD, 2.0, 3.5);
        assertThat(minThreshold, greaterThan(335.0));
    }

    private void setThrottlingAtCapacity(final double throttlingValue,
                                         final double vcpuCapacity,
                                         @Nonnull final VcpuThrottlingSampler sampler) {
        containerSpec.getCommoditySoldListBuilderList().get(VCPU_COMMODITY_INDEX)
            .setCapacity(vcpuCapacity);
        containerSpec.getCommoditySoldListBuilderList().get(THROTTLING_COMMODITY_INDEX)
            .setUsed(throttlingValue);

        updateContainerSpecTimestamp(Duration.ofMinutes(10).toMillis());
        sampler.addSample(SAMPLER_CONFIGURATION, VCPU_FIELD, PARTNER_FIELDS, commodityFieldAccessor);
    }

    private void elapseHours(final double throttlingValue,
                             final double vcpuValue,
                             final long hoursToElapse,
                             @Nonnull final VcpuThrottlingSampler sampler) {
        elapseHoursByIncrement(throttlingValue, vcpuValue, hoursToElapse,
            TimeUnit.HOURS.toMillis(1), sampler);
    }

    private void elapseHoursByIncrement(final double throttlingValue,
                                        final double vcpuValue,
                                        final long hoursToElapse,
                                        final long incrementMillis,
                                        @Nonnull final VcpuThrottlingSampler sampler) {
        containerSpec.getCommoditySoldListBuilderList().get(VCPU_COMMODITY_INDEX)
            .setUsed(vcpuValue);
        containerSpec.getCommoditySoldListBuilderList().get(THROTTLING_COMMODITY_INDEX)
            .setUsed(throttlingValue);
        final long numUpdatePeriods = TimeUnit.HOURS.toMillis(hoursToElapse) / incrementMillis;

        for (int i = 0; i < numUpdatePeriods; i++) {
            updateContainerSpecTimestamp(incrementMillis);
            sampler.addSample(SAMPLER_CONFIGURATION, VCPU_FIELD, PARTNER_FIELDS, commodityFieldAccessor);
        }
    }

    private void updateContainerSpecTimestamp(final long incrementMillis) {
        final DiscoveryOrigin.Builder discoveryOrigin =
            containerSpec.getOriginBuilder().getDiscoveryOriginBuilder();
        discoveryOrigin.setLastUpdatedTime(discoveryOrigin.getLastUpdatedTime() + incrementMillis);
    }

    private long getContainerspecTimestamp() {
        final DiscoveryOrigin.Builder discoveryOrigin =
            containerSpec.getOriginBuilder().getDiscoveryOriginBuilder();
        return discoveryOrigin.getLastUpdatedTime();
    }

    private void assertHistoricalThrottling(final double expectedThrottling,
                                            @Nonnull VcpuThrottlingSampler sampler, double delta) {
        assertEquals(expectedThrottling, sampler.meanPlusSigma(THROTTLING_FIELD, 0), delta);
    }
}
