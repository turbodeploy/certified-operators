package com.vmturbo.topology.processor.history.movingstats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.exceptions.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.Builder;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.MovingStatisticsSamplerDataCase;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;

/**
 * Unit tests for {@link MovingStatisticsCommodityData}.
 */
public class MovingStatisticsCommodityDataTest extends MovingStatisticsBaseTest {

    private final MovingStatisticsCommodityData data = new MovingStatisticsCommodityData();

    private final EntityCommodityFieldReference memRef = new EntityCommodityFieldReference(
        1L, CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE).build(),
        CommodityField.USED);

    private final EntityCommodityFieldReference cpuRef = new EntityCommodityFieldReference(
        2L, CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE).build(),
        CommodityField.USED);

    private final Clock clock = Mockito.mock(Clock.class);

    private static final MovingStatisticsRecord.Builder RECORD_BUILDER = MovingStatisticsRecord.newBuilder();

    private final VcpuThrottlingSampler mockSampler = mock(VcpuThrottlingSampler.class);

    private final MovingStatisticsSamplingConfiguration<VcpuThrottlingSampler> memSamplerConfig =
        mockSamplingConfig(CommodityDTO.CommodityType.VMEM_VALUE, CommodityDTO.CommodityType.BALLOONING_VALUE,
            Duration.ofHours(1), VcpuThrottlingSampler::new, MovingStatisticsSamplerDataCase.THROTTLING_RECORD);

    private final MovingStatisticsSamplingConfiguration<VcpuThrottlingSampler> cpuSamplerConfig =
        mockSamplingConfig(CommodityDTO.CommodityType.VCPU_VALUE, CommodityDTO.CommodityType.BALLOONING_VALUE,
            Duration.ofHours(2), (ref) -> mockSampler, MovingStatisticsSamplerDataCase.MOVINGSTATISTICSSAMPLERDATA_NOT_SET);

    private final MovingStatisticsHistoricalEditorConfig config =
        movingStatisticsHistoricalEditorConfig(clock, Arrays.asList(memSamplerConfig, cpuSamplerConfig));

    private final HistoryAggregationContext context = mock(HistoryAggregationContext.class);

    private final ICommodityFieldAccessor fieldAccessor = mock(ICommodityFieldAccessor.class);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Setup tests.
     */
    @Before
    public void setup() {
        when(context.getAccessor()).thenReturn(fieldAccessor);
    }

    /**
     * Test proper initialization.
     */
    @Test
    public void testInit() {
        assertTrue(data.needsReinitialization(memRef, context, config));
        data.init(memRef, null, config, context);
        assertFalse(data.needsReinitialization(memRef, context, config));
    }

    /**
     * Test valid serialization.
     *
     * @throws InvalidHistoryDataException on exception
     */
    @Test
    public void testValidSerialize() throws InvalidHistoryDataException {
        data.init(memRef, null, config, context);
        final Builder serializedData = data.serialize();
        assertEquals(memRef.getEntityOid(), serializedData.getEntityOid());
    }

    /**
     * Test invalid serialization.
     *
     * @throws InvalidHistoryDataException on exception
     */
    @Test
    public void testInvalidSerialize() throws InvalidHistoryDataException {
        expectedException.expect(InvalidHistoryDataException.class);
        data.serialize();
    }

    /**
     * Test deserialization does not throw exception after initialization.
     *
     * @throws InvalidHistoryDataException on exception
     */
    @Test
    public void testDeserialize() throws InvalidHistoryDataException {
        data.init(memRef, null, config, context);
        data.deserialize(MovingStatisticsRecord.newBuilder()
            .setEntityOid(memRef.getEntityOid())
            .setThrottlingRecord(ThrottlingMovingStatisticsRecord.getDefaultInstance())
            .build());
    }

    /**
     * Test invalid deserialization.
     *
     * @throws InvalidHistoryDataException on exception
     */
    @Test
    public void testInvalidDeserialize() throws InvalidHistoryDataException {
        expectedException.expect(InvalidHistoryDataException.class);
        data.deserialize(RECORD_BUILDER.build());
    }

    /**
     * Test aggregation with insufficient history.
     */
    @Test
    public void testAggregateNoHistoryUpdate() {
        when(mockSampler.meanPlusSigma(any(EntityCommodityFieldReference.class), anyDouble()))
            .thenReturn(null);

        data.init(cpuRef, null, config, context);
        data.aggregate(cpuRef, config, context);

        verify(fieldAccessor, never()).updateHistoryValue(any(EntityCommodityFieldReference.class),
            any(), anyString());
    }

    /**
     * Test aggregation with history but should not set thresholds.
     */
    @Test
    public void testAggregateHistoryNoThresholds() {
        when(mockSampler.meanPlusSigma(any(EntityCommodityFieldReference.class), anyDouble()))
            .thenReturn(2.0);
        when(mockSampler.getMinThreshold(any(EntityCommodityFieldReference.class), anyDouble(), anyDouble()))
            .thenReturn(null);

        final ArgumentCaptor<Consumer> histValuesCaptor = ArgumentCaptor.forClass(Consumer.class);

        data.init(cpuRef, null, config, context);
        data.aggregate(cpuRef, config, context);

        verify(fieldAccessor).updateHistoryValue(argThat(refMatching(CommodityDTO.CommodityType.VCPU_VALUE)),
            histValuesCaptor.capture(), anyString());
        verify(fieldAccessor).updateHistoryValue(argThat(refMatching(CommodityDTO.CommodityType.BALLOONING_VALUE)),
            any(), anyString());
        verify(fieldAccessor, never()).updateThresholds(any(EntityCommodityFieldReference.class), any(), anyString());

        final HistoricalValues.Builder histValuesBuilder = HistoricalValues.newBuilder();
        histValuesCaptor.getValue().accept(histValuesBuilder);
        assertEquals(2.0, histValuesBuilder.getMovingMeanPlusStandardDeviations(), 0);
    }

    /**
     * Test aggregation setting both history and thresholds.
     */
    @Test
    public void testAggregateHistoryAndThresholds() {
        when(mockSampler.meanPlusSigma(any(EntityCommodityFieldReference.class), anyDouble()))
            .thenReturn(null);
        when(mockSampler.getMinThreshold(any(EntityCommodityFieldReference.class), anyDouble(), anyDouble()))
            .thenReturn(3.0);

        final ArgumentCaptor<Consumer> thresholdsCaptor = ArgumentCaptor.forClass(Consumer.class);

        data.init(cpuRef, null, config, context);
        data.aggregate(cpuRef, config, context);

        // Even when there is no analysis value, we should still set thresholds when they are available.
        verify(fieldAccessor, never()).updateHistoryValue(argThat(refMatching(CommodityDTO.CommodityType.VCPU_VALUE)),
            any(), anyString());
        verify(fieldAccessor, never()).updateHistoryValue(argThat(refMatching(CommodityDTO.CommodityType.BALLOONING_VALUE)),
            any(), anyString());

        verify(fieldAccessor).updateThresholds(argThat(refMatching(CommodityDTO.CommodityType.VCPU_VALUE)),
            thresholdsCaptor.capture(), anyString());
        verify(fieldAccessor).updateThresholds(argThat(refMatching(CommodityDTO.CommodityType.BALLOONING_VALUE)),
            any(), anyString());

        final Thresholds.Builder thresholdsBuilder = Thresholds.newBuilder();
        thresholdsCaptor.getValue().accept(thresholdsBuilder);
        assertEquals(3.0, thresholdsBuilder.getMin(), 0);
        assertFalse(thresholdsBuilder.hasMax());
    }

    /**
     * Test that expired data gets cleared on aggregate.
     */
    @Test
    public void testClearExpiredData() {
        when(clock.millis()).thenReturn(500L);
        data.init(cpuRef, null, config, context);
        data.aggregate(cpuRef, config, context);
        verify(data.getSampler()).cleanExpiredData(eq(500L), eq(cpuSamplerConfig));
    }

    private static RefMatcher refMatching(final int commType) {
        return new RefMatcher(commType);
    }

    /**
     * Argument matcher for finding {@link EntityCommodityFieldReference} of a given type.
     */
    private static class RefMatcher extends ArgumentMatcher<EntityCommodityFieldReference> {

        private final int commType;

        private RefMatcher(final int commType) {
            this.commType = commType;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof EntityCommodityFieldReference) {
                final EntityCommodityFieldReference ref = (EntityCommodityFieldReference)o;
                return ref.getCommodityType().getType() == commType;
            }
            return false;
        }
    }
}
