package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for UtilizationCountStore.
 */
public class UtilizationCountStoreTest {
    private static final double delta = 0.001;
    private UtilizationCountStore store;
    private EntityCommodityFieldReference ref;
    private Clock clock;

    /**
     * Set up the test.
     *
     * @throws HistoryCalculationException when failed
     */
    @Before
    public void setUp() throws HistoryCalculationException {
        ref =
            new EntityCommodityFieldReference(134L,
                                              CommodityType.newBuilder().setKey("efds").setType(12).build(),
                                              4857L, CommodityField.USED);
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(Instant.now().toEpochMilli());
        store = new UtilizationCountStore(new PercentileBuckets(), ref, 30);
    }

    /**
     * Test the points added to the store produce proper percentile rank.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testAddPoints() throws HistoryCalculationException {
        store.addPoints(ImmutableList.of(10d, 10d, 10d, 10d, 10d), 100d, 100);
        Assert.assertEquals(10, store.getPercentile(90));
        // adding for the same time should have no effect
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d, 100);
        Assert.assertEquals(10, store.getPercentile(90));
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d, 200);
        Assert.assertEquals(20, store.getPercentile(80));
    }

    /**
     * Test the serialization/deserialization of latest record.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testLatestRecord() throws HistoryCalculationException {
        PercentileRecord.Builder builder = PercentileRecord.newBuilder()
                        .setEntityOid(ref.getEntityOid())
                        .setCommodityType(ref.getCommodityType().getType())
                        .setKey(ref.getCommodityType().getKey())
                        .setProviderOid(ref.getProviderOid()).setCapacity(0f).setPeriod(30);
        for (int i = 0; i <= 100; ++i) {
            builder.addUtilization(20);
        }
        PercentileRecord rec1 = builder.build();
        store.setLatestCountsRecord(rec1);
        PercentileRecord rec2 = store.getLatestCountsRecord().build();
        Assert.assertEquals(rec1.getEntityOid(), rec2.getEntityOid());
        Assert.assertEquals(rec1.getCommodityType(), rec2.getCommodityType());
        Assert.assertEquals(rec1.getCapacity(), rec2.getCapacity(), delta);
        Assert.assertEquals(rec1.getUtilizationCount(), rec2.getUtilizationCount());
    }

    /**
     * Test the full record checkpoint behavior.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckpoint() throws HistoryCalculationException {
        final float capacity = 100f;
        PercentileRecord.Builder oldest = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        store.addPoints(ImmutableList.of(10d, 20d, 30d, 30d), capacity, 100);
        for (int i = 0; i <= 100; ++i) {
            oldest.addUtilization(i == 20 ? 1 : 0);
        }
        PercentileRecord.Builder full = store.checkpoint(Collections.singleton(oldest.build()));
        Assert.assertNotNull(full);
        PercentileRecord record = full.build();

        Assert.assertTrue(record.hasCapacity());
        Assert.assertEquals(capacity, record.getCapacity(), delta);
        Assert.assertEquals(101, record.getUtilizationCount());
        Assert.assertEquals(0, record.getUtilization(0));
        Assert.assertEquals(1, record.getUtilization(10));
        Assert.assertEquals(0, record.getUtilization(20));
        Assert.assertEquals(2, record.getUtilization(30));
    }

    /**
     * Checks that in case min observation setting is disabled (equal to 0) then this case will be
     * treated as entity has enough history data.
     */
    @Test
    public void checkMinObservationWindowDisabled() {
        final PercentileHistoricalEditorConfig config =
                        Mockito.mock(PercentileHistoricalEditorConfig.class);
        Mockito.when(config.getMinObservationPeriod(134L)).thenReturn(0);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(true));
    }

    /**
     * Checks that in case min observation period setting enabled, but TP is doing its first
     * discovery cycle, i.e. we have no historical data then this case will be treated as entity has
     * no enough history data.
     */
    @Test
    public void checkMinObservationWindowStartTimestampNotInitialized() {
        final PercentileHistoricalEditorConfig config =
                        Mockito.mock(PercentileHistoricalEditorConfig.class);
        Mockito.when(config.getMinObservationPeriod(134L)).thenReturn(1);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(false));
    }

    /**
     * Checks that in case min observation setting enabled and its value equal to one day, but our
     * earliest point collected by TP greater than one day before, then we are treating this case
     * that entity has no enough history data.
     *
     * @throws HistoryCalculationException in case of error while adding point to the store.
     */
    @Test
    public void checkMinObservationWindowHasNoEnoughData() throws HistoryCalculationException {
        final PercentileHistoricalEditorConfig config =
                        Mockito.mock(PercentileHistoricalEditorConfig.class);
        Mockito.when(config.getMinObservationPeriod(134L)).thenReturn(1);
        final long currentTime = Duration.ofDays(2).toMillis();
        Mockito.when(clock.millis()).thenReturn(currentTime);
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d, currentTime - 100);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(false));
    }

    /**
     * Checks that in case min observation setting enabled and its value equal to one day, but our
     * earliest point collected by TP lower than one day before and latest point collected by TP was
     * collected more than hour ago(default allowable gap in data), then we are treating this case
     * that entity has no enough comprehensive history data. Checks that min observation period
     * start point will be reset for the entity.
     *
     * @throws HistoryCalculationException in case of error while adding point to
     *                 the store.
     */
    @Test
    public void checkMinObservationWindowHasEnoughDataWithGaps()
                    throws HistoryCalculationException {
        final PercentileHistoricalEditorConfig config =
                        Mockito.mock(PercentileHistoricalEditorConfig.class);
        Mockito.when(config.getMinObservationPeriod(134L)).thenReturn(1);
        final long currentTime = Duration.ofDays(2).toMillis();
        Mockito.when(clock.millis()).thenReturn(currentTime);
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d,
                        currentTime - Duration.ofDays(1).toMillis() - 100);
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d,
                        currentTime - Duration.ofHours(1).toMillis() - 100);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(false));
        final long newCurrentTime = currentTime + Duration.ofDays(1).toMillis() + 100;
        Mockito.when(clock.millis()).thenReturn(newCurrentTime);
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d, newCurrentTime + 50);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(true));
    }

    /**
     * Checks that in case there is more data collected that required by min observation period and
     * latest point collected by TP has been collected recently (no longer than default allowable
     * gap period), then we would treat this case as entity has enough history data.
     *
     * @throws HistoryCalculationException in case of error while adding point to
     *                 the store.
     */
    @Test
    public void checkMinObservationWindowHasEnoughDataWithoutGaps()
                    throws HistoryCalculationException {
        final PercentileHistoricalEditorConfig config =
                        Mockito.mock(PercentileHistoricalEditorConfig.class);
        Mockito.when(config.getMinObservationPeriod(134L)).thenReturn(1);
        final long currentTime = Duration.ofDays(2).toMillis();
        Mockito.when(clock.millis()).thenReturn(currentTime);
        final long defaultAllowableGap = Duration.ofDays(1).toMillis();
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d,
                        currentTime - defaultAllowableGap - 100);
        store.addPoints(ImmutableList.of(20d, 20d, 20d, 20d, 20d), 100d, currentTime + 100);
        Assert.assertThat(store.isMinHistoryDataAvailable(config, clock), CoreMatchers.is(true));
    }
}
