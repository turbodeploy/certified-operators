package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;

public class TimeRangeTest {

    private static final long LATEST_TABLE_TIME_WINDOW_MS = 1;

    private HistorydbIO historydbIO = mock(HistorydbIO.class);

    private TimeFrameCalculator timeFrameCalculator = spy(new TimeFrameCalculator(mock(Clock.class),
            mock(RetentionPeriodFetcher.class)));

    private final TimeRangeFactory timeRangeFactory =
            new DefaultTimeRangeFactory(historydbIO,
                    timeFrameCalculator,
                    TimeUnit.MILLISECONDS.toNanos(LATEST_TABLE_TIME_WINDOW_MS),
                    TimeUnit.NANOSECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateSetEndDateNotSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setStartDate(1L)
                .build(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateNotSetEndDateSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setEndDate(1L)
                .build(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testFactoryDefaultTimeRangeUseRecentTimestamp() throws VmtDbException {

        final StatsFilter statsFilter = StatsFilter.getDefaultInstance();

        final Timestamp timestamp = new Timestamp(1L);
        when(historydbIO.getClosestTimestampBefore(statsFilter, Optional.empty(), Optional.empty()))
                .thenReturn(Optional.of(timestamp));
        doReturn(TimeFrame.LATEST).when(timeFrameCalculator).millis2TimeFrame(1L);

        final TimeRange timeRange =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty()).get();
        assertThat(timeRange.getStartTime(), is(1L));
        assertThat(timeRange.getEndTime(), is(1L));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryDefaultTimeRangeWithTimeFrame() throws VmtDbException {

        final StatsFilter statsFilter = StatsFilter.getDefaultInstance();

        final Timestamp timestamp = new Timestamp(1L);
        when(historydbIO.getClosestTimestampBefore(statsFilter, Optional.empty(), Optional.of(TimeFrame.DAY)))
                .thenReturn(Optional.of(timestamp));

        final TimeRange timeRange =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.of(TimeFrame.DAY)).get();
        verify(historydbIO).getClosestTimestampBefore(any(), any(), eq(Optional.of(TimeFrame.DAY)));
        verify(timeFrameCalculator, never()).millis2TimeFrame(1L);
        assertThat(timeRange.getStartTime(), is(1L));
        assertThat(timeRange.getEndTime(), is(1L));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.DAY));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryDefaultTimeRangeNoRecentTimestamp() throws VmtDbException {
        final StatsFilter statsFilter = StatsFilter.getDefaultInstance();
        when(historydbIO.getClosestTimestampBefore(statsFilter, Optional.empty(), Optional.empty()))
                .thenReturn(Optional.empty());
        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty());
        assertFalse(timeRangeOpt.isPresent());
    }

    @Test
    public void testFactoryExpandLatestTimeWindow() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startAndEndTime = 10L;
        StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startAndEndTime)
                .setEndDate(startAndEndTime)
                .build();
        // Window expansion should only apply when time frame is latest.
        doReturn(TimeFrame.LATEST).when(timeFrameCalculator).millis2TimeFrame(startAndEndTime);
        when(historydbIO.getClosestTimestampBefore(eq(statsFilter), any(), any()))
                .thenReturn(Optional.of(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty());
        verify(timeFrameCalculator).millis2TimeFrame(startAndEndTime);

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(timestamp.getTime()));
        assertThat(timeRange.getEndTime(), is(startAndEndTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));

    }

    @Test
    public void testFactoryExpandLatestTimeWindowWithTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startAndEndTime = 10L;
        StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startAndEndTime)
                .setEndDate(startAndEndTime)
                .build();
        when(historydbIO.getClosestTimestampBefore(eq(statsFilter), any(), any()))
                .thenReturn(Optional.of(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.of(TimeFrame.HOUR));

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(timestamp.getTime()));
        assertThat(timeRange.getEndTime(), is(startAndEndTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.HOUR));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));

    }

    @Test
    public void testFactoryNoExpandLatestTimeWindowBecauseUnequalStartEnd() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        // Time frame still latest, but start and end time are no longer equal.
        doReturn(TimeFrame.LATEST).when(timeFrameCalculator).millis2TimeFrame(startTime);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime, Optional.empty(), Optional.empty()))
                .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        verify(timeFrameCalculator).millis2TimeFrame(startTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime, Optional.empty(), Optional.empty());

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(startTime));
        assertThat(timeRange.getEndTime(), is(endTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryNoExpandLatestTimeWindowUnequalStartEndWithTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;

        when(historydbIO.getTimestampsInRange(TimeFrame.MONTH,
                startTime, endTime, Optional.empty(), Optional.empty()))
                .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                                .setStartDate(startTime)
                                .setEndDate(endTime)
                                .build(), Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.of(TimeFrame.MONTH));

        verify(historydbIO).getTimestampsInRange(TimeFrame.MONTH,
                startTime, endTime, Optional.empty(), Optional.empty());

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(startTime));
        assertThat(timeRange.getEndTime(), is(endTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.MONTH));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryNoExpandLatestTimeWindowBecauseNotLatestTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        // Time frame not latest.
        doReturn(TimeFrame.DAY).when(timeFrameCalculator).millis2TimeFrame(startTime);
        when(historydbIO.getTimestampsInRange(TimeFrame.DAY,
                startTime, endTime, Optional.empty(), Optional.empty()))
                .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        verify(timeFrameCalculator).millis2TimeFrame(startTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.DAY,
                startTime, endTime, Optional.empty(), Optional.empty());

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(startTime));
        assertThat(timeRange.getEndTime(), is(endTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.DAY));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryNoTimestampsInRange() throws VmtDbException {
        final long startTime = 10L;
        final long endTime = 15L;
        // Window expansion should only apply when time frame is latest.
        doReturn(TimeFrame.LATEST).when(timeFrameCalculator).millis2TimeFrame(startTime);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime, Optional.empty(), Optional.empty()))
            .thenReturn(Collections.emptyList());

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        assertFalse(timeRangeOpt.isPresent());
    }

    @Test
    public void testResolveTimeWithMixedCommInStatsFilter() throws VmtDbException {
        final long startTime = 10L;
        final long endTime = 10L;
        final Timestamp cpuTimestamp = new Timestamp(9L);
        StatsFilter filterWithMixedComm = StatsFilter.newBuilder().setStartDate(startTime)
                        .setEndDate(endTime).addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(StringConstants.PRICE_INDEX))
                        .addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(StringConstants.CPU))
                        .build();
        doReturn(TimeFrame.LATEST).when(timeFrameCalculator).millis2TimeFrame(startTime);
        when(historydbIO.getClosestTimestampBefore(eq(filterWithMixedComm),eq(Optional.of(startTime)), any()))
                .thenReturn(Optional.of(cpuTimestamp));

        final Optional<TimeRange> timeRangeOpt1 = timeRangeFactory.resolveTimeRange(filterWithMixedComm,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        assertTrue(timeRangeOpt1.isPresent());
        assertTrue(timeRangeOpt1.get().getMostRecentSnapshotTime().getTime() == cpuTimestamp.getTime());
    }

    /**
     * Checks that rollup period takes precedence calculated timeframe from start/end time
     * difference.
     *
     * @throws VmtDbException in case of error while doing request to DB to get
     *                 timestamps in range.
     */
    @Test
    public void testRollupPeriodTakesPrecedenceAsTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        final long rollupPeriod = 30L;
        // Time frame not latest.
        doReturn(TimeFrame.DAY).when(timeFrameCalculator).millis2TimeFrame(startTime);
        doReturn(TimeFrame.MONTH).when(timeFrameCalculator).millis2TimeFrame(rollupPeriod);
        Mockito.when(historydbIO.getTimestampsInRange(TimeFrame.MONTH,
                        startTime, endTime, Optional.empty(), Optional.empty()))
                        .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                                        .setStartDate(startTime)
                                        .setEndDate(endTime)
                                        .setRollupPeriod(rollupPeriod)
                                        .build(),
                                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        Mockito.verify(timeFrameCalculator).millis2TimeFrame(rollupPeriod);
        Mockito.verify(historydbIO).getTimestampsInRange(TimeFrame.MONTH,
                        startTime, endTime, Optional.empty(), Optional.empty());

        final TimeRange timeRange = timeRangeOpt.get();
        Assert.assertThat(timeRange.getStartTime(), is(startTime));
        Assert.assertThat(timeRange.getEndTime(), is(endTime));
        Assert.assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        Assert.assertThat(timeRange.getTimeFrame(), is(TimeFrame.MONTH));
        Assert.assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }
}
