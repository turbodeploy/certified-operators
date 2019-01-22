package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;

public class TimeRangeTest {

    private static final long LATEST_TABLE_TIME_WINDOW_MS = 1;

    private HistorydbIO historydbIO = mock(HistorydbIO.class);

    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private final TimeRangeFactory timeRangeFactory =
            new DefaultTimeRangeFactory(historydbIO,
                    timeFrameCalculator,
                    TimeUnit.MILLISECONDS.toNanos(LATEST_TABLE_TIME_WINDOW_MS),
                    TimeUnit.NANOSECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateSetEndDateNotSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setStartDate(1L)
                .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateNotSetEndDateSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setEndDate(1L)
                .build());
    }

    @Test
    public void testFactoryDefaultTimeRangeUseRecentTimestamp() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(1L);
        when(historydbIO.getMostRecentTimestamp()).thenReturn(Optional.of(timestamp));
        when(timeFrameCalculator.millis2TimeFrame(1L)).thenReturn(TimeFrame.LATEST);

        final TimeRange timeRange =
                timeRangeFactory.resolveTimeRange(StatsFilter.getDefaultInstance()).get();
        assertThat(timeRange.getStartTime(), is(1L));
        assertThat(timeRange.getEndTime(), is(1L));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryDefaultTimeRangeNoRecentTimestamp() throws VmtDbException {
        when(historydbIO.getMostRecentTimestamp()).thenReturn(Optional.empty());
        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.getDefaultInstance());
        assertFalse(timeRangeOpt.isPresent());
    }

    @Test
    public void testFactoryExpandLatestTimeWindow() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startAndEndTime = 10L;
        // Window expansion should only apply when time frame is latest.
        when(timeFrameCalculator.millis2TimeFrame(startAndEndTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
            startAndEndTime - LATEST_TABLE_TIME_WINDOW_MS,
            startAndEndTime))
                    .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                    .setStartDate(startAndEndTime)
                    .setEndDate(startAndEndTime)
                    .build());
        verify(timeFrameCalculator).millis2TimeFrame(startAndEndTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.LATEST,
                startAndEndTime - LATEST_TABLE_TIME_WINDOW_MS,
                startAndEndTime);

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(startAndEndTime - LATEST_TABLE_TIME_WINDOW_MS));
        assertThat(timeRange.getEndTime(), is(startAndEndTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));

    }

    @Test
    public void testFactoryNoExpandLatestTimeWindowBecauseUnequalStartEnd() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        // Time frame still latest, but start and end time are no longer equal.
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime)).thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build());
        verify(timeFrameCalculator).millis2TimeFrame(startTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime);

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(startTime));
        assertThat(timeRange.getEndTime(), is(endTime));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryNoExpandLatestTimeWindowBecauseNotLatestTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        // Time frame not latest.
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.DAY);
        when(historydbIO.getTimestampsInRange(TimeFrame.DAY,
                startTime, endTime)).thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build());
        verify(timeFrameCalculator).millis2TimeFrame(startTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.DAY,
                startTime, endTime);

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
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime)).thenReturn(Collections.emptyList());

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build());
        assertFalse(timeRangeOpt.isPresent());
    }

}
