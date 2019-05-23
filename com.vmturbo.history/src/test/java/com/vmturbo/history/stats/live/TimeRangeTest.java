package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.AssertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;

public class TimeRangeTest {

    private static final long LATEST_TABLE_TIME_WINDOW_MS = 1;

    private HistorydbIO historydbIO = mock(HistorydbIO.class);

    private HistoryTimeFrameCalculator timeFrameCalculator = mock(HistoryTimeFrameCalculator.class);

    private final TimeRangeFactory timeRangeFactory =
            new DefaultTimeRangeFactory(historydbIO,
                    timeFrameCalculator,
                    TimeUnit.MILLISECONDS.toNanos(LATEST_TABLE_TIME_WINDOW_MS),
                    TimeUnit.NANOSECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateSetEndDateNotSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setStartDate(1L)
                .build(), Optional.empty(), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateNotSetEndDateSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setEndDate(1L)
                .build(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testFactoryDefaultTimeRangeUseRecentTimestamp() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(1L);
        when(historydbIO.getMostRecentTimestamp(Optional.empty(), Optional.empty())).thenReturn(Optional.of(timestamp));
        when(timeFrameCalculator.millis2TimeFrame(1L)).thenReturn(TimeFrame.LATEST);

        final TimeRange timeRange =
                timeRangeFactory.resolveTimeRange(StatsFilter.getDefaultInstance(), Optional.empty(), Optional.empty()).get();
        assertThat(timeRange.getStartTime(), is(1L));
        assertThat(timeRange.getEndTime(), is(1L));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryDefaultTimeRangeNoRecentTimestamp() throws VmtDbException {
        when(historydbIO.getMostRecentTimestamp(Optional.empty(), Optional.empty())).thenReturn(Optional.empty());
        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.getDefaultInstance(), Optional.empty(), Optional.empty());
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
            startAndEndTime, Optional.empty(), Optional.empty()))
                    .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                    .setStartDate(startAndEndTime)
                    .setEndDate(startAndEndTime)
                    .build(), Optional.empty(), Optional.empty());
        verify(timeFrameCalculator).millis2TimeFrame(startAndEndTime);
        verify(historydbIO).getTimestampsInRange(TimeFrame.LATEST,
                startAndEndTime - LATEST_TABLE_TIME_WINDOW_MS,
                startAndEndTime, Optional.empty(), Optional.empty());

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
                startTime, endTime, Optional.empty(), Optional.empty()))
            .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty());
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
    public void testFactoryNoExpandLatestTimeWindowBecauseNotLatestTimeFrame() throws VmtDbException {
        final Timestamp timestamp = new Timestamp(9L);
        final long startTime = 10L;
        final long endTime = 15L;
        // Time frame not latest.
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.DAY);
        when(historydbIO.getTimestampsInRange(TimeFrame.DAY,
                startTime, endTime, Optional.empty(), Optional.empty()))
            .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty());
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
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST,
                startTime, endTime, Optional.empty(), Optional.empty()))
            .thenReturn(Collections.emptyList());

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .build(), Optional.empty(), Optional.empty());
        assertFalse(timeRangeOpt.isPresent());
    }

    @Test
    public void testResolveTimeRangeSpecificEntityType() throws VmtDbException {

        final String specificEntityOid = "1";
        final EntityType vmType = EntityType.VIRTUAL_MACHINE;
        final Timestamp vmTimestamp = new Timestamp(9L);
        final EntityType pmType = EntityType.PHYSICAL_MACHINE;
        final Timestamp pmTimestamp = new Timestamp(12L);

        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.LATEST);

        // when there is no start/end date
        final StatsFilter statsFilterNoDate = StatsFilter.newBuilder().build();

        when(historydbIO.getMostRecentTimestamp(Optional.of(EntityType.VIRTUAL_MACHINE), Optional.of(specificEntityOid)))
            .thenReturn(Optional.of(vmTimestamp));

        when(historydbIO.getMostRecentTimestamp(Optional.of(EntityType.PHYSICAL_MACHINE), Optional.of(specificEntityOid)))
            .thenReturn(Optional.of(pmTimestamp));

        Optional<TimeRange> vmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterNoDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(vmType));

        Optional<TimeRange> pmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterNoDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(pmType));


        assertThat(vmTimeRange.get().getMostRecentSnapshotTime(), is(vmTimestamp));
        assertThat(pmTimeRange.get().getMostRecentSnapshotTime(), is(pmTimestamp));


        // when we have start/end date
        final long start = 5L;
        final long end = 10L;
        final StatsFilter statsFilterWithDate = StatsFilter.newBuilder()
            .setStartDate(start)
            .setEndDate(end)
            .build();

        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST, start, end, Optional.of(vmType), Optional.of(specificEntityOid)))
            .thenReturn(ImmutableList.of(vmTimestamp));

        when(historydbIO.getTimestampsInRange(TimeFrame.LATEST, start, end, Optional.of(pmType), Optional.of(specificEntityOid)))
            .thenReturn(ImmutableList.of(pmTimestamp));

        vmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterWithDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(vmType));

        pmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterWithDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(pmType));


        assertThat(vmTimeRange.get().getMostRecentSnapshotTime(), is(vmTimestamp));
        assertThat(pmTimeRange.get().getMostRecentSnapshotTime(), is(pmTimestamp));

    }

}
