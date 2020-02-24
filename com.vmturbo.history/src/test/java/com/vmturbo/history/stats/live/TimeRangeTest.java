package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
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
                .build(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryStartDateNotSetEndDateSet() throws VmtDbException {
        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                .setEndDate(1L)
                .build(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testFactoryDefaultTimeRangeUseRecentTimestamp() throws VmtDbException {

        final StatsFilter statsFilter = StatsFilter.getDefaultInstance();

        final Timestamp timestamp = new Timestamp(1L);
        when(historydbIO.getClosestTimestampBefore(statsFilter, Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty()))
                .thenReturn(Optional.of(timestamp));
        when(timeFrameCalculator.millis2TimeFrame(1L)).thenReturn(TimeFrame.LATEST);

        final TimeRange timeRange =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                        Optional.empty(), Optional.empty()).get();
        assertThat(timeRange.getStartTime(), is(1L));
        assertThat(timeRange.getEndTime(), is(1L));
        assertThat(timeRange.getMostRecentSnapshotTime(), is(timestamp));
        assertThat(timeRange.getTimeFrame(), is(TimeFrame.LATEST));
        assertThat(timeRange.getSnapshotTimesInRange(), containsInAnyOrder(timestamp));
    }

    @Test
    public void testFactoryDefaultTimeRangeNoRecentTimestamp() throws VmtDbException {
        final StatsFilter statsFilter = StatsFilter.getDefaultInstance();
        when(historydbIO.getClosestTimestampBefore(statsFilter, Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty())).thenReturn(Optional.empty());
        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                        Optional.empty());
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
        when(timeFrameCalculator.millis2TimeFrame(startAndEndTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getClosestTimestampBefore(eq(statsFilter), any(), any(), any(), any()))
                .thenReturn(Optional.of(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                        Optional.empty());
        verify(timeFrameCalculator).millis2TimeFrame(startAndEndTime);

        final TimeRange timeRange = timeRangeOpt.get();
        assertThat(timeRange.getStartTime(), is(timestamp.getTime()));
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
                        .build(), Optional.empty(), Optional.empty(), Optional.empty());
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
                        .build(), Optional.empty(), Optional.empty(), Optional.empty());
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
                        .build(), Optional.empty(), Optional.empty(), Optional.empty());
        assertFalse(timeRangeOpt.isPresent());
    }

    @Test
    public void testResolveTimeWithMixedCommInStatsFilter() throws VmtDbException {
        final long startTime = 10L;
        final long endTime = 10L;
        final Timestamp piTimestamp = new Timestamp(8L);
        final Timestamp cpuTimestamp = new Timestamp(9L);
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        StatsFilter filterWithMixedComm = StatsFilter.newBuilder().setStartDate(startTime)
                        .setEndDate(endTime).addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(StringConstants.PRICE_INDEX))
                        .addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(StringConstants.CPU))
                        .build();
        when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.LATEST);
        when(historydbIO.getClosestTimestampBefore(filterWithMixedComm, Optional.empty(), Optional.empty(),
                Optional.of(startTime), Optional.empty())).thenReturn(Optional.of(cpuTimestamp));
        when(historydbIO.getClosestTimestampBefore(filterWithMixedComm, Optional.empty(), Optional.empty(),
                Optional.of(startTime), Optional.of(paginationParam))).thenReturn(Optional.of(piTimestamp));
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);

        final Optional<TimeRange> timeRangeOpt1 = timeRangeFactory.resolveTimeRange(filterWithMixedComm,
                Optional.empty(), Optional.empty(), Optional.empty());
        assertTrue(timeRangeOpt1.isPresent());
        assertTrue(timeRangeOpt1.get().getMostRecentSnapshotTime().getTime() == cpuTimestamp.getTime());
        final Optional<TimeRange> timeRangeOpt2 = timeRangeFactory.resolveTimeRange(filterWithMixedComm,
                Optional.empty(), Optional.empty(), Optional.of(paginationParam));
        assertTrue(timeRangeOpt2.isPresent());
        assertTrue(timeRangeOpt2.get().getMostRecentSnapshotTime().getTime() == piTimestamp.getTime());
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
        Mockito.when(timeFrameCalculator.millis2TimeFrame(startTime)).thenReturn(TimeFrame.DAY);
        Mockito.when(timeFrameCalculator.millis2TimeFrame(rollupPeriod)).thenReturn(TimeFrame.MONTH);
        Mockito.when(historydbIO.getTimestampsInRange(TimeFrame.MONTH,
                        startTime, endTime, Optional.empty(), Optional.empty()))
                        .thenReturn(Collections.singletonList(timestamp));

        final Optional<TimeRange> timeRangeOpt =
                        timeRangeFactory.resolveTimeRange(StatsFilter.newBuilder()
                                        .setStartDate(startTime)
                                        .setEndDate(endTime)
                                        .setRollupPeriod(rollupPeriod)
                                        .build(), Optional.empty(), Optional.empty(), Optional.empty());
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

        when(historydbIO.getClosestTimestampBefore(statsFilterNoDate,
                Optional.of(EntityType.VIRTUAL_MACHINE), Optional.of(specificEntityOid),
                Optional.empty(), Optional.empty())).thenReturn(Optional.of(vmTimestamp));

        when(historydbIO.getClosestTimestampBefore(statsFilterNoDate,
                Optional.of(EntityType.PHYSICAL_MACHINE), Optional.of(specificEntityOid),
                Optional.empty(), Optional.empty())).thenReturn(Optional.of(pmTimestamp));

        Optional<TimeRange> vmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterNoDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(vmType), Optional.empty());

        Optional<TimeRange> pmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterNoDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(pmType), Optional.empty());


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
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(vmType), Optional.empty());

        pmTimeRange = timeRangeFactory.resolveTimeRange(statsFilterWithDate,
            Optional.of(ImmutableList.of(specificEntityOid)), Optional.of(pmType), Optional.empty());


        assertThat(vmTimeRange.get().getMostRecentSnapshotTime(), is(vmTimestamp));
        assertThat(pmTimeRange.get().getMostRecentSnapshotTime(), is(pmTimestamp));

    }

}
