package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.jooq.Result;
import org.jooq.Select;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.PmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.live.LiveStatsReader.StatRecordPage;
import com.vmturbo.history.stats.live.StatsQueryFactory.AGGREGATE;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;

public class LiveStatsReaderTest {

    private HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

    private TimeRangeFactory timeRangeFactory = mock(TimeRangeFactory.class);

    private StatsQueryFactory statsQueryFactory = mock(StatsQueryFactory.class);

    private LiveStatsReader liveStatsReader =
            new LiveStatsReader(mockHistorydbIO, timeRangeFactory, statsQueryFactory);

    private static final Timestamp TIMESTAMP = new Timestamp(0);

    private static final TimeFrame TIME_FRAME = TimeFrame.LATEST;

    private final PmStatsLatest TABLE = PmStatsLatest.PM_STATS_LATEST;

    @Test
    public void testGetStatPage() throws VmtDbException {
        final Set<String> entityIds = Collections.singleton("1");
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(123L)
                .build();
        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter)).thenReturn(Optional.of(timeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        when(nextPageInfo.getEntityOids()).thenReturn(Lists.newArrayList(entityIds));
        when(nextPageInfo.getTable()).thenReturn(TABLE);
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(mockHistorydbIO.getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams))
                .thenReturn(nextPageInfo);

        Select<?> query = mock(Select.class);
        when(statsQueryFactory.createStatsQuery(nextPageInfo.getEntityOids(), TABLE, statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG))
                .thenReturn(Optional.of(query));

        final PmStatsLatestRecord testRecord = new PmStatsLatestRecord();
        testRecord.setUuid("1");
        final Result records = mock(Result.class);
        doAnswer((Answer<Void>) invocation -> {
            invocation.getArgumentAt(0, Consumer.class).accept(testRecord);
            return null;
        }).when(records).forEach(any());
        when(mockHistorydbIO.execute(Style.FORCED, query)).thenReturn(records);

        final StatRecordPage result =
                liveStatsReader.getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        verify(timeRangeFactory).resolveTimeRange(statsFilter);
        verify(mockHistorydbIO).getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams);
        verify(statsQueryFactory).createStatsQuery(nextPageInfo.getEntityOids(), TABLE, statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);

        assertThat(result.getNextCursor(), is(nextCursor));
        assertThat(result.getNextPageRecords(), is(ImmutableMap.of(1L, Collections.singletonList(testRecord))));
    }

    @Test
    public void testGetStatPageNextPageIsEmpty() throws VmtDbException {
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(123L)
                .build();
        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter)).thenReturn(Optional.of(timeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        // entity OIDs are empty
        when(nextPageInfo.getEntityOids()).thenReturn(Collections.emptyList());
        when(nextPageInfo.getTable()).thenReturn(TABLE);
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(mockHistorydbIO.getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams))
                .thenReturn(nextPageInfo);

        final StatRecordPage result =
                liveStatsReader.getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        verify(timeRangeFactory).resolveTimeRange(statsFilter);
        verify(mockHistorydbIO).getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams);
        // verify early return
        verify(statsQueryFactory, never()).createStatsQuery(nextPageInfo.getEntityOids(), TABLE, statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);

        assertThat(result.getNextCursor(), is(Optional.empty()));
        assertThat(result.getNextPageRecords(), is(Collections.emptyMap()));
    }
}