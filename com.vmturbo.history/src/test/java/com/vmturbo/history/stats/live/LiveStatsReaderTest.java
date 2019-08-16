package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.PmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor.FullMarketRatioProcessorFactory;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.LiveStatsReader.StatRecordPage;
import com.vmturbo.history.stats.live.StatsQueryFactory.AGGREGATE;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;

public class LiveStatsReaderTest {

    private HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

    private TimeRangeFactory timeRangeFactory = mock(TimeRangeFactory.class);

    private StatsQueryFactory statsQueryFactory = mock(StatsQueryFactory.class);

    private RatioRecordFactory ratioRecordFactory = mock(RatioRecordFactory.class);

    private FullMarketRatioProcessorFactory fullMarketRatioProcessor =
        mock(FullMarketRatioProcessorFactory.class);

    private LiveStatsReader liveStatsReader =
            new LiveStatsReader(mockHistorydbIO, timeRangeFactory,
                statsQueryFactory,
                fullMarketRatioProcessor,
                ratioRecordFactory);

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
        when(paginationParams.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(), Optional.empty()))
                .thenReturn(Optional.of(timeRange));
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                Optional.of(paginationParams))).thenReturn(Optional.of(timeRange));

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

        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(), Optional.empty());
        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(), Optional.of(paginationParams));
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
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                 Optional.of(paginationParams))).thenReturn(Optional.of(timeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        // entity OIDs are empty
        when(nextPageInfo.getEntityOids()).thenReturn(Collections.emptyList());
        when(nextPageInfo.getTable()).thenReturn(TABLE);
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(mockHistorydbIO.getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams))
                .thenReturn(nextPageInfo);

        final StatRecordPage result =
                liveStatsReader.getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(), Optional.of(paginationParams));
        verify(mockHistorydbIO).getNextPage(scope, TIMESTAMP, TIME_FRAME, paginationParams);
        // verify early return
        verify(statsQueryFactory, never()).createStatsQuery(nextPageInfo.getEntityOids(), TABLE, statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);

        assertThat(result.getNextCursor(), is(Optional.empty()));
        assertThat(result.getNextPageRecords(), is(Collections.emptyMap()));
    }

    @Test
    public void testGetStatPageWithPaginationParamPI() throws VmtDbException {
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();
        final long startAndEndDate = 123L;
        final CommodityRequest cpu = CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.CPU).build();
        final CommodityRequest pi = CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.PRICE_INDEX).build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startAndEndDate)
                .setEndDate(startAndEndDate)
                .addCommodityRequests(cpu)
                .addCommodityRequests(pi)
                .build();
        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);

        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange piTimeRange = mock(TimeRange.class);
        Timestamp piTimestamp = TIMESTAMP;
        when(piTimeRange.getMostRecentSnapshotTime()).thenReturn(piTimestamp);
        when(piTimeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                Optional.empty(), Optional.of(paginationParams))).thenReturn(Optional.of(piTimeRange));
        final TimeRange cpuTimeRange = mock(TimeRange.class);
        Timestamp cpuTimestamp = new Timestamp(1);
        when(cpuTimeRange.getMostRecentSnapshotTime()).thenReturn(cpuTimestamp);
        when(cpuTimeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                 Optional.empty(), Optional.empty())).thenReturn(Optional.of(cpuTimeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        long oid = 111l;
        Map<Long, List<Record>> recordsByEntityId = new HashMap<>();
        recordsByEntityId.put(oid, new ArrayList<>());
        List<String> entityOid = Arrays.asList(String.valueOf(oid));
        when(nextPageInfo.getEntityOids()).thenReturn(entityOid);
        when(nextPageInfo.getTable()).thenReturn(TABLE);
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(mockHistorydbIO.getNextPage(scope, piTimestamp, TIME_FRAME, paginationParams))
                .thenReturn(nextPageInfo);

        Select<?> query = mock(Select.class);
        when(statsQueryFactory.createStatsQuery(entityOid, TABLE,
                statsFilter.getCommodityRequestsList(), cpuTimeRange, AGGREGATE.NO_AGG))
                .thenReturn(Optional.of(query));
        final PmStatsLatestRecord pmRecord = new PmStatsLatestRecord();
        pmRecord.setUuid(String.valueOf(oid));
        final Result records = mock(Result.class);
        doAnswer((Answer<Void>) invocation -> {
            invocation.getArgumentAt(0, Consumer.class).accept(pmRecord);
            return null;
        }).when(records).forEach(any());
        when(mockHistorydbIO.execute(Style.FORCED, query)).thenReturn(records);

        final StatRecordPage result =
                liveStatsReader.getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(),
                Optional.empty(), Optional.of(paginationParams));
        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(),
                Optional.empty(), Optional.empty());
        verify(mockHistorydbIO).getNextPage(scope, piTimestamp, TIME_FRAME, paginationParams);
        // verify early return
        verify(statsQueryFactory).createStatsQuery(nextPageInfo.getEntityOids(),
                TABLE, statsFilter.getCommodityRequestsList(), cpuTimeRange, AGGREGATE.NO_AGG);
        assertThat(result.getNextCursor(), is(nextCursor));
        assertTrue(result.getNextPageRecords().get(oid).size() == 1);
        assertTrue(result.getNextPageRecords().get(oid).get(0).equals(pmRecord));

    }

    @Test
    public void testGetFullMarketStats() throws VmtDbException {
        // ARRANGE
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .setStartDate(123L)
            .build();

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(), Optional.empty()))
                .thenReturn(Optional.of(timeRange));

        final FullMarketRatioProcessor ratioProcessor = mock(FullMarketRatioProcessor.class);
        final StatsFilter statsFilterWithCounts = StatsFilter.newBuilder()
            .setStartDate(123L)
            // Something to distinguish it from the regular stats filter.
            .addCommodityRequests(
                CommodityRequest.newBuilder()
                    .setCommodityName("foo")
                    .setRelatedEntityType(StringConstants.VIRTUAL_MACHINE))
            .build();
        when(ratioProcessor.getFilterWithCounts()).thenReturn(statsFilterWithCounts);
        when(fullMarketRatioProcessor.newProcessor(statsFilter)).thenReturn(ratioProcessor);

        final Condition condition = mock(Condition.class);
        when(statsQueryFactory.createCommodityRequestsCond(any(), any()))
            .thenReturn(Optional.of(condition));
        when(statsQueryFactory.entityTypeCond(any(), any())).thenReturn(Optional.empty());
        when(statsQueryFactory.environmentTypeCond(any(), any())).thenReturn(Optional.empty());

        final Result result = mock(Result.class);
        when(mockHistorydbIO.execute(eq(Style.FORCED), isA(Query.class))).thenReturn(result);

        final Record processedRecord = mock(Record.class);
        when(ratioProcessor.processResults(result)).thenReturn(Collections.singletonList(processedRecord));

        final DSLContext jooqBuilderSpy = spy(DSL.using(SQLDialect.MARIADB));
        when(mockHistorydbIO.JooqBuilder()).thenReturn(jooqBuilderSpy);

        // ACT
        final List<Record> records = liveStatsReader.getFullMarketStatsRecords(statsFilter,
            GlobalFilter.newBuilder()
                .addRelatedEntityType(StringConstants.PHYSICAL_MACHINE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());

        // ASSERT
        // Verify the individual steps - this should help track down what failed if the final
        // assertion does not pass.
        verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(), Optional.empty(),
                Optional.empty());
        verify(fullMarketRatioProcessor).newProcessor(statsFilter);
        verify(ratioProcessor).getFilterWithCounts();
        verify(statsQueryFactory)
            .createCommodityRequestsCond(statsFilterWithCounts.getCommodityRequestsList(),
                Tables.MARKET_STATS_LATEST);
        verify(statsQueryFactory).environmentTypeCond(EnvironmentType.CLOUD, Tables.MARKET_STATS_LATEST);
        verify(statsQueryFactory).entityTypeCond(Sets.newHashSet(StringConstants.PHYSICAL_MACHINE), Tables.MARKET_STATS_LATEST);
        verify(mockHistorydbIO).execute(eq(Style.FORCED), isA(Query.class));
        verify(ratioProcessor).processResults(result);

        // The returned record should be the one coming out of the ratio processor.
        assertThat(records, contains(processedRecord));
    }
}
