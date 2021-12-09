package com.vmturbo.history.stats.live;

import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.schema.abstraction.tables.PmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.INonPaginatingStatsReader;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.AGGREGATE;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.readers.HistUtilizationReader;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.LiveStatsReader.StatRecordPage;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

public class LiveStatsReaderTest {

    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = EntityType.named(
            PHYSICAL_MACHINE).get();
    private final HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

    private final TimeRangeFactory timeRangeFactory = mock(TimeRangeFactory.class);

    private final StatsQueryFactory statsQueryFactory = mock(DefaultStatsQueryFactory.class);

    private final ComputedPropertiesProcessor computedPropertiesProcessor =
            mock(ComputedPropertiesProcessor.class);
    private final INonPaginatingStatsReader<HistUtilizationRecord> histUtilizationReader =
            mock(HistUtilizationReader.class);

    private final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory =
            mock(ComputedPropertiesProcessorFactory.class);

    private static final Timestamp TIMESTAMP = new Timestamp(0);

    private static final TimeFrame TIME_FRAME = TimeFrame.LATEST;

    private static final PmStatsLatest TABLE = PmStatsLatest.PM_STATS_LATEST;

    private DSLContext mkDsl(Record... records) {
        Result<Record> result = DSL.using(SQLDialect.MARIADB).newResult(
                records.length > 0 ? records[0].fields() : new Field<?>[0]);
        result.addAll(Arrays.asList(records));
        return DSL.using(new MockConnection(ctx ->
                new MockResult[]{new MockResult(records.length, result)}), SQLDialect.MARIADB);
    }

    private LiveStatsReader mkReader(DSLContext dsl) {
        return new LiveStatsReader(mockHistorydbIO, dsl, timeRangeFactory, statsQueryFactory,
                computedPropertiesProcessorFactory, histUtilizationReader, 5000);
    }

    @Test
    public void testGetStatPage() throws DataAccessException {
        final Set<String> entityIds = Collections.singleton("1");
        final EntityType entityType = PHYSICAL_MACHINE_ENTITY_TYPE;
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();

        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope))
                .thenReturn(entityType);

        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(123L)
                .build();
        final EntityStatsPaginationParams paginationParams = mock(
                EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                Optional.of(entityType), Optional.empty(), Optional.empty())).thenReturn(
                Optional.of(timeRange));
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                Optional.of(entityType), Optional.of(paginationParams),
                Optional.empty())).thenReturn(Optional.of(timeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        when(nextPageInfo.getEntityOids()).thenReturn(Lists.newArrayList(entityIds));
        doReturn(TABLE).when(nextPageInfo).getTable();
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(nextPageInfo.getTotalRecordCount()).thenReturn(Optional.of(100));
        when(mockHistorydbIO.getNextPage(scope, timeRange, paginationParams, entityType,
                statsFilter))
                .thenReturn(nextPageInfo);
        final PmStatsLatestRecord testRecord = new PmStatsLatestRecord()
                .with(PM_STATS_LATEST.UUID, "1");
        DSLContext dsl = mkDsl(testRecord);
        ResultQuery<?> query = dsl.selectFrom(PM_STATS_LATEST);
        when(statsQueryFactory.createStatsQuery(nextPageInfo.getEntityOids(), TABLE,
                statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG))
                .thenReturn(Optional.of(query));

        final StatRecordPage result =
                mkReader(dsl).getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        assertThat(result.getNextCursor(), is(nextCursor));
        assertThat(result.getNextPageRecords(),
                is(ImmutableMap.of(1L, Collections.singletonList(testRecord))));
        assertTrue(result.getTotalRecordCount().get().equals(100));
    }

    @Test
    public void testGetStatPageNextPageIsEmpty() throws DataAccessException {
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();
        final EntityType entityType = PHYSICAL_MACHINE_ENTITY_TYPE;
        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope))
                .thenReturn(entityType);

        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(123L)
                .build();
        final EntityStatsPaginationParams paginationParams = mock(
                EntityStatsPaginationParams.class);
        final Optional<String> nextCursor = Optional.of("ROSIETNROSIENTR");

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                Optional.of(entityType),
                Optional.of(paginationParams), Optional.empty())).thenReturn(
                Optional.of(timeRange));

        final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
        // entity OIDs are empty
        when(nextPageInfo.getEntityOids()).thenReturn(Collections.emptyList());
        doReturn(TABLE).when(nextPageInfo).getTable();
        when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
        when(mockHistorydbIO.getNextPage(scope, timeRange, paginationParams, entityType,
                statsFilter))
                .thenReturn(nextPageInfo);

        final StatRecordPage result = mkReader(mkDsl())
                .getPaginatedStatsRecords(scope, statsFilter, paginationParams);

        //            verify(timeRangeFactory).resolveTimeRange(statsFilter, Optional.empty(),
        //                    Optional.of(entityType), Optional.of(paginationParams), Optional.empty());
        //            verify(mockHistorydbIO).getNextPage(scope, timeRange, paginationParams, entityType,
        //                statsFilter);
        //            // verify early return
        //            verify(statsQueryFactory, never()).createStatsQuery(nextPageInfo.getEntityOids(), TABLE, statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);
        //
        assertThat(result.getNextCursor(), is(Optional.empty()));
        assertThat(result.getNextPageRecords(), is(Collections.emptyMap()));
    }

    @Test
    public void testGetStatPageWithPaginationParamPI() throws DataAccessException {
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1))
                .build();
        final EntityType entityType = PHYSICAL_MACHINE_ENTITY_TYPE;
        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope))
                .thenReturn(entityType);

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
            Timestamp cpuTimestamp = new Timestamp(1);
            Timestamp piTimestamp = TIMESTAMP;
            final TimeRange timeRange = mock(TimeRange.class);
            when(timeRange.getMostRecentSnapshotTime()).thenReturn(cpuTimestamp);
            when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
            when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.of(piTimestamp));

            when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                    Optional.of(entityType), Optional.of(paginationParams), Optional.empty()))
                    .thenReturn(Optional.of(timeRange));

            final NextPageInfo nextPageInfo = mock(NextPageInfo.class);
            long oid = 111L;
            Map<Long, List<Record>> recordsByEntityId = new HashMap<>();
            recordsByEntityId.put(oid, new ArrayList<>());
            List<String> entityOid = Arrays.asList(String.valueOf(oid));
            when(nextPageInfo.getEntityOids()).thenReturn(entityOid);
            doReturn(TABLE).when(nextPageInfo).getTable();
            when(nextPageInfo.getNextCursor()).thenReturn(nextCursor);
            when(mockHistorydbIO.getNextPage(scope, timeRange, paginationParams, entityType,
                statsFilter))
                    .thenReturn(nextPageInfo);
            final PmStatsLatestRecord pmRecord = new PmStatsLatestRecord()
                    .with(PM_STATS_LATEST.UUID, String.valueOf(oid));
            DSLContext dsl = mkDsl(pmRecord);
            ResultQuery<?> query = dsl.selectFrom(PM_STATS_LATEST);
            when(statsQueryFactory.createStatsQuery(entityOid, TABLE,
                    statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG))
                    .thenReturn(Optional.of(query));
            final StatRecordPage result =
                    mkReader(dsl).getPaginatedStatsRecords(scope, statsFilter, paginationParams);

            assertThat(result.getNextCursor(), is(nextCursor));
            assertEquals(1, result.getNextPageRecords().get(oid).size());
            assertEquals(result.getNextPageRecords().get(oid).get(0), pmRecord);

        }

    @Test
    public void testGetFullMarketStats() throws DataAccessException {
        // ARRANGE
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(123L)
                .build();

        final TimeRange timeRange = mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
        when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
        when(timeRangeFactory.resolveTimeRange(statsFilter, Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty()))
                    .thenReturn(Optional.of(timeRange));


            final StatsFilter statsFilterWithCounts = StatsFilter.newBuilder()
                .setStartDate(123L)
                // Something to distinguish it from the regular stats filter.
                .addCommodityRequests(
                    CommodityRequest.newBuilder()
                            .setCommodityName("foo")
                            .setRelatedEntityType(StringConstants.VIRTUAL_MACHINE))
                    .build();
            when(computedPropertiesProcessorFactory.getProcessor(any(), any()))
                    .thenReturn(computedPropertiesProcessor);
            when(computedPropertiesProcessor.getAugmentedFilter()).thenReturn(statsFilterWithCounts);

//            final Condition condition = mock(Condition.class);
            when(statsQueryFactory.createCommodityRequestsCond(any(), any()))
                    .thenCallRealMethod();
            when(statsQueryFactory.createExcludeZeroCountRecordsCond(any(), any()))
                    .thenCallRealMethod();
            when(statsQueryFactory.entityTypeCond(any(), any())).thenCallRealMethod();
            when(statsQueryFactory.environmentTypeCond(any(), any())).thenCallRealMethod();

            final DSLContext dsl = mkDsl();
            final LiveStatsReader liveStatsReader = mkReader(dsl);

            final Record processedRecord = mock(Record.class);
            when(computedPropertiesProcessor.processResults(any(Result.class), any(Timestamp.class)))
                    .thenReturn(Collections.singletonList(processedRecord));

            final List<Record> records = liveStatsReader.getFullMarketStatsRecords(statsFilter,
                GlobalFilter.newBuilder()
                    .addRelatedEntityType(PHYSICAL_MACHINE)
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .build());

            // The returned record should be the one coming out of the ratio processor.
            assertThat(records, contains(processedRecord));
        }

        /**
         * Test getEntityDisplayNameForId.
         *
         */
        @Test
        public void testGetEntityDisplayNameForId() {
            final Long entityOid = 1L;
            final String entityDisplayName = "Entity_1";
            final LiveStatsReader liveStatsReader = mkReader(mkDsl());
            when(liveStatsReader.getEntityDisplayNameForId(entityOid)).thenReturn(entityDisplayName);
            assertNull(liveStatsReader.getEntityDisplayNameForId(null));
            assertEquals(entityDisplayName, liveStatsReader.getEntityDisplayNameForId(entityOid));
        }

    /**
     * Test getting the counted stats, like 'numVMs'.
     */
    @Test
    public void testGetCountStats() throws DataAccessException {
        // arrange
        final Set<String> entityIds = ImmutableSet.of("1", "2", "3");
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .addCommodityRequests(
                        CommodityRequest.newBuilder()
                                .setCommodityName("numVMs"))
                .build();
        // Simulates expansion of scope from Host -> VMs
        final List<Integer> derivedEntityTypes =
                Collections.singletonList(EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber());
            // The original scope was a single host; scope expansion added two VMs to the scope
            final Map<String, String> entityIdToTypeMap = new HashMap<>();
            entityIdToTypeMap.put("1", ApiEntityType.PHYSICAL_MACHINE.apiStr());
            entityIdToTypeMap.put("2", ApiEntityType.VIRTUAL_MACHINE.apiStr());
            entityIdToTypeMap.put("3", ApiEntityType.VIRTUAL_MACHINE.apiStr());
            when(mockHistorydbIO.getTypesForEntities(entityIds)).thenReturn(entityIdToTypeMap);
            final TimeRange timeRange = mock(TimeRange.class);
            when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
            when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
            when(timeRange.getStartTime()).thenReturn(0L);
            when(timeRange.getEndTime()).thenReturn(0L);
            when(timeRangeFactory.resolveTimeRange(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.of(timeRange));
            when(statsQueryFactory.createStatsQuery(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.empty());

            final LiveStatsReader liveStatsReader = mkReader(mkDsl());
            List<Record> records = liveStatsReader.getRecords(entityIds, statsFilter, derivedEntityTypes);

            // verify
            assertEquals(1, records.size());
            Optional<Record> numVMsRecord = records.stream()
                    .filter(record -> record.valuesRow().indexOf("numVMs") >= 0)
                    .findAny();
            assertTrue(numVMsRecord.isPresent());
            Record record = numVMsRecord.get();
            assertEquals(2.0, (Double)record.getValue("avg_value"), 0.0);
        }

        /**
         * Test getting the counted stats, like 'numVMs'.
         */
        @Test
        public void testGetCountStatsZeroResult() throws DataAccessException {
            // arrange
            final Set<String> entityIds = Stream.of("1")
                    .collect(Collectors.toSet());
            final StatsFilter statsFilter = StatsFilter.newBuilder()
                    .addCommodityRequests(
                            CommodityRequest.newBuilder()
                                    .setCommodityName("numVMs"))
                    .build();
            // Simulates expansion of scope from Host -> VMs
            final List<Integer> derivedEntityTypes = Stream.of(
                            EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber())
                    .collect(Collectors.toList());
            // The original scope was a single host; scope expansion added zero VMs to the scope
            final Map<String, String> entityIdToTypeMap = new HashMap<>();
            entityIdToTypeMap.put("1", ApiEntityType.PHYSICAL_MACHINE.apiStr());
            when(mockHistorydbIO.getTypesForEntities(entityIds)).thenReturn(entityIdToTypeMap);
            final TimeRange timeRange = mock(TimeRange.class);
            when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
            when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
            when(timeRange.getStartTime()).thenReturn(0L);
            when(timeRange.getEndTime()).thenReturn(0L);
            when(timeRangeFactory.resolveTimeRange(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.of(timeRange));
            when(statsQueryFactory.createStatsQuery(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.empty());

            List<Record> records = mkReader(mkDsl())
                    .getRecords(entityIds, statsFilter, derivedEntityTypes);

            assertEquals(1, records.size());
            Optional<Record> numVMsRecord = records.stream()
                    .filter(record -> record.valuesRow().indexOf("numVMs") >= 0)
                    .findAny();
            assertTrue(numVMsRecord.isPresent());
            Record record = numVMsRecord.get();
            assertEquals(0.0, (Double)record.getValue("avg_value"), 0.0);
        }

        /**
         * Test getting the counted stats, like 'numVMs'.
         */
        @Test
        public void testGetCountStatsNoResult() throws DataAccessException {
            // arrange
            final Set<String> entityIds = Stream.of("1")
                    .collect(Collectors.toSet());
            final StatsFilter statsFilter = StatsFilter.newBuilder()
                    .addCommodityRequests(
                            CommodityRequest.newBuilder()
                                    .setCommodityName("numVMs"))
                    .build();
            // Simulates no scope expansion being performed
            final List<Integer> derivedEntityTypes = Collections.emptyList();
            // The original scope was a single host; scope expansion was not performed
            final Map<String, String> entityIdToTypeMap = new HashMap<>();
            entityIdToTypeMap.put("1", ApiEntityType.PHYSICAL_MACHINE.apiStr());
            when(mockHistorydbIO.getTypesForEntities(entityIds)).thenReturn(entityIdToTypeMap);
            final TimeRange timeRange = mock(TimeRange.class);
            when(timeRange.getMostRecentSnapshotTime()).thenReturn(TIMESTAMP);
            when(timeRange.getTimeFrame()).thenReturn(TIME_FRAME);
            when(timeRange.getStartTime()).thenReturn(0L);
            when(timeRange.getEndTime()).thenReturn(0L);
            when(timeRangeFactory.resolveTimeRange(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.of(timeRange));
            when(statsQueryFactory.createStatsQuery(any(), any(), any(), any(), any()))
                    .thenReturn(Optional.empty());

            List<Record> records = mkReader(mkDsl())
                    .getRecords(entityIds, statsFilter, derivedEntityTypes);

            assertEquals(0, records.size());
        }
}
