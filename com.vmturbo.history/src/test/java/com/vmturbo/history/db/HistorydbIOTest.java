package com.vmturbo.history.db;

import static com.vmturbo.common.protobuf.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRICE_INDEX;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.common.protobuf.utils.StringConstants.VCPU;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.assertj.core.util.Sets;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DaemonSetInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.HistorydbIO.SeekPaginationCursor;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.AvailableTimestamps;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.stats.live.TimeRange;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests for HistorydbIO.
 */
public class HistorydbIOTest {

    private static final Logger logger = LogManager.getLogger();
    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = EntityType.named(
            PHYSICAL_MACHINE).get();
    private static final EntityType VIRTUAL_MACHINE_ENTITY_TYPE = EntityType.named(VIRTUAL_MACHINE)
            .get();
    private static final String HISTORY_VARIETY = "history_variety";
    private static final String TIMESTAMP = "time_stamp";
    private static final String TIME_FRAME = "time_frame";
    private static final String LATEST = "LATEST";

    /**
     * Provision and provide access to a test database.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Vmtdb.VMTDB);

    /**
     * Clean up tables in the test database before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final DSLContext dsl = dbConfig.getDslContext();
    private final HistorydbIO historydbIO = new HistorydbIO(dsl, dsl);

    final Field<BigDecimal> avgValue =
            avg(JooqUtils.getDoubleField(VmStatsLatest.VM_STATS_LATEST,
                    StringConstants.AVG_VALUE)).as(StringConstants.AVG_VALUE);
    final Field<BigDecimal> avgCapacity =
            avg(JooqUtils.getDoubleField(VmStatsLatest.VM_STATS_LATEST,
                    StringConstants.CAPACITY)).as(StringConstants.CAPACITY);
    final Field<String> uuid =
            getStringField(VmStatsLatest.VM_STATS_LATEST, UUID);
    Table<Record3<String, BigDecimal, BigDecimal>> aggregatedStats =
            select(uuid, avgValue, avgCapacity)
                    .from(VmStatsLatest.VM_STATS_LATEST).asTable("aggregatedStats");

    @Test
    public void testGetValueFieldSortByPriceIndex() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        final SeekPaginationCursor seekPaginationCursor = new SeekPaginationCursor(Optional.of("1"),
                Optional.of(new BigDecimal(1)));
        Field<BigDecimal> field =
                SeekPaginationCursor.getValueField(paginationParam, aggregatedStats);
        assertEquals("avg_value", field.getName());
    }

    @Test
    public void testGetValueFieldSortByCUP() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.CPU);
        final SeekPaginationCursor seekPaginationCursor = new SeekPaginationCursor(Optional.of("1"),
                Optional.of(new BigDecimal(1)));
        Field<BigDecimal> field =
                SeekPaginationCursor.getValueField(paginationParam, aggregatedStats);
        // the filed type is org.jooq.impl.Expression which is not exposed. There seems no other way to validate
        // the output other than filed.toString.
        assertEquals("(\"aggregatedStats\".\"avg_value\" / \"aggregatedStats\".\"capacity\")",
                field.toString());
    }

    /**
     * Tests getting entityType from {@link Stats.EntityStatsScope} when set to entityType.
     *
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityType()
            throws IllegalArgumentException {
        //GIVEN
        CommonDTO.EntityDTO.EntityType type = CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;

        final Stats.EntityStatsScope scope = Stats.EntityStatsScope.newBuilder()
                .setEntityType(type.getNumber())
                .build();

        //WHEN
        EntityType responseType = historydbIO.getEntityTypeFromEntityStatsScope(scope);

        //THEN
        assertEquals(responseType, PHYSICAL_MACHINE_ENTITY_TYPE);

    }

    /**
     * Tests getting entityType from {@link Stats.EntityStatsScope} when entityList is set.
     *
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityList() throws IllegalArgumentException {
        //GIVEN
        HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

        final Stats.EntityStatsScope scope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1))
                .build();

        final HashMap<String, String> entityTypesMap = new HashMap<>();
        entityTypesMap.put("foo", PHYSICAL_MACHINE_ENTITY_TYPE.getName());
        when(mockHistorydbIO.getTypesForEntities(Mockito.anySet())).thenReturn(entityTypesMap);
        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(any())).thenCallRealMethod();

        //THEN
        assertEquals(mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope), PHYSICAL_MACHINE_ENTITY_TYPE);
    }

    /**
     * Tests getting entityType from {@link Stats.EntityStatsScope} when entityList is set.
     *
     * <p>Expect null when no entityTypes from entityList can be determined</p>
     *
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityListMapsToNoEntityType() throws
            IllegalArgumentException {
        //GIVEN
        HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

        final Stats.EntityStatsScope scope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1))
                .build();

        final Map<String, String> entityTypes = new HashMap<>();
        when(mockHistorydbIO.getTypesForEntities(Mockito.anySet())).thenReturn(entityTypes);
        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(any())).thenCallRealMethod();

        //THEN
        assertNull(mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope));
    }

    /**
     * Tests getEntityTypeFromEntityStatsScope when invalid entityType is found.
     *
     * <p>Expect {@link IllegalArgumentException} to be thrown</p>
     *
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEntityTypeFromEntityStatsScopeEntityListMapsToMultipleEntityType() throws
            IllegalArgumentException {
        //GIVEN
        HistorydbIO mockHistorydbIO = mock(HistorydbIO.class);

        final Stats.EntityStatsScope scope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1))
                .build();

        final HashMap<String, String> entityTypesMap = new HashMap<>();
        entityTypesMap.put("foo1", "bar");
        when(mockHistorydbIO.getTypesForEntities(Mockito.anySet())).thenReturn(entityTypesMap);
        when(mockHistorydbIO.getEntityTypeFromEntityStatsScope(any())).thenCallRealMethod();

        //When
        mockHistorydbIO.getEntityTypeFromEntityStatsScope(scope);
    }

    /**
     * Tests totalRecordCount from query is being returned in nextPageInfo.
     *
     * @throws DaemonSetInfo if there's an error querying DB for types of entities
     */
    @Test
    public void testGetNextPageGettingTotalRecordCount() throws DataAccessException {
        //GIVEN
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1))
                .build();

        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                20,
                100,
                "sortBy",
                PaginationParameters.newBuilder().setCursor("sdf:2134").build());

        HistorydbIO historydbIOSpy = spy(historydbIO);
        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(new Timestamp(1L));
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        //WHEN
        NextPageInfo nextPageInfo = historydbIOSpy.getNextPage(entityStatsScope,
                timeRange, paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());

        //THEN
        assertTrue(nextPageInfo.getTotalRecordCount().get().equals(1));
    }

    /**
     * Tests that getTotalRecordsCount returns the correct number of entities.
     */
    @Test
    public void testGetTotalRecordCount() {
        // check the case of empty scope list
        //GIVEN
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();
        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, "1", VCPU, USED, 1.0, 1.0, RelationType.COMMODITIES)
                .values(timestamp, "2", VCPU, USED, 2.0, 1.0, RelationType.COMMODITIES)
                .execute();
        Set<String> requestedIds = Sets.newHashSet();

        //WHEN
        int totalRecordCount = historydbIO.getTotalRecordsCount(dsl, table, requestedIds);

        //THEN
        assertEquals(2, totalRecordCount);


        // check the case of specific scope list
        //GIVEN
        requestedIds.add("5");

        //WHEN
        totalRecordCount = historydbIO.getTotalRecordsCount(dsl, table, requestedIds);

        //THEN
        assertEquals(1, totalRecordCount);
    }

    /**
     * Tests that in a table that has only entities with the orderBy commodity,
     * the cursors returned by subsequent calls to getNextPage are correct.
     *
     * @throws DataAccessException on db error
     */
    @Test
    public void testGetNextPageWithOnlyEntitiesThatHaveTheOrderByCommodity()
            throws DataAccessException {
        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final double entityValue1 = 16.0;
        final double entityValue2 = 5.0;
        final double capacity = 1.0;
        final int paginationLimit = 1;
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(Long.parseLong(entityUuid1))
                        .addEntities(Long.parseLong(entityUuid2)))
                .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder().setAscending(true).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();

        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, entityUuid1, VCPU, USED, entityValue1, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid2, VCPU, USED, entityValue2, capacity,
                        RelationType.COMMODITIES)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        // initial call: no cursor provided, cursor to next records returned
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope,
            timeRange, paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        boolean cursorShouldHaveValue = true;
        // ascending order, 2 is expected to be returned first
        validateNextPageValues(nextPageInfo, entityUuid2, cursorShouldHaveValue, entityValue2, 2);
        SeekPaginationCursor cursor = SeekPaginationCursor.parseCursor(
                nextPageInfo.getNextCursor().get());
        // For the next call, use the cursor returned from the initial call


        // second call: cursor provided, empty cursor returned
        paginationParams = new EntityStatsPaginationParams(paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());
        timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid1, nextPageInfo.getEntityOids().get(0));
        // we don't have any more results, so the next cursor should now be empty
        assertFalse(nextPageInfo.getNextCursor().isPresent());
        assertEquals(2, nextPageInfo.getTotalRecordCount().get().longValue());
    }

    /**
     * Tests that in a table that has only entities without the orderBy commodity,
     * the cursors returned by subsequent calls to getNextPage are correct.
     *
     * @throws DataAccessException on db error
     */
    @Test
    public void testGetNextPageWithOnlyEntitiesThatDontHaveTheOrderByCommodity()
            throws DataAccessException {
        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final double entityValue1 = 1.01;
        final double entityValue2 = 1.04;
        final double capacity = 1.0;
        final int paginationLimit = 1;
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(Long.parseLong(entityUuid1))
                        .addEntities(Long.parseLong(entityUuid2)))
                .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder().setAscending(false).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();

        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, "1", PRICE_INDEX, PRICE_INDEX, entityValue1, capacity,
                        RelationType.METRICS)
                .values(timestamp, "2", PRICE_INDEX, PRICE_INDEX, entityValue2, capacity,
                        RelationType.METRICS)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        // initial call: no cursor provided, cursor to next records returned
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
            paginationParams,
            VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        boolean cursorShouldHaveValue = false;
        validateNextPageValues(nextPageInfo, entityUuid1, cursorShouldHaveValue, 0.0, 2);
        SeekPaginationCursor cursor =  SeekPaginationCursor.parseCursor(
                nextPageInfo.getNextCursor().get());
        // For the next call, use the cursor returned from the initial call


        // second call: cursor provided, empty cursor returned
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(false)
                        .build());
        timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid2, nextPageInfo.getEntityOids().get(0));
        // we don't have any more results, so the next cursor should now be empty
        assertFalse(nextPageInfo.getNextCursor().isPresent());
        assertEquals(2, nextPageInfo.getTotalRecordCount().get().longValue());
    }

    /**
     * Tests that in a table that has entities with orderBy commodity,
     * and entities without orderBy commodity and no requested set of
     * ids is given and there is a requested timeframe.
     * The cursors and entities returned by subsequent calls to getNextPage are correct.
     * Expected return order:
     *                             **************************
     *        Entities that        *  uuid: 2, VCPU: 10     *
     *        have the orderBy     *  uuid: 6, VCPU: 16     *
     *        commodity            *                        *
     *      ---------------------- **************************
     *        Entities that        *  uuid: 1               *
     *        don't have the       *  uuid: 15              *
     *        orderBy commodity    **************************
     *
     *
     *
     *      @throws DataAccessException on db error
     */

    @Test
    public void testGetNextPageWithoutRequestedIdSet() throws DataAccessException {
        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final String entityUuid6 = "6";

        final String entityUuid15 = "15";
        final double entityValue1 = 1.01;
        final double entityValue2 = 10.0;
        final double entityValue6 = 16.0;
        final double entityValue15 = 1.03;
        final double capacity = 1.0;
        final int paginationLimit = 1;
        CommonDTO.EntityDTO.EntityType type = EntityDTO.EntityType.VIRTUAL_MACHINE;
        final Stats.EntityStatsScope entityStatsScope =
                Stats.EntityStatsScope.newBuilder().setEntityType(type.getNumber())
                        .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder().setAscending(true).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();
        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, entityUuid1, PRICE_INDEX, PRICE_INDEX, entityValue1, capacity,
                        RelationType.METRICS)
                .values(timestamp, entityUuid2, VCPU, USED, entityValue2, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid6, VCPU, USED, entityValue6, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid15, PRICE_INDEX, PRICE_INDEX, entityValue15, capacity,
                        RelationType.METRICS)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());

        // 1st call
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams,
                VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        boolean cursorShouldHaveValue = true;
        validateNextPageValues(nextPageInfo, entityUuid2, cursorShouldHaveValue, entityValue2, 4);
        SeekPaginationCursor cursor =
                SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        // For each subsequent call, use the cursor returned from the previous call

        // 2nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid6, cursorShouldHaveValue, entityValue6, 4);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        cursorShouldHaveValue = false;
        // 3nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid1, cursorShouldHaveValue, 0.0, 4);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());

        // 4nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid15, nextPageInfo.getEntityOids().get(0));
        // we don't have any more results, so the next cursor should now be empty
        assertFalse(nextPageInfo.getNextCursor().isPresent());
        assertEquals(4, nextPageInfo.getTotalRecordCount().get().longValue());

    }



    /**
     * Tests that in a table that has only entities without the orderBy commodity,
     * but in addition, stats are requested for entities that dont have stats in table
     * for requested time frame.
     * The cursors returned by subsequent calls to getNextPage are correct.
     * Expected return order:
     *                        **************************
     *   Entities that        *  uuid: 2, VCPU: 10     *
     *   have the orderBy     *  uuid: 6, VCPU: 16     *
     *   commodity            *                        *
     * ---------------------- **************************
     *   Entities that        *  uuid: 1               *
     *   don't have the       *  uuid: 11 non existent *
     *   orderBy commodity    *  uuid: 12 non existent *
     *   OR dont have stats   *  uuid: 15              *
     *                        **************************
     * @throws DataAccessException on db error
     */
    @Test
    public void testGetNextPageWithEntitiesThatDontHaveTheOrderByCommodityAndDontExistInStatistics() throws DataAccessException {

        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final String entityUuid6 = "6";

        final String entityUuid11 = "11";
        final String entityUuid12 = "12";
        final String entityUuid15 = "15";
        final double entityValue1 = 1.01;
        final double entityValue2 = 10.0;
        final double entityValue6 = 16.0;
        final double entityValue15 = 1.03;
        final double capacity = 1.0;
        final int paginationLimit = 1;
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1)
                        .addEntities(2)
                        .addEntities(6)
                        .addEntities(11)
                        .addEntities(12)
                        .addEntities(15))
                .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder().setAscending(true).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();
        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, entityUuid1, PRICE_INDEX, PRICE_INDEX, entityValue1, capacity,
                        RelationType.METRICS)
                .values(timestamp, entityUuid2, VCPU, USED, entityValue2, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid6, VCPU, USED, entityValue6, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid15, PRICE_INDEX, PRICE_INDEX, entityValue15, capacity,
                        RelationType.METRICS)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        // 1st call
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams,
                VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        boolean cursorShouldHaveValue = true;
        validateNextPageValues(nextPageInfo, entityUuid2, cursorShouldHaveValue, entityValue2,  6);
        SeekPaginationCursor cursor =
                SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        // For each subsequent call, use the cursor returned from the previous call

        // 2nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid6, cursorShouldHaveValue, entityValue6, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        cursorShouldHaveValue = false;
        // 3nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo,  entityUuid1,  cursorShouldHaveValue, 0.0, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());

        //4nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo,  entityUuid11,  cursorShouldHaveValue,  0.0,  6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());

        // 5th call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo,  entityUuid12,  cursorShouldHaveValue,  0.0,  6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());

        // 6th call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());

        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid15, nextPageInfo.getEntityOids().get(0));
        // we don't have any more results, so the next cursor should now be empty
        assertFalse(nextPageInfo.getNextCursor().isPresent());
        assertEquals(6, nextPageInfo.getTotalRecordCount().get().longValue());

    }

    /**
     * Tests that in a table that has both entities with and without the orderBy commodity,
     * the cursors returned by subsequent calls to getNextPage are correct.
     * Expected return order:
     *                      **********************
     *   Entities that      *  uuid: 5, VCPU: 5  *
     *   have the orderBy   *  uuid: 2, VCPU: 10 *
     *   commodity          *  uuid: 6, VCPU: 16 *
     * -------------------- **********************
     *   Entities that      *  uuid: 1           *
     *   don't have the     *  uuid: 4           *
     *   orderBy commodity  *  uuid: 9           *
     *                      **********************
     *
     * @throws DataAccessException on db error
     */
    @Test
    public void testGetNextPageWithBothEntities() throws DataAccessException {
        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final String entityUuid4 = "4";
        final String entityUuid5 = "5";
        final String entityUuid6 = "6";
        final String entityUuid9 = "9";
        final double entityValue1 = 1.01;
        final double entityValue2 = 10.0;
        final double entityValue4 = 1.04;
        final double entityValue5 = 5.0;
        final double entityValue6 = 16.0;
        final double entityValue9 = 1.09;
        final double capacity = 1.0;
        final int paginationLimit = 1;
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(1)
                        .addEntities(2)
                        .addEntities(4)
                        .addEntities(5)
                        .addEntities(6)
                        .addEntities(9))
                .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                VCPU,
                PaginationParameters.newBuilder().setAscending(true).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();

        Timestamp timestamp = new Timestamp(10000L);
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(timestamp, entityUuid1, PRICE_INDEX, PRICE_INDEX, entityValue1, capacity,
                        RelationType.METRICS)
                .values(timestamp, entityUuid2, VCPU, USED, entityValue2, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid4, PRICE_INDEX, PRICE_INDEX, entityValue4, capacity,
                        RelationType.METRICS)
                .values(timestamp, entityUuid5, VCPU, USED, entityValue5, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid6, VCPU, USED, entityValue6, capacity,
                        RelationType.COMMODITIES)
                .values(timestamp, entityUuid9, PRICE_INDEX, PRICE_INDEX, entityValue9, capacity,
                        RelationType.METRICS)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(timestamp);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.empty());
        // 1st call
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
            paginationParams,
            VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        boolean cursorShouldHaveValue = true;
        validateNextPageValues(nextPageInfo, entityUuid5, cursorShouldHaveValue, entityValue5, 6);
        SeekPaginationCursor cursor =
                SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        // For each subsequent call, use the cursor returned from the previous call


        // 2nd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid2, cursorShouldHaveValue, entityValue2, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        final SeekPaginationCursor cursorForExtraCase =
                SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        // 3rd call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid6, cursorShouldHaveValue, entityValue6, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        // No more entities with orderBy commodity, so the next cursors are expected to not have
        // a value
        cursorShouldHaveValue = false;

        // 4th call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid1, cursorShouldHaveValue, 0.0, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        // 5th call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .build());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        validateNextPageValues(nextPageInfo, entityUuid4, cursorShouldHaveValue, 0.0, 6);
        cursor =  SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());


        // 6th call
        paginationParams = new EntityStatsPaginationParams(paginationLimit, paginationLimit, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursor.toCursorString().get())
                        .setAscending(true)
                        .build());
        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid9, nextPageInfo.getEntityOids().get(0));
        // we don't have any more results, so the next cursor should now be empty
        assertFalse(nextPageInfo.getNextCursor().isPresent());
        assertEquals(6, nextPageInfo.getTotalRecordCount().get().longValue());


        // Extra case: include entities that both have and don't have the orderBy commodity.
        // Use the cursor after the 2nd call and 2 as the limit, so we expect to get 6 & 1 oids back
        // and 1:0 as the cursor.
        paginationParams = new EntityStatsPaginationParams(2, 2, VCPU,
                PaginationParameters.newBuilder()
                        .setCursor(cursorForExtraCase.toCursorString().get())
                        .build());

        nextPageInfo = historydbIO.getNextPage(entityStatsScope, timeRange,
                paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE, StatsFilter.newBuilder().build());
        assertEquals(2, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid6, nextPageInfo.getEntityOids().get(0));
        assertEquals(entityUuid1, nextPageInfo.getEntityOids().get(1));
        assertTrue(nextPageInfo.getNextCursor().isPresent());
        cursor = SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        assertTrue(cursor.getLastId().isPresent());
        assertEquals(entityUuid1, cursor.getLastId().get());
        assertFalse(cursor.getLastValue().isPresent());
        assertTrue(nextPageInfo.getTotalRecordCount().isPresent());
        assertEquals(6, nextPageInfo.getTotalRecordCount().get().longValue());
    }

    /**
     * Tests getting the most recent timestamps from the available_timestamps table. If the
     * request is sorting the commodities by price_index, we get the most recent time stamp among
     * the available price index data, if the table doesn't have entries with price data variety,
     * we get the latest value with the entity_stats variety.
     */
    @Test
    public void testAvailableTimeStamps() {
        final Table<?> table =
                AvailableTimestamps.AVAILABLE_TIMESTAMPS;
        StatsFilter statsFilter =
                StatsFilter.newBuilder().addAllCommodityRequests(Collections.singletonList(
                        CommodityRequest.newBuilder().setCommodityName("cpu").build())).build();
        Date date = new Date(100, 1, 1);
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);

        dsl.insertInto(table,
                        getField(table, HISTORY_VARIETY, String.class),
                        getTimestampField(table, TIMESTAMP),
                        getStringField(table, TIME_FRAME))
                .values(HistoryVariety.ENTITY_STATS.toString(), ts, LATEST)
                .execute();

        Optional<Timestamp> timeStamp = historydbIO.getClosestTimestampBefore(
                Optional.empty(),
                Optional.empty(), HistoryVariety.ENTITY_STATS);
        Assert.assertTrue(timeStamp.isPresent());
        Assert.assertEquals(ts, timeStamp.get());

        Date date2 = new Date(100, 1, 2);
        long time2 = date2.getTime();
        Timestamp ts2 = new Timestamp(time2);

        dsl.insertInto(table,
                        getField(table, HISTORY_VARIETY, String.class),
                        getTimestampField(table, TIMESTAMP),
                        getStringField(table, TIME_FRAME))
                .values(HistoryVariety.ENTITY_STATS.toString(), ts2, LATEST)
                .execute();

        Optional<Timestamp> timeStamp2 = historydbIO.getClosestTimestampBefore(Optional.empty(),
                Optional.empty(),
                HistoryVariety.ENTITY_STATS);
        Assert.assertTrue(timeStamp2.isPresent());
        Assert.assertEquals(ts2, timeStamp2.get());
    }

    /**
     * Utility Function used by testGetNextPageWithBothEntities(),
     * testGetNextPageWithOnlyEntitiesThatDontHaveTheOrderByCommodity(), and
     * testGetNextPageWithOnlyEntitiesThatHaveTheOrderByCommodity.
     * Asserts that the next page contains only the desired entity, and validates the cursor.
     *
     * @param nextPageInfo the next page info object returned by getNextPage()
     * @param expectedId the id of the entity expected to be returned
     * @param cursorShouldHaveValue flag that indicates whether the cursor is expected to have the
     *                              value populated or not
     * @param expectedCursorValue the value of the entity in the cursor
     * @param expectedTotalRecordCount the expected number of total entities that can be returned
     */
    private void validateNextPageValues(NextPageInfo nextPageInfo,
            String expectedId,
            boolean cursorShouldHaveValue,
            double expectedCursorValue,
            int expectedTotalRecordCount) {
        assertEquals(1, nextPageInfo.getEntityOids().size());
        assertEquals(expectedId, nextPageInfo.getEntityOids().get(0));
        assertTrue(nextPageInfo.getNextCursor().isPresent());
        SeekPaginationCursor cursor =
                SeekPaginationCursor.parseCursor(nextPageInfo.getNextCursor().get());
        assertTrue(cursor.getLastId().isPresent());
        assertEquals(expectedId, cursor.getLastId().get());
        if (cursorShouldHaveValue) {
            assertTrue(cursor.getLastValue().isPresent());
            assertEquals(expectedCursorValue,
                    Double.parseDouble(cursor.getLastValue().get().toString()), 0.00001);
        } else {
            assertFalse(cursor.getLastValue().isPresent());
        }
        assertTrue(nextPageInfo.getTotalRecordCount().isPresent());
        assertEquals(expectedTotalRecordCount,
                nextPageInfo.getTotalRecordCount().get().longValue());
    }

    /**
     * Tests that in a table that has only entities with the orderBy commodity,
     * the cursors returned by subsequent calls to getNextPage are correct.
     *
     * @throws DataAccessException on db error
     */
    @Test
    public void testSortByOlderPriceIndexValues()
        throws DataAccessException {
        // setup
        final String entityUuid1 = "1";
        final String entityUuid2 = "2";
        final double entityValue1 = 16.0;
        final double entityValue2 = 5.0;
        final int paginationLimit = 10;
        final Stats.EntityStatsScope entityStatsScope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder()
                        .addEntities(Long.parseLong(entityUuid1))
                        .addEntities(Long.parseLong(entityUuid2)))
                .build();
        EntityStatsPaginationParams paginationParams = new EntityStatsPaginationParams(
                paginationLimit,
                paginationLimit,
                PRICE_INDEX,
                PaginationParameters.newBuilder().setAscending(true).build());
        final Table<?> table =
                VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get();

        Timestamp t1 = new Timestamp(10000L);
        Timestamp t0 = new Timestamp(1000L);

        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(t1, entityUuid1, VCPU, USED, entityValue1, null,
                        RelationType.COMMODITIES)
                .execute();
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(t0, entityUuid1, PRICE_INDEX, USED, entityValue1, null,
                        RelationType.COMMODITIES)
                .execute();
        dsl.insertInto(table,
                        getTimestampField(table, SNAPSHOT_TIME),
                        getStringField(table, UUID),
                        getStringField(table, PROPERTY_TYPE),
                        getStringField(table, PROPERTY_SUBTYPE),
                        getDoubleField(table, AVG_VALUE),
                        getDoubleField(table, CAPACITY),
                        getRelationTypeField(table, RELATION))
                .values(t0, entityUuid2, PRICE_INDEX, USED, entityValue2, null,
                        RelationType.COMMODITIES)
                .execute();

        TimeRange timeRange = Mockito.mock(TimeRange.class);
        when(timeRange.getMostRecentSnapshotTime()).thenReturn(t1);
        when(timeRange.getTimeFrame()).thenReturn(TimeFrame.LATEST);
        when(timeRange.getLatestPriceIndexTimeStamp()).thenReturn(Optional.of(t0));
        // initial call: no cursor provided, cursor to next records returned
        NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope,
                timeRange, paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE,
                StatsFilter.newBuilder().build());
        assertEquals(2, nextPageInfo.getEntityOids().size());
        assertEquals(entityUuid2, nextPageInfo.getEntityOids().get(0));
    }

    /**
     * Test createTemporaryTableFromUuids with uuids lists of various lengths.
     *
     */
    @Test
    public void testCreateTemporaryTableFromUuids() {
        // Test boundary conditions, with lists of uuids of the following lengths
        final List<Integer> countsToTest = Arrays.asList(0, 1, HistorydbIO.DUMMY_BATCH_SIZE - 1,
                HistorydbIO.DUMMY_BATCH_SIZE, HistorydbIO.DUMMY_BATCH_SIZE + 1);

        for (int count : countsToTest) {
            List<Long> uuids = LongStream.range(1, count + 1).boxed().collect(Collectors.toList());
            assertEquals(count, uuids.size());

            dsl.connection(conn -> {
                Optional<String> tempTableName = historydbIO.createTemporaryTableFromUuids(uuids,
                        conn);

                assertEquals(count != 0, tempTableName.isPresent());
            });
        }
    }
}
