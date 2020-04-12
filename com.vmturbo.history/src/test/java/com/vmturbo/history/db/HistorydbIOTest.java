package com.vmturbo.history.db;

import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.HistorydbIO.SeekPaginationCursor;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.stats.DbTestConfig;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.sql.utils.DbCleanupRule;

/**
 * Tests for HistorydbIO
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
public class HistorydbIOTest {

    private static final Logger logger = LogManager.getLogger();
    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = EntityType.named(PHYSICAL_MACHINE).get();
    private static final EntityType VIRTUAL_MACHINE_ENTITY_TYPE = EntityType.named(VIRTUAL_MACHINE).get();

    @Autowired
    private DbTestConfig dbTestConfig;
    private static String testDbName;
    private static HistorydbIO historydbIO;

    final Field<BigDecimal> avgValue =
        avg(JooqUtils.getDoubleField(VmStatsLatest.VM_STATS_LATEST, StringConstants.AVG_VALUE)).as(StringConstants.AVG_VALUE);
    final Field<BigDecimal> avgCapacity =
        avg(JooqUtils.getDoubleField(VmStatsLatest.VM_STATS_LATEST, StringConstants.CAPACITY)).as(StringConstants.CAPACITY);
    final Field<String> uuid =
        JooqUtils.getStringField(VmStatsLatest.VM_STATS_LATEST, StringConstants.UUID);
    Table<Record3<String, BigDecimal, BigDecimal>> aggregatedStats =
        select(uuid, avgValue, avgCapacity)
            .from(VmStatsLatest.VM_STATS_LATEST).asTable("aggregatedStats");

    /**
     * Common setup code to run before every test.
     *
     * @throws VmtDbException If there is a DB error.
     */
    @Before
    public void setupDb() throws VmtDbException {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.mappedSchemaForTests = testDbName;
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        BasedbIO.setSharedInstance(historydbIO);
        // Do not clear old db - we rely on the "teardown" to delete data after each test.
        historydbIO.init(false, null, testDbName, Optional.empty());
    }

    /**
     * Delete all data in the database after every test.
     *
     * @throws Exception If database issue.
     */
    @After
    public void teardown() throws Exception {
        // Delete all data in any tables.
        try (Connection conn = historydbIO.connection()) {
            DbCleanupRule.cleanDb(historydbIO.using(conn), Vmtdb.VMTDB);
        } catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Teardown the database after ALL the tests are done.
     */
    @AfterClass
    public static void teardownDb() {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
        } catch (VmtDbException e) {
            logger.error("Problem dropping db: " + testDbName, e);
        }
    }

    @Test
    public void testConnectionTimeoutBeforeInitialization() throws VmtDbException {
        historydbIO.setQueryTimeoutSeconds(1234);
        assertEquals(1234, historydbIO.getQueryTimeoutSeconds());
    }

    @Test
    public void testSettingConnectionTimeoutAppliedBeforeInit() throws VmtDbException {
        try {
            historydbIO.setQueryTimeoutSeconds(1234);
            setupDatabase();
            assertEquals(1234, BasedbIO.getInternalConnectionPoolTimeoutSeconds());
        } finally {
            teardownDatabase();
        }
    }

    @Test
    public void testSettingConnectionTimeoutAppliedAfterInit() throws VmtDbException {
        try {
            setupDatabase();
            assertNotEquals(55, BasedbIO.getInternalConnectionPoolTimeoutSeconds());

            historydbIO.setQueryTimeoutSeconds(55);
            assertEquals(55, BasedbIO.getInternalConnectionPoolTimeoutSeconds());
        } finally {
            teardownDatabase();
        }
    }

    @Test
    public void testGetValueFieldSortByPriceIndex() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        final SeekPaginationCursor seekPaginationCursor = new SeekPaginationCursor(Optional.of("1"),
            Optional.of(new BigDecimal(1)));
        Field<BigDecimal> field =
            seekPaginationCursor.getValueField(paginationParam, aggregatedStats);
        assertEquals("avg_value", field.getName());
    }

    @Test
    public void testGetValueFieldSortByCUP() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.CPU);
        final SeekPaginationCursor seekPaginationCursor = new SeekPaginationCursor(Optional.of("1"),
            Optional.of(new BigDecimal(1)));
        Field<BigDecimal> field =
            seekPaginationCursor.getValueField(paginationParam, aggregatedStats);
        // the filed type is org.jooq.impl.Expression which is not exposed. There seems no other way to validate
        // the output other than filed.toString.
        assertEquals("(\"aggregatedStats\".\"avg_value\" / \"aggregatedStats\".\"capacity\")"
            , field.toString());
    }

    /**
     * Tests getting entityType from {@link Stats.EntityStatsScope} when set to entityType.
     *
     * @throws VmtDbException if there's an error querying DB for types of entities
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityType()
            throws VmtDbException, IllegalArgumentException {
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
     * @throws VmtDbException if there's an error querying DB for types of entities
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityList() throws VmtDbException, IllegalArgumentException {
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
     * @throws VmtDbException if there's an error querying DB for types of entities
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test
    public void testGetEntityTypeFromEntityStatsScopeEntityListMapsToNoEntityType() throws VmtDbException, IllegalArgumentException {
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
     * @throws VmtDbException if there's an error querying DB for types of entities
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEntityTypeFromEntityStatsScopeEntityListMapsToMultipleEntityType() throws VmtDbException, IllegalArgumentException {
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

    private void setupDatabase() throws VmtDbException {

    }

    private void teardownDatabase() {
    }

    /**
     * Tests totalRecordCount from query is being returned in nextPageInfo.
     *
     * @throws VmtDbException if there's an error querying DB for types of entities
     */
    @Test
    public void testGetNextPageGettingTotalRecordCount() throws VmtDbException {
        try {
            //GIVEN
            setupDatabase();

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
            doReturn(100).when(historydbIOSpy).getTotalRecordsCount(any(), any());

            //WHEN
            NextPageInfo nextPageInfo = historydbIOSpy.getNextPage(entityStatsScope,
                    new Timestamp(1L), TimeFrame.LATEST, paginationParams, VIRTUAL_MACHINE_ENTITY_TYPE);

            //THEN
            verify(historydbIOSpy, times(1)).getTotalRecordsCount(any(), any());
            assertTrue(nextPageInfo.getTotalRecordCount().get().equals(100));
        } finally {
            teardownDatabase();
        }
    }
}
