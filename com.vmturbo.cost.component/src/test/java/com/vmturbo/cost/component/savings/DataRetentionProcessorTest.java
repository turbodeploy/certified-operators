package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.BILLED_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_DAY;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.bottomup.AggregatedSavingsStats;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;
import com.vmturbo.cost.component.savings.bottomup.SqlEntityStateStoreTest;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Parent for bottom-up and bill-based data retention processor testers.
 * Doesn't have any tests, only contains helper/common methods.
 */
public class DataRetentionProcessorTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    protected final DSLContext dsl;

    /**
     * Handle to store for stats DB table access.
     */
    protected EntitySavingsStore statsSavingsStore;

    /**
     * Instance of retention processor.
     */
    protected DataRetentionProcessor retentionProcessor;

    /**
     * Clock to keeping track of times. UTC: June 4, 2021 12:00:00 AM
     */
    protected final MutableFixedClock clock = new MutableFixedClock(1622764800000L);

    /**
     * OIDs for VMs for testing.
     */
    protected final long vmOid1 = 101L;

    /**
     * VM2 oid.
     */
    protected final long vmOid2 = 202L;

    /**
     * Time that is 40 old and will get purged as part of retention processing.
     * 1619308800000: April 25, 2021 12:00:00 AM
     */
    protected final LocalDateTime timeOlder = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(40).truncatedTo(ChronoUnit.HOURS);

    /**
     * Time that is 4 days old and will get purged as part of retention processing.
     * 1622419200000: May 31, 2021 12:00:00 AM
     */
    protected final LocalDateTime timeOld = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(4).truncatedTo(ChronoUnit.HOURS);

    /**
     * Newer time that won't get purged.
     * 1622592000000: June 2, 2021 12:00:00 AM
     */
    protected final LocalDateTime timeNew = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(2).truncatedTo(ChronoUnit.HOURS);

    /**
     * Start time for queries.
     * 1617580800000: April 5, 2021 12:00:00 AM
     */
    protected final LocalDateTime timeStart = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(60).truncatedTo(ChronoUnit.HOURS);

    /**
     * End time for queries.
     * 1622851200000: June 5, 2021 12:00:00 AM
     */
    protected final LocalDateTime timeEnd = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().plusDays(1).truncatedTo(ChronoUnit.HOURS);

    /**
     * All types of stats for query.
     */
    private static final Set<EntitySavingsStatsType> allStatsTypes = new HashSet<>(Arrays.asList(
            EntitySavingsStatsType.values()));

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public DataRetentionProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Helper to check counts of records in daily stats table.
     *
     * @param expectedBottomUpDailyCount Count of records in bottom-up stats table.
     * @param expectedBillBasedDailyCount Count of records in bill-based stats table.
     */
    protected void checkDailyCounts(int expectedBottomUpDailyCount, int expectedBillBasedDailyCount) {
        assertEquals(expectedBottomUpDailyCount, dsl.fetchCount(ENTITY_SAVINGS_BY_DAY));
        assertEquals(expectedBillBasedDailyCount, dsl.fetchCount(BILLED_SAVINGS_BY_DAY));
    }

    /**
     * Adds required scope records to the cloud_scope_table, so that stats can be inserted into
     * the hour/day/month stats tables, otherwise FK constraint violation errors.
     */
    protected void addScopeRecords() {
        EntityCloudScopeRecord s1 = SqlEntityStateStoreTest.createEntityCloudScopeRecord(vmOid1,
                12345L, 12345L, 12345L, 12345L, 12345L);
        dsl.insertInto(ENTITY_CLOUD_SCOPE).set(s1).execute();

        EntityCloudScopeRecord s2 = SqlEntityStateStoreTest.createEntityCloudScopeRecord(vmOid2,
                12345L, 12345L, 12345L, 12345L, 12345L);
        dsl.insertInto(ENTITY_CLOUD_SCOPE).set(s2).execute();
    }

    /**
     * Convenience function query stats.
     *
     * @param startTime Stats start time.
     * @param endTime Stats end time.
     * @param tableType Which table (hourly, daily or monthly) to query.
     * @return Queried stats.
     * @throws EntitySavingsException Thrown on DB access error.
     */
    protected List<AggregatedSavingsStats> fetchStats(long startTime, long endTime,
            RollupDurationType tableType) throws EntitySavingsException {
        final Set<Long> entityOids = ImmutableSet.of(vmOid1, vmOid2);
        switch (tableType) {
            case HOURLY:
                return statsSavingsStore.getHourlyStats(allStatsTypes, startTime, endTime,
                        entityOids, Collections.emptyList(), Collections.emptyList());
            case DAILY:
                return statsSavingsStore.getDailyStats(allStatsTypes, startTime, endTime,
                        entityOids, Collections.emptyList(), Collections.emptyList());
            case MONTHLY:
                return statsSavingsStore.getMonthlyStats(allStatsTypes, startTime, endTime,
                        entityOids, Collections.emptyList(), Collections.emptyList());
            default:
                throw new EntitySavingsException("Unsupported table type '" + tableType
                        + "' for fetchStats() query.");
        }
    }
}
