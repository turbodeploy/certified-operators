package com.vmturbo.history.db.queries;

import java.sql.Timestamp;

import org.jooq.Query;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.Queries;
import com.vmturbo.history.db.QueryTestBase;

/**
 * Test class for {@link AvailableEntityTimestamps} query builder.
 */
public class AvailableEntityTimestampsTest extends QueryTestBase {
    private QueryChecker queryChecker;

    /**
     * Test setup.
     */
    @Before
    public void before() {
        setupJooq();
        // create a query builder representing defaults for all parameters, to use aa a base for individual tests
        this.queryChecker = new QueryChecker()
                .withDistinct(true)
                .withSelectFields("market_stats_latest.snapshot_time")
                .withTables("market_stats_latest")
                .withSortFields("market_stats_latest.snapshot_time DESC")
                .withLimit(0);
    }

    /**
     * Test the query builder with no optional constratins provided.
     */
    @Test
    public void testUnconstrainedQuery() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST, null, null, 0,
                null, null, false);
        queryChecker.check(query);
    }

    /**
     * Test the query builder with an alternate timeframe.
     */
    @Test
    public void testHourlyTimeFrame() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.HOUR, null, null, 0,
                null, null, false);
        queryChecker.withTables("market_stats_by_hour")
                .withSelectFields("market_stats_by_hour.snapshot_time")
                .withSortFields("market_stats_by_hour.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with an entity type specified.
     */
    @Test
    public void testWithEntityType() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                EntityType.PHYSICAL_MACHINE, null, 0,
                null, null, false);
        queryChecker.withTables("pm_stats_latest")
                .withSelectFields("pm_stats_latest.snapshot_time")
                .withSortFields("pm_stats_latest.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with entity type and entity oid both provided.
     */
    @Test
    public void testWithEntityOid() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                EntityType.PHYSICAL_MACHINE, "xyzzy", 0,
                null, null, false);
        queryChecker.withTables("pm_stats_latest FORCE INDEX \\(uuid\\)")
                .withSelectFields("pm_stats_latest.snapshot_time")
                .withConditions("pm_stats_latest.uuid = 'xyzzy'")
                .withSortFields("pm_stats_latest.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with required property types.
     */
    @Test
    public void testWithPropertyTypes() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 0,
                null, null, false, "p1", "p2");
        queryChecker.withConditions("market_stats_latest.property_type IN \\(\\s*'p1', \\s*'p2'\\s*\\)")
                .check(query);
    }

    /**
     * Test the query builder with excluded property types.
     */
    @Test
    public void testWithExcludedPropertyTypes() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 0,
                null, null, true, "p1", "p2");
        queryChecker.withConditions("market_stats_latest.property_type NOT IN \\(\\s*'p1', \\s*'p2'\\s*\\)")
                .check(query);
    }

    /**
     * Test the query builder with a lower bound on the timestamp.
     */
    @Test
    public void testWithMinTime() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 0,
                Timestamp.valueOf("2019-01-02 03:04:05"), null, false);
        queryChecker.withConditions("market_stats_latest.snapshot_time >= TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with an upper bound on the timestamp.
     */
    @Test
    public void testWithMaxTime() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 0,
                null, Timestamp.valueOf("2019-01-02 03:04:05"), false);
        queryChecker.withConditions("market_stats_latest.snapshot_time < TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with lower and upper bounds on the timestamp.
     */
    @Test
    public void testWithBothTimes() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 0,
                Timestamp.valueOf("2019-01-01 03:04:05"), Timestamp.valueOf("2019-01-02 03:04:05"), false);
        queryChecker
                .withConditions("market_stats_latest.snapshot_time " +
                        "BETWEEN TIMESTAMP '2019-01-01 03:04:05\\.0' " +
                        "AND TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with a limit of 1 (drops DISTINCT as well as setting the limit).
     */
    @Test
    public void testWithLimit1() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 1,
                null, null, false);
        queryChecker.withDistinct(false).withLimit(1).check(query);
    }

    /**
     * Test the query builder with a limit > 1.
     */
    @Test
    public void testWithLimit10() {
        Query query = Queries.getAvailableEntityTimestampsQuery(TimeFrame.LATEST,
                null, null, 10,
                null, null, false);
        queryChecker.withLimit(10).check(query);
    }
}
