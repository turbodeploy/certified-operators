package com.vmturbo.history.db.queries;

import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;

import java.sql.Timestamp;

import org.jooq.Query;
import org.jooq.ResultQuery;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.QueryTestBase;

/**
 * Test class for {@link AvailableEntityTimestampsQuery} query builder.
 */
public class AvailableEntityTimestampsQueryTest extends QueryTestBase {
    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = EntityType.named(PHYSICAL_MACHINE).get();
    private QueryChecker queryChecker;

    /**
     * Setup a query checker with defaults that apply to most tests.
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
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST, null, null, 0,
                null, null, false).getQuery();
        queryChecker.check(query);
    }

    /**
     * Test the query builder with an alternate timeframe.
     */
    @Test
    public void testHourlyTimeFrame() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.HOUR, null, null, 0,
                null, null, false).getQuery();
        queryChecker.withTables(true, "market_stats_by_hour")
                .withSelectFields(true, "market_stats_by_hour.snapshot_time")
                .withSortFields(true, "market_stats_by_hour.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with an entity type specified.
     */
    @Test
    public void testWithEntityType() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                PHYSICAL_MACHINE_ENTITY_TYPE, null, 0,
                null, null, false).getQuery();
        queryChecker.withTables(true, "pm_stats_latest")
                .withSelectFields(true, "pm_stats_latest.snapshot_time")
                .withSortFields(true, "pm_stats_latest.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with a cluster entity type specified.
     */
    @Test
    public void testWithClusterEntityType() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                EntityType.named(StringConstants.CLUSTER).get(), null, 0,
                null, null, false).getQuery();
        queryChecker.withTables(true, "cluster_stats_latest")
                .withSelectFields(true, "cluster_stats_latest.recorded_on")
                .withSortFields(true, "cluster_stats_latest.recorded_on")
                .check(query);
    }

    /**
     * Test the query builder with a cluster entity type and cluster oid specified.
     */
    @Test
    public void testWithClusterEntityOid() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                EntityType.named(StringConstants.CLUSTER).get(), "123", 0,
                null, null, false).getQuery();
        queryChecker.withTables(true, "cluster_stats_latest")
                .withSelectFields(true, "cluster_stats_latest.recorded_on")
                .withSortFields(true, "cluster_stats_latest.recorded_on")
                .withConditions(true, "cluster_stats_latest.internal_name = '123'")
                .check(query);
    }

    /**
     * Test the query builder with entity type and entity oid both provided.
     */
    @Test
    public void testWithEntityOid() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                PHYSICAL_MACHINE_ENTITY_TYPE, "xyzzy", 0, null, null, false).getQuery();
        queryChecker
                // TODO reinstate check for index hint when the hint is back in
                //                .withTables("pm_stats_latest FORCE INDEX \\(uuid\\)")
                .withTables(true, "pm_stats_latest")
                .withSelectFields(true, "pm_stats_latest.snapshot_time")
                .withConditions(true, "pm_stats_latest.uuid = 'xyzzy'")
                .withSortFields(true, "pm_stats_latest.snapshot_time")
                .check(query);
    }

    /**
     * Test the query builder with required property types.
     */
    @Test
    public void testWithPropertyTypes() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 0,
                null, null, false, "p1", "p2").getQuery();
        queryChecker.withConditions(true,
                        "market_stats_latest.property_type IN \\(\\s*'p1', \\s*'p2'\\s*\\)")
                .check(query);
    }

    /**
     * Test the query builder with excluded property types.
     */
    @Test
    public void testWithExcludedPropertyTypes() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 0,
                null, null, true, "p1", "p2").getQuery();
        queryChecker.withConditions(true,
                        "market_stats_latest.property_type NOT IN \\(\\s*'p1', \\s*'p2'\\s*\\)")
                .check(query);
    }

    /**
     * Test the query builder with a lower bound on the timestamp.
     */
    @Test
    public void testWithMinTime() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 0,
                Timestamp.valueOf("2019-01-02 03:04:05"), null, false).getQuery();
        queryChecker.withConditions(true,
                        "market_stats_latest.snapshot_time >= TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with an upper bound on the timestamp.
     */
    @Test
    public void testWithMaxTime() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 0,
                null, Timestamp.valueOf("2019-01-02 03:04:05"), false).getQuery();
        queryChecker.withConditions(true,
                        "market_stats_latest.snapshot_time <= TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with lower and upper bounds on the timestamp.
     */
    @Test
    public void testWithBothTimes() {
        final ResultQuery<?> query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 0,
                Timestamp.valueOf("2019-01-01 03:04:05"), Timestamp.valueOf("2019-01-02 03:04:05"),
                false).getQuery();
        queryChecker
                .withConditions(true, "market_stats_latest.snapshot_time "
                        + "BETWEEN TIMESTAMP '2019-01-01 03:04:05\\.0' "
                        + "AND TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with a limit of 1 (drops DISTINCT as well as setting the limit).
     */
    @Test
    public void testWithLimit1() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 1,
                null, null, false).getQuery();
        queryChecker.withDistinct(false).withLimit(1).check(query);
    }

    /**
     * Test the query builder with a limit > 1.
     */
    @Test
    public void testWithLimit10() {
        Query query = new AvailableEntityTimestampsQuery(dsl, TimeFrame.LATEST,
                null, null, 10,
                null, null, false).getQuery();
        queryChecker.withLimit(10).check(query);
    }
}
