package com.vmturbo.history.db.queries;

import java.sql.Timestamp;

import org.jooq.Query;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.Queries;
import com.vmturbo.history.db.QueryTestBase;
import com.vmturbo.history.schema.HistoryVariety;

/**
 * Test class for the {@link AvailableTimestamps} query builder.
 */
public class AvailableTimestampsTest extends QueryTestBase {

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
                .withSelectFields("available_timestamps.time_stamp")
                .withTables("available_timestamps")
                .withConditions("available_timestamps.time_frame = 'LATEST'",
                        "available_timestamps.history_variety = 'ENTITY_STATS'")
                .withSortFields("available_timestamps.time_stamp DESC")
                .withLimit(0);
    }

    /**
     * Test the query builder with defaults for all parameters.
     */
    @Test
    public void testUnconstrainedQuery() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 0, null, null);
        queryChecker.check(query);
    }

    /**
     * Test the query builder with an alternate time frame.
     */
    @Test
    public void testHourlyTimeFrame() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.HOUR, HistoryVariety.ENTITY_STATS, 0, null, null);
        queryChecker.withConditions("available_timestamps.time_frame = 'HOUR'",
                "available_timestamps.history_variety = 'ENTITY_STATS'")
                .check(query);
    }

    /**
     * Test the query builder with an alternate history variety.
     */
    @Test
    public void testPriceDataVariety() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.PRICE_DATA, 0, null, null);
        queryChecker.withConditions("available_timestamps.time_frame = 'LATEST'",
                "available_timestamps.history_variety = 'PRICE_DATA'")
                .check(query);
    }

    /**
     * Test the query builder with a limit of 1 (which removes DISTINCT in addition to setting LIMIT).
     */
    @Test
    public void testLimit1() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 1, null, null);
        queryChecker.withDistinct(false).withLimit(1).check(query);
    }

    /**
     * Test the query builder with a limit > 1.
     */
    @Test
    public void testLimit10() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 10, null, null);
        queryChecker.withLimit(10).check(query);
    }

    /**
     * Test the query builder with a lower bound specified.
     */
    @Test
    public void testMinTime() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 0,
                Timestamp.valueOf("2019-01-02 03:04:05"), null);
        queryChecker.withMoreConditions("available_timestamps.time_stamp >= TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with an upper bound specified.
     */
    @Test
    public void testMaxTime() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 0,
                null, Timestamp.valueOf("2019-01-02 03:04:05"));
        queryChecker.withMoreConditions("available_timestamps.time_stamp < TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }

    /**
     * Test the query builder with lower and upper bounds specified.
     */
    @Test
    public void testBothTimes() {
        Query query = Queries.getAvailableSnapshotTimesQuery(
                TimeFrame.LATEST, HistoryVariety.ENTITY_STATS, 0,
                Timestamp.valueOf("2019-01-01 03:04:05"), Timestamp.valueOf("2019-01-02 03:04:05"));
        queryChecker.withMoreConditions(
                "available_timestamps.time_stamp " +
                        "BETWEEN TIMESTAMP '2019-01-01 03:04:05\\.0' " +
                        "AND TIMESTAMP '2019-01-02 03:04:05\\.0'")
                .check(query);
    }
}
