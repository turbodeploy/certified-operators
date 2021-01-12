package com.vmturbo.history.stats;

import org.jooq.Record;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.history.schema.abstraction.tables.DbStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.DbStatsLatestRecord;
import com.vmturbo.history.stats.snapshots.UsageRecordVisitor;

/**
 * Test for {@link UsageRecordVisitor}.
 */
public class UsageRecordVisitorTest {

    private final double avg1 = 10.0;
    private final double avg2 = 20.0;
    private final double capacity = 100.0;


    private static final double EPSILON = 0.001;

    /**
     * Test DB record into {@link StatRecord} conversion  - used values (avg) is set.
     */
    @Test
    public void testWithUsed() {
        UsageRecordVisitor visitor = new UsageRecordVisitor(true, StatsConfig.USAGE_POPULATOR);

        Record r1 = new DbStatsLatestRecord();
        r1.set(DbStatsLatest.DB_STATS_LATEST.AVG_VALUE, avg1);
        r1.set(DbStatsLatest.DB_STATS_LATEST.PROPERTY_SUBTYPE, "StorageAmmount");
        visitor.visit(r1);

        Record r2 = new DbStatsLatestRecord();
        r2.set(DbStatsLatest.DB_STATS_LATEST.AVG_VALUE, avg2);
        r2.set(DbStatsLatest.DB_STATS_LATEST.PROPERTY_SUBTYPE, "StorageAmmount");
        visitor.visit(r2);

        StatRecord.Builder b = StatRecord.newBuilder();
        visitor.build(b);

        StatRecord r = b.build();

        Assert.assertTrue(r.hasUsed());
        Assert.assertEquals(avg1 + avg2, r.getUsed().getAvg(), EPSILON);
        Assert.assertEquals(avg1 + avg2, r.getUsed().getTotal(), EPSILON);
        Assert.assertEquals(avg1 + avg2, r.getUsed().getMax(), EPSILON);
    }

    /**
     * Test DB record into {@link StatRecord} conversion  - used values (avg) is NOT set.
     */
    @Test
    public void testNoUsed() {
        UsageRecordVisitor visitor = new UsageRecordVisitor(true, StatsConfig.USAGE_POPULATOR);

        Record r1 = new DbStatsLatestRecord();
        r1.set(DbStatsLatest.DB_STATS_LATEST.PROPERTY_SUBTYPE, "StorageAmmount");
        visitor.visit(r1);

        Record r2 = new DbStatsLatestRecord();
        r2.set(DbStatsLatest.DB_STATS_LATEST.PROPERTY_SUBTYPE, "StorageAmmount");
        visitor.visit(r2);

        StatRecord.Builder b = StatRecord.newBuilder();
        visitor.build(b);

        StatRecord r = b.build();
        //there is no used
        Assert.assertFalse(r.hasUsed());
    }
}
