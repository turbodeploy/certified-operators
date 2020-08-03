package com.vmturbo.history.listeners;

import java.sql.Timestamp;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.history.listeners.RollupProcessor.RollupType;

/**
 * Tests for hte {@link RollupType} class nested
 * within {@link RollupProcessor}.
 */
public class RollupTypeTest {

    /**
     * Test that rollup times are correctly calculated for provided snapshot times.
     */
    @Test
    public void testRollupTimes() {
        // hourly
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T00:15:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T00:59:59.999Z"))).toInstant());
        // daily
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T12:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getRollupTime(Timestamp.from(Instant.parse("2019-01-02T23:59:59.999Z"))).toInstant());
        // monthly
        Assert.assertEquals(Instant.parse("2019-01-31T00:00:00Z"),
                RollupType.BY_MONTH.getRollupTime(Timestamp.from(Instant.parse("2019-01-01T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-31T00:00:00Z"),
                RollupType.BY_MONTH.getRollupTime(Timestamp.from(Instant.parse("2019-01-15T03:23:39Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-31T00:00:00Z"),
                RollupType.BY_MONTH.getRollupTime(Timestamp.from(Instant.parse("2019-01-31T23:59:59.999Z"))).toInstant());
    }

    /**
     * Test that rollup period start times are correctly calculated for given snapshot times.
     */
    @Test
    public void testRollupPeriodStarts() {
        // hourly
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T00:15:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_HOUR.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T00:59:59.999Z"))).toInstant());
        // daily
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T12:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T00:00:00Z"),
                RollupType.BY_DAY.getPeriodStart(Timestamp.from(Instant.parse("2019-01-02T23:59:59.999Z"))).toInstant());
        // monthly
        Assert.assertEquals(Instant.parse("2019-01-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodStart(Timestamp.from(Instant.parse("2019-01-01T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodStart(Timestamp.from(Instant.parse("2019-01-15T03:23:39Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodStart(Timestamp.from(Instant.parse("2019-01-31T23:59:59.999Z"))).toInstant());

    }

    /**
     * Test that rollup period end times are correctly calculated for provided snapshot times.
     */
    @Test
    public void testRollupPeriodEnds() {
        // hourly
        Assert.assertEquals(Instant.parse("2019-01-02T01:00:00Z"),
                RollupType.BY_HOUR.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T01:00:00Z"),
                RollupType.BY_HOUR.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T00:15:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-02T01:00:00Z"),
                RollupType.BY_HOUR.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T00:59:59.999Z"))).toInstant());
        // daily
        Assert.assertEquals(Instant.parse("2019-01-03T00:00:00Z"),
                RollupType.BY_DAY.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-03T00:00:00Z"),
                RollupType.BY_DAY.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T12:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-01-03T00:00:00Z"),
                RollupType.BY_DAY.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-02T23:59:59.999Z"))).toInstant());
        // monthly
        Assert.assertEquals(Instant.parse("2019-02-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-01T00:00:00Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-02-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-15T03:23:39Z"))).toInstant());
        Assert.assertEquals(Instant.parse("2019-02-01T00:00:00Z"),
                RollupType.BY_MONTH.getPeriodEnd(Timestamp.from(Instant.parse("2019-01-31T23:59:59.999Z"))).toInstant());

    }
}
