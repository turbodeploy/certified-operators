package com.vmturbo.sql.utils.partition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Instant;

import org.junit.Test;

/**
 * Test class for {@link Partition} class.
 */
public class PartitionTest {
    private Partition<Instant> part = new Partition<Instant>("sname", "tname", "pname",
            Instant.ofEpochMilli(0L), Instant.ofEpochMilli(1000L));

    /**
     * Make sure we can retrieve info form a partition.
     */
    @Test
    public void testAccessorsWork() {
        assertThat(part.getSchemaName(), is("sname"));
        assertThat(part.getTableName(), is("tname"));
        assertThat(part.getPartitionName(), is("pname"));
        assertThat(part.getInclusiveLower(), is(Instant.ofEpochMilli(0L)));
        assertThat(part.getExclusiveUpper(), is(Instant.ofEpochMilli(1000L)));
    }

    /**
     * Test that partitions can reliably say what time points they include.
     */
    @Test
    public void testContainsMethod() {
        assertThat(part.contains(Instant.ofEpochMilli(0L)), is(true));
        assertThat(part.contains(Instant.ofEpochMilli(500L)), is(true));
        assertThat(part.contains(Instant.ofEpochMilli(999L)), is(true));
        assertThat(part.contains(Instant.ofEpochMilli(-1L)), is(false));
        assertThat(part.contains(Instant.ofEpochMilli(1000L)), is(false));
    }

    /**
     * See that that the string rendering of a patition is as eexpected.
     */
    @Test
    public void testToString() {
        assertThat(part.toString(), is("pname[1970-01-01T00:00:00Z, 1970-01-01T00:00:01Z)"));
    }
}