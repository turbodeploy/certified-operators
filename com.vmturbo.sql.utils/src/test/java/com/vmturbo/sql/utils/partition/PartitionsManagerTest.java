package com.vmturbo.sql.utils.partition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.threeten.extra.PeriodDuration;

import com.vmturbo.components.common.utils.RollupTimeFrame;

/**
 * Tests of hte {@link PartitionsManager} class.
 */
public class PartitionsManagerTest {

    private static final String TEST_SCHEMA_NAME = "schema";
    private static final String TEST_TABLE_NAME = "stats";

    private final IPartitionAdapter adapter = mock(IPartitionAdapter.class);
    private final PartitionsManager partmgr = new PartitionsManager(adapter);
    private final Schema testSchema = DSL.schema(DSL.name(TEST_SCHEMA_NAME));
    private final Table<Record> testTable = DSL.table(DSL.name(TEST_SCHEMA_NAME, TEST_TABLE_NAME));
    private final List<Partition<Instant>> addedPartitions = new ArrayList<>();
    private final List<Partition<Instant>> droppedPartitions = new ArrayList<>();

    /**
     * Set up standard responses for the mock adapter.
     *
     * @throws PartitionProcessingException if there's an issue
     */
    @Before
    public void before() throws PartitionProcessingException {
        Mockito.doAnswer(inv -> {
                    Object[] args = inv.getArguments();
                    Partition<Instant> part = new Partition<>((String)args[0], (String)args[1],
                            "part" + args[0] + args[2], (Instant)args[2], (Instant)args[3]);
                    addedPartitions.add(part);
                    return part;
                })
                .when(adapter).createPartition(anyString(), anyString(),
                        any(Instant.class), any(Instant.class), any(Partition.class));
        //noinspection unchecked
        Mockito.doAnswer(inv -> {
            //noinspection unchecked
            droppedPartitions.add(inv.getArgumentAt(0, Partition.class));
            return null;
        }).when(adapter).dropPartition(any(Partition.class));
    }

    /**
     * Test that ensurePartition only creates new partitions when needed.
     *
     * @throws PartitionProcessingException if any operation fails
     */
    @Test
    public void testEnsurePartitionUsesExistingPartitions() throws PartitionProcessingException {
        // start with just a single partition covering [1000, 2000)
        setInitialParts(mkPartition("p1", 1000L, 2000L));
        // that should cover 1000, 1999, and values in between
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(1000L),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(true));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(1500L),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(true));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(1999L),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(true));
        // next ensurePartition will create a new partition - make that cover [0, 1000)
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(999L),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(false));
        // make sure we just added a partition
        assertThat(addedPartitions.size(), is(1));
        // check a value inside the newly added partition
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(500L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(true));
        // next ensurePartition will create a new partition - make it cover [2000, 3000)
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(2000L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(false));
        // we should have just created another partition
        assertThat(addedPartitions.size(), is(2));
        // check a value within the newly created partition
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(2500L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(true));
        // finally, go back and check values from the other partitions, so we know we didn't
        // unexpectedly drop any of them
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(500L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(true));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(1500L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(true));
    }

    /**
     * In various scenarios, verify that the next closest ascending partition is correctly identified upon creating a
     * new partition. The identity of the next partition is only used when creating new partitions, and not every
     * partition adapter uses it.
     *
     * @throws PartitionProcessingException on error
     */
    @Test
    public void testThatTheNextAscendingPartitionIsCorrectlyIdentified() throws PartitionProcessingException {
        setInitialParts();

        final ArgumentCaptor<Partition> followingPartitionArgCaptor = ArgumentCaptor.forClass(Partition.class);

        assertThat("0[ ][ ][ ][X][ ]5000", partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(3000),
                PeriodDuration.of(Duration.ofMillis(1000))), is(false));

        verify(adapter, times(1)).createPartition(any(), any(), any(), any(), followingPartitionArgCaptor.capture());
        assertThat("no partition follows the new one because it is the first partition",
                followingPartitionArgCaptor.getValue(), nullValue());

        assertThat("0[ ][ ][X][X][ ]5000", partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(2000),
                PeriodDuration.of(Duration.ofMillis(1000))), is(false));

        verify(adapter, times(2)).createPartition(any(), any(), any(), any(), followingPartitionArgCaptor.capture());
        assertThat("the new partition is directly before the [3000, 4000) partition",
                followingPartitionArgCaptor.getValue().getInclusiveLower(), equalTo(Instant.ofEpochMilli(3000)));

        assertThat("0[X][ ][X][X][ ]5000", partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(0),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(false));

        verify(adapter, times(3)).createPartition(any(), any(), any(), any(), followingPartitionArgCaptor.capture());
        assertThat("the next closest ascending partition is [2000, 3000)",
                followingPartitionArgCaptor.getValue().getInclusiveLower(), equalTo(Instant.ofEpochMilli(2000)));

        assertThat("0[X][ ][X][X][X]5000", partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(4000),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(false));

        verify(adapter, times(4)).createPartition(any(), any(), any(), any(), followingPartitionArgCaptor.capture());
        assertThat("no partition follows the new one", followingPartitionArgCaptor.getValue(), nullValue());

        assertThat("0[X][X][X][X][X]5000", partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(1999),
                PeriodDuration.of(Duration.ofMillis(1000L))), is(false));

        verify(adapter, times(5)).createPartition(any(), any(), any(), any(), followingPartitionArgCaptor.capture());
        assertThat("the new value is only 1ms before [2000, 3000), so it should be the closest ascending",
                followingPartitionArgCaptor.getValue().getInclusiveLower(), equalTo(Instant.ofEpochMilli(2000)));
    }

    /**
     * Make sure that, when no bounds adjustments are required in order to fit among existing
     * partitions, the partitions that are created are correctly aligned to an exact multiple of the
     * partition size from epoch.
     *
     * @throws PartitionProcessingException if there's an issue executing the test
     */
    @Test
    public void testPartitionAlignmentWorks() throws PartitionProcessingException {
        // 1970 was not a leap year, so a 2-month partition size will be initially treated as a
        // fixed 59-day size, and that will be incorrect when applied to subsequent target times
        // that are beyond at least one post-epoch leap-day.
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable,
                        Instant.parse("1972-05-15T00:00:00Z"), PeriodDuration.of(Period.ofMonths(2))),
                is(false));
        // remember months are 1-indexed, so 2 x 2-months into 1972 is the beginning first
        // moment of May
        chkLastAddedPartBounds(Instant.parse("1972-05-01T00:00:00Z").toEpochMilli(),
                Instant.parse("1972-07-01T00:00:00Z").toEpochMilli());
        // now let's try one far into the future so the initial estimate will be further off. We
        // should still zero in on the correct bounds
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable,
                        Instant.parse("3000-05-15T00:00:00Z"), PeriodDuration.of(Period.ofMonths(2))),
                is(false));
        chkLastAddedPartBounds(Instant.parse("3000-05-01T00:00:00Z").toEpochMilli(),
                Instant.parse("3000-07-01T00:00:00Z").toEpochMilli());
        // Now let's try a size that will yield an initial estimate that will be a bit longer than it
        // should be: 3 years. Starting from the epoch, that will yield a 365+365+366 = 1096-day
        // period, which would be like having a leap day every three years rather than every four.
        // Four of those will already be off by a day because it will include one extra leap day
        // (one for each replication = 4, vs 3 actually appearing in that period).
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable,
                        Instant.parse("1982-01-15T00:00:00Z"), PeriodDuration.of(Period.ofYears(3))),
                is(false));
        chkLastAddedPartBounds(Instant.parse("1982-01-01T00:00:00Z").toEpochMilli(),
                Instant.parse("1985-01-01T00:00:00Z").toEpochMilli());
        // and again, with a date far into the future
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable,
                        Instant.parse("9990-01-15T00:00:00Z"), PeriodDuration.of(Period.ofYears(3))),
                is(false));
        chkLastAddedPartBounds(Instant.parse("9989-01-01T00:00:00Z").toEpochMilli(),
                Instant.parse("9992-01-01T00:00:00Z").toEpochMilli());
    }

    /**
     * Make sure that created partition sizes are properly computed.
     *
     * @throws PartitionProcessingException if there's a problem executing the test
     */
    @Test
    public void testAddedPartitionBounds() throws PartitionProcessingException {
        setInitialParts(
                mkPartition("p1", 10000L, 20000L),
                mkPartition("p2", 25000L, 55000L),
                mkPartition("p3", 60000L, 73000L),
                mkPartition("p4", 77000L, 80000L));
        // this should create a new partition spanning [0, 10000) just in front of p1
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(7730),
                PeriodDuration.of(Duration.ofMillis(10000L))), is(false));
        chkLastAddedPartBounds(0L, 10000L);
        // this one will fit within the gap between p1 and p2, with 1-second gaps to either size
        // (note partition size = 3s, not 10)
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(22345L),
                PeriodDuration.of(Duration.ofMillis(3000L))), is(false));
        chkLastAddedPartBounds(21000L, 24000L);
        // Now let's squeeze something inside the [21000,24000) gap we just created. Both
        // natural alignment bounds will need to be pulled in for it to fit.
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(20400L),
                PeriodDuration.of(Duration.ofMillis(10000L))), is(false));
        chkLastAddedPartBounds(20000L, 21000L);
        // next we move into the [73000,77000) gap. Our 5000 ask will be truncated on the low end
        // but not the high end
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(73000),
                PeriodDuration.of(Duration.ofMillis(5000L))), is(false));
        chkLastAddedPartBounds(73000L, 75000L);
        // this time we'll throw in something with a size of 2000 whose natural spot would be
        // [76000,78000). The low end will be fine, but the high end will be truncated
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(76999),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(false));
        chkLastAddedPartBounds(76000L, 77000L);
        // one last case, where our target time is beyond any existing partitions, but the
        // natural alignment of the new partition would overlap the current last partition
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(80053L),
                PeriodDuration.of(Duration.ofMillis(3000L))), is(false));
        chkLastAddedPartBounds(80000L, 81000L);
    }

    /**
     * Make sure that retention processing correctly drops expired partitions.
     *
     * @throws PartitionProcessingException if there's an issue executing the test
     */
    @Test
    public void testRetentionProcessing() throws PartitionProcessingException {
        setInitialParts(
                mkPartition("p1", 10000L, 20000L),
                mkPartition("p2", 20000L, 30000L),
                mkPartition("p3", 30000L, 40000L),
                mkPartition("p4", 40000L, 50000L),
                mkPartition("p5", 50000L, 60000L),
                mkPartition("p6", 60000L, 70000L),
                mkPartition("p7", 70000L, 80000L),
                mkPartition("p8", 80000L, 90000L),
                mkPartition("p9", 90000L, 100000L));
        RetentionSettings retentionSettings = mock(RetentionSettings.class);
        when(retentionSettings.getRetentionPeriod(RollupTimeFrame.LATEST))
                .thenReturn(PeriodDuration.of(Duration.ofMillis(5000)));
        // nothing should expire at 1000ms
        partmgr.dropExpiredPartitions(TEST_SCHEMA_NAME, t -> Optional.of(RollupTimeFrame.LATEST),
                retentionSettings, Instant.ofEpochMilli(10000L));
        assertThat(droppedPartitions.size(), is(0));
        // Same here - the last msec in the partition is still 500 msecs from the reference time,
        // which is just barely still in bounds
        partmgr.dropExpiredPartitions(TEST_SCHEMA_NAME, t -> Optional.of(RollupTimeFrame.LATEST),
                retentionSettings, Instant.ofEpochMilli(24999L));
        assertThat(droppedPartitions.size(), is(0));
        // this one should finally - and just barely - get rid of p1
        partmgr.dropExpiredPartitions(TEST_SCHEMA_NAME, t -> Optional.of(RollupTimeFrame.LATEST),
                retentionSettings, Instant.ofEpochMilli(25000L));
        assertThat(droppedPartitions.size(), is(1));
        // make sure we can drop several partitions in one operation
        partmgr.dropExpiredPartitions(TEST_SCHEMA_NAME, t -> Optional.of(RollupTimeFrame.LATEST),
                retentionSettings, Instant.ofEpochMilli(78000L));
        assertThat(droppedPartitions.size(), is(6));
        // just double-check that we dropped the right partitions
        List<Long> droppedMillis = droppedPartitions.stream()
                .map(Partition::getInclusiveLower)
                .map(Instant::toEpochMilli)
                .collect(Collectors.toList());
        assertThat(droppedMillis,
                Matchers.containsInAnyOrder(10000L, 20000L, 30000L, 40000L, 50000L, 60000L));
        // finally, let's just double-check that the other partitions are still in place and
        // accepting records
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(75000L),
                PeriodDuration.of(Duration.ofMillis(1000))), is(true));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(85000L),
                PeriodDuration.of(Duration.ofMillis(1000))), is(true));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(95000L),
                PeriodDuration.of(Duration.ofMillis(1000))), is(true));
    }

    /**
     * Check that reloading replaces the current partition's data with newly retrieved data obtained
     * from the DB adapter.
     *
     * @throws PartitionProcessingException if there's an issue executing the test
     */
    @Test
    public void testReloadMethod() throws PartitionProcessingException {
        // start with a single partition that contains a target time
        setInitialParts(mkPartition("p0", 10000L, 20000L));
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(15000L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(true));
        // Now set up a different set of partitions that doesn't include that target time, and
        // reload, then verify that we now end up creating a new partition creation
        setInitialParts(false, mkPartition("p1", 20000L, 30000L));
        partmgr.reload();
        assertThat(partmgr.ensurePartition(TEST_SCHEMA_NAME, testTable, Instant.ofEpochMilli(15000L),
                PeriodDuration.of(Duration.ofMillis(2000L))), is(false));
    }

    private void chkLastAddedPartBounds(long lower, long upper) {
        Partition<Instant> part = addedPartitions.get(addedPartitions.size() - 1);
        assertThat(part.getInclusiveLower().toEpochMilli(), is(lower));
        assertThat(part.getExclusiveUpper().toEpochMilli(), is(upper));
    }

    private Partition<Instant> mkPartition(String name, long fromMills, long toMillis) {
        return new Partition<>(testSchema.getName(), testTable.getName(), name,
                Instant.ofEpochMilli(fromMills), Instant.ofEpochMilli(toMillis));
    }

    @SafeVarargs
    private final void setInitialParts(Partition<Instant>... parts)
            throws PartitionProcessingException {
        setInitialParts(true, parts);
    }

    @SafeVarargs
    private final void setInitialParts(boolean doLoad, Partition<Instant>... parts)
            throws PartitionProcessingException {
        List<Partition<Instant>> mutablePartList = new ArrayList<>(Arrays.asList(parts));
        Map<String, List<Partition<Instant>>> result = new HashMap<>();
        result.put(TEST_TABLE_NAME, mutablePartList);
        when(adapter.getSchemaPartitions(anyString())).thenReturn(result);
        if (doLoad) {
            assertThat(partmgr.load(TEST_SCHEMA_NAME), is(1));
        }
    }
}
