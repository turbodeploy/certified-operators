package com.vmturbo.extractor.models;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;

/**
 * Tests of Table class and its inner {@link Record} class.
 */
public class TableTest {

    private static final Column<Integer> C1 = new Column<>("c1", ColType.INT);
    private static final Column<Long[]> C2 = new Column<>("c2", ColType.LONG_SET);
    private static final Column<String> C3 = new Column<>("c3", ColType.STRING);
    private static final Table table = Table.named("foo").withColumns(C1, C2, C3).build();

    /**
     * Test that the model builder retains column order.
     */
    @Test
    public void testColumnsRetainOrderInTable() {
        assertThat(table.getColumns(), contains(C1, C2, C3));
    }

    /**
     * Test that model builder captures model name.
     */
    @Test
    public void testTableNameWorks() {
        assertThat(table.getName(), is("foo"));
    }

    /**
     * Test that columns are accessible by column name.
     */
    @Test
    public void testThatTableAccessByNameWorks() {
        assertThat(table.getColumn("c1"), is(C1));
        assertThat(table.getColumn("c2"), is(C2));
    }

    /**
     * Test that when a record sink is attached to a table, it correctly forwards records to the
     * sink as expected.
     */
    @Test
    public void testThatTableForwardsRecordsToSink() {
        TestRecordSink sink = new TestRecordSink();
        // create a record for this table through the record constructor,
        // since the table doesn't yet have a record sink
        Record r0 = new Record(table);
        r0.set(C1, 0);
        try (TableWriter writer = table.open(sink, "test writer", LogManager.getLogger())) {
            Record r1 = writer.open();
            // manually open and close record
            r1.set(C1, 1);
            r1.close();
            // open and auto-close
            try (Record r2 = writer.open()) {
                r2.set(C1, 2);
            }
            // open, then re-open as partial and auto-close
            Record r3 = writer.open();
            assertThat(sink.size(), is(2));
            try (Record reopenedR3 = writer.open(r3)) {
                reopenedR3.set(C1, 3);
            }
            // send that record we created before attaching a sink
            try (Record reopenedR0 = writer.open(r0)) {
            }
            assertThat(sink.size(), is(4));
            assertThat(sink.stream().map(r -> r.get(C1)).collect(Collectors.toList()),
                    containsInAnyOrder(0, 1, 2, 3));
            assertFalse(sink.isClosed());
            writer.close();
            assertTrue(writer.isClosed());
            assertTrue(sink.isClosed());
        }
    }

    /**
     * Test that the various methods used to set record values work as expected.
     */
    @Test
    public void testVariousSetters() {
        Record r = new Record(table);
        r.set(C1, 1);
        assertNotNull(r.get(C1));
        r.setIf(false, C3, () -> "hello");
        assertNull(r.get(C3));
        r.setIf(true, C3, () -> "so long");
        assertThat(r.get(C3), is("so long"));
        r.mergeIf(false, C1, () -> 2, (a, b) -> (int)a + (int)b);
        r.mergeIf(true, C1, () -> 3, (a, b) -> (int)a + (int)b);
        assertThat(r.get(C1), is(4));
        r.merge(C3, "!", (a, b) -> (a + (String)b));
        assertThat(r.get(C3), is("so long!"));
        r.set(C2, new Long[]{1L, 2L});
        assertThat(Arrays.asList(r.get(C2)), contains(1L, 2L));
    }

    /**
     * Test that the record correctly joins CSV column values into a CSV row.
     */
    @Test
    public void testRowCSV() {
        Record r = new Record(table);
        r.set(C1, 1);
        r.set(C2, new Long[]{1L, 2L, 3L});
        r.set(C3, "Hello There!");
        assertThat(r.toCSVRow(ImmutableList.of(C1, C2, C3)), is("1,\"{1,2,3}\",\"Hello There!\""));
    }

    /**
     * Test that record hashing works properly.
     */
    @Test
    public void testXxHash() {
        // create a handful of records that have different field values, and test that they
        // have different hashes.
        Record r = new Record(table);
        r.set(C1, 1);
        r.set(C2, new Long[]{1L, 2L, 3L});
        r.set(C3, "Hello There!");
        long hash1 = r.getXxHash(ImmutableSet.of(C1, C2, C3));
        r.set(C1, 3);
        long hash2 = r.getXxHash(ImmutableSet.of(C1, C2, C3));
        assertThat(hash1, not(hash2));
        r.set(C2, new Long[]{10L, 20L, 30L});
        long hash3 = r.getXxHash(ImmutableSet.of(C1, C2, C3));
        assertThat(hash1, not(hash3));
        assertThat(hash2, not(hash3));
        r.set(C3, "uh huh");
        long hash4 = r.getXxHash(ImmutableSet.of(C1, C2, C3));
        assertThat(hash1, not(hash4));
        assertThat(hash2, not(hash4));
        assertThat(hash3, not(hash4));
        // and one that's got the same values as our first, which should hav the same hash
        r.set(C1, 1);
        r.set(C2, new Long[]{1L, 2L, 3L});
        r.set(C3, "Hello There!");
        long hash5 = r.getXxHash(ImmutableSet.of(C1, C2, C3));
        assertThat(hash1, is(hash5));
    }

    /**
     * A record sink to use for testing accept/close behavior.
     */
    private static class TestRecordSink extends ArrayList<Record> implements Consumer<Record> {
        private boolean closed = false;

        @Override

        public void accept(final Record record) {
            if (record == null) {
                this.closed = true;
            } else if (closed) {
                throw new IllegalStateException();
            } else {
                add(record);
            }
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
