package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.extractor.models.Table.Record;

/**
 * Unit tests for JdbcBatchInsertRecordSink.
 */
public class JdbcBatchInsertRecordSinkTest {
    private static final String TABLE = "qqq";
    private static final Column<String> COL1 = Column.stringColumn("aaa");

    private Connection conn;
    private PreparedStatement stmt;

    /**
     * Set up the tests.
     *
     * @throws SQLException should not happen
     */
    @Before
    public void before() throws SQLException {
        conn = Mockito.mock(Connection.class);
        stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(stmt);
    }

    /**
     * Test that writing is done in batches.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testBatchSeparation() throws InterruptedException, SQLException {
        final AtomicLong batches = new AtomicLong(0L);
        Mockito.doAnswer(invocation -> {
            batches.incrementAndGet();
            return new int[] {};
        }).when(stmt).executeBatch();
        int batchSize = 10;
        int rows = 15;

        JdbcBatchInsertRecordSink sink = new JdbcBatchInsertRecordSink(null, batchSize, conn);
        Table table = createTable(COL1);
        for (int i = 0; i < rows; ++i) {
            Record record = new Record(table);
            sink.accept(record);
        }
        sink.accept(null);

        Assert.assertEquals(2L, batches.get());
    }

    /**
     * Test that pre/post execution hooks are not invoked when no rows are passed.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testNoHooksWhenNoRows() throws InterruptedException, SQLException {
        JdbcBatchInsertRecordSinkWithHooks sink =
                        new JdbcBatchInsertRecordSinkWithHooks(null, 10, conn);
        sink.accept(null);
        Assert.assertFalse(sink.isInvoked());
    }

    /**
     * Test that insert statement is built correctly.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testInsertStmt() throws InterruptedException, SQLException {
        Mockito.when(conn.prepareStatement(Mockito.anyString()))
            .thenAnswer(new Answer<PreparedStatement>() {
                @Override
                public PreparedStatement answer(InvocationOnMock invocation) {
                    String insert = invocation.getArgumentAt(0, String.class);
                    Assert.assertTrue(insert.contains("INSERT INTO " + TABLE));
                    Assert.assertTrue(insert.contains(") VALUES ("));
                    Assert.assertEquals(1L, insert.chars().filter(ch -> ch == '?').count());
                    return stmt;
                }
            });
        JdbcBatchInsertRecordSink sink = new JdbcBatchInsertRecordSink(null, 10, conn);
        sink.accept(new Record(createTable(COL1)));
        sink.accept(null);
    }

    /**
     * Test the value types implicit conversion for columns of the row.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testValuesConversion() throws InterruptedException, SQLException {
        String col2Name = "bbb";
        String col3Name = "ccc";
        Column<?> col2 = Column.doubleColumn(col2Name);
        Column<?> col3 = Column.doubleColumn(col3Name);
        Table table = createTable(COL1, col2, col3);
        Record rec1 = new Record(table);
        final String val1 = "qqq";
        final long val3 = 10L;
        rec1.set(COL1, val1);
        rec1.set(col2Name, "fff");
        rec1.set(col3Name, val3);
        // passed string
        Mockito.doAnswer(invocation -> {
            Assert.assertEquals(1, (int)invocation.getArgumentAt(0, Integer.class));
            Assert.assertEquals(val1, invocation.getArgumentAt(1, String.class));
            return null;
        }).when(stmt).setString(Mockito.anyInt(), Mockito.anyString());
        // type mismatch - should set null
        Mockito.doAnswer(invocation -> {
            Assert.assertEquals(2, (int)invocation.getArgumentAt(0, Integer.class));
            return null;
        }).when(stmt).setNull(Mockito.anyInt(), Mockito.anyInt());
        // convert long to double
        Mockito.doAnswer(invocation -> {
            Assert.assertEquals(3, (int)invocation.getArgumentAt(0, Integer.class));
            Assert.assertEquals((double)val3, invocation.getArgumentAt(1, Double.class), 0.001);
            return null;
        }).when(stmt).setDouble(Mockito.anyInt(), Mockito.anyDouble());

        JdbcBatchInsertRecordSink sink = new JdbcBatchInsertRecordSink(null, 10, conn);
        sink.accept(rec1);
        sink.accept(null);
    }

    private static Table createTable(Column<?>... cols) {
        return Table.named(TABLE).withColumns(cols).build();
    }

    /**
     * Sink with pre/post hooks defined, for testing.
     */
    private static class JdbcBatchInsertRecordSinkWithHooks extends JdbcBatchInsertRecordSink {
        private boolean invoked;

        /**
         * Construct an instance.
         *
         * @param dsl jooq context
         * @param batchSize batch size
         * @param conn jdbc connection
         */
        JdbcBatchInsertRecordSinkWithHooks(DSLContext dsl, int batchSize, Connection conn) {
            super(dsl, batchSize, conn);
        }

        @Override
        protected List<String> getPreCopyHookSql(final Connection transConn) throws SQLException {
            invoked = true;
            return Collections.emptyList();
        }

        @Override
        protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
            invoked = true;
            return Collections.emptyList();
        }

        boolean isInvoked() {
            return invoked;
        }
    }
}
