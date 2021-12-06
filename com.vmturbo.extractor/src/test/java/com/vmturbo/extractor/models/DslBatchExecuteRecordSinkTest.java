package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.Column.doubleColumn;
import static com.vmturbo.extractor.models.Column.longColumn;
import static com.vmturbo.extractor.models.Column.stringColumn;
import static com.vmturbo.extractor.util.ExtractorTestUtil.config;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.extractor.models.DslRecordSink.InsertResults;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.util.RecordTestUtil;

/**
 * Tests for common functionality of all {@link DslBatchExecuteRecordSink} and its subclasses.
 *
 */
public class DslBatchExecuteRecordSinkTest {

    private DSLContext dsl;
    private Table table;
    private DslRecordSink sink;
    private Column<Long> idColumn;
    private Column<String> descColumn;
    private Column<Double> valueColumn;
    private List<Record> sinkCapture;
    private final int batchSize = 3;
    private ExecutorService pool;
    private static final Logger logger = LogManager.getLogger();

    /**
     * Test setup.
     *
     * @throws Exception if there's a problem
     */
    @Before
    public void before() throws Exception {
        // set up a test table with test columns
        this.idColumn = longColumn("id");
        this.descColumn = stringColumn("description");
        this.valueColumn = doubleColumn("value");
        this.table = Table.named("t").withColumns(idColumn, descColumn, valueColumn).build();

        // we don't have a live database, so mock a dsl context
        this.dsl = getMockDslContext();
        pool = spy(Executors.newFixedThreadPool(2));

        // we won't spawn a task to perform inserts, so we need a stand-in for the future
        // normally associated with it
        final CompletableFuture<InsertResults> insertResultsFuture =
                new CompletableFuture<>();
        insertResultsFuture.complete(new InsertResults(1L, 1L));

        this.sink = spy(new DslBatchExecuteRecordSink(dsl, table, config, pool, batchSize));
        // when the sink accepts a record, we stash it away, so we can test what was inserted
        this.sinkCapture = RecordTestUtil.captureSink(sink, true);
    }

    /**
     * Destroy pool.
     */
    @After
    public void tearDown() {
        pool.shutdownNow();
    }

    /**
     * Create a mock DSLContext for use in tests.
     *
     * @return the mock
     * @throws SQLException if there's a problem
     */
    private DSLContext getMockDslContext() throws SQLException {
        dsl = mock(DSLContext.class);
        // we need a DSL on which we can call dsl.configuration().connectionProvider().acquire()
        // and get back a mock connection. So we set up mocks for that whole chain
        final Connection conn = getMockConnection(null);
        ConnectionProvider cp = mock(ConnectionProvider.class);
        doReturn(conn).when(cp).acquire();
        doNothing().when(cp).release(any(Connection.class));
        Configuration conf = mock(Configuration.class);
        doReturn(cp).when(conf).connectionProvider();
        doReturn(conf).when(dsl).configuration();
        return dsl;
    }

    /**
     * Create a mock db connection that captures the SQL statements it executes.
     *
     * @param saveTo array to capture SQL statements to
     * @return mock connection
     * @throws SQLException if there's a problem
     */
    private Connection getMockConnection(@Nullable List<String> saveTo) throws SQLException {
        // our connection object needs to be capable of executing conn.createStatement() yielding
        // a (mock) Statement object. And if we we're given a saveTo list, that mock should
        // capture SQL strings from #execute() methods to that list
        final Statement stmt = mock(Statement.class);
        if (saveTo != null) {
            doAnswer(inv -> {
                saveTo.add(inv.getArgumentAt(0, String.class));
                return null;
            }).when(stmt).execute(anyString());
        }
        final Connection conn = mock(Connection.class);
        doReturn(stmt).when(conn).createStatement();
        return conn;
    }

    /**
     * Test that sinks accept the records their given and correctly post those records to the
     * database.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testSinkAcceptsRecords() throws SQLException, InterruptedException {
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(createRecord(2L, "yyy", 1.0));
        sink.accept(null);
        assertThat(sinkCapture.size(), is(2));
        final Record r1 = sinkCapture.get(0);
        assertThat(r1.get(idColumn), is(1L));
        assertThat(r1.get(descColumn), is("xxx"));
        assertThat(r1.get(valueColumn), is(1.0));
        final Record r2 = sinkCapture.get(1);
        assertThat(r2.get(idColumn), is(2L));
        assertThat(r2.get(descColumn), is("yyy"));
        assertThat(r2.get(valueColumn), is(1.0));
    }

    /**
     * Test that sinks correctly refuse to accept records after accepting a null record.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test(expected = SQLException.class)
    public void testCantWriteAfterClose() throws SQLException, InterruptedException {
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        sink.accept(createRecord(2L, "yyy", 1.0));
    }

    /**
     * Test that pre-copy hooks are executed as expected.
     *
     * @throws SQLException if there's a problem
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testThatPreCopyHooksAreCorrect() throws SQLException, InterruptedException {
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        assertThat(sink.getPreCopyHookSql(mock(Connection.class)), emptyIterable());
    }

    /**
     * Test that post-copy hooks are executed as expected.
     *
     * @throws SQLException if there's an exception.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testThatPostCopyHooksAreCorrect() throws SQLException, InterruptedException {
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        assertThat(sink.getPostCopyHookSql(mock(Connection.class)), emptyIterable());
    }

    /**
     * Ensure that even after batch size accepts, it continues to accept records.
     *
     * @throws SQLException if there's an exception.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testAcceptForBatch() throws SQLException, InterruptedException {
        for (int i = 0; i <= batchSize; i++) {
            sink.accept(createRecord((long)i, String.valueOf('a' + i), 1.0));
        }

        assertThat(sinkCapture.size(), is(batchSize + 1));
    }

    private Record createRecord(Long id, String desc, Double value) {
        return RecordTestUtil.createRecord(table, ImmutableMap.<Column<?>, Object>builder()
                .put(idColumn, id)
                .put(descColumn, desc)
                .put(valueColumn, value)
                .build());
    }
}
