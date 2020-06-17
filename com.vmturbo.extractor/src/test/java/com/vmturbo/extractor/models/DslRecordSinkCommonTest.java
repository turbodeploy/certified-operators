package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.Column.doubleColumn;
import static com.vmturbo.extractor.models.Column.longColumn;
import static com.vmturbo.extractor.models.Column.stringColumn;
import static com.vmturbo.extractor.util.ExtractorTestUtil.config;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.Matcher;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.extractor.models.DslRecordSink.InsertResults;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.util.RecordTestUtil;

/**
 * Tests for common functionality of all {@link DslRecordSink} and its subclasses.
 *
 * <p>Tests of actual DB write functionality are found in {@link DslRecordSinkWriterTest}.</p>
 */
@RunWith(Parameterized.class)
public class DslRecordSinkCommonTest {

    private final String sinkType;
    private final Class<? extends DslRecordSink> sinkClass;
    private Table table;
    private DSLContext dsl = mock(DSLContext.class);
    private DslRecordSink sink;
    private Column<Long> idColumn;
    private Column<String> descColumn;
    private Column<Double> valueColumn;
    private final List<String> preHookSql = new ArrayList<>();
    private final List<String> postHookSql = new ArrayList<>();
    private List<Record> sinkCapture;

    /**
     * Tests are parameterized by the sink type.
     *
     * @return list of parameter lists, each with a sink type name, and a sink class type
     */
    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> testParameters() {
        return ImmutableList.of(
                new Object[]{"insert", DslRecordSink.class},
                new Object[]{"update", DslUpdateRecordSink.class},
                new Object[]{"upsert", DslUpsertRecordSink.class}
        );
    }

    /**
     * Create a new test class instance, for a given parameter list.
     *
     * @param sinkType  name of sink type, for test labeling
     * @param sinkClass sink implementation class
     */
    public DslRecordSinkCommonTest(String sinkType, Class<? extends DslRecordSink> sinkClass) {
        this.sinkType = sinkType;
        this.sinkClass = sinkClass;
    }

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
        // and we won't need a thread pool, so mock that too
        final ExecutorService pool = mock(ExecutorService.class);
        // we won't spawn a task to perform inserts, so we need a stand-in for the future
        // normally associated with it
        final CompletableFuture<InsertResults> insertResultsFuture =
                new CompletableFuture<>();
        insertResultsFuture.complete(new InsertResults(1L, 1L));
        doReturn(insertResultsFuture).when(pool).submit(any(Callable.class));
        // Instantiate whichever type of sink we're supposed to be testing, and spy on it
        if (sinkClass == DslRecordSink.class) {
            this.sink = spy(new DslRecordSink(dsl, table, config, pool));
        } else if (sinkClass == DslUpdateRecordSink.class) {
            this.sink = spy(new DslUpdateRecordSink(dsl, table, config, pool, "update",
                    null, ImmutableList.of(idColumn), ImmutableList.of(valueColumn)));
        } else if (sinkClass == DslUpsertRecordSink.class) {
            this.sink = spy(new DslUpsertRecordSink(dsl, table, config, pool, "upsert",
                    ImmutableList.of(idColumn), ImmutableList.of(valueColumn)));
        }
        // when the sink accepts a record, we stash it away so we can test what was inserted
        this.sinkCapture = RecordTestUtil.captureSink(sink, true);
    }

    /**
     * Create a mock DSLContext for use in tests.
     *
     * @return the mock
     * @throws SQLException if there's a problem
     */
    private DSLContext getMockDslContext() throws SQLException {
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
        // a (mock) Statement object. And if we we'er given a saveTo list, that mock should
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
     * Test that sinks are properly configured with their table and its columns.
     */
    @Test
    public void testSinkKnowsTableColumns() {
        assertThat(sink.getWriteTableName(), is("t" + (sinkType.equals("insert") ? "" : "_" + sinkType)));
        assertThat(sink.getRecordColumns(), contains(idColumn, descColumn, valueColumn));
    }

    /**
     * Test that sinks accept the records their given and correctly post those records to the
     * database.
     */
    @Test
    public void testSinkAcceptsRecords() {
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
     */
    @Test(expected = IllegalStateException.class)
    public void testCantWriteAfterClose() {
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        sink.accept(createRecord(2L, "yyy", 1.0));
    }

    /**
     * Test that pre-copy hooks are executed as expected.
     *
     * @throws SQLException if there's a problem
     */
    @Test
    public void testThatPreCopyHooksAreCorrect() throws SQLException {
        Matcher<Iterable<? extends String>> matcher;
        switch (sinkType) {
            case "insert":
                matcher = emptyIterable();
                break;
            case "update":
                matcher = contains("CREATE TEMPORARY TABLE t_update (id int8, description varchar, value float8)");
                break;
            case "upsert":
                matcher = contains("CREATE TEMPORARY TABLE t_upsert (LIKE t)");
                break;
            default:
                throw new IllegalArgumentException("Unknown sink type: " + sinkType);
        }
        doAnswer(inv -> {
            inv.getArguments()[0] = getMockConnection(preHookSql);
            return inv.callRealMethod();
        }).when(sink).preCopyHook(any(Connection.class));
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        assertThat(preHookSql, matcher);
    }

    /**
     * Test that post-copy hooks are executed as expected.
     *
     * @throws SQLException if there's an exception.
     */
    @Test
    public void testThatPostCopyHooksAreCorrect() throws SQLException {
        Matcher<Iterable<? extends String>> matcher;
        switch (sinkType) {
            case "insert":
                matcher = emptyIterable();
                break;
            case "update":
                matcher = contains("UPDATE t AS _t SET value = _temp.value FROM t_update AS _temp "
                        + "WHERE _t.id = _temp.id");
                break;
            case "upsert":
                matcher = contains("INSERT INTO t SELECT * FROM t_upsert "
                        + "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value");
                break;
            default:
                throw new IllegalArgumentException("Unknown sink type: " + sinkType);
        }
        doAnswer(inv -> {
            inv.getArguments()[0] = getMockConnection(postHookSql);
            return inv.callRealMethod();
        }).when(sink).postCopyHook(any(Connection.class));
        sink.accept(createRecord(1L, "xxx", 1.0));
        sink.accept(null);
        assertThat(postHookSql, matcher);
    }

    /**
     * Check that the {@link InsertResults} object property reports results.
     */
    @Test
    public void testInsertResults() {
        final InsertResults insertResults = new InsertResults(1L, 2L);
        assertThat(insertResults.getMsec(), is(2L));
        assertThat(insertResults.getRecordCount(), is(1L));
    }

    private Record createRecord(Long id, String desc, Double value) {
        return RecordTestUtil.createRecord(table, ImmutableMap.<Column<?>, Object>builder()
                .put(idColumn, id)
                .put(descColumn, desc)
                .put(valueColumn, value)
                .build());

    }
}
