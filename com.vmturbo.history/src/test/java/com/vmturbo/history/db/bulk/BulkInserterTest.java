package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.schema.abstraction.Tables.ENTITIES;
import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static com.vmturbo.history.schema.abstraction.tables.Notifications.NOTIFICATIONS;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Objects;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory.RollupKeyTransfomer;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.NotificationsRecord;
import com.vmturbo.history.stats.DbTestConfig;

/**
 * Tests of BulkInserter and related classes.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
public class BulkInserterTest extends Assert {

    @Autowired
    private DbTestConfig dbTestConfig;

    private static String testDbName;
    private static HistorydbIO historydbIO;
    private static ExecutorService threadPool;
    private SimpleBulkLoaderFactory loaders;

    private static BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
            .batchSize(2)
            .maxPendingBatches(2)
            .maxBatchRetries(3)
            .maxRetryBackoffMsec(1000)
            .build();

    /**
     * Set up a thread pool for bulk inserters.
     */
    @BeforeClass
    public static void beforeClass() {
        threadPool = Executors.newFixedThreadPool(2);
    }

    /**
     * Set up temporary database for use in tests.
     *
     * <p>We only do this for the first test that runs, even though it's in the @Before method.
     * Tables that are actually changed by any given tests are cleared out after that test, which
     * is much lest costly than tearing the database down and recreating it.</p>
     *
     * @throws VmtDbException if a database operation fails
     */
    @Before
    public void before() throws VmtDbException {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        historydbIO.init(false, null, testDbName, Optional.empty());
        // entities table starts out with a couple of records during migration that make
        // the tests a little clumsier, so we just get rid of them during setup.
        try (Connection conn = historydbIO.connection()) {
            historydbIO.using(conn).deleteFrom(ENTITIES).execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loaders = new SimpleBulkLoaderFactory(historydbIO, config, threadPool);
    }

    /**
     * Delete all records from tables into which records were deleted during this test.
     *
     * <p>Since this class is testing the bulk inserter, the {@link BulkInserterFactoryStats}
     * available after a given test has completed provides a list of every table that received
     * reords during the test, so we just clear out those tables.</p>
     *
     * @throws VmtDbException if a database operation fails
     * @throws SQLException   other database operation failures
     */
    @After
    public void after() throws VmtDbException, SQLException {
        try {
            loaders.close();
        } catch (InterruptedException e) {
        }
        final BulkInserterFactoryStats stats = loaders.getStats();
        // stats comes back null in some mocked tests
        if (stats != null) {
            try (Connection conn = historydbIO.connection()) {
                for (Table table : stats.getOutTables()) {
                    historydbIO.using(conn).deleteFrom(table).execute();
                }
            }
        }
    }

    /**
     * Shut down the threadpool and destroy the test database when finished.
     */
    @AfterClass
    public static void afterClass() {
        threadPool.shutdownNow();
        try (Connection conn = historydbIO.connection()) {
            historydbIO.using(conn).execute("DROP DATABASE " + testDbName);
        } catch (VmtDbException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test to make sure that when multiple requests are made to obtain bulk loaders for the same
     * table, the same bulk loader instance is used (except when different "key" values are
     * provided; see {@link #testDiffInstanceForDifferentKeySameTable()}.
     */
    @Test
    public void testSameInstanceForSameTable() {
        assertTrue(loaders.getLoader(NOTIFICATIONS) == loaders.getLoader(NOTIFICATIONS));
    }

    /**
     * Test that when multiple requests are made for BulkInserter with different key values,
     * different instances are provided.
     *
     * <p>This requires the more detailed {@link BulkInserterFactory} api, rather than
     * the simple {@link SimpleBulkLoaderFactory} api.</p>
     */
    @Test
    public void testDiffInstanceForDifferentKeySameTable() {
        BulkInserter<NotificationsRecord, NotificationsRecord> keyedLoader
                = loaders.getFactory().getInserter(
                "KEY", NOTIFICATIONS, NOTIFICATIONS, RecordTransformer.identity(),
                DbInserters.batchStoreInserter(NOTIFICATIONS, historydbIO),
                Optional.empty());
        assertTrue(loaders.getLoader(NOTIFICATIONS) != keyedLoader);
    }

    /**
     * Test configuration of {@link SimpleBulkLoaderFactory} created bulk loaders.
     *
     * <p>These instances are configured with specific {@link RecordTransformer} and
     * {@link DbInserter} objects, depending on the target table.</p>
     */
    @Test
    public void testInserterAndTransfomerConfigurations() {
        final BulkInserterFactory factory = mock(BulkInserterFactory.class);
        loaders = new SimpleBulkLoaderFactory(historydbIO, config, threadPool, factory);
        final ArgumentCaptor<DbInserter> inserterCaptor = ArgumentCaptor.forClass(DbInserter.class);
        final ArgumentCaptor<RecordTransformer> transformerCaptor
                = ArgumentCaptor.forClass(RecordTransformer.class);

        loaders.getLoader(ENTITIES);
        verify(factory).getInserter(any(Entities.class), any(Entities.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.simpleUpserter(ENTITIES, historydbIO).getClass()));
        assertTrue(transformerCaptor.getValue() == RecordTransformer.IDENTITY);
        reset(factory);

        loaders.getLoader(VM_STATS_LATEST);
        verify(factory).getInserter(any(VmStatsLatest.class), any(VmStatsLatest.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.valuesInserter(VM_STATS_LATEST, historydbIO).getClass()));
        assertTrue(transformerCaptor.getValue() instanceof RollupKeyTransfomer);
        reset(factory);

        loaders.getLoader(SYSTEM_LOAD);
        verify(factory).getInserter(any(SystemLoad.class), any(SystemLoad.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.valuesInserter(SYSTEM_LOAD, historydbIO).getClass()));
        assertTrue(transformerCaptor.getValue() == RecordTransformer.IDENTITY);
        reset(factory);
    }

    /**
     * Test that when records that do not have any inherent conflicts are successfully loaded
     * into the target table.
     *
     * <p>We insert a small number of records to a single table with different values for a
     * unique key, in in small batches.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       if database operations fail
     */
    @Test
    public void testSuccessfulFullBatches() throws InterruptedException, VmtDbException {
        checkRecordLoad(
                NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(1, 3 * config.batchSize()));
    }

    /**
     * Test that a partial final batch is successfully written to the database.
     *
     * <p>This is identical to the {@link #testSuccessfulFullBatches()} except that the final batch
     * is partially filled.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       if a database operation fails
     */
    @Test
    public void testSuccessfulWithPartialBatch() throws InterruptedException, VmtDbException {
        checkRecordLoad(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(1, 1 + 3 * config.batchSize()));
    }

    /**
     * Test that when some batches fail, the records from other batches successfully make it into
     * the target table.
     *
     * <p>We arrange for specific batches to fail by filling them with records that all contain the
     * same value for a unique key column, while other batches get unique values.</p>
     *
     * @throws InterruptedException if interuppted
     * @throws VmtDbException       if database operations fail
     */
    @Test
    public void testWithFailedBatches() throws InterruptedException, VmtDbException {
        checkRecordLoad(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(1, 1 + 3 * config.batchSize()),
                2, 4);
    }

    /**
     * Test that when multiple inserters are operating with the same bulk loader, all records
     * loaded by all threads make it into the target table.
     *
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if a database operation fails
     * @throws VmtDbException       other db failures
     */
    @Test
    public void testParallelLoaders() throws InterruptedException, ExecutionException, VmtDbException {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<Void>> futures = new ArrayList<>();
        for (long i = 0; i < 4000; i += 1000) {
            final long finalI = i;
            futures.add(pool.submit(() -> {
                performInserts(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(finalI, 1000));
                return null;
            }));
        }
        for (Future<Void> future : futures) {
            future.get();
        }
        loaders.close();
        verifyInserts(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(4000));
    }

    List<EntitiesRecord> entitiesSet1 = ImmutableList.of(
            new EntitiesRecord(100L, "e1", "entity1", "e1uuid", null, null),
            new EntitiesRecord(200L, "e2", "entity2", "e2uuid", null, null),
            new EntitiesRecord(300L, "e3", "entity3", "e3uuid", null, null)
    );

    List<EntitiesRecord> entitiesSet2 = ImmutableList.of(
            new EntitiesRecord(400L, "e4", "entity4", "e4uuid", null, null),
            new EntitiesRecord(500L, "e5", "entity5", "e5uuid", null, null),
            new EntitiesRecord(600L, "e6", "entity6", "e6uuid", null, null)
    );

    /**
     * Test that the {@link DbInserter#simpleUpserter(Table, BasedbIO)} inserter works properly.
     *
     * <p>This inserter uses an "upsert" statement - i.e. an INSERT... ON DUPLICATE UPDATE
     * statement in order to either insert or update the supplied records, depending on whether
     * the records' keys already exist in the database.</p>
     *
     * <p>We test this by first inserting a small collection of records, to establish as set
     * of existing records. We then retrieve those records from the database, modify them, and
     * intermingle them with another set of new records. The intermingled records are loaded into
     * the database, and we look for the changes we made to the intiial batch.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       if database operations fail
     */
    @Test
    public void testUpserter() throws InterruptedException, VmtDbException {
        final BulkLoader<EntitiesRecord> entitiesLoader = loaders.getLoader(ENTITIES);
        // send in initial batch of records
        entitiesLoader.insertAll(entitiesSet1);
        loaders.flushAll();
        verifyInserts(ENTITIES, ENTITIES.ID,
                entitiesSet1.stream().map(EntitiesRecord::getId).collect(Collectors.toList()));
        // load these records and make modifications
        final List<EntitiesRecord> mixedRecords = getRecords(ENTITIES);
        mixedRecords.forEach(r -> r.set(ENTITIES.DISPLAY_NAME,
                "xxx" + r.get(ENTITIES.DISPLAY_NAME)));
        // add in some new records and shuffle them a bit, in a predictable fashion
        mixedRecords.addAll(entitiesSet2);
        mixedRecords.sort(Comparator.comparing(r -> r.hashCode()));
        entitiesLoader.insertAll(mixedRecords);
        loaders.close();
        // load all records and ensure everything is as expected
        final List<EntitiesRecord> finalRecords = getRecords(ENTITIES);
        Set<Long> expectedIds = new HashSet<>();
        entitiesSet1.stream().map(EntitiesRecord::getId).forEach(expectedIds::add);
        entitiesSet2.stream().map(EntitiesRecord::getId).forEach(expectedIds::add);
        Set<Long> actualIds = ImmutableSet.copyOf(
                finalRecords.stream().map(EntitiesRecord::getId).collect(Collectors.toList()));
        // all records are present, as identified their id columns
        Assert.assertEquals(expectedIds, actualIds);
        final Map<Long, EntitiesRecord> map
                = finalRecords.stream()
                .collect(Collectors.toMap(EntitiesRecord::getId, Functions.identity()));
        // get ids of records in initial batch
        final Set<Long> initialBatchIds = entitiesSet1.stream()
                .map(EntitiesRecord::getId)
                .collect(Collectors.toSet());
        // make ser the initial records contain the display name modification, and others
        // do not.
        Assert.assertTrue(mixedRecords.stream()
                .allMatch(r -> initialBatchIds.contains(r.getId())
                        ? r.getDisplayName().startsWith("xxx")
                        : !r.getDisplayName().startsWith("xxx")));
    }

    /**
     * Make sure the batch inserter works properly. This makes use of a prepared statement with
     * placeholders and a batches of bound values.
     *
     * <p>This is not currently used by default for any of the tables known to
     * {@link SimpleBulkLoaderFactory}, so the {@link BulkInserterFactory} API is used instead</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       if a database operation fails.
     */
    @Test
    public void testBachInserter() throws InterruptedException, VmtDbException {
        final BulkInserter<NotificationsRecord, NotificationsRecord> loader =
                loaders.getFactory().getInserter(NOTIFICATIONS, NOTIFICATIONS,
                        RecordTransformer.identity(),
                        DbInserters.batchInserter(Tables.NOTIFICATIONS, historydbIO));
        performInserts(loader, NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(10));
        loaders.close();
        verifyInserts(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(10));
    }

    /**
     * Method to load a collection of records into a table, read them back, and check to make
     * sure the load succeeded.
     *
     * @param table          table to receive records
     * @param field          field that will be poopulated by provided values
     * @param values         values be loaded into specified column
     * @param failingBatches list of batches that should be caused to fail
     * @param <R>            underlying record type
     * @param <T>            underlying (Java) field type
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       if db operation fails
     */
    private <R extends Record, T> void checkRecordLoad(
            Table<R> table, TableField<R, T> field, List<T> values, Integer... failingBatches)
            throws InterruptedException, VmtDbException {

        performInserts(table, field, values, failingBatches);
        loaders.close();
        verifyInserts(table, field, values, failingBatches);
    }

    /**
     * Utility to verify that records loaded into a table are as expected.
     *
     * @param table          table that received records
     * @param field          table field with varying values supplied by the caller
     * @param values         that set of values
     * @param failingBatches batch number of batches that should have failed
     * @param <R>            underlying record type
     * @param <T>            underlying (java) field value type
     * @throws VmtDbException if a database operation fails
     */
    private <R extends Record, T> void verifyInserts(final Table<R> table,
            final TableField<R, T> field,
            final List<T> values,
            final Integer... failingBatches) throws VmtDbException {
        // verify that the records have the correct field values
        verifyValues(table, field, values, failingBatches);
        // then check some of the basic stats values obtained from the loader
        final BulkInserterStats stats = loaders.getStats().getByKey(table);
        final long batchCount = (long)Math.ceil((values.size() + 0.0) / config.batchSize());
        assertEquals(batchCount - failingBatches.length, stats.getBatches());
        assertEquals(failingBatches.length, stats.getFailedBatches());
        Set<Integer> failingBatchSet = toSet(failingBatches);
        final Integer written = getRecordCount(table);
        assertEquals((long)written, stats.getWritten());
    }

    /**
     * Utility to load a collection of records into a table.
     *
     * @param table          the table that will receive the records
     * @param field          a field whose values are supplied by the caller
     * @param values         those values
     * @param failingBatches batch numbers of batches that that should be rigged to fail
     * @param <R>            underlying record type
     * @param <T>            underlying java type of designated field
     * @throws InterruptedException if interrupted
     */
    private <R extends Record, T> void performInserts(final Table<R> table,
            final TableField<R, T> field,
            final List<T> values,
            final Integer... failingBatches)
            throws InterruptedException {
        // obtain a loader and use it to perform the inserts
        final BulkLoader<R> loader = loaders.getLoader(table);
        performInserts(loader, table, field, values, failingBatches);
    }

    /**
     * Utility to insert a collection of records into a database tables using a bulk loader.
     *
     * @param loader         the loader
     * @param table          the underying table
     * @param field          a designated field whose values are provided by the caller
     * @param values         those values
     * @param failingBatches the batch numbers of batches that should be rigged to fail
     * @param <R>            the underlying record type
     * @param <T>            the java type of the designated field
     * @throws InterruptedException if interrupted
     */
    private <R extends Record, T> void performInserts(final BulkLoader<R> loader,
            final Table<R> table,
            final TableField<R, T> field,
            final List<T> values,
            final Integer... failingBatches)
            throws InterruptedException {
        final Set<Integer> fails = Stream.of(failingBatches).collect(Collectors.toSet());
        int batchNo = 0;
        for (int i = 0; i < values.size(); i++) {
            // keep track of which batch each record will fall into
            if (i % config.batchSize() == 0) {
                batchNo += 1;
            }
            R record = table.newRecord();
            final boolean fail = fails.contains(batchNo);
            // duplicate the first record's batch no to cause a duplicate key error if this batch
            // should fail, otherwise use the supplied values as-is.
            record.set(field, fail ? values.get(0) : values.get(i));
            loader.insert(record);
        }
    }

    /**
     * Create a list of consective long values starting with 0L.
     *
     * @param n exclusive max value in list
     * @return the list of values, in ascending order
     */
    private List<Long> longIdRange(long n) {
        return longIdRange(0, n);
    }

    /**
     * Create a list of consecutive long values.
     *
     * @param startInclusive first value in list
     * @param length         length of this
     * @return the generated list
     */
    private List<Long> longIdRange(long startInclusive, long length) {
        return LongStream.range(startInclusive, startInclusive + length).boxed()
                .collect(Collectors.toList());
    }


    /**
     * Check that the values currently in a given table are as requested for the current test.
     *
     * @param table          the table containing the records
     * @param field          a designated field whose values will be checked
     * @param values         values that should appear in the table
     * @param failingBatches batch numbers of batches that should have failed (so their values
     *                       should not be found in the table)
     * @param <R>            underlying record type
     * @param <T>            type of designated field
     * @throws VmtDbException if a database operation fails
     */
    private <R extends Record, T> void verifyValues(Table<R> table,
            TableField<R, T> field, List<T> values,
            Integer... failingBatches) throws VmtDbException {
        Set<Integer> fails = Stream.of(failingBatches).collect(Collectors.toSet());
        // construct list of all the values we expect to find, including only the values supplied
        // for non-failing batches
        List<T> expected = new ArrayList<>();
        int batchNo = 0;
        for (int i = 0; i < values.size(); i++) {
            if (i % config.batchSize() == 0) {
                batchNo += 1;
            }
            if (!fails.contains(batchNo)) {
                expected.add(values.get(i));
            }
        }
        // make sure that the expected values are identical to what we find when we retrieve all
        // the records from the database table.
        assertEquals(new HashSet<>(expected),
                getRecords(table).stream().map(r -> ((R)r).getValue(field)).collect(Collectors.toSet()));
    }

    /**
     * Get the number of records in a given table.
     *
     * @param table the table
     * @return the record count
     * @throws VmtDbException if the db operation fails
     */
    private int getRecordCount(Table<?> table) throws VmtDbException {
        return (int)historydbIO.execute(DSL.select(DSL.count()).from(table)).getValue(0, 0);
    }

    /**
     * Retrieve all records from a given database table.
     *
     * @param table the table containing the records
     * @param <R>   underlying record type
     * @return list of retrieved records
     * @throws VmtDbException if a DB operation fails
     */
    private <R extends Record> List<R> getRecords(Table<R> table) throws VmtDbException {
        return (List<R>)historydbIO.execute(DSL.selectFrom(table)).into(table.newRecord().getClass());
    }

    /**
     * Create a set out of the given values.
     *
     * @param members values to appear in the set
     * @param <T>     underlying element type
     * @return the constructed set
     */
    private <T> Set<T> toSet(T... members) {
        final ImmutableSet.Builder builder = new ImmutableSet.Builder<>();
        for (final T member : members) {
            builder.add(member);
        }
        return builder.build();
    }
}
