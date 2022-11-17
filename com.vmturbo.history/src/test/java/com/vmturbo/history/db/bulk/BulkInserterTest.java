package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.schema.abstraction.Tables.ENTITIES;
import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static com.vmturbo.history.schema.abstraction.tables.Notifications.NOTIFICATIONS;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.assertj.core.util.Objects;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;

import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory.RollupKeyTransfomer;
import com.vmturbo.history.listeners.PartmanHelper;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.NotificationsRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests of BulkInserter and related classes.
 */
@RunWith(Parameterized.class)
public class BulkInserterTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public BulkInserterTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        this.dsl = super.getDslContext();
    }

    private static BulkInserterConfig config;
    private SimpleBulkLoaderFactory loaders;

    private SimpleBulkLoaderFactory mkLoaders(int batchSize, int poolSize, int fetchTimeout) {
        this.config = ImmutableBulkInserterConfig.builder()
                .batchSize(batchSize)
                .maxPendingBatches(2)
                .maxBatchRetries(3)
                .maxRetryBackoffMsec(1000)
                .flushTimeoutSecs(fetchTimeout)
                .build();
        return new SimpleBulkLoaderFactory(dsl, config, mock(PartmanHelper.class),
                () -> Executors.newFixedThreadPool(poolSize));
    }

    /**
     * Create a factory for record laoders, using default config.
     *
     * <p>Any test that requires different configuration should close this factory and
     * create another.</p>
     */
    @Before
    public void before() {
        this.loaders = mkLoaders(2, 2, 10);
    }

    /**
     * Delete all records from tables into which records were deleted during this test.
     *
     * <p>Since this class is testing the bulk inserter, the {@link BulkInserterFactoryStats}
     * available after a given test has completed provides a list of every table that received
     * reords during the test, so we just clear out those tables.</p>
     *
     * @throws DataAccessException if a database operation fails
     */
    @After
    public void after() throws DataAccessException {
        try {
            loaders.close();
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Test to make sure that when multiple requests are made to obtain bulk loaders for the same
     * table, the same bulk loader instance is used (except when different "key" values are
     * provided; see {@link #testDiffInstanceForDifferentKeySameTable()}.
     */
    @Test
    public void testSameInstanceForSameTable() {
        assertSame(loaders.getLoader(NOTIFICATIONS), loaders.getLoader(NOTIFICATIONS));
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
                DbInserters.batchStoreInserter(),
                Optional.empty());
        assertNotSame(loaders.getLoader(NOTIFICATIONS), keyedLoader);
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
        loaders = new SimpleBulkLoaderFactory(dsl, factory, mock(PartmanHelper.class));
        final ArgumentCaptor<DbInserter> inserterCaptor = ArgumentCaptor.forClass(DbInserter.class);
        final ArgumentCaptor<RecordTransformer> transformerCaptor
                = ArgumentCaptor.forClass(RecordTransformer.class);

        loaders.getLoader(ENTITIES);
        verify(factory).getInserter(any(Entities.class), any(Entities.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.simpleUpserter().getClass()));
        assertSame(transformerCaptor.getValue(), RecordTransformer.IDENTITY);
        reset(factory);

        doReturn(mock(BulkInserter.class)).when(factory).getInserter(any(), any(), any(), any());
        loaders.getLoader(VM_STATS_LATEST);
        verify(factory).getInserter(any(VmStatsLatest.class), any(VmStatsLatest.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.valuesInserter().getClass()));
        assertTrue(transformerCaptor.getValue() instanceof RollupKeyTransfomer);
        reset(factory);

        loaders.getLoader(SYSTEM_LOAD);
        verify(factory).getInserter(any(SystemLoad.class), any(SystemLoad.class),
                transformerCaptor.capture(), inserterCaptor.capture());
        assertNotNull(Objects.castIfBelongsToType(inserterCaptor.getValue(),
                DbInserters.valuesInserter().getClass()));
        assertSame(transformerCaptor.getValue(), RecordTransformer.IDENTITY);
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
     * @throws DataAccessException       if database operations fail
     */
    @Test
    public void testSuccessfulFullBatches() throws InterruptedException, DataAccessException {
        checkRecordLoad(
                NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(1, 3L * config.batchSize()));
    }

    /**
     * Test that a partial final batch is successfully written to the database.
     *
     * <p>This is identical to the {@link #testSuccessfulFullBatches()} except that the final batch
     * is partially filled.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws DataAccessException       if a database operation fails
     */
    @Test
    public void testSuccessfulWithPartialBatch() throws InterruptedException, DataAccessException {
        checkRecordLoad(NOTIFICATIONS, NOTIFICATIONS.ID,
                longIdRange(1, 1 + 3L * config.batchSize()));
    }

    /**
     * Test that when some batches fail, the records from other batches successfully make it into
     * the target table.
     *
     * <p>We arrange for specific batches to fail by sending NULL for a non-null field.</p>
     *
     * @throws InterruptedException if interuppted
     * @throws DataAccessException       if database operations fail
     */
    @Test
    public void testWithFailedBatches() throws InterruptedException, DataAccessException {
        checkRecordLoad(NOTIFICATIONS, NOTIFICATIONS.ID,
                longIdRange(1, 1 + 3L * config.batchSize()),
                2, 4);
    }

    /**
     * Test that when multiple inserters are operating with the same bulk loader, all records
     * loaded by all threads make it into the target table.
     *
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if a database operation fails
     * @throws DataAccessException       other db failures
     */
    @Test
    public void testParallelLoaders() throws InterruptedException, ExecutionException, DataAccessException {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        loaders.close();
        this.loaders = mkLoaders(100, 2, 10);
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
        verifyInserts(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(4000), 0);
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
     * Test that the {@link DbInserter#simpleUpserter()} inserter works properly.
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
     * @throws DataAccessException       if database operations fail
     */
    @Test
    public void testUpserter() throws InterruptedException, DataAccessException {
        dsl.deleteFrom(ENTITIES).execute();
        final BulkLoader<EntitiesRecord> entitiesLoader = loaders.getLoader(ENTITIES);
        // send in initial batch of records
        entitiesLoader.insertAll(entitiesSet1);
        loaders.flushAll();
        verifyInserts(ENTITIES, ENTITIES.ID,
                entitiesSet1.stream().map(EntitiesRecord::getId).collect(Collectors.toList()), 0);
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
        assertEquals(expectedIds, actualIds);
        // get ids of records in initial batch
        final Set<Long> initialBatchIds = entitiesSet1.stream()
                .map(EntitiesRecord::getId)
                .collect(Collectors.toSet());
        // make sure the initial records contain the display name modification, and others
        // do not.
        assertTrue(mixedRecords.stream()
                .allMatch(r -> initialBatchIds.contains(r.getId())
                        == r.getDisplayName().startsWith("xxx")));
    }

    /**
     * Make sure the batch inserter works properly. This makes use of a prepared statement with
     * placeholders and a batches of bound values.
     *
     * <p>This is not currently used by default for any of the tables known to
     * {@link SimpleBulkLoaderFactory}, so the {@link BulkInserterFactory} API is used instead</p>
     *
     * @throws InterruptedException if interrupted
     * @throws DataAccessException       if a database operation fails.
     */
    @Test
    public void testBachInserter() throws InterruptedException, DataAccessException {
        final BulkInserter<NotificationsRecord, NotificationsRecord> loader =
                loaders.getFactory().getInserter(NOTIFICATIONS, NOTIFICATIONS,
                        RecordTransformer.identity(),
                        DbInserters.batchInserter());
        performInserts(loader, NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(10));
        loaders.close();
        verifyInserts(NOTIFICATIONS, NOTIFICATIONS.ID, longIdRange(10), 0);
    }

    /**
     * Make sure that the timeout mechanism built into factory close works as intended. We test once
     * with normal test settings, and once with very tight timeouts and an artificially delayed
     * inserter. The timeout should kick occur in the second scenario only. We test it by checking
     * whether the {@link BulkInserterStats} object delivered from the inserter is marked as
     * "partial" - which should only happen in the timeout-triggering scenario.
     *
     * @throws InterruptedException if we're interrupted
     */
    @Test
    public void testCloseTimeout() throws InterruptedException {
        // use default laoder to show timeout is not fired (as evidenced by the stats object not
        // being marked "partial"
        BulkInserter<EntitiesRecord, EntitiesRecord> loader =
                loaders.getFactory().getInserter(ENTITIES, ENTITIES, RecordTransformer.IDENTITY,
                        DbInserters.valuesInserter());
        loader.insertAll(entitiesSet1);
        loader.close(null);
        assertFalse(loader.getStats().mayBePartial());
        // now use a factory that has a short flush/drain timeout. It will be paid for both loader
        // flush and completer drain, and will run for all DB scenarios, so the 10-second default
        // can really add up
        loaders.close();
        loaders = mkLoaders(2, 2, 1);
        // and use an inserter that artificially takes a long time to insert a batch of records
        DbInserter<EntitiesRecord> inserter = new DbInserter() {
            DbInserter<EntitiesRecord> valuesInserter = DbInserters.valuesInserter();

            @Override
            public void insert(Table table, List records, DSLContext dsl)
                    throws DataAccessException {
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    valuesInserter.insert(table, records, dsl);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        loader = loaders.getFactory().getInserter(ENTITIES, ENTITIES, RecordTransformer.IDENTITY,
                inserter);
        loader.insertAll(entitiesSet1);
        loader.close(null);
        assertTrue(loader.getStats().mayBePartial());
    }

    /**
     * Make sure the batch inserter is disabled in case pre-insertion hook returns false.
     *
     * <p>We test this by first inserting a set of entities when the pre-insertion hook is null.
     * Then, we set the pre-insertion hook to always return false, simulating a corruption in the
     * partition. We expect the table to only contain entities from the first set and the count of
     * the discarded records to equal the size of the second set of entities.</p>
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testDisabledInserter() throws InterruptedException {
        dsl.deleteFrom(ENTITIES).execute();
        BulkInserter<EntitiesRecord, EntitiesRecord> loader =
                loaders.getFactory().getInserter(ENTITIES, ENTITIES, RecordTransformer.IDENTITY,
                        DbInserters.valuesInserter());
        loader.insertAll(entitiesSet1);
        loader.setPreInsertionHook(r -> false);
        loader.insertAll(entitiesSet2);
        loader.close(null);
        verifyInserts(ENTITIES, ENTITIES.ID,
                entitiesSet1.stream().map(EntitiesRecord::getId).collect(Collectors.toList()),
                entitiesSet2.size());
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
     * @throws DataAccessException       if db operation fails
     */
    private <R extends Record, T> void checkRecordLoad(
            Table<R> table, TableField<R, T> field, List<T> values, Integer... failingBatches)
            throws InterruptedException, DataAccessException {

        performInserts(table, field, values, failingBatches);
        loaders.close();
        verifyInserts(table, field, values, 0, failingBatches);
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
     * @throws DataAccessException if a database operation fails
     */
    private <R extends Record, T> void verifyInserts(final Table<R> table,
            final TableField<R, T> field,
            final List<T> values,
            final Integer discardedRecords,
            final Integer... failingBatches) throws DataAccessException {
        // verify that the records have the correct field values
        verifyValues(table, field, values, failingBatches);
        // then check some of the basic stats values obtained from the loader
        final BulkInserterStats stats = loaders.getStats().getByKey(table);
        final long batchCount = (long)Math.ceil((values.size() + 0.0) / config.batchSize());
        assertEquals(batchCount - failingBatches.length, stats.getBatches());
        assertEquals(failingBatches.length, stats.getFailedBatches());
        assertEquals((long)discardedRecords, stats.getDiscardedRecords());
        final int written = getRecordCount(table);
        assertEquals(written, stats.getWritten());
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
     * @param field          a designated field whose values are provided by the caller. THis should
     *                       be a NOT NULL field
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
            // use a null value to cause this record - and hence its batch - to fail
            record.set(field, fail ? null : values.get(i));
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
     * @throws DataAccessException if a database operation fails
     */
    private <R extends Record, T> void verifyValues(Table<R> table,
            TableField<R, T> field, List<T> values,
            Integer... failingBatches) throws DataAccessException {
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
                getRecords(table).stream().map(r -> r.getValue(field)).collect(Collectors.toSet()));
    }

    /**
     * Get the number of records in a given table.
     *
     * @param table the table
     * @return the record count
     */
    private int getRecordCount(Table<?> table) {
        return dsl.selectCount().from(table).fetchOne(0, Integer.class);
    }

    /**
     * Retrieve all records from a given database table.
     *
     * @param table the table containing the records
     * @param <R>   underlying record type
     * @return list of retrieved records
     */
    private <R extends Record> List<R> getRecords(Table<R> table) {
        @SuppressWarnings("unchecked") List<R> records = (List<R>)dsl.selectFrom(table)
                .fetch()
                .into(table.newRecord().getClass());
        return records;
    }
}
