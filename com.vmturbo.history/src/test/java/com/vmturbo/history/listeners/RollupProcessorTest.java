package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.RollupProcessor.VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD;
import static org.mockito.Mockito.mock;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.VolumeAttachmentHistory;
import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to test the rollup processor and the stored procedures it depends on.
 */
@RunWith(Parameterized.class)
public class RollupProcessorTest extends MultiDbTestBase {

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
    public RollupProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws SQLException If a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10)
                .maxBatchRetries(1)
                .maxRetryBackoffMsec(1000)
                .maxPendingBatches(1)
                .build();
        loaders = new SimpleBulkLoaderFactory(dsl, config, mock(PartmanHelper.class),
                Executors.newSingleThreadExecutor());
        rollupProcessor = new RollupProcessor(dsl, dsl, mock(PartmanHelper.class),
                Executors.newFixedThreadPool(8));
        RetentionPolicy.init(dsl);
        IdentityGenerator.initPrefix(1L);
    }

    private static final String UUID_FIELD = "uuid";
    private static final String PRODUCER_UUID_FIELD = "producer_uuid";
    private static final String PROPERTY_TYPE_FIELD = "property_type";
    private static final String PROPERTY_SUBTYPE_FIELD = "property_subtype";
    private static final String COMMODITY_KEY_FIELD = "commodity_key";
    private static final String RELATION_FIELD = "relation";
    private static final String CAPACITY_FIELD = "capacity";
    private static final String EFFECTIVE_CAPACITY_FIELD = "effective_capacity";
    private static final String AVG_VALUE_FIELD = "avg_value";
    private static final String MAX_VALUE_FIELD = "max_value";
    private static final String MIN_VALUE_FIELD = "min_value";
    private static final String SNAPSHOT_TIME_FIELD = "snapshot_time";
    private static final String SAMPLES_FIELD = "samples";

    private static final String INTERNAL_NAME_FIELD = "internal_name";
    private static final String RECORDED_ON_FIELD = "recorded_on";
    private static final String VALUE_FIELD = "value";

    /**
     * Seed for random number generator.
     *
     * <p>There's considerable awkwardness in this test due to the fact that the database type of
     * value- and capacity-related columns in the stats tables is DECIMAL(15,3), but we have a jOOQ
     * conversion to Double configured for the component. This makes it difficult to compare values
     * of internally computed averages to those computed in the database. We use approximate
     * equality tests to help address this, but even then, the test with different values could
     * suffer false failures. While there's value in creating a stream of values with internal
     * randomness (mostly in terms of conciseness of test specification), there's no value in having
     * different random values from one execution to the next. So we fix the seed to a value that is
     * known to work. If a change to this test class causes some tests to fail for no other likely
     * reason, one thing to try is changing this seed value to find one that works with the other
     * code changes.</p>
     */
    private static final long RANDOM_SEED = 0L;

    private static String testDbName;
    private SimpleBulkLoaderFactory loaders;
    private RollupProcessor rollupProcessor;
    private final Random rand = new Random(RANDOM_SEED);

    /**
     * Delete any records inserted during this test.
     *
     * <p>We truncate all the tables identified as output tables in the bulk loader stats object,
     * as well as all associated rollup tables.</p>
     *
     * <p>If any other tables are populated by a given test, that test should clean them up.</p>
     *
     * @throws InterruptedException if interrupted
     */
    @After
    public void after() throws InterruptedException {
        loaders.close(null);
    }


    private static final long VOLUME_OID = 11111L;
    private static final long VOLUME_OID_2 = 22222L;
    private static final long VM_OID = 33333L;
    private static final long VM_OID_2 = 44444L;

    /**
     * Test that retention processing removes the record related to the only volume from the
     * volume_attachment_history table that is older than retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsRemoval() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long outsideRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, outsideRetentionPeriod,
                outsideRetentionPeriod);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> records =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        Assert.assertFalse(records.isEmpty());

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        Assert.assertTrue(recordsAfterPurge.isEmpty());
    }

    /**
     * Test that retention processing does not remove any records related to the volume from the
     * volume_attachment_history table as it has one entry discovered within the retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsNoRemovals() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long withinRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD - 1);
        final long outsideRetentionPeriod =
                currentTime - TimeUnit.DAYS.toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, VM_OID, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, withinRetentionPeriod,
                withinRetentionPeriod);

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        final Record3<Long, Long, Date> record = recordsAfterPurge.iterator().next();
        Assert.assertEquals(VOLUME_OID, (long)record.component1());
        Assert.assertEquals(VM_OID, (long)record.component2());
    }

    /**
     * Test that retention processing removes records related to one volume but retains records
     * related to another volume as the former has no entries within the retention period while the
     * latter has one entry within the last retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsOneRemoval() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long withinRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD - 1);
        final long outsideRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, VM_OID, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, withinRetentionPeriod,
                withinRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID_2, VM_OID_2, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID_2, 0, outsideRetentionPeriod,
                outsideRetentionPeriod);

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Stream.of(VOLUME_OID, VOLUME_OID_2)
                        .collect(Collectors.toList()));
        final Record3<Long, Long, Date> record = recordsAfterPurge.iterator().next();
        Assert.assertEquals(VOLUME_OID, (long)record.component1());
        Assert.assertEquals(VM_OID, (long)record.component2());
    }

    private void insertIntoVolumeAttachmentHistoryTable(final long volumeOid, final long vmOid,
            final long lastAttachedTime,
            final long lastDiscoveredTime)
            throws DataAccessException {
        dsl.insertInto(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.VOLUME_OID,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.VM_OID,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.LAST_ATTACHED_DATE,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.LAST_DISCOVERED_DATE)
                .values(volumeOid, vmOid, new Date(lastAttachedTime),
                        new Date(lastDiscoveredTime))
                .execute();
    }
}
