package com.vmturbo.extractor.scope;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * This class processes chunks in the scope hypertable for compression.
 */
public class ScopeTableManager {
    private final Logger logger = LogManager.getLogger();

    private static final String SCOPE_BACKUP_TABLE_NAME = "scope_original";
    private static final String SCOPE_TABLE_NAME = "scope";

    private static final String GET_NUM_OF_UNCOMPRESSED_CHUNKS = "SELECT count(*) "
            + "FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = '%s' AND hypertable_name = '%s' "
            + "AND is_compressed=false";

    private static final String GET_UNCOMPRESSED_CHUNKS =
            "SELECT chunk_schema, chunk_name, range_start, range_end "
                    + "FROM timescaledb_information.chunks c "
                    + "WHERE c.hypertable_schema = '%s' AND c.hypertable_name = '%s' "
                    + "AND is_compressed=false order by range_start asc";

    private static final String SPLIT_SCOPE_RECORD = "INSERT INTO %1$s "
            + "SELECT seed_oid, scoped_oid, scoped_type, '%2$s' AS start, finish FROM %1$s "
            + "WHERE start BETWEEN '%3$s' AND '%4$s' AND finish > '%4$s' ON CONFLICT DO NOTHING";

    private static final String UPDATE_END_DATES = "UPDATE %1$s SET finish = '%2$s' "
            + "WHERE start >= '%3$s' AND start < '%4$s' AND finish > '%4$s'";

    private static final String COMPRESS_CHUNK = "SELECT compress_chunk('%s.%s')";

    private static final String SCOPE_HYPERTABLE_EXISTS =
            "SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = '%s'";

    private static final String RENAME_ORIGINAL_SCOPE_TABLE =
            "ALTER TABLE scope RENAME TO %s";

    private static final String CREATE_SCOPE_REGULAR_TABLE =
            "CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)";

    private static final String CREATE_SCOPE_HYPERTABLE =
            "SELECT create_hypertable('%s', 'start', chunk_time_interval => INTERVAL '1 month')";

    private static final String SET_SCOPE_HT_SETTINGS = "ALTER TABLE %s SET (timescaledb.compress, "
            + "timescaledb.compress_segmentby = 'seed_oid, scoped_type', "
            + "timescaledb.compress_orderby = 'scoped_oid')";

    /**
     * This constant defines the number of chunks that should be kept uncompressed.
     * E.g. If the value is 2, the most recent two chunks are uncompressed; the third most recent
     * chunk can be compressed.
     */
    private static final int NUM_OF_UNCOMPRESSED_CHUNKS_TO_KEEP = 2;

    private DSLContext dsl;

    private final DbEndpoint dbEndpoint;

    private long lastTimeChunkInfoWasRetrieved = 0;

    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);

    /**
     * Constructor.
     *
     * @param dbEndpoint Database endpoint
     */
    public ScopeTableManager(DbEndpoint dbEndpoint) {
        this.dbEndpoint = dbEndpoint;
    }

    /**
     * This method is the task that is invoked periodically to check if chunk(s) are ready to be
     * processed for compression. If so, make necessary data updates and run compress the chunks.
     */
    public synchronized void execute() {
        try {
            if (dsl == null) {
                this.dsl = dbEndpoint.dslContext();
            }

            // Create hypertable is it does not already exist. This logic will be removed when
            // the migration process is in place. We will expect the hypertable to be present.
            if (!isScopeHypertableExists()) {
                createScopeHypertable();
            }

            if (isTimeToCompressChunks()) {
                processUncompressedChunks();
            }
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Error occurred when processing scope table maintenance.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted when instantiating ScopeTableManager.", e);
        } catch (Exception e) {
            // Catch all other exceptions to make sure the scheduled thread won't die.
            logger.error("Unexpected exception caught while handling scope records for compression.", e);
        }
    }

    /**
     * Check if it is time to process the scope table records for compression.
     * Since this method can be called quite frequently (once per topology broadcast), we don't
     * need to check the database every time the method is being invoked. We will get the chunk info
     * from database once a day. The compression needs to be done once a month.
     *
     * @return true if it is time to process the scope table.
     */
    private boolean isTimeToCompressChunks() {
        long now = System.currentTimeMillis();
        if (now - lastTimeChunkInfoWasRetrieved < ONE_DAY_IN_MILLIS) {
            return false;
        }
        lastTimeChunkInfoWasRetrieved = now;
        return getNumOfUncompressedChunks() > NUM_OF_UNCOMPRESSED_CHUNKS_TO_KEEP;
    }

    /**
     * Get number of uncompressed chunks.
     *
     * @return the number of uncompressed chunks.
     */
    private int getNumOfUncompressedChunks() {
        String sql = String.format(GET_NUM_OF_UNCOMPRESSED_CHUNKS,
                dbEndpoint.getConfig().getSchemaName(), SCOPE_TABLE_NAME);
        ResultQuery<Record> query = dsl.resultQuery(sql);
        Record record = query.fetchAny();
        int num = 0;
        if (record != null) {
            int numOfUncompressedChunks = record.getValue(0, Integer.class);
            logger.info("There are {} uncompressed chunks.", numOfUncompressedChunks);
            return numOfUncompressedChunks;
        }
        return num;
    }

    /**
     * Process all uncompressed chunks, except the latest one, and compress them.
     * For each uncompressed chunk, split the records and adjust the finish date before compressing
     * the chunk.
     */
    private void processUncompressedChunks() {
        String sql = String.format(GET_UNCOMPRESSED_CHUNKS, dbEndpoint.getConfig().getSchemaName(),
                SCOPE_TABLE_NAME);
        ResultQuery<Record> query = dsl.resultQuery(sql);
        List<ChunkInfo> chunks = query.fetch().into(ChunkInfo.class);

        if (chunks.size() > NUM_OF_UNCOMPRESSED_CHUNKS_TO_KEEP) {
            logger.info("Start processing uncompressed chunks of scope table for compression.");
            logger.info("There are {} uncompressed chunks in total. {} chunk(s) will be compressed.",
                    chunks.size(), chunks.size() - NUM_OF_UNCOMPRESSED_CHUNKS_TO_KEEP);
            ChunkInfo currentChunk = chunks.get(0);

            for (int i = 1; i < chunks.size() - (NUM_OF_UNCOMPRESSED_CHUNKS_TO_KEEP - 1); i++) {
                logger.info("Processing chunk {}.", currentChunk);

                final ChunkInfo currChunk = currentChunk;
                final ChunkInfo nextChunk = chunks.get(i);

                // Execute the SQL for creating the continuation records and updating finish date
                // in a transaction, so we won't process the records partially in case of error.
                dsl.transaction(transactionConfig -> {
                    DSLContext transactionDsl = DSL.using(transactionConfig);

                    splitActiveRecords(transactionDsl, currChunk, nextChunk);
                    updateEndDates(transactionDsl, currChunk, nextChunk);
                });
                compressChunk(currentChunk);

                currentChunk = nextChunk;
            }
        }
    }

    /**
     * If a scope relation is still valid beyond the range_end timestamp of the chunk, create a
     * "continuation record" that starts at the range_start timestamp of the next chunk and end with
     * the "infinity timestamp".
     *
     * @param transactionDsl DSL context used for the transaction
     * @param currentChunk current chunk
     * @param nextChunk next chunk
     */
    private void splitActiveRecords(DSLContext transactionDsl, ChunkInfo currentChunk, ChunkInfo nextChunk) {
        final Stopwatch executionTime = Stopwatch.createStarted();
        String splitSql = String.format(SPLIT_SCOPE_RECORD, SCOPE_TABLE_NAME, nextChunk.getRangeStart(),
                currentChunk.getRangeStart(), currentChunk.getRangeEnd());
        int numRecordsInserted = transactionDsl.execute(splitSql);
        logger.info("Splitting records for chunk {} took {}. {} records were inserted into "
                        + "the next chunk.",
                currentChunk.getChunkName(), executionTime, numRecordsInserted);
    }

    /**
     * Set the finish data of all scope records that currently have the "infinity timestamp" to the
     * range_start timestamp of the next chunk.
     *
     * @param transactionDsl DSL context used for the transaction
     * @param currentChunk current chunk
     * @param nextChunk next chunk
     */
    private void updateEndDates(DSLContext transactionDsl, ChunkInfo currentChunk, ChunkInfo nextChunk) {
        String updateSql = String.format(UPDATE_END_DATES, SCOPE_TABLE_NAME, nextChunk.getRangeStart(),
                currentChunk.getRangeStart(), currentChunk.getRangeEnd());
        final Stopwatch executionTime = Stopwatch.createStarted();
        int recordsUpdated = transactionDsl.execute(updateSql);
        logger.info("Updating end dates for {} records in chunk {} took {}. ", recordsUpdated,
                currentChunk.getChunkName(), executionTime);
    }

    /**
     * Compress a chunk.
     *
     * @param chunk the chunk to be compressed
     */
    private void compressChunk(ChunkInfo chunk) {
        String sql = String.format(COMPRESS_CHUNK, chunk.getChunkSchema(), chunk.getChunkName());
        final Stopwatch executionTime = Stopwatch.createStarted();
        dsl.execute(sql);
        logger.info("Compressed chunk {} in {}.", chunk.getChunkName(), executionTime);
    }

    /**
     * Create the hypertable version of the scope table, if it does not already exist.
     * This hypertable will not be created is the hypertable feature flag is disabled.
     * The way the hypertable is created will change after the migration process is implemented.
     */
    private void createScopeHypertable() {
        String renameSql = String.format(RENAME_ORIGINAL_SCOPE_TABLE, SCOPE_BACKUP_TABLE_NAME);
        dsl.execute(renameSql);

        String createScopeTableSql = String.format(CREATE_SCOPE_REGULAR_TABLE, SCOPE_TABLE_NAME,
                SCOPE_BACKUP_TABLE_NAME);
        dsl.execute(createScopeTableSql);

        String createHypertableSql = String.format(CREATE_SCOPE_HYPERTABLE, SCOPE_TABLE_NAME);
        dsl.execute(createHypertableSql);

        String hypertableSettingsSql = String.format(SET_SCOPE_HT_SETTINGS, SCOPE_TABLE_NAME);
        dsl.execute(hypertableSettingsSql);
    }

    /**
     * Used by unit test only.
     *
     * @param dsl DSL Context
     */
    @VisibleForTesting
    void createScopeHypertable(DSLContext dsl) {
        this.dsl = dsl;
        createScopeHypertable();
    }

    /**
     * Check if the scope hypertable exists.
     *
     * @return true if the hypertable exists, false otherwise
     */
    private boolean isScopeHypertableExists() {
        String sql = String.format(SCOPE_HYPERTABLE_EXISTS, SCOPE_TABLE_NAME);
        ResultQuery<Record> query = dsl.resultQuery(sql);
        return query.iterator().hasNext();
    }
}
