package com.vmturbo.extractor.topology.fetcher;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.GetEntitySavingsStatsRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.commons.Units;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.EntitySavings;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.enums.SavingsType;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Entity cloud savings data fetcher. Knows how to read from MySQL savings table and write that
 * data to the Postgres savings table.
 */
public class CloudSavingsFetcher {
    private static final Logger logger = LogManager.getLogger();

    private final DbEndpoint dbEndpoint;
    private final WriterConfig config;
    private final ExecutorService pool;

    private final CostServiceBlockingStub costServiceRpc;

    /**
     * Hourly savings data is normally stored for 72 hours only, but in case that is configured
     * to be longer, get all the data (up to this limit) from the MySQL savings table. We query
     * data one day at a time and break out if no more data is retrieved for a day range.
     */
    private final int maxDaysBack = 60;

    /**
     * Set to true initially on startup, but then false subsequently. Used to perform initial
     * bulk migration only once on startup if required, rather than on every notification received.
     */
    private boolean firstTimeAfterStartup = true;

    /**
     * Create new fetcher.
     *
     * @param dbEndpoint Postgres DB endpoint.
     * @param costSvc Cost Service to query MySQL data.
     * @param pool DB writer pool.
     * @param config DB writer config.
     */
    public CloudSavingsFetcher(final DbEndpoint dbEndpoint,
            @Nonnull final CostServiceBlockingStub costSvc, final ExecutorService pool,
            WriterConfig config) {
        this.costServiceRpc = costSvc;
        this.dbEndpoint = dbEndpoint;
        this.pool = pool;
        this.config = config;
        logger.info("Created cloud savings listener for endpoint: {}", dbEndpoint);
    }

    /**
     * Checks the current row count in Postgres entity_savings DB table. If non-0, then no
     * migration is required, otherwise, we do initial bulk migration once.
     *
     * @return True if entity_savings DB table is empty and migration is required.
     */
    private boolean checkDataMigrationRequired() {
        final String query = String.format("SELECT count(*) FROM %s", EntitySavings.name());
        long rowCount = 0;
        try {
            Object count = dbEndpoint.dslContext().fetchValue(query);
            if (count instanceof Long) {
                rowCount = (Long)count;
                logger.info("Found {} current cloud savings rows.", rowCount);
            }
        } catch (UnsupportedDialectException | SQLException | DataAccessException e) {
            logger.warn("Unable to get Cloud Savings table count.", e);
        } catch (InterruptedException ie) {
            logger.warn("Interrupt when getting Cloud Savings table count: {}", ie.getMessage());
            Thread.currentThread().interrupt();
        }
        return rowCount == 0;
    }

    /**
     * Queries savings data from cost component and writes to Postgres DB table. Performs one time
     * bulk migration of older savings hourly data also if required. Called when notification
     * is received from cost component that a savings hourly data has been written.
     *
     * @param startTime Savings stats hourly start time written this time to MySQL DB table.
     * @param endTime Savings stats end time.
     * @throws UnsupportedDialectException Thrown on bad DB dialect.
     * @throws SQLException Thrown on general DB error.
     * @throws DataAccessException Thrown on DB access errors.
     * @throws InterruptedException Thrown on interrupt during DB operation.
     */
    public void syncCloudSavingsData(long startTime, long endTime)
            throws UnsupportedDialectException, SQLException, DataAccessException,
            InterruptedException {
        // Do migration check, this needs to be done here rather than constructor, so that DB
        // connection stuff gets time to initialize. Do it once after startup only.
        boolean isMigrationRequired = firstTimeAfterStartup && checkDataMigrationRequired();
        firstTimeAfterStartup = false;

        // Write the data that we got notified about this time.
        writeCloudSavingsData(readCloudSavingsData(startTime, endTime));

        if (!isMigrationRequired) {
            logger.trace("No migration is required this time. Start: {}, End: {}",
                    startTime, endTime);
            return;
        }
        // Do any initial migration (first time only).
        long migrationEndTime = startTime; // exclusive
        for (int j = 1; j <= maxDaysBack; j++) {
            long migrationStartTime = migrationEndTime - (long)Units.DAY_MS;
            logger.info("Migrating cloud savings data {} days back, between {} and {}.", j,
                    migrationStartTime, migrationEndTime);
            final Iterator<EntitySavingsStatsRecord> records = readCloudSavingsData(
                    migrationStartTime, migrationEndTime);
            if (!records.hasNext()) {
                logger.info("No more migration records to fetch after {} days back", j);
                break;
            }
            writeCloudSavingsData(records);
            migrationEndTime = migrationStartTime;
        }
    }

    /**
     * Reads savings hourly data from cost component for the given time range.
     *
     * @param startTime Min time of query.
     * @param endTime Max time of query.
     * @return Iterator of savings records.
     */
    private Iterator<EntitySavingsStatsRecord> readCloudSavingsData(long startTime, long endTime) {
        return costServiceRpc.getEntitySavingsStats(
                GetEntitySavingsStatsRequest.newBuilder()
                        .setStartDate(startTime)
                        .setEndDate(endTime)
                        .setAggregateRecords(false).build());
    }

    /**
     * Writes savings records to extractor Postgres entity_savings DB table.
     *
     * @param statsRecords Stats records fetched from cost component.
     * @throws UnsupportedDialectException Thrown on bad DB dialect.
     * @throws SQLException Thrown on general DB error.
     * @throws DataAccessException Thrown on DB access errors.
     * @throws InterruptedException Thrown on interrupt during DB operation.
     */
    private void writeCloudSavingsData(@Nonnull final Iterator<EntitySavingsStatsRecord> statsRecords)
            throws UnsupportedDialectException, SQLException, DataAccessException, InterruptedException {
        final DslRecordSink sink = new DslRecordSink(dbEndpoint.dslContext(), EntitySavings.TABLE,
                config, pool);
        writeCloudSavingsData(statsRecords, sink);
    }

    /**
     * Writes savings records to extractor Postgres entity_savings DB table.
     *
     * @param statsRecords Stats records fetched from cost component.
     * @param sink Sink to write records to - extractor entity_savings DB table.
     * @throws DataAccessException Thrown on DB access errors.
     */
    @VisibleForTesting
    void writeCloudSavingsData(@Nonnull final Iterator<EntitySavingsStatsRecord> statsRecords,
            @Nonnull final DslRecordSink sink) throws DataAccessException {
        try (TableWriter writer = EntitySavings.TABLE.open(sink,
                String.format("%s inserter", EntitySavings.name()), logger)) {
            statsRecords.forEachRemaining(statsRecord -> processRecord(writer, statsRecord));
        }
    }

    /**
     * Converts stats record to a Postgres DB record.
     *
     * @param consumer Writer of postgres DB record.
     * @param statsRecord Stats record read from cost component.
     */
    private static void processRecord(Consumer<Record> consumer,
            @Nonnull final EntitySavingsStatsRecord statsRecord) {
        final Record record = new Record(EntitySavings.TABLE);
        record.set(EntitySavings.TIME, new Timestamp(statsRecord.getSnapshotDate()));
        record.set(EntitySavings.ENTITY_OID, statsRecord.getEntityOid());
        record.set(EntitySavings.SAVINGS_TYPE, SavingsType.valueOf(statsRecord.getStatRecords(0).getName()));
        record.set(EntitySavings.STATS_VALUE, statsRecord.getStatRecords(0).getValue());
        consumer.accept(record);
    }
}
