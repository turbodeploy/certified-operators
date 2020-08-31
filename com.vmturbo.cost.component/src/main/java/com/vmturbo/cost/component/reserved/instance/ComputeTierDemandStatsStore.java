package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK;
import static com.vmturbo.cost.component.db.Tables.LAST_UPDATED;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;

public class ComputeTierDemandStatsStore implements DiagsRestorable<Void> {

    private static final String computeTierDemandFile = "computeTierDemand_dump";

    private final Logger logger = LogManager.getLogger();
    /**
     * Number of records to commit in one batch.
     */
    private int statsRecordsCommitBatchSize;

    /**
     * Number of results to return when fetching lazily.
     */
    private int statsRecordsQueryBatchSize;

    private final DSLContext dslContext;

    /*
     * for Junit tests only
     */
    @VisibleForTesting
    ComputeTierDemandStatsStore() {
        dslContext = null;
    }

    public ComputeTierDemandStatsStore(@Nonnull final DSLContext dslContext,
                                       int statsRecordsCommitBatchSize,
                                       int statsRecordsQueryBatchSize) {

        this.dslContext = Objects.requireNonNull(dslContext);
        this.statsRecordsCommitBatchSize = statsRecordsCommitBatchSize;
        this.statsRecordsQueryBatchSize = statsRecordsQueryBatchSize;
    }

    /**
     * @param hour hour for which the ComputeTier demand stats are requested
     * @param day  day for which the ComputeTier demand stats are requested
     * @return Stream of InstanceTypeHourlyByWeekRecord records.
     * <p>
     * NOTE: This returns a resourceful stream. Resource should be closed
     * by the caller.
     */
    public Stream<ComputeTierTypeHourlyByWeekRecord> getStats(byte hour, byte day)
            throws DataAccessException {
        return dslContext.selectFrom(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .where(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.HOUR.eq(hour)
                        .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.DAY.eq(day)))
                .fetchSize(statsRecordsQueryBatchSize)
                .stream();
    }

    public void persistComputeTierDemandStats(
            @Nonnull Collection<ComputeTierTypeHourlyByWeekRecord> demandStats,
            boolean isProjectedTopology)
            throws DataAccessException {

        List<Query> batchQueries = new ArrayList<>();
        logger.debug("Storing stats. NumRecords = {} ", demandStats.size());
        // Each batch is run in a separate transaction. If some of the batch inserts fail,
        // it's ok as the stats are independent. Better to have some of the stats than no
        // stats at all.

        batchQueries.add(updateLastUpdatedTime(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.getName(),
                        isProjectedTopology ?
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COUNT_FROM_SOURCE_TOPOLOGY.getName() :
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COUNT_FROM_PROJECTED_TOPOLOGY.getName()));

        for (List<ComputeTierTypeHourlyByWeekRecord> statsBatch :
                Iterables.partition(demandStats, statsRecordsCommitBatchSize)) {
            dslContext.transaction(configuration -> {
                try {
                    DSLContext localDslContext = DSL.using(configuration);
                    // TODO : karthikt. Check the difference in speed between multiple
                    // insert statements(createStatement) vs single stateement with multiple
                    // bind values(prepareStatement). Using createStatement here for syntax simplicity.
                    for (ComputeTierTypeHourlyByWeekRecord stat : statsBatch) {
                        batchQueries.add(
                                localDslContext.insertInto(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                                        .set(stat)
                                        .onDuplicateKeyUpdate()
                                        .set(stat));
                    }
                    localDslContext.batch(batchQueries).execute();
                } catch (DataAccessException ex) {
                    throw ex;
                }
            });
        }
    }

    /**
     * Updates/Inserts the Table.LAST_UPDATED with the current timestamp for the passed in table name
     * and column name.
     *
     * @param tableName The table name.
     * @param columnName The column name in the table.
     * @return a Query.
     */
    private Query updateLastUpdatedTime(String tableName, String columnName) {
        java.sql.Timestamp date = new java.sql.Timestamp(new java.util.Date().getTime());
        return dslContext.insertInto(Tables.LAST_UPDATED, LAST_UPDATED.TABLE_NAME,
                    LAST_UPDATED.COLUMN_NAME,
                    LAST_UPDATED.LAST_UPDATE)
                    .values(tableName,
                            columnName,
                            date)
                    .onDuplicateKeyUpdate()
                    .set(LAST_UPDATED.LAST_UPDATE, date);
    }

    /**
     * Checks whether an update has been performed for the topology created
     * hour for the tableName and columnName.
     *
     * @param tableName the table name.
     * @param columnName the column name.
     * @param topologyCreationDate the creation time of the current topology being processed.
     * @return whether an update has been performed in the current hour for the tableName and columnName.
     */
    public boolean isUpdatedInLastHour(String tableName, String columnName,
                                       Calendar topologyCreationDate) {
        java.sql.Timestamp date = new java.sql.Timestamp(topologyCreationDate.getTimeInMillis());

        Calendar start = Calendar.getInstance();
        start.setTime(date);
        start.set(Calendar.MINUTE, 0);
        start.set(Calendar.SECOND, 0);
        start.set(Calendar.MILLISECOND, 0);
        Timestamp startTimestamp = new Timestamp(start.getTimeInMillis());

        Calendar end = Calendar.getInstance();
        end.setTime(date);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 59);
        start.set(Calendar.MILLISECOND, 999);
        Timestamp endTimestamp = new Timestamp(end.getTimeInMillis());

        final List<Condition> conditions = new ArrayList<>(3);
        conditions.add(LAST_UPDATED.TABLE_NAME.eq(tableName));
        conditions.add(LAST_UPDATED.COLUMN_NAME.eq(columnName));
        conditions.add(LAST_UPDATED.LAST_UPDATE.between(startTimestamp, endTimestamp));

        return dslContext.fetchExists(dslContext.selectFrom(LAST_UPDATED).where(conditions));
    }

    /**
     * Queries for the unique set of demand clusters (account, region, compute tier, platform, tenancy).
     * @return A {@link Stream} of records representing the unique set of demand clusters.
     */
    public Stream<ComputeTierTypeHourlyByWeekRecord> getUniqueDemandClusters() {
        return dslContext
                .selectDistinct(
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.ACCOUNT_ID,
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.REGION_OR_ZONE_ID,
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COMPUTE_TIER_ID,
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.PLATFORM,
                        COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.TENANCY)
                .from(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .fetchStreamInto(ComputeTierTypeHourlyByWeekRecord.class);

    }

    /**
     * Fetch the demand stats for a target demand cluster.
     *
     * @param demandCluster The target demand cluster
     * @return The list of records for the target demand cluster.
     */
    @Nonnull
    public List<ComputeTierTypeHourlyByWeekRecord> fetchDemandStats(
            @Nonnull RIBuyDemandCluster demandCluster) {

        return dslContext.selectFrom(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .where(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.ACCOUNT_ID.eq(demandCluster.accountOid()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COMPUTE_TIER_ID.eq(demandCluster.computeTierOid()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.REGION_OR_ZONE_ID.eq(demandCluster.regionOrZoneOid()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.PLATFORM.eq((byte)demandCluster.platform().getNumber()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.TENANCY.eq((byte)demandCluster.tenancy().getNumber()))
                .orderBy(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.DAY, COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.HOUR)
                .fetch();
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        // TODO to be implemented as part of OM-58627
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        dslContext.transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            Stream<ComputeTierTypeHourlyByWeekRecord> latestRecords = transaction.selectFrom(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK).stream();
            latestRecords.forEach(s -> {
                try {
                    appender.appendString(s.formatJSON());
                } catch (DiagnosticsException e) {
                    logger.error("Exception encountered while appending compute tier weekly by hour records" +
                            " to the diags dump", e);
                }
            });
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return computeTierDemandFile;
    }
}
