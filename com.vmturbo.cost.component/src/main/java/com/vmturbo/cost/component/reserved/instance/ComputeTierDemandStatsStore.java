package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceZonalContext;

public class ComputeTierDemandStatsStore {

    private final Logger logger = LogManager.getLogger();

    private static final int RI_HOUR_ADDITION = 23;
    private static final int HOURS_A_DAY = 24;
    private static final int RI_MINIMUM_DATA_POINTS_LOWER_BOUND = 1;
    private static final int RI_MINIMUM_DATA_POINTS_UPPER_BOUND = 168;
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
            @Nonnull Collection<ComputeTierTypeHourlyByWeekRecord> demandStats)
            throws DataAccessException {

        logger.debug("Storing stats. NumRecords = {} ", demandStats.size());
        // Each batch is run in a separate transaction. If some of the batch inserts fail,
        // it's ok as the stats are independent. Better to have some of the stats than no
        // stats at all.
        for (List<ComputeTierTypeHourlyByWeekRecord> statsBatch :
                Iterables.partition(demandStats, statsRecordsCommitBatchSize)) {

            dslContext.transaction(configuration -> {
                try {
                    DSLContext localDslContext = DSL.using(configuration);
                    List<Query> batchQueries = new ArrayList<>();
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
     * @return All compute tier demand stats.
     * @throws DataAccessException
     *
     *  NOTE: This returns a resourceful stream. Resource should be closed
     *  by the caller.
     */
    public Stream<ComputeTierTypeHourlyByWeekRecord> getAllDemandStats()
            throws DataAccessException {

        return dslContext.selectFrom(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .fetchSize(statsRecordsQueryBatchSize)
                .stream();
    }

    public List<ComputeTierTypeHourlyByWeekRecord> fetchDemandStats(ReservedInstanceZonalContext context,
                                                                    Set<Long> accountNumbers) {

        // TODO: karthikt - Ignore accountNumbers for now as we don't yet support account scopes
        // introduced by OM-38894.
        return dslContext.selectFrom(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .where(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.ACCOUNT_ID.eq(context.getMasterAccountId()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COMPUTE_TIER_ID.eq(context.getComputeTier().getOid()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.AVAILABILITY_ZONE.eq(context.getAvailabilityZoneId()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.PLATFORM.eq((byte)context.getPlatform().getNumber()))
                    .and(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.TENANCY.eq((byte)context.getTenancy().getNumber()))
                .orderBy(COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.DAY, COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.HOUR)
                .fetch();
    }

}
