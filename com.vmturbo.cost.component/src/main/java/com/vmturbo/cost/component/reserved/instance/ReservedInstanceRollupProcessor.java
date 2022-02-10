package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.topology.IngestedTopologyStore;

/**
 * Responsible for rolling entity cost data into hourly, daily and monthly tables. This gets called
 * every hour, after the entity cost data has been processed.
 */
public class ReservedInstanceRollupProcessor {
    private final Logger logger = LogManager.getLogger();

    /**
     * DB interface to call rollup procedures.
     */
    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final RollupTimesStore rollupUtilizationTimesStore;
    private final RollupTimesStore rollupCoverageTimesStore;

    /**
     * UTC clock from config.
     */
    private final Clock clock;
    private final IngestedTopologyStore ingestedTopologyStore;
    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    /**
     * Info about last rollup times read from DB.
     */
    private LastRollupTimes lastRollupTimes;

    /**
     * Creates a new instance. Initialized from config.
     * @param reservedInstanceUtilizationStore DB store.
     * @param reservedInstanceCoverageStore DB store.
     * @param rollupUtilizationTimesStore Last rollup times store for RI utilization.
     * @param rollupCoverageTimesStore Last rollup times stores for RI coverage.
     * @param ingestedTopologyStore store for tracking topologies
     * @param clock UTC clock.
     */
    public ReservedInstanceRollupProcessor(@Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final RollupTimesStore rollupUtilizationTimesStore, RollupTimesStore rollupCoverageTimesStore,
            IngestedTopologyStore ingestedTopologyStore, @Nonnull final Clock clock) {
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.rollupUtilizationTimesStore = rollupUtilizationTimesStore;
        this.rollupCoverageTimesStore = rollupCoverageTimesStore;
        this.ingestedTopologyStore = ingestedTopologyStore;
        this.clock = clock;
    }

    /**
     * Checks if any rollup needs to be done for the entity stats data that has just been written
     * to DB. If needed, performs hourly/daily/monthly rollup of that data set.
     */
    void process() {

        // Check last time reserved instances updated metadata.
        if (lastRollupTimes == null) {
            logger.trace("Reading first time rollup last times from DB...");
            lastRollupTimes = rollupUtilizationTimesStore.getLastRollupTimes();
            logger.trace("Last times: {}", lastRollupTimes);
        }

        // Same to entity rollup store procedure, get the latest rollup time for hour/day/month
        final Optional<Long> lastTimeByHour = lastRollupTimes.hasLastTimeByHour() ? Optional.of(
                lastRollupTimes.getLastTimeByHour()) : Optional.empty();
        final Optional<Long> lastTimeByDay = lastRollupTimes.hasLastTimeByDay() ? Optional.of(
                lastRollupTimes.getLastTimeByDay()) : Optional.empty();
        final Optional<Long> lastTimeByMonth = lastRollupTimes.hasLastTimeByMonth() ? Optional.of(
                lastRollupTimes.getLastTimeByMonth()) : Optional.empty();

        // Same to entity rollup store procedure, get list of `creation_time` from ingested topology
        // store since the last rollup.
        final List<LocalDateTime> creationTimeHourList =
                ingestedTopologyStore.getCreationTimeSinceLastTopologyRollup(lastTimeByHour);
        final List<LocalDateTime> creationTimeDayList =
                ingestedTopologyStore.getCreationTimeSinceLastTopologyRollup(lastTimeByDay);
        final List<LocalDateTime> creationTimeMonthList =
                ingestedTopologyStore.getCreationTimeSinceLastTopologyRollup(lastTimeByMonth);

        extracted(creationTimeHourList, RollupDurationType.HOURLY);
        extracted(creationTimeDayList, RollupDurationType.DAILY);
        extracted(creationTimeMonthList, RollupDurationType.MONTHLY);

        lastRollupTimes.setLastTimeUpdated(Instant.now(clock).toEpochMilli());

        rollupCoverageTimesStore.setLastRollupTimes(lastRollupTimes);
        rollupUtilizationTimesStore.setLastRollupTimes(lastRollupTimes);


    }

    private void extracted(final List<LocalDateTime> creationTimeList,
            final RollupDurationType type) {
        if (!creationTimeList.isEmpty()) {
            reservedInstanceCoverageStore.performRollup(type, creationTimeList);
            reservedInstanceUtilizationStore.performRollup(type, creationTimeList);
            long localDateMilli = localDateTimeToMilli(Collections.max(creationTimeList));
            switch (type) {
                case HOURLY: {
                    lastRollupTimes.setLastTimeByHour(localDateMilli);
                    break;
                }
                case DAILY: {
                    lastRollupTimes.setLastTimeByDay(localDateMilli);
                    break;
                }
                case MONTHLY: {
                    lastRollupTimes.setLastTimeByMonth(localDateMilli);
                    break;
                }
            }
        }
    }

    private long localDateTimeToMilli(@Nonnull final LocalDateTime dateTime)        {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * This method is invoked once an hour.
     */
    public void execute() {
        try {
            process();
            logger.info("Executing rollup for reserved instance coverage and utilization tables");
        } catch (Throwable e) {
            logger.error("Exception was thrown in ReservedInstanceRollupProcessor:", e);
        }
    }


}
