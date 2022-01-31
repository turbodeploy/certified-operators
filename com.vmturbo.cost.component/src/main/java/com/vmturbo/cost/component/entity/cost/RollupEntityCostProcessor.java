package com.vmturbo.cost.component.entity.cost;

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
public class RollupEntityCostProcessor {
    private final Logger logger = LogManager.getLogger();

    /**
     * DB interface to call rollup procedures.
     */
    private final EntityCostStore costStore;

    private final RollupTimesStore rollupTimesStore;

    /**
     * UTC clock from config.
     */
    private final Clock clock;
    private final IngestedTopologyStore ingestedTopologyStore;

    /**
     * Info about last rollup times read from DB.
     */
    private LastRollupTimes lastRollupTimes;

    /**
     * Creates a new instance. Initialized from config.
     *
     * @param costStore DB store.
     * @param rollupTimesStore Last rollup times store.
     * @param ingestedTopologyStore ingested topology store
     * @param clock UTC clock.
     */
    public RollupEntityCostProcessor(@Nonnull final EntityCostStore costStore,
            @Nonnull final RollupTimesStore rollupTimesStore,
            @Nonnull final IngestedTopologyStore ingestedTopologyStore, @Nonnull final Clock clock) {
        this.costStore = costStore;
        this.rollupTimesStore = rollupTimesStore;
        this.ingestedTopologyStore = ingestedTopologyStore;
        this.clock = clock;
    }

    /**
     * Checks if any rollup needs to be done for the entity stats data that has just been written
     * to DB. If needed, performs hourly/daily/monthly rollup of that data set.
     */
    void process() {

        // Check last time entity cost updated metadata.
        if (lastRollupTimes == null) {
            logger.trace("Reading first time rollup last times from DB...");
            lastRollupTimes = rollupTimesStore.getLastRollupTimes();
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
        final List<LocalDateTime> creationTimeMonth =
                ingestedTopologyStore.getCreationTimeSinceLastTopologyRollup(lastTimeByMonth);

        performRollup(creationTimeHourList, RollupDurationType.HOURLY);
        performRollup(creationTimeDayList, RollupDurationType.DAILY);
        performRollup(creationTimeMonth, RollupDurationType.MONTHLY);

        lastRollupTimes.setLastTimeUpdated(Instant.now(clock).toEpochMilli());

        rollupTimesStore.setLastRollupTimes(lastRollupTimes);
    }

    private void performRollup(final List<LocalDateTime> creationTimeList,
            final RollupDurationType type) {
        if (!creationTimeList.isEmpty()) {
            costStore.performRollup(type, creationTimeList, clock);
            long mostRecentTimestamp = localDateTimeToMilli(Collections.max(creationTimeList));
            switch (type) {
                case HOURLY:
                    lastRollupTimes.setLastTimeByHour(mostRecentTimestamp);
                    return;
                case DAILY:
                    lastRollupTimes.setLastTimeByDay(mostRecentTimestamp);
                    return;
                case MONTHLY:
                    lastRollupTimes.setLastTimeByMonth(mostRecentTimestamp);
                    return;
                default:
                    logger.error("Found unknown RollupDurationType during Entity Cost rollup: {}",
                            type);
            }
        }
    }

    private long localDateTimeToMilli(@Nonnull final LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * This method is invoked once an hour.
     */
    public void execute() {
        try {
            logger.info("START: Invoking RollupEntityCostProcessor to process Entity Cost rollup.");
            process();
            logger.info("END: RollupEntityCostProcessor rollup.");
        } catch (Throwable e) {
            logger.error("Exception was thrown in the RollupEntityCostProcessor:", e);
        }
    }
}
