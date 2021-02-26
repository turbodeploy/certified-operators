package com.vmturbo.cost.component.savings;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * Module to track entity realized/missed savings/investments stats.
 */
public class EntitySavingsTracker {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    private final EntitySavingsStore entitySavingsStore;

    private final EntityEventsJournal entityEventsJournal;

    private final EntityStateCache entityStateCache;

    private final SavingsCalculator savingsCalculator;

    private Calendar lastPeriodEndTime;

    /**
     * Constructor.
     *
     * @param entitySavingsStore entitySavingsStore
     * @param entityEventsJournal entityEventsJournal
     * @param entityStateCache entityStateCache
     */
    EntitySavingsTracker(@Nonnull EntitySavingsStore entitySavingsStore,
                         @Nonnull EntityEventsJournal entityEventsJournal,
                         @Nonnull EntityStateCache entityStateCache) {
        this.entitySavingsStore = Objects.requireNonNull(entitySavingsStore);
        this.entityEventsJournal = Objects.requireNonNull(entityEventsJournal);
        this.entityStateCache = Objects.requireNonNull(entityStateCache);
        this.savingsCalculator = new SavingsCalculator();
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.
     */
    void processEvents() {
        processEvents(getCurrentTime());
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.  Processing will stop at the supplied time.
     *
     * @param end current time in milliseconds indicating when to stop generating savings entries.
     */
    void processEvents(long end) {
        logger.debug("Processing savings/investment.");

        Calendar periodStartTime = getPeriodStartTime();
        if (periodStartTime == null) {
            logger.debug("There are no events in event journal and there are no states in states map. "
                    + "Events tracker has nothing to process.");
            return;
        }

        // Set period end time to 1 hour after start time.
        Calendar periodEndTime = Calendar.getInstance();
        periodEndTime.setTime(periodStartTime.getTime());
        periodEndTime.add(Calendar.HOUR_OF_DAY, 1);

        // There should not be any events before period start time left in the journal.
        // If for some reasons old events are left in the journal, remove them.
        List<SavingsEvent> events =
                entityEventsJournal.removeEventsBetween(0, periodStartTime.getTimeInMillis());
        if (events.size() > 0) {
            logger.warn("There are {} in the events journal that have timestamps before period start time of {}.",
                    events.size(), periodStartTime);
        }

        while (periodEndTime.getTimeInMillis() < end) {
            logger.debug("Entity Savings Tracker is processing events between {} and {}",
                    formatDateTime(periodStartTime), formatDateTime(periodEndTime));

            // Read from entity event journal.
            final long startTime = periodStartTime.getTimeInMillis();
            final long endTime = periodEndTime.getTimeInMillis();
            events = entityEventsJournal.removeEventsBetween(startTime, endTime);

            if (events.isEmpty()) {
                logger.debug("There are no events in this period.");
            } else {
                logger.debug("Entity Savings Tracker retrieved {} events from events journal.", events.size());
            }

            // TODO Get all entity IDs from the events
            //Set<Long> entityIds = events.stream().map(SavingsEvent::getEntityId).collect(Collectors.toSet());

            // TODO Get states for these entities from the state map (if they exist).
            // Map<Long, EntityState> entityStates = entityStateStore.getEntityStates(entityIds);
            // TODO Remove this after we support reading entity state from peristent storage.
            Map<Long, EntityState> entityStates = entityStateCache.getStateMap();

            // Invoke calculator
            savingsCalculator.calculate(entityStates, events, startTime, endTime);
            // TODO Update entity states. Also insert new states to track new entities.
            //entityStateStore.updateEntityStates(entityStates);

            try {
                // create stats records from state map for this period.
                generateStats(startTime);
            } catch (EntitySavingsException e) {
                logger.error("Error occurred when Entity Savings Tracker writes stats to entity savings store. "
                                + "Start time: {} End time: {}", startTime, endTime, e);
                // Stop processing and don't update the last period end time.
                break;
            }
            // We delete inactive entity state after the stats for the entity have been flushed
            // a final time.
            entityStateCache.removeInactiveState();
            // TODO remove entities states that has pendingDelete flag = true.
            // entityStateStore.removeInactiveStates(entityStates);

            // Save the period end time so we won't need to get it from DB next time the tracker runs.
            lastPeriodEndTime = periodEndTime;

            // Advance time period by 1 hour.
            periodStartTime.add(Calendar.HOUR_OF_DAY, 1);
            periodEndTime.add(Calendar.HOUR_OF_DAY, 1);
        }

        logger.debug("Savings/investment processing complete.");
    }

    /**
     * Gets the period start time.
     * If the timestamp of the end time of the period the tracker last executed was cached, simply
     * return it.
     * If it is the first time the tracker is executed after the pod started up, get the latest
     * timestamp from the savings stats hourly table.
     * If the table is empty, get the timestamp from the first event from the events journal.
     *
     * @return the start time of the period to be processed.
     */
    @VisibleForTesting
    @Nullable
    Calendar getPeriodStartTime() {
        // EntitySavingsTracker is a singleton class running in a single thread. The cached value
        // is only read and written by this thread.
        if (lastPeriodEndTime != null) {
            return  lastPeriodEndTime;
        }

        Long maxStatsTime = entitySavingsStore.getMaxStatsTime();
        Calendar periodStartTime = Calendar.getInstance();
        if (maxStatsTime != null) {
            periodStartTime.setTimeInMillis(maxStatsTime);
            // Period start time is one hour after the most recent stats record because the
            // timestamp of the stats record represent the start time of an one-hour period.
            periodStartTime.add(Calendar.HOUR_OF_DAY, 1);
        } else {
            // The stats table is empty.
            // Get earliest event timestamp in events journal.
            Long oldestEventTime = entityEventsJournal.getOldestEventTime();
            if (oldestEventTime != null) {
                periodStartTime.setTimeInMillis(oldestEventTime);
            } else {
                // No events in the events journal.
                return null;
            }
        }
        // Set time to "top of the hour". e.g. if timestamp is 8:05, period start time is 8:00.
        periodStartTime.set(Calendar.MINUTE, 0);
        periodStartTime.set(Calendar.SECOND, 0);
        periodStartTime.set(Calendar.MILLISECOND, 0);
        return periodStartTime;
    }

    /**
     * Generate savings stats records from the state map.
     *
     * @param statTime Timestamp of the stats records which is the start time of a period.
     * @throws EntitySavingsException Error occurred when inserting the DB records.
     */
    @VisibleForTesting
    void generateStats(long statTime) throws EntitySavingsException {
        Set<EntitySavingsStats> stats = new HashSet<>();
        entityStateCache.getAll().forEach(state -> {
            long entityId = state.getEntityId();
            Double savings = state.getRealizedSavings();
            if (savings != null) {
                stats.add(new EntitySavingsStats(entityId, statTime,
                        EntitySavingsStatsType.REALIZED_SAVINGS, savings));
            }
            Double investments = state.getRealizedInvestments();
            if (investments != null) {
                stats.add(new EntitySavingsStats(entityId, statTime,
                        EntitySavingsStatsType.REALIZED_INVESTMENTS, investments));
            }
            savings = state.getMissedSavings();
            if (savings != null) {
                stats.add(new EntitySavingsStats(entityId, statTime,
                        EntitySavingsStatsType.MISSED_SAVINGS, savings));
            }
            investments = state.getMissedInvestments();
            if (investments != null) {
                stats.add(new EntitySavingsStats(entityId, statTime,
                        EntitySavingsStatsType.MISSED_INVESTMENTS, investments));
            }
        });

        entitySavingsStore.addHourlyStats(stats);
    }

    @VisibleForTesting
    long getCurrentTime() {
        return System.currentTimeMillis();
    }

    private String formatDateTime(Calendar calendar) {
        final Date date = calendar.getTime();
        final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateTimeFormat.format(date);
    }
}
