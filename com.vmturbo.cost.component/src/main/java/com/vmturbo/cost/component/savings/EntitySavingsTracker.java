package com.vmturbo.cost.component.savings;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final EntityStateStore entityStateStore;

    private final SavingsCalculator savingsCalculator;

    private final AuditLogWriter auditLogWriter;

    private Calendar lastPeriodEndTime;

    private final TimeZone timeZone;

    private final int chunkSize;

    /**
     * Constructor.
     *
     * @param entitySavingsStore entitySavingsStore
     * @param entityEventsJournal entityEventsJournal
     * @param entityStateStore Persistent state store.
     * @param clock clock
     * @param chunkSize chunkSize for database batch operations
     * @param auditLogWriter Audit log writer helper.
     */
    EntitySavingsTracker(@Nonnull EntitySavingsStore entitySavingsStore,
                         @Nonnull EntityEventsJournal entityEventsJournal,
                         @Nonnull EntityStateStore entityStateStore,
                         @Nonnull final Clock clock,
                         @Nonnull AuditLogWriter auditLogWriter,
                         final int chunkSize) {
        this.entitySavingsStore = Objects.requireNonNull(entitySavingsStore);
        this.entityEventsJournal = Objects.requireNonNull(entityEventsJournal);
        this.entityStateStore = Objects.requireNonNull(entityStateStore);
        this.savingsCalculator = new SavingsCalculator();
        timeZone = TimeZone.getTimeZone(clock.getZone());
        this.auditLogWriter = auditLogWriter;
        this.chunkSize = chunkSize;
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.
     *
     * @return List of times to the hour mark for which we have wrote stats this time, so these
     *      hours are now eligible for daily and monthly rollups, if applicable.
     */
    List<Long> processEvents() {
        return processEvents(getCurrentTime());
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.  Processing will stop at the supplied time.
     *
     * @param end current time in milliseconds indicating when to stop generating savings entries.
     * @return List of times to the hour mark for which we have wrote stats this time, so these
     *      hours are now eligible for daily and monthly rollups, if applicable.
     */
    @Nonnull
    List<Long> processEvents(long end) {
        logger.debug("Processing savings/investment.");

        Calendar periodStartTime = getPeriodStartTime();
        if (periodStartTime == null) {
            logger.debug("There are no events in event journal and there are no states in states map. "
                    + "Events tracker has nothing to process.");
            return Collections.emptyList();
        }

        final List<Long> hourlyStatsTimes = new ArrayList<>();
        // Set period end time to 1 hour after start time.
        Calendar periodEndTime = Calendar.getInstance(timeZone);
        periodEndTime.setTime(periodStartTime.getTime());
        periodEndTime.add(Calendar.HOUR_OF_DAY, 1);

        // There should not be any events before period start time left in the journal.
        // If for some reasons old events are left in the journal, remove them.
        List<SavingsEvent> events =
                entityEventsJournal.removeEventsBetween(0, periodStartTime.getTimeInMillis());
        if (events.size() > 0) {
            logger.warn("There are {} in the events journal that have timestamps before period start time of {}.",
                    events.size(), formatDateTime(periodStartTime));
        }

        try {
            while (periodEndTime.getTimeInMillis() < end) {
                // Read from entity event journal.
                final long startTime = periodStartTime.getTimeInMillis();
                final long endTime = periodEndTime.getTimeInMillis();
                events = entityEventsJournal.removeEventsBetween(startTime, endTime);

                // Get all entity IDs from the events
                Set<Long> entityIds = events.stream()
                        .map(SavingsEvent::getEntityId)
                        .collect(Collectors.toSet());

                logger.info("Process {} events for {} entities between {} ({}) & {} ({}).",
                        events.size(), entityIds.size(), startTime, formatDateTime(periodStartTime),
                        endTime, formatDateTime(periodEndTime));

                // Get states for these entities from the state map (if they exist).
                Map<Long, EntityState> entityStates = entityStateStore.getEntityStates(entityIds);
                entityStates.putAll(entityStateStore.getUpdatedEntityStates());

                // Clear the updated_by_event flags
                entityStateStore.clearUpdatedFlags();

                // Invoke calculator
                savingsCalculator.calculate(entityStates, events, startTime, endTime);

                // Update entity states. Also insert new states to track new entities.
                entityStateStore.updateEntityStates(entityStates);

                auditLogWriter.write(events);
                try {
                    // create stats records from state map for this period.
                    generateStats(startTime);
                    hourlyStatsTimes.add(startTime);
                } catch (EntitySavingsException e) {
                    logger.error("Error occurred when Entity Savings Tracker writes stats to entity savings store. "
                            + "Start time: {} End time: {}", startTime, endTime, e);
                    // Stop processing and don't update the last period end time.
                    break;
                }
                // We delete inactive entity state after the stats for the entity have been flushed
                // a final time.
                Set<Long> statesToRemove = entityStates.values().stream()
                        .filter(EntityState::isDeletePending)
                        .map(EntityState::getEntityId)
                        .collect(Collectors.toSet());
                entityStateStore.deleteEntityStates(statesToRemove);

                // Save the period end time so we won't need to get it from DB next time the tracker runs.
                lastPeriodEndTime = Calendar.getInstance(timeZone);
                lastPeriodEndTime.setTimeInMillis(periodEndTime.getTimeInMillis());

                // Advance time period by 1 hour.
                periodStartTime.add(Calendar.HOUR_OF_DAY, 1);
                periodEndTime.add(Calendar.HOUR_OF_DAY, 1);
            }
        } catch (EntitySavingsException e) {
            logger.error("Operation error in entity state store.", e);
        }

        logger.debug("Savings/investment processing complete for {} hourly times.",
                hourlyStatsTimes);
        return hourlyStatsTimes;
    }

    /**
     * Set the last period end time.  This is used by the event injector to set the end time to
     * null, which will force the savings tracker to recalculate the period start time.  Doing this
     * enables processing of events with older timestamps.
     *
     * @param endTime Calendar representing the new lastPeriodEndTime.  Can be set to null to force
     *                recalculation of the period start time.
     */
    public void setLastPeriodEndTime(@Nullable Calendar endTime) {
        this.lastPeriodEndTime = endTime;
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
        Calendar periodStartTime = Calendar.getInstance(timeZone);
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
        // Use try with resource here because the stream implementation uses an open cursor that
        // need to be closed.
        try (Stream<EntityState> stateStream = entityStateStore.getAllEntityStates()) {
            stateStream.forEach(state -> {
                stats.addAll(stateToStats(state, statTime));
                if (stats.size() >= chunkSize) {
                    try {
                        entitySavingsStore.addHourlyStats(stats);
                    } catch (EntitySavingsException e) {
                        // Wrap exception in RuntimeException and rethrow because it is within a lambda.
                        throw new RuntimeException(e);
                    }
                    stats.clear();
                }
            });
            if (!stats.isEmpty()) {
                // Flush partial chunk
                entitySavingsStore.addHourlyStats(stats);
            }
        } catch (RuntimeException e) {
            if (e.getCause() instanceof EntitySavingsException) {
                throw new EntitySavingsException("Error occurred when adding stats to database.", e.getCause());
            }
            throw e;
        }
    }

    private Set<EntitySavingsStats> stateToStats(@Nonnull EntityState state, long statTime) {
        Set<EntitySavingsStats> stats = new HashSet<>();
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
        return stats;
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
