package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.CloudSavingsAvailable;
import com.vmturbo.commons.Units;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;

/**
 * This class implements the task that is executed periodically (once an hour) to process entity
 * events and create savings and investment statistics.
 */
class EntitySavingsProcessor {

    private TopologyEventsPoller topologyEventsPoller;

    private EntitySavingsTracker entitySavingsTracker;

    private RollupSavingsProcessor rollupProcessor;

    private final EntitySavingsStore entitySavingsStore;

    private final EntityEventsJournal entityEventsJournal;

    private final Clock clock;

    private final DataRetentionProcessor dataRetentionProcessor;

    private final CostNotificationSender costNotificationSender;

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param entitySavingsTracker entitySavingsTracker
     * @param topologyEventsPoller topologyEventsPoller
     * @param rollupProcessor For rolling up savings records.
     * @param entitySavingsStore entity savings store
     * @param entityEventsJournal entity events journal
     * @param clock clock
     * @param dataRetentionProcessor stats retention processor.
     * @param notificationSender For sending out savings broadcast notification.
     */
    EntitySavingsProcessor(@Nonnull EntitySavingsTracker entitySavingsTracker,
            @Nonnull TopologyEventsPoller topologyEventsPoller,
            @Nonnull RollupSavingsProcessor rollupProcessor,
            @Nonnull EntitySavingsStore entitySavingsStore,
            @Nonnull EntityEventsJournal entityEventsJournal,
            @Nonnull final Clock clock,
            @Nonnull final DataRetentionProcessor dataRetentionProcessor,
            @Nonnull final CostNotificationSender notificationSender) {
        this.topologyEventsPoller = topologyEventsPoller;
        this.entitySavingsTracker = entitySavingsTracker;
        this.rollupProcessor = rollupProcessor;
        this.entitySavingsStore = Objects.requireNonNull(entitySavingsStore);
        this.entityEventsJournal = Objects.requireNonNull(entityEventsJournal);
        this.clock = clock;
        this.dataRetentionProcessor = dataRetentionProcessor;
        this.costNotificationSender = notificationSender;
    }

    /**
     * This method is invoked once an hour.
     */
    void execute() {
        logger.info("START: Processing savings/investment.");
        try {
            LocalDateTime startTime = getPeriodStartTime();
            LocalDateTime endTime = getCurrentDateTime().truncatedTo(ChronoUnit.HOURS);

            // If there is less than 1 hour of data, there is no need to proceed
            // with processing.
            if (startTime.isEqual(endTime) || startTime.isAfter(endTime)) {
                logger.info("Not processing savings because we don't have 1 hour of data.");
                return;
            }

            // TEP requires the latest topology broadcast should not be done within the within start
            // and end time. i.e. we need one topology broadcast that happen after the end time.
            // If we don't have a topology broadcast after the end time, we move the end time backwards
            // by 1 hour.
            while (!topologyEventsPoller.isTopologyBroadcasted(endTime)) {
                endTime = endTime.minusHours(1);
                logger.info("Checking topology broadcast status again for polling window {} to {}",
                        startTime, endTime);
                if (startTime.isEqual(endTime) || startTime.isAfter(endTime)) {
                    logger.info("Not processing savings because there is no topology broadcasted.");
                    return;
                }
            }
            logger.info("Suitable Latest topology found, polling window set to {} to {}", startTime,
                    endTime);
            topologyEventsPoller.poll(startTime, endTime);

            logger.info("Invoke EntitySavingsTracker to process events.");
            final List<Long> hourlyStatsTimes = entitySavingsTracker.processEvents(startTime,
                    endTime, Collections.emptySet());

            logger.info("Invoking RollupSavingsProcessor to process rollup.");
            rollupProcessor.process(hourlyStatsTimes);

            logger.info("Invoking data retention processor.");
            dataRetentionProcessor.process(false);

            sendSavingsNotification(hourlyStatsTimes);
            logger.info("END: Processing savings/investment. {} Hourly stats.",
                    hourlyStatsTimes.size());
        } catch (Throwable e) {
            logger.error("Exception was thrown in the EntitySavingsProcessor:", e);
        }
    }

    /**
     * Determine the start time of the first period to be processed.
     * If it is the first time the tracker is executed after the pod started up, get the latest
     * timestamp from the savings stats hourly table.
     * If the table is empty, get the timestamp from the first event from the events journal.
     *
     * @return the start time of the period to be processed.
     */
    @Nonnull
    LocalDateTime getPeriodStartTime() {
        Long maxStatsTime = getLastHourlyStatsTime();
        LocalDateTime periodStartTime;
        if (maxStatsTime != 0) {
            periodStartTime = SavingsUtil.getLocalDateTime(maxStatsTime, clock);
            // Period start time is one hour after the most recent stats record because the
            // timestamp of the stats record represent the start time of an one-hour period.
            periodStartTime = periodStartTime.plusHours(1);
        } else {
            // The stats table is empty.
            // Get earliest event timestamp in events journal.
            Long oldestEventTime = entityEventsJournal.getOldestEventTime();
            if (oldestEventTime != null) {
                periodStartTime = SavingsUtil.getLocalDateTime(oldestEventTime, clock);
            } else {
                // No events in the events journal. i.e. no action events. We will poll TEP events
                // for the last hour.
                periodStartTime = getCurrentDateTime().minusHours(1);
            }
        }
        // Set time to "top of the hour". e.g. if timestamp is 8:05, period start time is 8:00.
        periodStartTime = periodStartTime.truncatedTo(ChronoUnit.HOURS);
        return periodStartTime;
    }

    /**
     * Make the call to get current date time a separate method so unit tests can force it to a
     * specific value.
     *
     * @return now
     */
    @VisibleForTesting
    LocalDateTime getCurrentDateTime() {
        return LocalDateTime.now(clock);
    }

    /**
     * The last rollup time by hour will reflect the newest timestamp of the entity_savings_by_hour
     * table. If there are no records in the table, 0 wil be returned.
     *
     * @return the newest record time in the entity_savings_by_hour table
     */
    private long getLastHourlyStatsTime() {
        LastRollupTimes lastRollupTimes = entitySavingsStore.getLastRollupTimes();
        return lastRollupTimes.getLastTimeByHour();
    }

    /**
     * Sends out broadcast notification about Cloud Savings available.
     *
     * @param hourlyStatsTimes Times for which stats data was written this time.
     */
    private void sendSavingsNotification(@Nonnull final List<Long> hourlyStatsTimes) {
        if (hourlyStatsTimes.isEmpty()) {
            return;
        }
        // Start time is inclusive and end time is exclusive.
        // E.g if hourlyStatsTime is 7:00 PM (only 1 value), then startTime: 7:00 PM, and
        // endTime: 8:00 PM. If hourlyStatsTime is 6:00 PM and 7:00 PM, then startTime: 6:00 PM,
        // endTime: 8:00 PM. All stats including start time (greater than equal to) and
        // excluding (less than) are returned when these times are used later for query.
        hourlyStatsTimes.sort(Long::compare);
        long startTime = hourlyStatsTimes.get(0);
        long endTime = hourlyStatsTimes.get(hourlyStatsTimes.size() - 1);
        endTime += (long)Units.HOUR_MS;

        try {
            costNotificationSender.sendCostNotification(CostNotification.newBuilder()
                    .setCloudSavingsAvailable(CloudSavingsAvailable.newBuilder().setStartDate(
                            startTime).setEndDate(endTime).build())
                    .build());
            logger.info("Sent out Cloud Savings available notification. Start: {}, End: {}.",
                    startTime, endTime);
        } catch (CommunicationException ce) {
            logger.warn("Unable to send out Cloud Savings notification. Start: {}, End: {}.",
                    startTime, endTime, ce);
        } catch (InterruptedException ie) {
            logger.warn("Interrupted on Cloud Savings notification. Start: {}, End: {}: {}",
                    startTime, endTime, ie.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
