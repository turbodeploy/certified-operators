package com.vmturbo.cost.component.savings.calculator;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;

/**
 * Savings calculation using the bill and action chain.
 */
public class Calculator {
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private final Logger logger = LogManager.getLogger();
    private final long deleteActionExpiryMs;
    private final Clock clock;

    /**
     * Constructor.
     *
     * @param deleteActionExpiryMs Volume expiry time in milliseconds
     * @param clock clock
     */
    public Calculator(long deleteActionExpiryMs, Clock clock) {
        this.deleteActionExpiryMs = deleteActionExpiryMs;
        this.clock = clock;
    }

    /**
     * Calculate savings and investment of an entity.
     *
     * @param entityOid OID of the entity
     * @param billRecords bill records of an entity. Records can be for more than 1 day. For the day
     *                    that the entity has at least one new or updated record, all bill records
     *                    for the entity on that day must be included.
     * @param actionChain all actions that were executed on this entity that lead up to current
     *                    provider. Assume action chain is valid.
     * @param lastProcessedDate the timestamp of the day that was last processed for savings
     * @param periodEndTime end of the processing period. Normally it is the current time.
     * @return result of the calculation
     */
    @Nonnull
    public List<SavingsValues> calculate(long entityOid, @Nonnull Set<BillingRecord> billRecords,
            @Nonnull NavigableSet<ExecutedActionsChangeWindow> actionChain, long lastProcessedDate,
            @Nonnull LocalDateTime periodEndTime) {
        if (actionChain.isEmpty()) {
            logger.error("Savings calculator invoked for an entity {} with no actions.",
                    entityOid);
            return Collections.emptyList();
        }

        // Make sure all actions in the action chain are for the same entity.
        boolean actionChainInvalid = actionChain.stream().map(a -> {
            try {
                return ActionDTOUtil.getPrimaryEntity(a.getActionSpec().getRecommendation()).getId();
            } catch (UnsupportedActionException e) {
                return 0L;
            }
        }).anyMatch(x -> x != entityOid);
        if (actionChainInvalid) {
            logger.error("Action chain contains actions that are not for entity with OID {}.",
                    entityOid);
            return Collections.emptyList();
        }

        // Create the high-low graph from the action chain.
        SavingsGraph savingsGraph = new SavingsGraph(actionChain, deleteActionExpiryMs);
        logger.debug(() -> savingsGraph);

        // Create a map of bill records associated for each day.
        final Map<LocalDateTime, Set<BillingRecord>> billRecordsByDay = new HashMap<>();
        billRecords.forEach(r -> {
            LocalDateTime recordTime = r.getSampleTime().truncatedTo(ChronoUnit.DAYS);
            billRecordsByDay.computeIfAbsent(recordTime, t -> new HashSet<>()).add(r);
        });

        final List<SavingsValues> results = new ArrayList<>();
        for (Entry<LocalDateTime, Set<BillingRecord>> dailyRecords: billRecordsByDay.entrySet()) {
            results.add(calculateDay(entityOid, dailyRecords.getKey(), savingsGraph,
                    dailyRecords.getValue()));
        }

        // Calculate savings for delete actions.
        // Start on the day after a delete action, there will not be bill records for the deleted
        // entity. We want to claim savings for the delete action every day after the action until
        // the action expires.
        if (actionChain.last().getActionSpec().getRecommendation().getInfo().hasDelete()
                && actionChain.last().getActionSpec().getExecutionStep().hasCompletionTime()) {
            logger.debug("Delete action exists for entity OID {}.", entityOid);
            // Start processing on the day after the last processed day.
            LocalDateTime processDay = TimeUtil.millisToLocalDateTime(lastProcessedDate, clock).plusDays(1);
            // We expect a bill record for the deleted entity for the day on which the delete action
            // was executed. So start on the day after the delete action execution if start day is
            // before the delete action execution day.
            long deleteActionTime = actionChain.last().getActionSpec().getExecutionStep().getCompletionTime();
            LocalDateTime deleteActionDateTime = TimeUtil.millisToLocalDateTime(deleteActionTime, clock);
            if (processDay.isBefore(deleteActionDateTime)) {
                // Start processing on the day after the day when the delete action was executed.
                processDay = deleteActionDateTime.plusDays(1).truncatedTo(ChronoUnit.DAYS);
            }
            final LocalDateTime periodEnd = periodEndTime.truncatedTo(ChronoUnit.DAYS);
            final LocalDateTime volumeActionExpiryDate =
                    TimeUtil.millisToLocalDateTime(deleteActionTime + deleteActionExpiryMs, clock);
            // Process savings from the first day that require processing to period end time and
            // before the volume action expires. Period end time is normally the current time.
            // If we run a test scenario, the period end time can be set to a specific date.
            // We stop accruing savings for delete actions after a predefined period of time. (e.g. 1 year)
            while (processDay.isBefore(periodEnd) && processDay.isBefore(volumeActionExpiryDate)) {
                results.add(calculateDay(entityOid, processDay, savingsGraph, Collections.emptySet()));
                processDay = processDay.plusDays(1);
            }
        }
        return results;
    }

    /**
     * Calculate the savings/investments of an entity for a specific day.
     *
     * @param entityOid entity OID
     * @param date The date (the beginning of the day at 00:00)
     * @param savingsGraph the watermark graph
     * @param billRecords bill records
     * @return savings/investments of this day
     */
    @Nonnull
    private SavingsValues calculateDay(long entityOid, LocalDateTime date,
            SavingsGraph savingsGraph, Set<BillingRecord> billRecords) {
        logger.debug("Calculating savings for entity {} for date {}.", entityOid, date);
        // Use the high-low graph and the bill records to determine the billing segments in the day.
        List<Segment> segments = createBillingSegments(
                date.toInstant(ZoneOffset.UTC).toEpochMilli(), savingsGraph);
        if (logger.isDebugEnabled()) {
            StringBuilder debugText = new StringBuilder();
            debugText.append("Number of segments: ").append(segments.size());
            debugText.append("\n=== Segments ===\n");
            segments.forEach(s -> debugText.append(s).append("\n"));
            logger.debug(debugText);
        }

        // For each segment, calculate the savings/investment.
        double totalDailySavings = 0;
        double totalDailyInvestments = 0;
        for (Segment segment : segments) {
            if (segment.actionDataPoint instanceof ScaleActionDataPoint) {
                ScaleActionDataPoint dataPoint = (ScaleActionDataPoint)segment.actionDataPoint;
                long providerOid = dataPoint.getDestinationProviderOid();
                // Get all bill records of this provider and sum up the cost
                Set<BillingRecord> recordsForProvider = billRecords.stream()
                        .filter(r -> r.getProviderId() == providerOid).collect(Collectors.toSet());
                double costForProvider = recordsForProvider.stream()
                        .map(BillingRecord::getCost)
                        .reduce(0d, Double::sum);
                double usageAmount = getUsageAmount(recordsForProvider);
                double investments = Math.max(0,
                        (costForProvider - dataPoint.getLowWatermark() * usageAmount) * segment.multiplier);
                double savings = calculateSavings(billRecords, dataPoint, costForProvider, usageAmount, segment.multiplier);
                totalDailyInvestments += investments;
                totalDailySavings += savings;
            } else if (segment.actionDataPoint instanceof DeleteActionDataPoint) {
                DeleteActionDataPoint deleteActionDataPoint = (DeleteActionDataPoint)segment.actionDataPoint;
                double savingsPerHour = deleteActionDataPoint.savingsPerHour();
                totalDailySavings += savingsPerHour * TimeUnit.MILLISECONDS.toHours(segment.duration);
            }
        }

        // Return the total savings/investment for the day.
        return new SavingsValues.Builder()
                .savings(totalDailySavings)
                .investments(totalDailyInvestments)
                .timestamp(date)
                .entityOid(entityOid)
                .build();
    }

    private double calculateSavings(Set<BillingRecord> billRecords, ScaleActionDataPoint dataPoint,
            final double costForProvider, double usageAmount, double multiplier) {
        double adjustedCostOfProvider = costForProvider;
        // check if the entity is covered by RI on the day
        boolean isEntityRICovered = billRecords.stream()
                .anyMatch(r -> r.getPriceModel() == PriceModel.RESERVED);
        if (isEntityRICovered && !dataPoint.isCloudCommitmentExpectedAfterAction()) {
            // The entity is covered by RI, but the action didn't expect it to be covered.
            if (dataPoint.isSavingsExpectedAfterAction()) {
                // If it was a scale-down (efficiency) action, savings cannot be more than
                // the difference between the high watermark and the on-demand cost of the destination tier.
                // e.g. scale from 5 -> 3, savings cannot be more than $2. In this example,
                // adjustedCostOfProvider is 3.
                adjustedCostOfProvider = Math.max(dataPoint.getDestinationOnDemandCost() * usageAmount, costForProvider);
            } else {
                // If last action was a scale-up (performance) action, we cannot claim
                // savings for this action.
                // e.g. scale from 3 -> 5, cost of provider cannot be less than 3.
                // Note: Before action cost includes RI discount if present.
                adjustedCostOfProvider = Math.max(dataPoint.getBeforeActionCost() * usageAmount, costForProvider);
            }
        }
        return Math.max(0, (dataPoint.getHighWatermark() * usageAmount - adjustedCostOfProvider) * multiplier);
    }

    /**
     * Get the usage amount from the bill for a given provider. Usage amount is the time (in hours)
     * the entity spent on this provider.
     *
     * @param recordsForProvider set of bill records for a provider for an entity for one day
     * @return usage amount (in hours)
     */
    private static double getUsageAmount(Set<BillingRecord> recordsForProvider) {
        int entityType = recordsForProvider.stream()
                .findAny()
                .map(BillingRecord::getEntityType)
                .orElse(EntityType.UNKNOWN_VALUE);
        double usageAmount = recordsForProvider.stream()
                    .map(BillingRecord::getUsageAmount)
                    .reduce(0d, Double::sum);
        // For azure volumes, the time unit for usage amount is expressed as the fraction of a month.
        // 730 is the number of hours in a month. May need a more precise number if the bill
        // considers the actual number of hours in the month. e.g. January has 31 days and longer
        // than February.
        // TODO Normalize all usage amount to hourly in the probe.
        if (entityType == EntityType.VIRTUAL_VOLUME_VALUE) {
            // Cap value to 24 (number of hours in a day).
            usageAmount = Math.min(24, 730 * usageAmount);
        }
        return usageAmount;
    }

    private List<Segment> createBillingSegments(long datestamp, SavingsGraph savingsGraph) {
        long segmentStart = datestamp;
        long segmentEnd;

        // Find all watermark data points on this day
        final SortedSet<ActionDataPoint> dataPointsInDay = savingsGraph.getDataPointsInDay(datestamp);
        // Find data point with timestamp at or before the start of the day.
        ActionDataPoint dataPoint = savingsGraph.getDataPoint(datestamp);
        boolean noActionBeforeStartOfDay = false;

        if (dataPoint == null) {
            if (!dataPointsInDay.isEmpty()) {
                noActionBeforeStartOfDay = true;
                dataPoint = dataPointsInDay.first();
                segmentStart = dataPoint.getTimestamp();
            } else {
                // No action happened before this day or on this day. Should not happen.
                logger.error("When creating segments for {}, found no watermark data points at or "
                        + "before this time, and there are no other actions executed on this day.",
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(datestamp), ZoneOffset.UTC));
                return Collections.emptyList();
            }
        }

        final List<Segment> segments = new ArrayList<>();
        // Provider OID mapped to a list segment durations (in milliseconds).
        final Map<Long, List<Long>> segmentDurationMap = new HashMap<>();

        // If there are one or more actions executed on this day, loop through the points.
        for (ActionDataPoint actionDatapoint : dataPointsInDay) {
            // If first point is at exactly the beginning of day (edge case), or no actions were
            // executed before start of the day, skip to next point.
            if (actionDatapoint.getTimestamp() == datestamp || noActionBeforeStartOfDay) {
                continue;
            }
            segmentEnd = actionDatapoint.getTimestamp();
            final long segmentDuration = segmentEnd - segmentStart;
            // Put segment in a list and add duration in map.
            segments.add(new Segment(segmentDuration, dataPoint));
            segmentDurationMap.computeIfAbsent(dataPoint.getDestinationProviderOid(), d -> new ArrayList<>())
                    .add(segmentDuration);
            dataPoint = actionDatapoint;
            segmentStart = actionDatapoint.getTimestamp();
        }

        // Close the last segment.
        segmentEnd = datestamp + MILLIS_IN_DAY;
        final long segmentDuration = segmentEnd - segmentStart;
        segments.add(new Segment(segmentDuration, dataPoint));
        // The check for duration is non-zero is a defensive logic which should not happen because
        // value of segmentStart comes from action times which excludes the end of the day.
        if (segmentDuration > 0) {
            segmentDurationMap.computeIfAbsent(dataPoint.getDestinationProviderOid(),
                    d -> new ArrayList<>()).add(segmentDuration);
        }

        // Loop through the segment list and assign value for the multiplier.
        // For each segment, find the duration of all segments of this provider from the map.
        // e.g. p1 -> 3hr, 6hr
        // multiplier for first segment = 3 / (3 + 6)
        for (Segment segment : segments) {
            final Long sum = segmentDurationMap.get(segment.actionDataPoint.getDestinationProviderOid()).stream()
                    .reduce(0L, Long::sum);
            segment.multiplier = (double)segment.duration / (double)sum;
        }

        return segments;
    }

    /**
     * A segment is a period of time in a day when the entity is on the same provider.
     */
    private static class Segment {
        private final long duration;
        private final ActionDataPoint actionDataPoint;
        private double multiplier;

        Segment(long duration, ActionDataPoint actionDataPoint) {
            this.duration = duration;
            this.actionDataPoint = actionDataPoint;
        }

        @Override
        public String toString() {
            return "Segment{" + "duration(hours)=" + TimeUnit.MILLISECONDS.toHours(duration)
                    + ", actionDataPoint=" + actionDataPoint + ", multiplier=" + multiplier + '}';
        }
    }
}
