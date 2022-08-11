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
import java.util.TreeMap;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * Savings calculation using the bill and action chain.
 */
public class Calculator {
    private static final long MILLIS_IN_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    // Cost categories of bill records to be excluded from the analysis of time spent on a provider.
    private static final Set<CostCategory> COST_CATEGORIES_EXCLUDE = Collections.singleton(CostCategory.LICENSE);

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
     * @param billRecords bill records of this day
     * @return savings/investments of this day
     */
    @Nonnull
    private SavingsValues calculateDay(long entityOid, LocalDateTime date,
            SavingsGraph savingsGraph, Set<BillingRecord> billRecords) {
        logger.debug("Calculating savings for entity {} for date {}.", entityOid, date);
        // Use the high-low graph and the bill records to determine the billing segments in the day.
        List<Segment> segments = createBillingSegments(
                date.toInstant(ZoneOffset.UTC).toEpochMilli(), savingsGraph, billRecords);
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
                double costForProviderInSegment = recordsForProvider.stream()
                        .map(r -> r.getCost() * segment.commTypeToMultiplierMap.getOrDefault(r.getCommodityType(), 1.0))
                        .reduce(0d, Double::sum);
                double investments = Math.max(0,
                        costForProviderInSegment - dataPoint.getLowWatermark() * TimeUnit.MILLISECONDS.toHours(segment.duration));
                double savings = calculateSavings(billRecords, dataPoint, costForProviderInSegment, segment.duration);
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
            final double costForProviderInSegment, long segmentDurationMillis) {
        double segmentDurationHours = TimeUnit.MILLISECONDS.toHours(segmentDurationMillis);
        double adjustedCostOfProvider = costForProviderInSegment;
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
                adjustedCostOfProvider = Math.max(dataPoint.getDestinationOnDemandCost() * segmentDurationHours, costForProviderInSegment);
            } else {
                // If last action was a scale-up (performance) action, we cannot claim
                // savings for this action.
                // e.g. scale from 3 -> 5, cost of provider cannot be less than 3.
                // Note: Before action cost includes RI discount if present.
                adjustedCostOfProvider = Math.max(dataPoint.getBeforeActionCost() * segmentDurationHours, costForProviderInSegment);
            }
        }
        return Math.max(0, (dataPoint.getHighWatermark() * segmentDurationHours - adjustedCostOfProvider));
    }

    private List<Segment> createBillingSegments(long datestamp, SavingsGraph savingsGraph,
            Set<BillingRecord> billRecords) {
        long segmentStart = datestamp;
        long segmentEnd;

        // Find all watermark data points on this day
        final SortedSet<ActionDataPoint> dataPointsInDay = savingsGraph.getDataPointsInDay(datestamp);
        // Find data point with timestamp at or before the start of the day.
        ActionDataPoint dataPoint = savingsGraph.getDataPoint(datestamp);
        boolean noActionBeforeStartOfDay = false;

        if (dataPoint == null) {
            // There were no actions at or before beginning of this day.
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
        // If there are one or more actions executed on this day, loop through the points.
        for (ActionDataPoint actionDatapoint : dataPointsInDay) {
            // If first point is at exactly the beginning of day (edge case), or no actions were
            // executed before start of the day, skip to next point.
            if (actionDatapoint.getTimestamp() == datestamp || noActionBeforeStartOfDay) {
                noActionBeforeStartOfDay = false;
                continue;
            }
            segmentEnd = actionDatapoint.getTimestamp();
            final long segmentDuration = segmentEnd - segmentStart;
            // Put segment in a list and add duration in map.
            segments.add(new Segment(segmentDuration, dataPoint));
            dataPoint = actionDatapoint;
            segmentStart = actionDatapoint.getTimestamp();
        }

        // Close the last segment.
        segmentEnd = datestamp + MILLIS_IN_DAY;
        final long segmentDuration = segmentEnd - segmentStart;
        segments.add(new Segment(segmentDuration, dataPoint));

        assignDurationAndMultiplier(segments, billRecords, datestamp, dataPointsInDay);

        return segments;
    }

    /**
     * When there are more than one action on an entity on a day and one action changes a commodity
     * value but the other action does not, we need to know the capacity of the commodity when
     * processing the second action so that we can calculate the multiplier accurately.
     *
     * @param datestamp timestamp of the beginning of the day.
     * @param dataPointsInDay All action data points for this day.
     * @return a map of commodity type to a tree map that maps timestamp to commodity capacity.
     */
    @Nonnull
    private Map<Integer, TreeMap<Long, Double>> getCommodityCapacityMap(long datestamp,
            SortedSet<ActionDataPoint> dataPointsInDay) {
        Map<Integer, TreeMap<Long, Double>> capacityMap = new TreeMap<>();
        for (ActionDataPoint action : dataPointsInDay) {
            if (!(action instanceof ScaleActionDataPoint)) {
                continue;
            }
            ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)action;
            for (CommodityResize commodityResize : scaleActionDataPoint.getCommodityResizes()) {
                int commType = commodityResize.getCommodityType();
                if (!capacityMap.containsKey(commType)) {
                    capacityMap.put(commType, new TreeMap<>());
                    capacityMap.get(commType).put(datestamp, commodityResize.getOldCapacity());
                }
                capacityMap.get(commType).put(action.getTimestamp(), commodityResize.getNewCapacity());
            }
        }
        return capacityMap;
    }

    /**
     * Set the duration for each segment and set the multiplier for each commodity type in each segment.
     *
     * @param segments all segments in the day
     * @param billRecords all bill records for the day
     */
    private void assignDurationAndMultiplier(List<Segment> segments, Set<BillingRecord> billRecords,
            long startOfDay, SortedSet<ActionDataPoint> dataPointsInDay) {
        Map<Long, List<Segment>> segmentsByProvider = segments.stream()
                .collect(Collectors.groupingBy(s -> s.getActionDataPoint().getDestinationProviderOid()));
        for (Entry<Long, List<Segment>> providerSegmentsEntry : segmentsByProvider.entrySet()) {
            long providerId = providerSegmentsEntry.getKey();
            // Get a map of commodity type to bill records for this provider for this day.
            Map<Integer, List<BillingRecord>> recordsByCommType = billRecords.stream()
                    .filter(r -> r.getProviderId() == providerId)
                    .collect(Collectors.groupingBy(BillingRecord::getCommodityType));

            // If there are no bill records for the specified provider, the segment duration is 0
            // because this segment is not billed. The exception is for delete actions which accrue
            // savings by not getting billed.
            if (recordsByCommType.isEmpty()) {
                for (Segment segment : providerSegmentsEntry.getValue()) {
                    if (!(segment.getActionDataPoint() instanceof DeleteActionDataPoint)) {
                        segment.duration = 0;
                    }
                }
            }

            // If the first segment for the day is for this provider and it did not start from
            // the beginning of the day and the provider before the first segment is the same as
            // the first segment, we need to account for the time and cost incurred
            // before the first segment.
            long providerRemoveUsageBeforeFirstSegment = 0;
            if (!segments.isEmpty() && segments.get(0).getActionDataPoint() instanceof ScaleActionDataPoint) {
                ScaleActionDataPoint firstSegment = (ScaleActionDataPoint)segments.get(0).getActionDataPoint();
                if (firstSegment.getTimestamp() > startOfDay
                        && providerId == firstSegment.getDestinationProviderOid()
                        && firstSegment.getSourceProviderOid() == firstSegment.getDestinationProviderOid()) {
                    providerRemoveUsageBeforeFirstSegment = firstSegment.getSourceProviderOid();
                }
            }

            long totalTimeOnProvider = 0;
            double timeBeforeFirstSegment = 0;
            for (Entry<Integer, List<BillingRecord>> commTypeEntry : recordsByCommType.entrySet()) {
                List<BillingRecord> billingRecordsForCommType = commTypeEntry.getValue();
                if (billingRecordsForCommType.isEmpty()) {
                    continue;
                }

                // Get total amount billed for this commodity and for this provider by adding the
                // costs in the corresponding bill records. Exclude LICENSE costs which accompany
                // compute costs because the time for the VM is already included in the on-demand
                // and reserved costs. Including time from LICENSE records will double count the time.
                double totalUsageBilled = billingRecordsForCommType.stream()
                        .filter(r -> !COST_CATEGORIES_EXCLUDE.contains(r.getCostCategory()))
                        .map(BillingRecord::getUsageAmount)
                        .reduce(0d, Double::sum);

                // usageRemaining variable is for tracking how much billed time has been allocated
                // to segment. e.g. If the bill shows usageAmount is 15 hours and this provider has
                // 2 segments on this day. According to the actions, both segments is 10 hours long.
                // The first segment will take the full 10 hours (usageRemaining = 15 - 10 = 5).
                // The second segment cannot assume the whole 10 hours because there are only 5
                // billed hours remaining. In this case, adjust the second segment to 5 hours.
                double usageRemaining = totalUsageBilled;
                Integer commType = commTypeEntry.getKey();
                boolean isFirstSegmentForProvider = true;
                for (Segment segment : providerSegmentsEntry.getValue()) {
                    if (commType == CommodityType.UNKNOWN_VALUE) {
                        // UsageAmount is TIME.
                        double totalUsageBilledMillis = totalUsageBilled * MILLIS_IN_HOUR;
                        double usageRemainingMillis = usageRemaining * MILLIS_IN_HOUR;
                        if (usageRemainingMillis - segment.duration < 0) {
                            segment.duration = Double.valueOf(usageRemainingMillis).longValue();
                        }
                        segment.commTypeToMultiplierMap.put(commType, segment.duration / totalUsageBilledMillis);
                        usageRemaining -= segment.duration / (double)MILLIS_IN_HOUR;
                    } else {
                        // UsageAmount is commodity quantity times TIME.
                        Map<Integer, TreeMap<Long, Double>> commCapMap = getCommodityCapacityMap(startOfDay, dataPointsInDay);
                        if (segment.getActionDataPoint() instanceof ScaleActionDataPoint) {
                            ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)segment.getActionDataPoint();
                            if (commCapMap.get(commType) == null) {
                                // This commType is not changed on this day.
                                // We use the proportion of the lengths of the segments to assign the multiplier.
                                // If the bill record does not include the charge for the full day yet,
                                // this multiplier can be inaccurate, but the value will be corrected
                                // once the bill is updated.
                                if (totalTimeOnProvider == 0) {
                                    totalTimeOnProvider = providerSegmentsEntry.getValue().stream()
                                            .map(Segment::getDuration).reduce(0L, Long::sum);
                                    // Account for the time before the first segment if it is the
                                    // first segment (of any provider) for the day and it does not
                                    // start at the beginning of the day.
                                    if (providerRemoveUsageBeforeFirstSegment == providerId) {
                                        timeBeforeFirstSegment = (segments.get(0).getActionDataPoint().getTimestamp() - startOfDay);
                                        totalTimeOnProvider += timeBeforeFirstSegment;
                                    }
                                }
                                double multiplier = totalTimeOnProvider != 0
                                        ? (double)segment.duration / (double)totalTimeOnProvider : 1;
                                segment.commTypeToMultiplierMap.put(commType, multiplier);
                                usageRemaining -= multiplier * billingRecordsForCommType.stream()
                                        .map(BillingRecord::getUsageAmount)
                                        .reduce(0d, Double::sum);
                                if (isFirstSegmentForProvider && providerRemoveUsageBeforeFirstSegment == providerId) {
                                    usageRemaining -= timeBeforeFirstSegment / totalTimeOnProvider * billingRecordsForCommType.stream().map(
                                            BillingRecord::getUsageAmount).reduce(0d, Double::sum);
                                }
                            } else {
                                long segmentStartTime = Math.max(scaleActionDataPoint.getTimestamp(), startOfDay);
                                if (isFirstSegmentForProvider && providerRemoveUsageBeforeFirstSegment == providerId) {
                                    double oldCapacity = commCapMap.get(commType).lowerEntry(segmentStartTime).getValue();
                                    usageRemaining -= oldCapacity
                                            * TimeUnit.MILLISECONDS.toHours(segment.getActionDataPoint().getTimestamp() - startOfDay);
                                }
                                double newCapacity = commCapMap.get(commType).floorEntry(segmentStartTime).getValue();
                                double quantityTimesHours = newCapacity * segment.duration / MILLIS_IN_HOUR;
                                if (usageRemaining - quantityTimesHours < 0) {
                                    segment.duration = Double.valueOf(usageRemaining / newCapacity).longValue() * MILLIS_IN_HOUR;
                                    quantityTimesHours = newCapacity * segment.duration / MILLIS_IN_HOUR;
                                }
                                segment.commTypeToMultiplierMap.put(commType, quantityTimesHours / totalUsageBilled);
                                usageRemaining -= quantityTimesHours;
                            }
                        } else {
                            segment.commTypeToMultiplierMap.put(commType, 1.0);
                        }
                    }
                    isFirstSegmentForProvider = false;
                }
            }
        }
    }

    /**
     * A segment is a period of time in a day when the entity is on the same provider.
     */
    private static class Segment {
        private long duration;
        private final ActionDataPoint actionDataPoint;
        private final Map<Integer, Double> commTypeToMultiplierMap = new HashMap<>();

        Segment(long duration, ActionDataPoint actionDataPoint) {
            this.duration = duration;
            this.actionDataPoint = actionDataPoint;
        }

        /**
         * Get duration of the segment in milliseconds.
         *
         * @return segment duration.
         */
        public long getDuration() {
            return duration;
        }

        /**
         * Get the action that correspond to this segment.
         *
         * @return action datapoint
         */
        public ActionDataPoint getActionDataPoint() {
            return actionDataPoint;
        }

        @Override
        public String toString() {
            return "Segment{" + "duration(hours)=" + TimeUnit.MILLISECONDS.toHours(duration)
                    + ", actionDataPoint=" + actionDataPoint + ", commTypeToMultiplierMap=" + commTypeToMultiplierMap + '}';
        }
    }
}
