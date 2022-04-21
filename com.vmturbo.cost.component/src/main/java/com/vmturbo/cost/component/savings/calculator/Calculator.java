package com.vmturbo.cost.component.savings.calculator;

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

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.cost.component.savings.BillingRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Savings calculation using the bill and action chain.
 */
public class Calculator {
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final Logger logger = LogManager.getLogger();

    private Calculator() {
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
     * @return result of the calculation
     */
    @Nonnull
    public static List<SavingsValues> calculate(long entityOid, Set<BillingRecord> billRecords,
            NavigableSet<ActionSpec> actionChain) {
        if (actionChain.isEmpty() || billRecords.isEmpty()) {
            logger.error("Savings calculator invoked for an entity {} with no actions or has no bill records.",
                    entityOid);
            return Collections.emptyList();
        }

        // Make sure all actions in the action chain are for the same entity.
        boolean actionChainInvalid = actionChain.stream().map(a -> {
            try {
                return ActionDTOUtil.getPrimaryEntity(a.getRecommendation()).getId();
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
        WatermarkGraph watermarkGraph = new WatermarkGraph(actionChain);
        logger.debug(() -> watermarkGraph);

        // Create a map of bill records associated for each day.
        final Map<LocalDateTime, Set<BillingRecord>> billRecordsByDay = new HashMap<>();
        billRecords.forEach(r -> {
            LocalDateTime recordTime = r.getSampleTime().truncatedTo(ChronoUnit.DAYS);
            billRecordsByDay.computeIfAbsent(recordTime, t -> new HashSet<>()).add(r);
        });

        final List<SavingsValues> results = new ArrayList<>();
        for (Entry<LocalDateTime, Set<BillingRecord>> dailyRecords: billRecordsByDay.entrySet()) {
            results.add(calculateDay(entityOid, dailyRecords.getKey(), watermarkGraph,
                    dailyRecords.getValue()));
        }
        return results;
    }

    /**
     * Calculate the savings/investments of an entity for a specific day.
     *
     * @param entityOid entity OID
     * @param date The date
     * @param watermarkGraph the watermark graph
     * @param billRecords bill records
     * @return savings/investments of this day
     */
    @Nonnull
    private static SavingsValues calculateDay(long entityOid, LocalDateTime date,
            WatermarkGraph watermarkGraph, Set<BillingRecord> billRecords) {
        logger.debug("Calculating savings for entity {} for date {}.", entityOid, date);
        // Use the high-low graph and the bill records to determine the billing segments in the day.
        List<Segment> segments = createBillingSegments(
                date.toInstant(ZoneOffset.UTC).toEpochMilli(), watermarkGraph);
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
            Watermark watermark = segment.watermark;
            long providerOid = watermark.getDestinationProviderOid();
            // Get all bill records of this provider and sum up the cost
            Set<BillingRecord> recordsForProvider = billRecords.stream()
                    .filter(r -> r.getProviderId() == providerOid).collect(Collectors.toSet());
            double costOfProvider = recordsForProvider.stream()
                    .map(BillingRecord::getCost)
                    .reduce(0d, Double::sum);
            double usageAmount = getUsageAmount(recordsForProvider);
            double investments = Math.max(0,
                    (costOfProvider - watermark.getLowWatermark() * usageAmount) * segment.multiplier);
            double savings = Math.max(0,
                    (watermark.getHighWatermark() * usageAmount - costOfProvider) * segment.multiplier);
            totalDailyInvestments += investments;
            totalDailySavings += savings;
        }

        // Return the total savings/investment for the day.
        SavingsValues result = new SavingsValues.Builder()
                .savings(totalDailySavings)
                .investments(totalDailyInvestments)
                .timestamp(date)
                .entityOid(entityOid)
                .build();
        logger.debug("Result: {}", result);
        return result;
    }

    /**
     * Get the usage amount from the bill for a given provider. Usage amount is the time (in hours)
     * the entity spent on this provider.
     *
     * @param recordsForProvider set of bill records for a provider for an entity for one day
     * @return usage amount
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
            usageAmount = 730 * usageAmount;
        }
        return usageAmount;
    }

    private static List<Segment> createBillingSegments(long datestamp, WatermarkGraph watermarkGraph) {
        long segmentStart = datestamp;
        long segmentEnd;

        // Find all watermark data points in this day
        final SortedSet<Watermark> dataPointsInDay = watermarkGraph.getDataPointsInDay(datestamp);
        // Find watermark data point with timestamp at or before the start of the day.
        Watermark watermark = watermarkGraph.getWatermark(datestamp);
        boolean noActionBeforeStartOfDay = false;

        if (watermark == null) {
            if (!dataPointsInDay.isEmpty()) {
                noActionBeforeStartOfDay = true;
                watermark = dataPointsInDay.first();
                segmentStart = watermark.getTimestamp();
            } else {
                // No action happened before this day or on this day. Should not happen.
                logger.error("When creating segments for {}, found no watermark data points at or "
                        + "before this time, and there are no other actions executed on this day.",
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(datestamp), ZoneOffset.UTC));
                return Collections.emptyList();
            }
        }

        final List<Segment> segments = new ArrayList<>();
        final Map<Long, List<Long>> segmentDurationMap = new HashMap<>();

        // If there are one or more actions executed on this day, loop through the points.
        for (Watermark datapoint : dataPointsInDay) {
            // If first point is at exactly the beginning of day (edge case), or no actions were
            // executed before start of the day, skip to next point.
            if (datapoint.getTimestamp() == datestamp || noActionBeforeStartOfDay) {
                continue;
            }
            segmentEnd = datapoint.getTimestamp();
            final long segmentDuration = segmentEnd - segmentStart;
            // Put segment in a list and add duration in map.
            segments.add(new Segment(segmentDuration, watermark));
            segmentDurationMap.computeIfAbsent(watermark.getDestinationProviderOid(), d -> new ArrayList<>())
                    .add(segmentDuration);
            watermark = datapoint;
            segmentStart = datapoint.getTimestamp();
        }

        // Close the last segment.
        segmentEnd = datestamp + MILLIS_IN_DAY;
        final long segmentDuration = segmentEnd - segmentStart;
        segments.add(new Segment(segmentDuration, watermark));
        // The check for duration is non-zero is a defensive logic which should not happen because
        // value of segmentStart comes from action times which excludes the end of the day.
        if (segmentDuration > 0) {
            segmentDurationMap.computeIfAbsent(watermark.getDestinationProviderOid(), d -> new ArrayList<>()).add(segmentDuration);
        }

        // Loop through the segment list and assign value for the multiplier.
        // For each segment, find the duration of all segments of this provider from the map.
        // e.g. p1 -> 3hr, 6hr
        // multiplier for first segment = 3 / (3 + 6)
        for (Segment segment : segments) {
            final Long sum = segmentDurationMap.get(segment.watermark.getDestinationProviderOid()).stream()
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
        private final Watermark watermark;
        private double multiplier;

        Segment(long duration, Watermark watermark) {
            this.duration = duration;
            this.watermark = watermark;
        }

        @Override
        public String toString() {
            return "Segment{" + "duration(hours)=" + TimeUnit.MILLISECONDS.toHours(duration)
                    + ", watermark=" + watermark + ", multiplier=" + multiplier + '}';
        }
    }
}
