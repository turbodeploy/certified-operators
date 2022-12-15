package com.vmturbo.cost.component.savings.calculator;

import static com.vmturbo.trax.Trax.trax;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxCollectors;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxNumber;

/**
 * Savings calculation using the bill and action chain.
 */
public class Calculator {
    private static final long MILLIS_IN_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    // Cost categories of bill records to be excluded from the analysis of time spent on a provider.
    private static final Set<CostCategory> COST_CATEGORIES_EXCLUDE = Collections.singleton(CostCategory.LICENSE);
    /**
     * Trax topic name for savings calculations.
     */
    public static final String TRAX_TOPIC = "BILL_BASED_SAVINGS";

    private final Logger logger = LogManager.getLogger();
    private final long deleteActionExpiryMs;
    private final Clock clock;
    private final StoragePriceStructure storagePriceStructure;

    /**
     * Constructor.
     *
     * @param deleteActionExpiryMs Volume expiry time in milliseconds
     * @param clock clock
     */
    public Calculator(long deleteActionExpiryMs, Clock clock, StoragePriceStructure storagePriceStructure) {
        this.deleteActionExpiryMs = deleteActionExpiryMs;
        this.clock = clock;
        this.storagePriceStructure = storagePriceStructure;
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
        logger.debug("Calculating savings for entity {} on {}.", entityOid, date);
        long dateStampMillis = date.toInstant(ZoneOffset.UTC).toEpochMilli();
        if (skipSavingsForDay(dateStampMillis, savingsGraph, billRecords)) {
            logger.debug("Skipping savings calculation for entity {} on {}.", entityOid, date);
            return new SavingsValues.Builder()
                    .savings(0)
                    .investments(0)
                    .timestamp(date)
                    .entityOid(entityOid)
                    .build();
        }

        // Use the high-low graph and the bill records to determine the billing segments in the day.
        NavigableSet<Segment> segments = createBillingSegments(dateStampMillis, savingsGraph, billRecords);
        if (logger.isDebugEnabled()) {
            StringBuilder debugText = new StringBuilder();
            debugText.append("\n=== Segments ===\n");
            segments.forEach(s -> debugText.append(s).append("\n"));
            debugText.append("\n=== Bill records ===\n");
            billRecords.forEach(r -> debugText.append(r).append("\n"));
            logger.debug(debugText);
        }

        // For each segment, calculate the savings/investment.
        List<TraxNumber> segmentSavings = new ArrayList<>();
        List<TraxNumber> segmentInvestments = new ArrayList<>();
        TraxNumber totalDailySavings;
        TraxNumber totalDailyInvestments;
        try (TraxContext traxContext = Trax.track(TRAX_TOPIC)) {
            for (Segment segment : segments) {
                if (segment.actionDataPoint instanceof ScaleActionDataPoint) {
                    ScaleActionDataPoint dataPoint = (ScaleActionDataPoint)segment.actionDataPoint;
                    long providerOid = dataPoint.getDestinationProviderOid();
                    // Get all bill records of this provider and sum up the cost.
                    // Include records with provider ID equals 0 as well which are bill records that apply to all segments.
                    Set<BillingRecord> recordsForProvider = billRecords.stream()
                            .filter(r -> r.getProviderId() == providerOid || r.getProviderId() == 0)
                            .collect(Collectors.toSet());
                    TraxNumber costForProviderInSegment = recordsForProvider.stream().map(
                            r -> trax(r.getCost()).named("Cost in bill record")
                                    .times(segment.commTypeToMultiplierMap.getOrDefault(r.getCommodityType(), trax(1.0).named("Multiplier")))
                                    .compute("Cost of " + r.getCostCategory() + " (commodity: " + CommodityType.forNumber(r.getCommodityType()) + ") in segment"))
                            .collect(TraxCollectors.sum("Cost in segment"));
                    TraxNumber investments = Trax.max(trax(0).named("Minimum investments"),
                            costForProviderInSegment.minus(trax(dataPoint.getLowWatermark()).named("Low watermark")
                                    .times(trax(segment.duration).dividedBy(MILLIS_IN_HOUR).compute("Segment duration in hours"))
                                    .compute("Lowest cost if action was not executed")).compute("Investments"))
                            .compute("Investments of segment " + TimeUtil.millisToLocalDateTime(segment.segmentStart, clock));
                    TraxNumber savings = calculateSavings(recordsForProvider, dataPoint,
                            costForProviderInSegment, segment.duration)
                            .named("Savings of segment " + TimeUtil.millisToLocalDateTime(segment.segmentStart, clock));
                    segmentSavings.add(savings);
                    segmentInvestments.add(investments);
                } else if (segment.actionDataPoint instanceof DeleteActionDataPoint) {
                    DeleteActionDataPoint deleteActionDataPoint = (DeleteActionDataPoint)segment.actionDataPoint;
                    TraxNumber savingsPerHour = trax(deleteActionDataPoint.savingsPerHour()).named("Savings per hour");
                    TraxNumber duration = trax((double)segment.duration / MILLIS_IN_HOUR).named("Duration in hours");
                    segmentSavings.add(savingsPerHour.times(duration)
                            .compute("Savings from delete action for segment " + TimeUtil.millisToLocalDateTime(segment.segmentStart, clock)));
                }
            }
            totalDailySavings = segmentSavings.stream().collect(TraxCollectors.sum("Total savings for the day"));
            totalDailyInvestments = segmentInvestments.stream().collect(TraxCollectors.sum("Total investments for the day"));

            if (traxContext.on()) {
                final String savingsCalculationStack;
                final String investmentCalculationStack;
                savingsCalculationStack = totalDailySavings.calculationStack();
                investmentCalculationStack = totalDailyInvestments.calculationStack();
                logger.info("Savings for entity {} on {}:\n{}",
                        () -> entityOid, () -> date,
                        () -> savingsCalculationStack);
                logger.info("Investments for entity {} on {}:\n{}",
                        () -> entityOid, () -> date,
                        () -> investmentCalculationStack);
            }
        }

        // Return the total savings/investment for the day.
        return new SavingsValues.Builder()
                .savings(totalDailySavings.getValue())
                .investments(totalDailyInvestments.getValue())
                .timestamp(date)
                .entityOid(entityOid)
                .build();
    }

    /**
     * There are some conditions where we won't calculate the savings/investments for the day.
     * Currently, the condition is when some bill records that cannot be mapped to a specific
     * provider. It's mainly for Azure databases.
     *
     * @param dateStampMillis timestamp in milliseconds for the beginning of the day
     * @param savingsGraph savings graph
     * @param billRecords bill records of the day
     * @return true if savings calculations should be skipped for this day, otherwise false.
     */
    private boolean skipSavingsForDay(long dateStampMillis,
            SavingsGraph savingsGraph, Set<BillingRecord> billRecords) {
        return billRecords.stream().anyMatch(r -> r.getProviderId() == 0)
                && !savingsGraph.getDataPointsInDay(dateStampMillis).isEmpty();
    }

    /**
     * Calculate savings of a day, taking into account of RI coverage changes.
     *
     * @param billRecords bill records of the provider associated with the segment
     * @param dataPoint action data point
     * @param costForProviderInSegment cost spent on the provider in this segment
     * @param segmentDurationMillis segment length in milliseconds
     * @return savings for a segment
     */
    private TraxNumber calculateSavings(Set<BillingRecord> billRecords, ScaleActionDataPoint dataPoint,
            final TraxNumber costForProviderInSegment, long segmentDurationMillis) {
        try (TraxContext traxContext = Trax.track(TRAX_TOPIC)) {
            TraxNumber segmentDurationHours = trax(segmentDurationMillis).dividedBy(MILLIS_IN_HOUR).compute("Segment duration in hours");
            TraxNumber adjustedCostOfProvider = costForProviderInSegment;
            // check if the entity is covered by RI on the day
            boolean isEntityRICovered = billRecords.stream().anyMatch(r -> r.getPriceModel() == PriceModel.RESERVED);
            if (isEntityRICovered && !dataPoint.isCloudCommitmentExpectedAfterAction()) {
                // The entity is covered by RI, but the action didn't expect it to be covered.
                if (dataPoint.isSavingsExpectedAfterAction()) {
                    // If it was a scale-down (efficiency) action, savings cannot be more than
                    // the difference between the high watermark and the on-demand cost of the destination tier.
                    // e.g. scale from 5 -> 3, savings cannot be more than $2. In this example,
                    // adjustedCostOfProvider is 3.
                    adjustedCostOfProvider = Trax.max(trax(dataPoint.getDestinationOnDemandCost()).times(segmentDurationHours)
                            .compute("Expected cost in segment"), costForProviderInSegment).compute(
                            "Adjusted cost of provider due to RI and scale down action");
                } else {
                    // If last action was a scale-up (performance) action, we cannot claim
                    // savings for this action.
                    // e.g. scale from 3 -> 5, cost of provider cannot be less than 3.
                    // Note: Before action cost includes RI discount if present.
                    adjustedCostOfProvider = Trax.max(trax(dataPoint.getBeforeActionCost()).times(
                                    segmentDurationHours).compute("Cost of segment if action was not executed"),
                            costForProviderInSegment).compute(
                            "Adjusted cost of provider due to RI and scale up action");
                }
            }
            return Trax.max(trax(0).named("Minimum Savings"), (trax(dataPoint.getHighWatermark()).named("High watermark").times(segmentDurationHours.named("Segment duration in hours"))).compute(
                    "Highest cost if action was not executed").minus(adjustedCostOfProvider).compute("Savings")).compute("Savings of segment");
        }
    }

    /**
     * Create billing segment of the day.
     *
     * @param datestamp timestamp of the beginning of the day in milliseconds
     * @param savingsGraph savings graph
     * @param billRecords all bill records of the day
     * @return segments of the day
     */
    private NavigableSet<Segment> createBillingSegments(long datestamp, SavingsGraph savingsGraph,
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
                return Collections.emptyNavigableSet();
            }
        }

        final TreeSet<Segment> segments = new TreeSet<>(Comparator.comparingLong(s -> s.getActionDataPoint().getTimestamp()));
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
            segments.add(new Segment(segmentStart, segmentDuration, dataPoint));
            dataPoint = actionDatapoint;
            segmentStart = actionDatapoint.getTimestamp();
        }

        // Close the last segment.
        segmentEnd = datestamp + MILLIS_IN_DAY;
        final long segmentDuration = segmentEnd - segmentStart;
        segments.add(new Segment(segmentStart, segmentDuration, dataPoint));

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
                    // Put the capacity before the first action of the day in the map.
                    // To find the before action capacity, find the entry in the map that is strictly
                    // less than the timestamp of the action.
                    // Key is the timestamp for the beginning of the day minus 1 so that even if the
                    // action is executed at exactly the beginning of the day, it can still find an
                    // entry strictly lower than it.
                    capacityMap.get(commType).put(datestamp - 1, commodityResize.getOldCapacity());
                }
                capacityMap.get(commType).put(action.getTimestamp(), commodityResize.getNewCapacity());
            }
        }
        return capacityMap;
    }

    /**
     * Set the duration for each segment and set the multiplier for each commodity type in each segment.
     * @param segments all segments of the day (sorted by time)
     * @param billRecords all bill records of the day
     * @param startOfDay timestamp of the start of the day (in milliseconds)
     * @param dataPointsInDay all data points of the day
     */
    private void assignDurationAndMultiplier(NavigableSet<Segment> segments, Set<BillingRecord> billRecords,
            long startOfDay, SortedSet<ActionDataPoint> dataPointsInDay) {
        try (TraxContext traxContext = Trax.track(TRAX_TOPIC)) {
            Map<Long, List<Segment>> segmentsByProvider = segments.stream().collect(Collectors.groupingBy(s -> s.getActionDataPoint().getDestinationProviderOid()));

            // If the first segment for the day did not start from
            // the beginning of the day and the provider before the first segment is the same as
            // the first segment, we need to account for the time and cost incurred
            // before the first segment.
            boolean initialSegmentNoProviderChange = isInitialSegmentNoProviderChange(segments, startOfDay);
            long timeBeforeFirstSegment = Math.max(0, segments.first().getActionDataPoint().getTimestamp() - startOfDay);

            for (Entry<Long, List<Segment>> providerSegmentsEntry : segmentsByProvider.entrySet()) {
                long providerId = providerSegmentsEntry.getKey();
                // Get a map of commodity type to bill records for this provider for this day.
                Map<Integer, List<BillingRecord>> recordsByCommType = billRecords.stream()
                        .filter(r -> !COST_CATEGORIES_EXCLUDE.contains(r.getCostCategory()))
                        .filter(r -> r.getProviderId() == providerId)
                        .collect(Collectors.groupingBy(BillingRecord::getCommodityType));

                for (Entry<Integer, List<BillingRecord>> commTypeEntry : recordsByCommType.entrySet()) {
                    Integer commType = commTypeEntry.getKey();
                    List<BillingRecord> billingRecordsForCommType = commTypeEntry.getValue();

                    // Get total amount billed for this commodity and for this provider by adding the
                    // costs in the corresponding bill records.
                    double totalUsageBilled = billingRecordsForCommType.stream()
                            .map(BillingRecord::getUsageAmount)
                            .reduce(0d, Double::sum);

                    Map<Integer, TreeMap<Long, Double>> commCapMap = getCommodityCapacityMap(startOfDay, dataPointsInDay);

                    long totalTimeOnProvider = getTotalTimeOnProvider(segments, providerId,
                            initialSegmentNoProviderChange, timeBeforeFirstSegment);

                    // The variable usageRemaining is for tracking how much billed time has not been allocated segments.
                    // Here we assign the initial value by considering the period of time before the first segment.
                    double usageRemaining = getUsageRemaining(segments, totalUsageBilled,
                            initialSegmentNoProviderChange, commCapMap, commType, startOfDay,
                            timeBeforeFirstSegment, totalTimeOnProvider, billingRecordsForCommType, providerId);

                    for (Segment segment : providerSegmentsEntry.getValue()) {
                        if (!(segment.getActionDataPoint() instanceof ScaleActionDataPoint)) {
                            continue;
                        }
                        if (commType == CommodityType.UNKNOWN_VALUE) {
                            // UsageAmount is TIME.
                            double totalUsageBilledMillis = totalUsageBilled * MILLIS_IN_HOUR;
                            double usageRemainingMillis = usageRemaining * MILLIS_IN_HOUR;
                            if (usageRemainingMillis - segment.duration < 0) {
                                segment.duration = Double.valueOf(usageRemainingMillis).longValue();
                            }
                            TraxNumber multiplier = totalUsageBilledMillis != 0
                                    ? trax(segment.duration).named("Segment duration in milliseconds")
                                    .dividedBy(trax(totalUsageBilledMillis).named("Total usage billed in milliseconds"))
                                    .compute("Multiplier") : trax(1).named("Multiplier");
                            segment.commTypeToMultiplierMap.put(commType, multiplier);
                            usageRemaining -= segment.duration / (double)MILLIS_IN_HOUR;
                        } else {
                            // UsageAmount is commodity quantity times TIME.
                            ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)segment.getActionDataPoint();
                            if (commCapMap.get(commType) == null) {
                                // This commType is not changed on this day.
                                // We use the proportion of the lengths of the segments to assign the multiplier.
                                // If the bill record does not include the charge for the full day yet,
                                // this multiplier can be inaccurate, but the value will be corrected
                                // once the bill is updated.
                                TraxNumber multiplier = totalTimeOnProvider != 0
                                        ? trax(segment.duration).named("Segment duration in milliseconds")
                                        .dividedBy(trax(totalTimeOnProvider).named("Total time on provider"))
                                        .compute("Multiplier") : trax(1).named("Multiplier");
                                segment.commTypeToMultiplierMap.put(commType, multiplier);
                                usageRemaining -= multiplier.getValue() * billingRecordsForCommType.stream()
                                        .map(BillingRecord::getUsageAmount)
                                        .reduce(0d, Double::sum);
                            } else {
                                long segmentStartTime = Math.max(scaleActionDataPoint.getTimestamp(), startOfDay);

                                // Calculate the product of quantity, duration and rate for entities that have varying rates.
                                setQdrForSegment(scaleActionDataPoint, recordsByCommType, segment);

                                TraxNumber newCapacity = trax(commCapMap.get(commType)
                                        .floorEntry(segmentStartTime)
                                        .getValue()).named("Destination capacity");
                                if (commType == CommodityType.STORAGE_AMOUNT_VALUE) {
                                    double adjustedStorageAmount = adjustStorageAmount(newCapacity.getValue(),
                                            recordsByCommType.get(commType));
                                    if (adjustedStorageAmount != newCapacity.getValue()) {
                                        newCapacity = trax(adjustedStorageAmount).named("Adjusted storage amount");
                                    }
                                } else {
                                    double adjustCommodityAmount = adjustCommodityAmount(billingRecordsForCommType, providerId,
                                            commType, newCapacity.getValue());
                                    if (adjustCommodityAmount != newCapacity.getValue()) {
                                        newCapacity = trax(adjustCommodityAmount).named("Adjusted commodity capacity");
                                    }
                                }
                                TraxNumber quantityTimesHours = newCapacity
                                        .times(trax((double)segment.duration / MILLIS_IN_HOUR)
                                                .named("Duration in hours"))
                                        .compute("Quantity times duration");
                                // If the bill shows usageAmount is 15 hours and this provider has
                                // 2 segments on this day. According to the actions, both segments is 10 hours long.
                                // The first segment will take the full 10 hours (usageRemaining = 15 - 10 = 5).
                                // The second segment cannot assume the whole 10 hours because there are only 5
                                // billed hours remaining. In this case, adjust the second segment to 5 hours.
                                if (usageRemaining - quantityTimesHours.getValue() < 0) {
                                    segment.duration = Double.valueOf(
                                            usageRemaining / newCapacity.getValue() * MILLIS_IN_HOUR).longValue();
                                    quantityTimesHours = newCapacity
                                            .times(trax((double)segment.duration / MILLIS_IN_HOUR)
                                                    .named("Adjusted duration in hours"))
                                            .compute("Quantity times hours");
                                }
                                TraxNumber multiplier = quantityTimesHours
                                        .dividedBy(trax(totalUsageBilled).named("Total usage billed"))
                                        .compute("Multiplier");
                                segment.commTypeToMultiplierMap.put(commType, multiplier);
                                usageRemaining -= quantityTimesHours.getValue();
                            }
                        }
                    }
                }
            }

            // Adjust the multiplier for entities that have varying rates.
            adjustMultiplierForEntitiesVaryingRates(segments, segmentsByProvider, startOfDay);
        }
    }

    /**
     * If the first segment does not begin at the beginning of the day, it is the first segment
     * of the entity. If the scale action that is associated with this segment does not have a
     * provider change, we will need logic to handle the usage and cost before the segment begins.
     *
     * @param segments all segments for the day
     * @param startOfDay timestamp for the beginning of the day
     * @return boolean indicating if the day has an initial segment with no provider change
     */
    private boolean isInitialSegmentNoProviderChange(NavigableSet<Segment> segments, long startOfDay) {
        if (!segments.isEmpty() && segments.first().getActionDataPoint() instanceof ScaleActionDataPoint) {
            ScaleActionDataPoint firstSegment = (ScaleActionDataPoint)segments.first().getActionDataPoint();
            return firstSegment.getTimestamp() > startOfDay && firstSegment.getSourceProviderOid()
                    == firstSegment.getDestinationProviderOid();
        }
        return false;
    }

    /**
     * Calculate the total time the entity spent on a given provider.
     *
     * @param allSegments all segments in the day
     * @param providerId provider ID
     * @param initialSegmentNoProviderChange the first segment started after the beginning of the day with no provider change
     * @param timeBeforeFirstSegment time elapsed before the first segment of the day in milliseconds
     * @return time spent on the provider in milliseconds
     */
    private long getTotalTimeOnProvider(NavigableSet<Segment> allSegments, long providerId,
            boolean initialSegmentNoProviderChange, long timeBeforeFirstSegment) {
        long totalTimeOnProvider = allSegments.stream()
                .filter(s -> s.getActionDataPoint().getDestinationProviderOid() == providerId)
                .map(Segment::getDuration).reduce(0L, Long::sum);
        // Account for the time before the first segment if it is the first segment (of any provider)
        // for the day, and it does not start at the beginning of the day.
        if (initialSegmentNoProviderChange
                && allSegments.first().getActionDataPoint().getDestinationProviderOid() == providerId) {
            totalTimeOnProvider += timeBeforeFirstSegment;
        }
        for (Segment segment : allSegments) {
            // If the previous segment (of any provider) is a revert or external
            // modification and the before and after providers are the same
            // for this segment, assume the previous segment was running on
            // the same provider for the whole segment. Include the time of
            // the previous segment to the time on provider.
            Segment previousSegment = allSegments.lower(segment);
            if (previousSegment != null && isPrevSegTerminationAndSameProvider(segment, previousSegment)) {
                totalTimeOnProvider += previousSegment.duration;
            }
        }
        return totalTimeOnProvider;
    }

    /**
     * "Usage remaining" is for tracking how much billed time has not been allocated segments.
     * Calculate the initial value by considering the period of time before the first segment.
     *
     * @param segments all segments of the day ordered by time
     * @param totalUsageBilled total usage amount billed for the given commodity
     * @param initialSegmentNoProviderChange the first segment started after the beginning of the day with no provider change
     * @param commCapMap commodity capacity map
     * @param commType commodity type
     * @param startOfDay timestamp of the start of the day in milliseconds
     * @param timeBeforeFirstSegment time before the first segment
     * @param totalTimeOnProvider total time spent on the provider
     * @param billingRecordsForCommType bill records for the specified commodity type
     * @param providerId provider ID
     * @return usage amount remaining.
     */
    private double getUsageRemaining(NavigableSet<Segment> segments, double totalUsageBilled,
            boolean initialSegmentNoProviderChange, Map<Integer, TreeMap<Long, Double>> commCapMap,
            int commType, long startOfDay, long timeBeforeFirstSegment, long totalTimeOnProvider,
            List<BillingRecord> billingRecordsForCommType, long providerId) {
        double usageRemaining = totalUsageBilled;
        if (segments.isEmpty()) {
            return usageRemaining;
        }
        // If the first segment of the day did not start at the beginning of the day, and there were
        // no provider change when starting the first segment, subtract the usage amount used before
        // the first segment.
        if (initialSegmentNoProviderChange
                && segments.first().getActionDataPoint().getDestinationProviderOid() == providerId) {
            if (commCapMap.get(commType) == null) {
                // If the commodity is not changed on this day, use time to determine the cost before the first segment.
                usageRemaining -= (double)timeBeforeFirstSegment / totalTimeOnProvider * billingRecordsForCommType.stream()
                        .map(BillingRecord::getUsageAmount)
                        .reduce(0d, Double::sum);
            } else {
                // If the commodity was changed on this day, subtract the quantity times duration
                // used before the first segment.
                Segment firstSegment = segments.first();
                long firstSegmentTimestamp = firstSegment.getActionDataPoint().getTimestamp();
                if (commCapMap.get(commType).lowerEntry(firstSegmentTimestamp) != null) {
                    double oldCapacity = commCapMap.get(commType).lowerEntry(firstSegmentTimestamp).getValue();
                    if (commType == CommodityType.STORAGE_AMOUNT_VALUE) {
                        oldCapacity = adjustStorageAmount(oldCapacity, billingRecordsForCommType);
                    } else {
                        oldCapacity = adjustCommodityAmount(billingRecordsForCommType, providerId, commType, oldCapacity);
                    }
                    final double adjustedUsageRemaining =
                            (usageRemaining - oldCapacity * (firstSegmentTimestamp - startOfDay) / MILLIS_IN_HOUR);
                    usageRemaining = adjustedUsageRemaining > 0 ? adjustedUsageRemaining : 0;
                }
            }
        }
        // If any segment is preceded by a revert or external execution, and current segment shows
        // no provider change, assume we spent the whole time on the same provider.
        List<Segment> segmentsOfCurrentProvider = segments.stream()
                .filter(s -> s.getActionDataPoint().getDestinationProviderOid() == providerId)
                .collect(Collectors.toList());
        if (commCapMap.get(commType) != null) {
            for (Segment segment : segmentsOfCurrentProvider) {
                Segment previousSegment = segments.lower(segment);
                long segmentStartTime = segment.getActionDataPoint().getTimestamp();
                if (previousSegment != null && isPrevSegTerminationAndSameProvider(segment, previousSegment)
                        && commCapMap.get(commType).lowerEntry(segmentStartTime) != null) {
                    double oldCapacity = commCapMap.get(commType).lowerEntry(segmentStartTime).getValue();
                    if (commType == CommodityType.STORAGE_AMOUNT_VALUE) {
                        oldCapacity = adjustStorageAmount(oldCapacity, billingRecordsForCommType);
                    } else {
                        oldCapacity = adjustCommodityAmount(billingRecordsForCommType, providerId, commType, oldCapacity);
                    }
                    usageRemaining -= oldCapacity * previousSegment.getDuration() / MILLIS_IN_HOUR;
                }
            }
        }
        return usageRemaining;
    }

    /**
     * The cost of storage amount can increase as the size of the volume increase, even if the volume
     * is still in the same service provider. If an entity only has one bill record for each day
     * and it is for storage amount, we set the product of the quantity, segment duration and the
     * cost per hour per GB in the segment. It will be used to calculate the multiplier.
     *
     * @param scaleActionDataPoint action data point
     * @param recordsByCommType records for the entity for the day
     * @param segment the segment
     */
    private void setQdrForSegment(ScaleActionDataPoint scaleActionDataPoint, Map<Integer, List<BillingRecord>> recordsByCommType, Segment segment) {
        try (TraxContext traxContext = Trax.track(TRAX_TOPIC)) {
            // If the bill only has record for one commodity type and it is storage amount of a storage tier,
            // store the value of quantity x duration x after-action rate per hour (QDR) in the segment.
            if (recordsByCommType.size() == 1 && recordsByCommType.get(CommodityType.STORAGE_AMOUNT_VALUE) != null
                    && recordsByCommType.get(CommodityType.STORAGE_AMOUNT_VALUE).get(0).getProviderType() == EntityType.STORAGE_TIER_VALUE
                    && scaleActionDataPoint.getSourceProviderOid() == scaleActionDataPoint.getDestinationProviderOid()) {
                Optional<CommodityResize> storageAmountResize =
                        scaleActionDataPoint.getCommodityResizes().stream().filter(r -> r.getCommodityType()
                                == CommodityType.STORAGE_AMOUNT_VALUE).findFirst();
                if (storageAmountResize.isPresent()) {
                    TraxNumber newCapacity = trax(storageAmountResize.get().getNewCapacity()).named("Quantity");
                    TraxNumber duration = trax((double)segment.duration / MILLIS_IN_HOUR).named("Duration");
                    TraxNumber afterActionRatePerGBPerHour = trax(scaleActionDataPoint.getDestinationOnDemandCost()).named("Destination cost")
                            .dividedBy(newCapacity).compute("Rate");
                    segment.qdr = newCapacity.times(duration).compute("Quantity times duration").times(
                            afterActionRatePerGBPerHour).compute("QDR");
                }
            }
        }
    }

    /**
     * This method update the multiplier for segments of entities (mainly volumes) that have varying
     * rate for storage amount.
     * QDR is the quantity, segment duration and the cost per hour per GB in the segment.
     * Multiplier = QDR of the segment / (sum of QDR of all segments of the same provider for the day)
     *
     * @param segments segments
     * @param segmentsByProvider a map of provider ID to segments
     * @param startOfDay timestamp of the start of the day
     */
    private void adjustMultiplierForEntitiesVaryingRates(NavigableSet<Segment> segments,
            Map<Long, List<Segment>> segmentsByProvider, long startOfDay) {
        try (TraxContext traxContext = Trax.track(TRAX_TOPIC)) {
            // Loop over all providers -> segment,
            // for each segment for each commodity, if the QDR variable is set, change the multiplier to
            // multiplier = QDR / sum(QDR of all segments of this provider)
            for (Entry<Long, List<Segment>> providerSegmentsEntry : segmentsByProvider.entrySet()) {
                if (providerSegmentsEntry.getValue().stream().allMatch(s -> s.qdr != null && s.qdr.getValue() == 0)) {
                    continue;
                }
                TraxNumber totalQdr = segments.stream()
                        .filter(s -> s.getActionDataPoint().getDestinationProviderOid() == providerSegmentsEntry.getKey())
                        .map(s -> s.qdr)
                        .filter(Objects::nonNull)
                        .collect(TraxCollectors.sum("total QDR"));
                // If segment is first segment of the day, add qdr of the period before the segment to the total.
                ActionDataPoint firstAction = segments.first().actionDataPoint;
                if (firstAction.getTimestamp() > startOfDay && firstAction instanceof ScaleActionDataPoint
                        && firstAction.getDestinationProviderOid() == providerSegmentsEntry.getKey()) {
                    ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)segments.first().actionDataPoint;
                    TraxNumber duration = trax((double)(firstAction.getTimestamp() - startOfDay) / MILLIS_IN_HOUR).named("Duration before first segment");
                    TraxNumber quantityTimesRate = trax(scaleActionDataPoint.getBeforeActionCost()).named("Quantity times rate");
                    totalQdr = duration.times(quantityTimesRate).compute("QDR").plus(totalQdr).compute("Total QDR for provider");
                }
                // Go through each segment of this provider. If a segment is preceded by an action
                // termination (revert or external modification) event and the current action does not
                // involve change of provider, we include the QDR of the previous segment in the total.
                for (Segment segment : providerSegmentsEntry.getValue()) {
                    if (!(segment.getActionDataPoint() instanceof ScaleActionDataPoint)) {
                        continue;
                    }
                    Segment previousSegment = segments.lower(segment);
                    if (previousSegment != null && isPrevSegTerminationAndSameProvider(segment,
                            previousSegment)) {
                        ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)segment.getActionDataPoint();
                        TraxNumber duration = trax(previousSegment.duration).named(
                                "Duration of previous segment associated with action termination");
                        TraxNumber quantityTimesRate = trax(scaleActionDataPoint.getBeforeActionCost()).named("Quantity times time");
                        totalQdr = duration.times(quantityTimesRate).compute("Quantity times rate").plus(totalQdr).compute("Total QDR for provider");
                    }
                }
                if (totalQdr == null || totalQdr.getValue() == 0) {
                    continue;
                }
                for (Segment segment : providerSegmentsEntry.getValue()) {
                    if (!(segment.getActionDataPoint() instanceof ScaleActionDataPoint)) {
                        continue;
                    }
                    if (segment.qdr != null && segment.qdr.getValue() != 0) {
                        TraxNumber multiplier = segment.qdr.dividedBy(totalQdr).compute("Multiplier");
                        segment.commTypeToMultiplierMap.put(CommodityType.STORAGE_AMOUNT_VALUE,
                                multiplier);
                    }
                }
            }
        }
    }

    /**
     * Return true is previous segment is associated with an action termination event (revert or
     * external modification) but the previous segment has the same provider as the current segment.
     * This condition check is for determining if the bill record includes the usage of the previous
     * segment.
     *
     * @param segment current segment
     * @param previousSegment previous segment
     * @return true is previous segment is associated with an action termination event (revert or
     *      * external modification) but the previous segment has the same provider as the current segment.
     */
    private boolean isPrevSegTerminationAndSameProvider(Segment segment, Segment previousSegment) {
        if (!(segment.getActionDataPoint() instanceof ScaleActionDataPoint)) {
            return false;
        }
        ScaleActionDataPoint scaleActionDataPoint = (ScaleActionDataPoint)segment.getActionDataPoint();
        return previousSegment != null
                && previousSegment.getActionDataPoint() instanceof ActionChainTermination
                && scaleActionDataPoint.getSourceProviderOid() == scaleActionDataPoint.getDestinationProviderOid();
    }

    private double adjustStorageAmount(double capacity, List<BillingRecord> billingRecords) {
        if (billingRecords != null && !billingRecords.isEmpty()) {
            BillingRecord record = billingRecords.get(0);
            return storagePriceStructure.getEndRangeInPriceTier(capacity,
                    record.getAccountId(), record.getRegionId(), record.getProviderId());
        }
        return capacity;
    }

    /**
     * Some commodities are free up to a certain amount before a charge will be applied on the
     * amount above that free limit. E.g. AWS GP3 IOPS does not have a charge for provisioned IOPS
     * below 3000.
     *
     * @param billingRecords bill records for the given commodity
     * @param providerId provider ID
     * @param commType commodity type
     * @param capacity after-action capacity of the commodity
     * @return the adjusted (if needed) new capacity
     */
    private double adjustCommodityAmount(List<BillingRecord> billingRecords, long providerId,
            int commType, final double capacity) {
        double adjustedCapacity = capacity;
        BillingRecord record = billingRecords.stream().findFirst().orElse(null);
        if (record != null) {
            double freeEndRange = storagePriceStructure.getEndRangeInFreePriceTier(
                    record.getAccountId(), record.getRegionId(), providerId, commType);
            if (freeEndRange != 0) {
                adjustedCapacity = capacity > freeEndRange ? capacity - freeEndRange : 0.0;
            }
        }
        return adjustedCapacity;
    }

    /**
     * A segment is a period of time in a day when the entity is on the same provider.
     */
    private static class Segment {
        private final long segmentStart;
        private long duration;
        private final ActionDataPoint actionDataPoint;
        private final Map<Integer, TraxNumber> commTypeToMultiplierMap = new HashMap<>();
        /**
         * qdr is the product of (quantity x duration x rate).
         * Some entities have a varying rate within the same service tier. E.g. Azure Premium SSD
         * has a different rate for each performance tier, but they are modelled as the same service
         * tier. In these cases, the multiplier will need to include the cost in the calculation of
         * the ratio.
         */
        private TraxNumber qdr;

        Segment(long segmentStart, long duration, @Nonnull ActionDataPoint actionDataPoint) {
            this.segmentStart = segmentStart;
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
        @Nonnull
        public ActionDataPoint getActionDataPoint() {
            return actionDataPoint;
        }

        @Override
        public String toString() {
            return "Segment{"
                    + "segmentStart=" + TimeUtil.millisToLocalDateTime(segmentStart, Clock.systemUTC())
                    + ", duration(hours)=" + (double)(duration) / MILLIS_IN_HOUR
                    + ", actionDataPoint=" + actionDataPoint
                    + ", commTypeToMultiplierMap=" + commTypeToMultiplierMap.entrySet().stream()
                    .map(e -> String.format("%d -> %.5f", e.getKey(), e.getValue().getValue()))
                    .collect(Collectors.joining(", ", "{", "}")) + '}';
        }
    }
}
