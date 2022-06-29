package com.vmturbo.cost.component.savings;

import java.time.Clock;
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
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingDataInjector.BillingScriptEvent;
import com.vmturbo.cost.component.savings.BillingRecord.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * ScenarioGenerator is used for generating action chains and bill records from script events.
 * The logic in this class can be called only if the ENABLE_SAVINGS_TEST_INPUT feature flag is
 * enabled.
 */
public class ScenarioGenerator {
    private static final Logger logger = LogManager.getLogger();
    private static final Clock clock = Clock.systemUTC();

    private static final int HOURS_IN_A_MONTH = 730;

    private ScenarioGenerator() {
    }

    /**
     * Generate action chains from script events.
     *
     * @param events script events
     * @param uuidMap display name to OID map
     * @return map of entity OID to sorted (by time) set of action chains
     */
    static Map<Long, NavigableSet<ExecutedActionsChangeWindow>> generateActionChains(List<BillingScriptEvent> events,
            Map<String, Long> uuidMap) {
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains = new HashMap<>();
        logger.info("Generating actions from script events:");
        for (BillingScriptEvent event : events) {
            Long oid = uuidMap.getOrDefault(event.uuid, 0L);
            long actionTime = event.timestamp != null ? event.timestamp : 0;
            if (actionTime == 0) {
                logger.error("Action time is missing in event: {}", event);
                continue;
            }
            LocalDateTime actionDateTime = TimeUtil.millisToLocalDateTime(actionTime, clock);
            if ("RESIZE".equals(event.eventType)) {
                ExecutedActionsChangeWindow action = createVMActionChangeWindow(oid, actionDateTime,
                        event.sourceOnDemandRate, event.destinationOnDemandRate,
                        generateProviderIdFromRate(event.sourceOnDemandRate),
                        generateProviderIdFromRate(event.destinationOnDemandRate), null,
                        LivenessState.LIVE, event.expectedCloudCommitment);
                logger.info("Scale action time: {}, entity OID: {}, source rate: {}, "
                                + "destination rate: {} source provider: {}, destination provider: {}, "
                                + "expected RI coverage: {}",
                        actionDateTime, oid, event.sourceOnDemandRate, event.destinationOnDemandRate,
                        generateProviderIdFromRate(event.sourceOnDemandRate),
                        generateProviderIdFromRate(event.destinationOnDemandRate), event.expectedCloudCommitment);
                actionChains.computeIfAbsent(oid, c -> new TreeSet<>(GrpcActionChainStore.changeWindowComparator))
                        .add(action);
            } else if ("DELVOL".equals(event.eventType)) {
                long dummyStorageTierOid = 34343434343L;
                ExecutedActionsChangeWindow action = createVolumeDeleteActionSpec(oid, actionDateTime,
                        event.sourceOnDemandRate, dummyStorageTierOid);
                logger.info("Delete action time: {}, entity OID: {}, source rate: {}",
                        actionDateTime, oid, event.sourceOnDemandRate);
                actionChains.computeIfAbsent(oid, c -> new TreeSet<>(GrpcActionChainStore.changeWindowComparator))
                        .add(action);
            } else if ("REVERT".equals(event.eventType)) {
                NavigableSet<ExecutedActionsChangeWindow> changeWindows = actionChains.get(oid);
                if (changeWindows != null) {
                    ExecutedActionsChangeWindow lastAction = changeWindows.pollLast();
                    if (lastAction != null) {
                        logger.info("Revert action time: {} entity OID: {}", actionDateTime, oid);
                        changeWindows.add(lastAction.toBuilder()
                                .setEndTime(event.timestamp)
                                .setLivenessState(LivenessState.REVERTED)
                                .build());
                    }
                }
            }
        }
        return actionChains;
    }

    /**
     * Generate bill records from script events.
     *
     * @param events script events
     * @param uuidMap display name to OID map
     * @param startTime The start time of the scenario.
     * @param endTime The end time of the scenario.
     * @return map of entity OID to set of bill records
     */
    static Map<Long, Set<BillingRecord>> generateBillRecords(List<BillingScriptEvent> events,
            Map<String, Long> uuidMap, LocalDateTime startTime, LocalDateTime endTime) {
        final Map<Long, NavigableSet<BillingScriptEvent>> scaleEventsByEntity = new HashMap<>();
        final Map<Long, NavigableSet<BillingScriptEvent>> powerEventsByEntity = new HashMap<>();
        final Map<Long, BillingScriptEvent> deleteEventsByEntity = new HashMap<>();

        final NavigableSet<BillingScriptEvent> sortedEvents =
                new TreeSet<>(Comparator.comparingLong(BillingScriptEvent::getTimestamp));
        sortedEvents.addAll(events);

        for (BillingScriptEvent event : sortedEvents) {
            Long oid = uuidMap.getOrDefault(event.uuid, 0L);
            if ("RESIZE".equals(event.eventType) || "EXTMOD".equals(event.eventType)) {
                scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("POWER_STATE".equals(event.eventType)) {
                powerEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("DELVOL".equals(event.eventType)) {
                deleteEventsByEntity.put(oid, event);
            } else if ("REVERT".equals(event.eventType)) {
                // Model the revert as a scale action which in the opposite direction as the previous
                // scale action.
                BillingScriptEvent previousEvent = getPreviousScaleEvent(sortedEvents, event);
                if (previousEvent != null) {
                    BillingScriptEvent revertEvent = new BillingScriptEvent();
                    revertEvent.timestamp = event.timestamp;
                    revertEvent.sourceOnDemandRate = previousEvent.destinationOnDemandRate;
                    revertEvent.destinationOnDemandRate = previousEvent.sourceOnDemandRate;
                    scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                            .add(revertEvent);
                }
            } else if ("RI_COVERAGE".equals(event.eventType)) {
                // Model a RI coverage change action as a scale action. Copy the scale action before
                // the RI coverage change event and make both source and destination tiers the same
                // as the destination tier of the previous scale action. Set the expected RI
                // coverage to that passed in by the RI coverage event.
                BillingScriptEvent previousEvent = getPreviousScaleEvent(sortedEvents, event);
                if (previousEvent != null) {
                    BillingScriptEvent riCoverageEvent = new BillingScriptEvent();
                    riCoverageEvent.timestamp = event.timestamp;
                    riCoverageEvent.sourceOnDemandRate = previousEvent.destinationOnDemandRate;
                    riCoverageEvent.destinationOnDemandRate = previousEvent.destinationOnDemandRate;
                    riCoverageEvent.expectedCloudCommitment = event.expectedCloudCommitment;
                    scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                            .add(riCoverageEvent);
                }
            }
        }
        final Map<Long, Set<BillingRecord>> billRecords = new HashMap<>();
        scaleEventsByEntity.forEach((oid, scaleEvents) ->
            billRecords.put(oid, generateBillRecordForEntity(scaleEvents,
                    powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()),
                    deleteEventsByEntity.get(oid), oid, startTime, endTime)));
        // Generate bill records for entities that only have delete actions
        deleteEventsByEntity.forEach((oid, event) -> {
            if (!scaleEventsByEntity.containsKey(oid)) {
                billRecords.put(oid, generateBillRecordForEntity(new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)),
                        powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()), event,
                        oid, startTime, endTime));
            }
        });
        billRecords.forEach((oid, records) -> {
            logger.info("Generated bill records for entity {}:", oid);
            records.stream().sorted(Comparator.comparing(BillingRecord::getSampleTime))
                    .forEach(r -> logger.info("{}", r));
        });
        return billRecords;
    }

    @Nullable
    private static BillingScriptEvent getPreviousScaleEvent(NavigableSet<BillingScriptEvent> sortedEvents,
            BillingScriptEvent event) {
        BillingScriptEvent previousEvent = sortedEvents.lower(event);
        while (previousEvent != null && !"RESIZE".equals(previousEvent.eventType)) {
            previousEvent = sortedEvents.lower(previousEvent);
        }
        if (previousEvent == null) {
            logger.error("Unsupported scenario: RI Coverage change action is not after a scale action.");
            return null;
        }
        return previousEvent;
    }

    private static Set<BillingRecord> generateBillRecordForEntity(NavigableSet<BillingScriptEvent> scaleEvents,
            NavigableSet<BillingScriptEvent> powerEvents, @Nullable BillingScriptEvent deleteEvent,
            long entityOid, LocalDateTime startTime, LocalDateTime endTime) {
        // Start from the first day of the scenario.
        LocalDateTime dayStart = startTime.truncatedTo(ChronoUnit.DAYS);

        // Create powered off intervals, which can span over more than 1 day.
        List<Interval> poweredOffIntervals = createPoweredOffIntervals(startTime, endTime, powerEvents, deleteEvent);

        // Loop through each day between the first event and the end time of the scenario.
        Set<BillingRecord> generatedRecords = new HashSet<>();
        while (dayStart.isBefore(endTime)) {
            // Create segments for the day. Each segment has one provider.
            LocalDateTime dayEnd = endTime.isBefore(dayStart.plusDays(1)) ? endTime
                    : dayStart.plusDays(1);
            List<Segment> segments = createSegments(dayStart, dayEnd, scaleEvents, deleteEvent,
                    poweredOffIntervals);

            int entityType = EntityType.VIRTUAL_MACHINE_VALUE;
            CostCategory costCategory = CostCategory.COMPUTE_LICENSE_BUNDLE;
            int providerType = EntityType.COMPUTE_TIER_VALUE;
            if (deleteEvent != null) {
                entityType = EntityType.VIRTUAL_VOLUME_VALUE;
                costCategory = CostCategory.STORAGE;
                providerType = EntityType.STORAGE_TIER_VALUE;
            }

            Map<Long, Segment> providerToOnDemandSegment = new HashMap<>();
            Map<Long, Segment> providerToReservedSegment = new HashMap<>();
            for (Segment segment : segments) {
                // If there are two on-demand segments for the same provider, merge them into one
                // so that only one bill record will be generated.
                // Also, the cost and durationInHour values are adjusted to reflect the amount of
                // time spent.
                if (providerToOnDemandSegment.containsKey(segment.providerId)) {
                    Segment onDemandSegment = providerToOnDemandSegment.get(segment.providerId);
                    onDemandSegment.cost += segment.cost * (1 - segment.expectedRICoverage);
                    onDemandSegment.durationInHours += segment.durationInHours * (1 - segment.expectedRICoverage);
                } else {
                    Segment onDemandSegment = new Segment(segment);
                    onDemandSegment.cost = segment.cost * (1 - segment.expectedRICoverage);
                    double duration = entityType == EntityType.VIRTUAL_VOLUME_VALUE
                            ? segment.durationInHours / HOURS_IN_A_MONTH : segment.durationInHours;
                    onDemandSegment.durationInHours = duration * (1 - segment.expectedRICoverage);
                    providerToOnDemandSegment.put(segment.providerId, onDemandSegment);
                }

                // If there are two reserved segments for the same provider, merge them into one
                // so that only one bill record will be generated.
                // Also, the cost and durationInHour values are adjusted to reflect the amount of
                // time spent.
                if (segment.expectedRICoverage > 0) {
                    if (providerToReservedSegment.containsKey(segment.providerId)) {
                        Segment reservedSegment = providerToReservedSegment.get(segment.providerId);
                        reservedSegment.cost = 0;
                        reservedSegment.durationInHours += segment.durationInHours * segment.expectedRICoverage;
                    } else {
                        Segment reservedSegment = new Segment(segment);
                        reservedSegment.cost = 0;
                        double duration = entityType == EntityType.VIRTUAL_VOLUME_VALUE
                                ? segment.durationInHours / 730 : segment.durationInHours;
                        reservedSegment.durationInHours = duration * segment.expectedRICoverage;
                        providerToReservedSegment.put(segment.providerId, reservedSegment);
                    }
                }
            }

            // Create bill records for each provider for the day.
            for (Segment segment : providerToOnDemandSegment.values()) {
                if (segment.durationInHours > 0) {
                    generatedRecords.add(new Builder()
                            .cost(segment.cost)
                            .providerId(segment.providerId)
                            .sampleTime(dayStart)
                            .entityId(entityOid)
                            .entityType(entityType)
                            .priceModel(PriceModel.ON_DEMAND)
                            .costCategory(costCategory)
                            .providerType(providerType)
                            .usageAmount(segment.durationInHours)
                            .build());
                }
            }
            for (Segment segment : providerToReservedSegment.values()) {
                if (segment.durationInHours > 0) {
                    generatedRecords.add(new Builder().cost(0)
                            .providerId(segment.providerId)
                            .sampleTime(dayStart)
                            .entityId(entityOid)
                            .entityType(entityType)
                            .priceModel(PriceModel.RESERVED)
                            .costCategory(CostCategory.COMMITMENT_USAGE)
                            .providerType(providerType)
                            .usageAmount(segment.durationInHours)
                            .build());
                }
            }

            dayStart = dayStart.plusDays(1);
        }
        return generatedRecords;
    }

    private static List<Interval> createPoweredOffIntervals(LocalDateTime startTime,
            LocalDateTime endTime, NavigableSet<BillingScriptEvent> powerEvents, BillingScriptEvent deleteEvent) {
        long scenarioStartMillis = TimeUtil.localTimeToMillis(startTime, Clock.systemUTC());
        long scenarioEndMillis = TimeUtil.localTimeToMillis(endTime, Clock.systemUTC());

        List<Interval> poweredOffIntervals = new ArrayList<>();
        long intervalStart = scenarioStartMillis;
        // If there is no power events, assume the entity is always powered on. Otherwise, the entity
        // is powered on at the beginning of the scenario if the first power is a power off event,
        // and vice versa.
        boolean poweredOn = powerEvents.size() == 0
                || (powerEvents.first().timestamp >= scenarioStartMillis && !powerEvents.first().state);
        for (BillingScriptEvent powerEvent : powerEvents) {
            if (powerEvent.state && !poweredOn) {
                // Transitioning from power OFF to power ON.
                poweredOffIntervals.add(new Interval(intervalStart, powerEvent.timestamp));
                poweredOn = true;
            } else if (!powerEvent.state && poweredOn) {
                // Transitioning from power ON to power OFF.
                intervalStart = powerEvent.timestamp;
                poweredOn = false;
            }
        }
        if (!poweredOn) {
            poweredOffIntervals.add(new Interval(intervalStart, scenarioEndMillis));
        }

        if (deleteEvent != null) {
            poweredOffIntervals.add(new Interval(deleteEvent.timestamp, scenarioEndMillis));
        }
        return poweredOffIntervals;
    }

    private static BillingScriptEvent eventAtTime(LocalDateTime dateTime) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = TimeUtil.localTimeToMillis(dateTime, clock);
        return event;
    }

    private static List<Segment> createSegments(@Nonnull LocalDateTime dayStart,
            @Nonnull LocalDateTime dayEnd,
            NavigableSet<BillingScriptEvent> scaleEvents, BillingScriptEvent deleteEvent,
            List<Interval> poweredOffIntervals) {
        final long dayStartMillis = TimeUtil.localTimeToMillis(dayStart, clock);
        final long dayEndMillis = TimeUtil.localTimeToMillis(dayEnd, clock);
        // Find all script events on the day.
        SortedSet<BillingScriptEvent> eventsForDay = scaleEvents.subSet(
                eventAtTime(dayStart), eventAtTime(dayStart.plusDays(1)));

        final List<Segment> segments = new ArrayList<>();
        // No events happened on that day.
        if (eventsForDay.isEmpty()) {
            // 1 segment for the whole day at most.
            BillingScriptEvent referenceEvent = eventAtTime(dayStart);
            // find the closest event before the start of the day.
            BillingScriptEvent event = scaleEvents.floor(referenceEvent);
            if (event == null) {
                // Find the closest event after the day.
                event = scaleEvents.higher(referenceEvent);
            }
            if (event == null) {
                if (deleteEvent != null && deleteEvent.timestamp > dayStartMillis) {
                    event = deleteEvent;
                } else {
                    return segments;
                }
            }
            if (event.timestamp < dayStartMillis) {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.destinationOnDemandRate,
                        generateProviderIdFromRate(event.destinationOnDemandRate),
                        event.expectedCloudCommitment);
                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
            } else {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.sourceOnDemandRate,
                        generateProviderIdFromRate(event.sourceOnDemandRate),
                        0.0);
                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
            }
            return segments;
        }

        long segmentStart = dayStartMillis;
        long segmentEnd;
        for (BillingScriptEvent event : eventsForDay) {
            BillingScriptEvent previousEvent = scaleEvents.lower(event);
            segmentEnd = event.timestamp;
            Segment segment = new Segment(segmentStart, segmentEnd, event.sourceOnDemandRate,
                    generateProviderIdFromRate(event.sourceOnDemandRate),
                    previousEvent == null ? 0.0 : previousEvent.expectedCloudCommitment);
            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
            segmentStart = segmentEnd;
        }
        // last segment
        if (segmentStart < dayEndMillis && !"DELVOL".equals(eventsForDay.last().eventType)) {
            Segment segment = new Segment(segmentStart, dayEndMillis,
                    eventsForDay.last().destinationOnDemandRate,
                    generateProviderIdFromRate(eventsForDay.last().destinationOnDemandRate),
                    eventsForDay.last().expectedCloudCommitment);
            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
        }
        return segments;
    }

    static long generateProviderIdFromRate(double rate) {
        return (long)(rate * 10000);
    }

    private static ActionInfo createScaleActionInfo(long entityOid, double sourceOnDemandRate, double destOnDemandRate,
            long sourceProviderId, long destProviderId, int entityType, int tierType, double expectedRiCoverage) {
        TierCostDetails.Builder destinationTierCostDetails = TierCostDetails.newBuilder()
                .setOnDemandRate(CurrencyAmount.newBuilder()
                        .setAmount(destOnDemandRate)
                        .build())
                .setOnDemandCost(CurrencyAmount.newBuilder()
                        .setAmount(destOnDemandRate * 24)
                        .build());
        if (expectedRiCoverage > 0) {
            final double riCapacity = 4;
            destinationTierCostDetails.setCloudCommitmentCoverage(
                    CloudCommitmentCoverage.newBuilder()
                            .setCapacity(CloudCommitmentAmount.newBuilder()
                                    .setAmount(CurrencyAmount.newBuilder()
                                            .setAmount(riCapacity)
                                            .build())
                                    .build())
                            .setUsed(CloudCommitmentAmount.newBuilder()
                                    .setAmount(CurrencyAmount.newBuilder()
                                            .setAmount(riCapacity * expectedRiCoverage)
                                            .build())
                                    .build())
                            .build());
        }

        return ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(destProviderId)
                                        .setType(tierType)
                                        .build())
                                .setSource(ActionEntity.newBuilder()
                                        .setId(sourceProviderId)
                                        .setType(EntityType.COMPUTE_TIER_VALUE)
                                        .build())
                                .build())
                        .setCloudSavingsDetails(CloudSavingsDetails.newBuilder()
                                .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                        .setOnDemandRate(CurrencyAmount.newBuilder()
                                                .setAmount(sourceOnDemandRate)
                                                .build())
                                        .setOnDemandCost(CurrencyAmount.newBuilder()
                                                .setAmount(sourceOnDemandRate * 24)
                                                .build())
                                        .build())
                                .setProjectedTierCostDetails(destinationTierCostDetails)
                                .build())
                        .setTarget(ActionEntity.newBuilder()
                                .setId(entityOid)
                                .setType(entityType)
                                .build())
                        .build())
                .build();
    }

    private static ActionInfo createDeleteActionInfo(long entityOid, long sourceTierOid) {
        return ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(entityOid)
                                .setType(EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
                                .build())
                        .setSource(ActionEntity.newBuilder()
                                .setId(sourceTierOid) // dummy value
                                .setType(CommonDTOREST.EntityDTO.EntityType.STORAGE_TIER.getValue())
                                .build())
                        .build())
                .build();
    }

    private static ExecutedActionsChangeWindow createExecutedActionsChangeWindow(long entityOid,
            ActionSpec actionSpec, @Nullable LocalDateTime endTime, @Nonnull LivenessState state) {
        ExecutedActionsChangeWindow.Builder changeWindow = ExecutedActionsChangeWindow.newBuilder()
                .setEntityOid(entityOid)
                .setActionOid(actionSpec.getRecommendation().getId())
                .setActionSpec(actionSpec)
                .setLivenessState(state)
                .setStartTime(actionSpec.getExecutionStep().getCompletionTime());
        if (endTime != null) {
            changeWindow.setEndTime(TimeUtil.localTimeToMillis(endTime, clock));
        }
        return changeWindow.build();
    }

    private static ActionSpec createActionSpec(LocalDateTime actionTime, ActionInfo actionInfo) {
        // Set savings per hour value to a dummy value of 0. The value is not used.
        return createActionSpec(actionTime, actionInfo, 0);
    }

    private static ActionSpec createActionSpec(LocalDateTime actionTime, ActionInfo actionInfo, double savingsPerHour) {
        return ActionSpec.newBuilder()
                .setExecutionStep(ExecutionStep.newBuilder()
                        .setStatus(Status.SUCCESS)
                        .setCompletionTime(actionTime.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .build())
                .setRecommendation(Action.newBuilder()
                        .setInfo(actionInfo)
                        .setId(actionTime.toInstant(ZoneOffset.UTC).toEpochMilli()) // Use the execution time to identify the action
                        .setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(savingsPerHour).build())
                        .setDeprecatedImportance(1d)
                        .setExplanation(Explanation.newBuilder()
                                .setScale(ScaleExplanation.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Create VM ActionSpec object.
     *
     * @param entityOid entity OID
     * @param actionTime action time
     * @param sourceOnDemandRate source on-demand rate
     * @param destOnDemandRate destination on-demand rate
     * @param sourceProviderId source provider ID
     * @param destProviderId destination provider ID
     * @param expectedRiCoverage the expected RI coverage after the action (between 0 - 1)
     * @return ActionSpec object
     */
    public static ExecutedActionsChangeWindow createVMActionChangeWindow(long entityOid, LocalDateTime actionTime, double sourceOnDemandRate,
            double destOnDemandRate, long sourceProviderId, long destProviderId, @Nullable LocalDateTime endTime, @Nullable LivenessState state,
            double expectedRiCoverage) {
        if (state == null) {
            state = LivenessState.LIVE;
        }
        ActionInfo scaleActionInfo = createScaleActionInfo(entityOid, sourceOnDemandRate, destOnDemandRate,
                sourceProviderId, destProviderId,
                CommonDTOREST.EntityDTO.EntityType.COMPUTE_TIER.getValue(),
                CommonDTOREST.EntityDTO.EntityType.VIRTUAL_MACHINE.getValue(), expectedRiCoverage);
        ActionSpec actionSpec = createActionSpec(actionTime, scaleActionInfo,
                sourceOnDemandRate - destOnDemandRate);
        return createExecutedActionsChangeWindow(entityOid, actionSpec, endTime, state);
    }

    /**
     * Create volume ActionSpec object.
     *
     * @param entityOid entity OID
     * @param actionTime action time
     * @param sourceOnDemandRate source on-demand rate
     * @param sourceTierOid source tier OID
     * @return ActionSpec object
     */
    public static ExecutedActionsChangeWindow createVolumeDeleteActionSpec(long entityOid, LocalDateTime actionTime,
            double sourceOnDemandRate, long sourceTierOid) {
        ActionInfo deleteActionInfo = createDeleteActionInfo(entityOid, sourceTierOid);
        ActionSpec actionSpec = createActionSpec(actionTime, deleteActionInfo, sourceOnDemandRate);
        return createExecutedActionsChangeWindow(entityOid, actionSpec, null, LivenessState.LIVE);
    }

    /**
     * The Interval class represent a period of time with start and end timestamps.
     */
    static class Interval {
        protected long start;
        protected long end;

        /**
         * Constructor.
         *
         * @param start start timestamp
         * @param end start timestamp
         */
        Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    /**
     * A segment of a period of time when an entity is running with the same provider.
     * It is a simple data structure to hold the cost and duration of a segment for the purpose
     * of generating bill records.
     */
    static class Segment extends Interval {
        // Provider ID.
        private final long providerId;
        // Cost per hour
        private final double costPerHour;
        // Cost in the segment
        private double cost;
        // Duration in hours
        private double durationInHours;
        // expected RI coverage after action
        private double expectedRICoverage;

        /**
         * Constructor.
         * @param start start time
         * @param end end time
         * @param costPerHour cost per hour
         * @param providerId provider ID
         */
        Segment(long start, long end, double costPerHour, long providerId, double expectedRICoverage) {
            super(start, end);
            this.costPerHour = costPerHour;
            this.providerId = providerId;
            this.expectedRICoverage = expectedRICoverage;
            updateCost();
        }

        Segment(Segment segment) {
            super(segment.start, segment.end);
            this.costPerHour = segment.costPerHour;
            this.cost = segment.cost;
            this.providerId = segment.providerId;
            this.durationInHours = segment.durationInHours;
            this.expectedRICoverage = segment.expectedRICoverage;
        }

        private void updateCost() {
            this.durationInHours = (end - start) / 1000d / 3600d;
            this.cost = durationInHours * costPerHour;
        }

        List<Segment> exclude(List<Interval> intervals) {
            List<Segment> segments = new ArrayList<>();
            segments.add(this);
            for (Interval interval : intervals) {
                List<Segment> newSegmentList = new ArrayList<>();
                for (Segment segment : segments) {
                    newSegmentList.addAll(segment.exclude(interval));
                }
                segments = newSegmentList;
            }
            return segments;
        }

        List<Segment> exclude(Interval interval) {
            final List<Segment> segments = new ArrayList<>();
            if (start <= interval.start && end > interval.start) {
                if (end <= interval.end) {
                    Segment segment = new Segment(this);
                    segment.end = interval.start;
                    segment.updateCost();
                    segments.add(segment);
                } else {
                    Segment segment1 = new Segment(this);
                    segment1.end = interval.start;
                    segment1.updateCost();
                    segments.add(segment1);
                    Segment segment2 = new Segment(this);
                    segment2.start = interval.end;
                    segment2.updateCost();
                    segments.add(segment2);
                }
            } else if (start > interval.start && start <= interval.end) {
                if (end > interval.end) {
                    Segment segment = new Segment(this);
                    segment.start = interval.end;
                    segment.updateCost();
                    segments.add(segment);
                } // else the whole segment is cancelled out. So return nothing.
            } else {
                segments.add(this);
            }
            return segments;
        }
    }
}
