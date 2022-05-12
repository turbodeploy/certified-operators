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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingDataInjector.BillingScriptEvent;
import com.vmturbo.cost.component.savings.BillingRecord.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * ScenarioGenerator is used for generating action chains and bill records from script events.
 */
public class ScenarioGenerator {
    private static final Logger logger = LogManager.getLogger();
    private static final Clock clock = Clock.systemUTC();

    private ScenarioGenerator() {
    }

    /**
     * Generate action chains from script events.
     *
     * @param events script events
     * @param uuidMap display name to OID map
     * @return map of entity OID to sorted (by time) set of action chains
     */
    static Map<Long, NavigableSet<ActionSpec>> generateActionChains(List<BillingScriptEvent> events,
            Map<String, Long> uuidMap) {
        final Map<Long, NavigableSet<ActionSpec>> actionChains = new HashMap<>();
        logger.info("Generating actions from script events:");
        for (BillingScriptEvent event : events) {
            if ("RESIZE".equals(event.eventType)) {
                Long oid = uuidMap.getOrDefault(event.uuid, 0L);
                long actionTime = event.timestamp != null ? event.timestamp : 0;
                if (oid == 0 || actionTime == 0) {
                    logger.error("Malformed script event: {}", event);
                    continue;
                }
                LocalDateTime actionDateTime = TimeUtil.millisToLocalDateTime(actionTime, clock);
                ActionSpec action = createActionSpec(actionDateTime, oid, event.sourceOnDemandRate,
                        event.destinationOnDemandRate, generateProviderIdFromRate(event.sourceOnDemandRate),
                        generateProviderIdFromRate(event.destinationOnDemandRate));
                logger.info("action time: {}, entity OID: {}, source rate: {}, "
                                + "destination rate: {} source provider: {}, destination provider: {}",
                        actionDateTime, oid, event.sourceOnDemandRate, event.destinationOnDemandRate,
                        generateProviderIdFromRate(event.sourceOnDemandRate),
                        generateProviderIdFromRate(event.destinationOnDemandRate));
                actionChains.computeIfAbsent(oid, c -> new TreeSet<>(GrpcActionChainStore.specComparator))
                        .add(action);
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
        for (BillingScriptEvent event : events) {
            if ("RESIZE".equals(event.eventType)) {
                Long oid = uuidMap.getOrDefault(event.uuid, 0L);
                scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("POWER_STATE".equals(event.eventType)) {
                Long oid = uuidMap.getOrDefault(event.uuid, 0L);
                powerEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            }
        }
        final Map<Long, Set<BillingRecord>> billRecords = new HashMap<>();
        scaleEventsByEntity.forEach((oid, scaleEvents) -> billRecords.put(oid,
                generateBillRecordForEntity(scaleEvents,
                        powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()), oid,
                        startTime, endTime)));
        billRecords.forEach((oid, records) -> {
            logger.info("Generated bill records for entity {}:", oid);
            records.stream().sorted(Comparator.comparing(BillingRecord::getSampleTime))
                    .forEach(r -> logger.info("{}", r));
        });
        return billRecords;
    }

    private static Set<BillingRecord> generateBillRecordForEntity(NavigableSet<BillingScriptEvent> scaleEvents,
            NavigableSet<BillingScriptEvent> powerEvents, long entityOid, LocalDateTime startTime,
            LocalDateTime endTime) {
        if (scaleEvents.isEmpty()) {
            return Collections.emptySet();
        }
        // Start with the day of the first scale action because the days before that won't have any
        // savings/investments anyways.
        LocalDateTime dayStart = TimeUtil.millisToLocalDateTime(scaleEvents.first().timestamp, clock)
                .truncatedTo(ChronoUnit.DAYS);

        // Create powered off intervals, which can span over more than 1 day.
        List<Interval> poweredOffIntervals = createPoweredOffIntervals(startTime, endTime, powerEvents);

        // Loop through each day between the first event and the end time of the scenario.
        Set<BillingRecord> generatedRecords = new HashSet<>();
        while (dayStart.isBefore(endTime)) {
            // Create segments for the day. Each segment has one provider.
            LocalDateTime dayEnd = endTime.isBefore(dayStart.plusDays(1)) ? endTime
                    : dayStart.plusDays(1);
            List<Segment> segments = createSegments(dayStart, dayEnd, scaleEvents, poweredOffIntervals);

            // Find usage amount and cost for each provider for the day.
            // If there are multiple segments for the same provider, combine them.
            Map<Long, Segment> providerToSegment = new HashMap<>();
            for (Segment segment : segments) {
                if (providerToSegment.containsKey(segment.providerId)) {
                    Segment providerSegment = providerToSegment.get(segment.providerId);
                    providerSegment.cost += segment.cost;
                    providerSegment.durationInHours += segment.durationInHours;
                } else {
                    providerToSegment.put(segment.providerId, segment);
                }
            }

            // Create bill records for each provider for the day.
            for (Segment segment : providerToSegment.values()) {
                generatedRecords.add(new Builder()
                        .cost(segment.cost)
                        .providerId(segment.providerId)
                        .sampleTime(dayStart)
                        .entityId(entityOid)
                        .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .priceModel(PriceModel.ON_DEMAND)
                        .costCategory(CostCategory.COMPUTE)
                        .providerType(EntityType.COMPUTE_TIER_VALUE)
                        .usageAmount(segment.durationInHours)
                        .build());
            }

            dayStart = dayStart.plusDays(1);
        }
        return generatedRecords;
    }

    private static List<Interval> createPoweredOffIntervals(LocalDateTime startTime,
            LocalDateTime endTime, NavigableSet<BillingScriptEvent> powerEvents) {
        long scenarioStartMillis = TimeUtil.localTimeToMillis(startTime, Clock.systemUTC());
        long scenarioEndMillis = TimeUtil.localTimeToMillis(endTime, Clock.systemUTC());

        List<Interval> poweredOffIntervals = new ArrayList<>();
        long intervalStart = scenarioStartMillis;
        boolean poweredOn = powerEvents.size() == 0
                || (powerEvents.first().timestamp >= scenarioStartMillis && !powerEvents.first().state);
        for (BillingScriptEvent powerEvent : powerEvents) {
            if (powerEvent.state && !poweredOn) {
                poweredOffIntervals.add(new Interval(intervalStart, powerEvent.timestamp));
                poweredOn = true;
            } else if (!powerEvent.state && poweredOn) {
                intervalStart = powerEvent.timestamp;
                poweredOn = false;
            }
        }
        if (!poweredOn) {
            poweredOffIntervals.add(new Interval(intervalStart, scenarioEndMillis));
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
            NavigableSet<BillingScriptEvent> events, List<Interval> poweredOffIntervals) {
        final long dayStartMillis = TimeUtil.localTimeToMillis(dayStart, clock);
        final long dayEndMillis = TimeUtil.localTimeToMillis(dayEnd, clock);
        // Find all script events on the day.
        SortedSet<BillingScriptEvent> eventsForDay = events.subSet(
                eventAtTime(dayStart), eventAtTime(dayStart.plusDays(1)));

        final List<Segment> segments = new ArrayList<>();
        // No events happened on that day.
        if (eventsForDay.isEmpty()) {
            // 1 segment for the whole day at most.
            BillingScriptEvent referenceEvent = eventAtTime(dayStart);
            // find the closest event before the start of the day.
            BillingScriptEvent event = events.floor(referenceEvent);
            if (event == null) {
                // Find the closest event after the day.
                event = events.higher(referenceEvent);
            }
            if (event == null) {
                return segments;
            }
            if (event.timestamp < dayStartMillis) {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.destinationOnDemandRate, generateProviderIdFromRate(event.destinationOnDemandRate));
                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
            } else {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.sourceOnDemandRate, generateProviderIdFromRate(event.sourceOnDemandRate));
                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
            }
            return segments;
        }

        long segmentStart = dayStartMillis;
        long segmentEnd;
        for (BillingScriptEvent event : eventsForDay) {
            segmentEnd = event.timestamp;
            Segment segment = new Segment(segmentStart, segmentEnd, event.sourceOnDemandRate,
                    generateProviderIdFromRate(event.sourceOnDemandRate));
            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
            segmentStart = segmentEnd;
        }
        // last segment
        if (segmentStart < dayEndMillis) {
            Segment segment = new Segment(segmentStart, dayEndMillis,
                    eventsForDay.last().destinationOnDemandRate,
                    generateProviderIdFromRate(eventsForDay.last().destinationOnDemandRate));
            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
        }
        return segments;
    }

    static long generateProviderIdFromRate(double rate) {
        return (long)(rate * 10000);
    }

    private static ActionSpec createActionSpec(LocalDateTime actionTime, long entityOid,
            double sourceOnDemandRate, double destOnDemandRate, long sourceProviderId, long destProviderId) {
        return ActionSpec.newBuilder()
                .setExecutionStep(ExecutionStep.newBuilder()
                        .setStatus(Status.SUCCESS)
                        .setCompletionTime(actionTime.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .build())
                .setRecommendation(Action.newBuilder()
                        .setInfo(ActionInfo.newBuilder()
                                .setScale(Scale.newBuilder()
                                        .addChanges(ChangeProvider.newBuilder()
                                                .setDestination(ActionEntity.newBuilder()
                                                        .setId(destProviderId)
                                                        .setType(EntityType.COMPUTE_TIER_VALUE)
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
                                                        .build())
                                                .setProjectedTierCostDetails(TierCostDetails.newBuilder()
                                                        .setOnDemandRate(CurrencyAmount.newBuilder()
                                                                .setAmount(destOnDemandRate)
                                                                .build())
                                                        .build())
                                                .build())
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(entityOid)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                .build())
                                        .build())
                                .build())
                        .setId(67676767676L)
                        .setDeprecatedImportance(1d)
                        .setExplanation(Explanation.newBuilder()
                                .setScale(ScaleExplanation.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build();
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

        /**
         * Constructor.
         * @param start start time
         * @param end end time
         * @param costPerHour cost per hour
         * @param providerId provider ID
         */
        Segment(long start, long end, double costPerHour, long providerId) {
            super(start, end);
            this.costPerHour = costPerHour;
            this.providerId = providerId;
            updateCost();
        }

        Segment(Segment segment) {
            super(segment.start, segment.end);
            this.costPerHour = segment.costPerHour;
            this.cost = segment.cost;
            this.providerId = segment.providerId;
            this.durationInHours = segment.durationInHours;
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
