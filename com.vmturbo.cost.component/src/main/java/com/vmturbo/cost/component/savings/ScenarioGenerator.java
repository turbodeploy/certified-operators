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
import java.util.concurrent.TimeUnit;

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
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private ScenarioGenerator() {
    }

    /**
     * Generate action chains from script events.
     *
     * @param events script events
     * @param uuidMap display name to OID map
     * @return map of entity OID to sorted (by time) set of action chains
     */
    public static Map<Long, NavigableSet<ActionSpec>> generateActionChains(List<BillingScriptEvent> events,
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
                        event.destinationOnDemandRate, event.sourceTierId, event.destinationTierId);
                logger.info("action time: {}, entity OID: {}, source rate: {}, "
                                + "destination rate: {} source provider: {}, destination provider: {}",
                        actionDateTime, oid, event.sourceOnDemandRate, event.destinationOnDemandRate,
                        event.sourceTierId, event.destinationTierId);
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
     * @param endTime The end time of the scenario.
     * @return map of entity OID to set of bill records
     */
    public static Map<Long, Set<BillingRecord>> generateBillRecords(List<BillingScriptEvent> events,
            Map<String, Long> uuidMap, LocalDateTime endTime) {
        final Map<Long, NavigableSet<BillingScriptEvent>> eventsByEntity = new HashMap<>();
        for (BillingScriptEvent event : events) {
            if ("RESIZE".equals(event.eventType)) {
                Long oid = uuidMap.getOrDefault(event.uuid, 0L);
                eventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            }
        }
        final Map<Long, Set<BillingRecord>> billRecords = new HashMap<>();
        eventsByEntity.forEach((oid, records) -> billRecords.put(oid, generateBillRecordForEntity(records, oid, endTime)));
        billRecords.forEach((oid, records) -> {
            logger.info("Generated bill records for entity {}:", oid);
            records.forEach(r -> logger.info("{}", r));
        });
        return billRecords;
    }

    private static Set<BillingRecord> generateBillRecordForEntity(NavigableSet<BillingScriptEvent> events,
            long entityOid, LocalDateTime endTime) {
        if (events.isEmpty()) {
            return Collections.emptySet();
        }
        LocalDateTime dayStart = TimeUtil.millisToLocalDateTime(events.first().timestamp, clock)
                .truncatedTo(ChronoUnit.DAYS);

        // Loop through each day between the first and last event.
        Set<BillingRecord> generatedRecords = new HashSet<>();
        while (dayStart.isBefore(endTime)) {
            // Create segments for the day. Each segment has one provider.
            List<Segment> segments = createSegments(dayStart, events);

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

    private static BillingScriptEvent eventAtTime(LocalDateTime dateTime) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = TimeUtil.localTimeToMillis(dateTime, clock);
        return event;
    }

    private static List<Segment> createSegments(@Nonnull LocalDateTime dayStart,
            NavigableSet<BillingScriptEvent> events) {
        final long dayStartMillis = TimeUtil.localTimeToMillis(dayStart, clock);
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
                segments.add(new Segment(event.destinationOnDemandRate * 24, event.destinationTierId, 24));
            } else {
                segments.add(new Segment(event.sourceOnDemandRate * 24, event.sourceTierId, 24));
            }
            return segments;
        }
        // At least one event on that day.
        long segmentStart = dayStartMillis;
        long segmentEnd;
        for (BillingScriptEvent event : eventsForDay) {
            segmentEnd = event.timestamp;
            double durationInHours = (segmentEnd - segmentStart) / 1000d / 3600d;
            double cost = durationInHours * event.sourceOnDemandRate;
            segments.add(new Segment(cost, event.sourceTierId, durationInHours));
            segmentStart = segmentEnd;
        }
        // last segment
        if (segmentStart < dayStartMillis + MILLIS_IN_DAY) {
            double durationInHours = (dayStartMillis + MILLIS_IN_DAY - segmentStart) / 1000d / 3600d;
            double cost = durationInHours * eventsForDay.last().destinationOnDemandRate;
            segments.add(new Segment(cost, eventsForDay.last().destinationTierId, durationInHours));
        }
        return segments;
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
     * A segment of a period of time when an entity is running with the same provider.
     * It is a simple data structure to hold the cost and duration of a segment for the purpose
     * of generating bill records.
     */
    private static class Segment {
        // Provider ID.
        private final long providerId;
        // Cost
        private double cost;
        // Duration in hours
        private double durationInHours;

        /**
         * Constructor.
         * @param cost cost
         * @param providerId provider ID
         * @param durationInHours duration in hours
         */
        Segment(double cost, long providerId, double durationInHours) {
            this.cost = cost;
            this.providerId = providerId;
            this.durationInHours = durationInHours;
        }
    }
}
