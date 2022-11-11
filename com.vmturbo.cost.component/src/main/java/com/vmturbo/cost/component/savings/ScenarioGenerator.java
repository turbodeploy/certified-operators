package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingDataInjector.BillingScriptEvent;
import com.vmturbo.cost.component.savings.BillingDataInjector.Commodity;
import com.vmturbo.cost.component.savings.BillingRecord.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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

    private static final Map<EntityType, List<CommodityType>> entityTypeToCommType = ImmutableMap.of(EntityType.VIRTUAL_VOLUME,
            ImmutableList.of(CommodityType.STORAGE_ACCESS, CommodityType.STORAGE_AMOUNT, CommodityType.IO_THROUGHPUT),
            EntityType.DATABASE, ImmutableList.of(CommodityType.STORAGE_AMOUNT),
            EntityType.VIRTUAL_MACHINE, ImmutableList.of());

    @VisibleForTesting
    static final Map<String, Long> providerTypeToProviderId = new HashMap<>();

    private static final Set<String> storageTierNames = new HashSet<>(Arrays.asList("STANDARDSDD", "STANDARDHDD", "PREMIUM", "ULTRA"));

    private static final Set<String> dtuTierNames = new HashSet<>(Arrays.asList("BASIC", "S0", "S1", "S2", "S3", "S4",
            "S6", "S7", "S9", "S12", "P1", "P2", "P4", "P6", "P11",
            "P15"));

    private static final Set<String> vCoreTierNames = new HashSet<>(Arrays.asList("GP_GEN5_2", "GP_GEN5_4", "GP_GEN5_6", "GP_GEN5_8",
            "GP_GEN5_10", "GP_GEN5_12", "GP_GEN5_14", "GP_GEN5_16", "GP_GEN5_18", "GP_GEN5_20", "GP_GEN5_24", "GP_GEN5_32",
            "GP_GEN5_40", "GP_GEN5_80", "BC_GEN5_2", "BC_GEN5_4", "BC_GEN5_6", "BC_GEN5_8",
            "BC_GEN5_10", "BC_GEN5_12", "BC_GEN5_14", "BC_GEN5_16", "BC_GEN5_18", "BC_GEN5_20", "BC_GEN5_24", "BC_GEN5_32",
            "BC_GEN5_40", "BC_GEN5_80", "HS_GEN5_2", "HS_GEN5_4", "HS_GEN5_6", "HS_GEN5_8",
            "HS_GEN5_10", "HS_GEN5_12", "HS_GEN5_14", "HS_GEN5_16", "HS_GEN5_18", "HS_GEN5_20", "HS_GEN5_24", "HS_GEN5_32",
            "HS_GEN5_40", "HS_GEN5_80"));

    static {
        populateProviderIdFromType();
    }

    private ScenarioGenerator() {
    }

    /**
     * Class to maintain context specific information.
     */
    private static class Context {
        // Map of latest commodities for an entity (that can scale commodities), based on all it's previous
        // resizes.
        private Map<Long, List<Commodity>> entityCommoditiesMap = new HashMap<>();

        Context() {}

        /**
         * Update entity commidities after each resize.
         *
         * <p>Resizes would involve one or more commodities. The current commodities for the entity are kept
         * updated, and processed during segment creation</p>
         * @param oid The entity Oid.
         * @param newCommodities The new commodities being used by the entity on creation or after a resize.
         * @param entityType The entity type.
         */
        private void updateCommodities(final long oid, final List<Commodity> newCommodities, final EntityType entityType) {
            if (newCommodities == null || newCommodities.isEmpty()) {
                return;
            }
            List<Commodity> existingCommodities = entityCommoditiesMap.get(oid);
            if (existingCommodities == null) {
                entityCommoditiesMap.put(oid, newCommodities);
                return;
            }
            List<CommodityType> commTypes = entityTypeToCommType.get(entityType);
            if (!commTypes.isEmpty() && newCommodities.size() == commTypes.size()) {
                entityCommoditiesMap.put(oid, newCommodities);
                return;
            }

            List<Commodity> updatedCommodities = new ArrayList<>();
            updatedCommodities.addAll(existingCommodities);
            for (CommodityType commType : commTypes) {
                Optional<Commodity> newCommodity = newCommodities.stream().filter(comm -> CommodityType.valueOf(comm.commType) == commType)
                        .findFirst();
                if (newCommodity.isPresent()) {
                    Optional<Commodity> existingCommodity = existingCommodities.stream()
                            .filter(comm -> CommodityType.valueOf(comm.commType) == commType)
                            .findFirst();
                    if (existingCommodity.isPresent()) {
                        updatedCommodities.remove(existingCommodity.get());

                    }
                    updatedCommodities.add(newCommodity.get());
                }
            }
            entityCommoditiesMap.put(oid, updatedCommodities);
        }
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
            } else if ("RESIZE-VOL".equals(event.eventType)) {
                final double sourceRate = event.commodities.stream().mapToDouble(comm -> comm.sourceRate).sum();
                final double destinationRate = event.commodities.stream().mapToDouble(comm -> comm.destinationRate).sum();
                ExecutedActionsChangeWindow action = createVolumeActionChangeWindow(oid, actionDateTime,
                        sourceRate, destinationRate,
                        generateProviderIdFromType(event.sourceType),
                        generateProviderIdFromType(event.destinationType), null,
                        LivenessState.LIVE, createResizeInfoList(event.commodities));
                logger.info("{} Scale action time: {}, entity OID: {}, source rate: {}, "
                                + "destination rate: {} source provider: {}, destination provider: {}, "
                        + "commodities: {}, expected RI coverage: {}", event.eventType,
                        actionDateTime, oid, event.sourceOnDemandRate, event.destinationOnDemandRate,
                        generateProviderIdFromType(event.sourceType),
                        generateProviderIdFromType(event.destinationType), event.commodities, 0);
                actionChains.computeIfAbsent(oid, c -> new TreeSet<>(GrpcActionChainStore.changeWindowComparator))
                        .add(action);
            } else if ("RESIZE-DB".equals(event.eventType)) {
                final double sourceStorageRate = event.commodities != null ? event.commodities.stream().mapToDouble(comm -> comm.sourceRate).sum() : 0.0;
                final double sourceRate = event.sourceOnDemandRate + event.sourceLicenseRate + sourceStorageRate;
                final double destinationStorageRate = event.commodities != null ? event.commodities.stream().mapToDouble(comm -> comm.sourceRate).sum() : 0.0;
                final double destinationRate = event.destinationOnDemandRate + event.destinationLicenseRate + destinationStorageRate;
                ExecutedActionsChangeWindow action = createDatabaseActionChangeWindow(oid, actionDateTime,
                        sourceRate, destinationRate,
                        event.sourceOnDemandRate != 0 ? generateProviderIdFromRate(event.sourceOnDemandRate)
                                : generateProviderIdFromType(event.sourceType),
                        event.destinationOnDemandRate != 0 ? generateProviderIdFromRate(event.destinationOnDemandRate)
                                : generateProviderIdFromType(event.destinationType), null,
                        LivenessState.LIVE, createResizeInfoList(event.commodities));
                logger.info("{} Scale action time: {}, entity OID: {}, source rate: {}, "
                                + "destination rate: {} source provider: {}, destination provider: {}, "
                                + "commodities: {}, expected RI coverage: {}", event.eventType,
                        actionDateTime, oid, sourceRate, destinationRate,
                        event.sourceOnDemandRate != 0 ? generateProviderIdFromRate(event.sourceOnDemandRate)
                                : generateProviderIdFromType(event.sourceType),
                        event.destinationOnDemandRate != 0 ? generateProviderIdFromRate(event.destinationOnDemandRate)
                                : generateProviderIdFromType(event.destinationType), event.commodities, 0);
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
            } else if ("REVERT".equals(event.eventType) || "REVERT-VOL".equals(event.eventType)
                    || "REVERT-DB".equals(event.eventType)) {
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
            } else if ("EXTMOD".equals(event.eventType) || "EXTMOD-VOL".equals(event.eventType)
                    || "EXTMOD-DB".equals(event.eventType)) {
                NavigableSet<ExecutedActionsChangeWindow> changeWindows = actionChains.get(oid);
                if (changeWindows != null) {
                    ExecutedActionsChangeWindow lastAction = changeWindows.pollLast();
                    if (lastAction != null) {
                        logger.info("External modification time: {} entity OID: {}", actionDateTime, oid);
                        changeWindows.add(lastAction.toBuilder()
                                .setEndTime(event.timestamp)
                                .setLivenessState(LivenessState.EXTERNAL_MODIFICATION)
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
        final Map<Long, NavigableSet<BillingScriptEvent>> scaleDbEventsByEntity = new HashMap<>();
        final Map<Long, NavigableSet<BillingScriptEvent>> scaleVolumeEventsByEntity = new HashMap<>();
        final Map<Long, NavigableSet<BillingScriptEvent>> powerEventsByEntity = new HashMap<>();
        final Map<Long, BillingScriptEvent> deleteEventsByEntity = new HashMap<>();

        final NavigableSet<BillingScriptEvent> sortedEvents =
                new TreeSet<>(Comparator.comparingLong(BillingScriptEvent::getTimestamp));
        sortedEvents.addAll(events);

        Context context = new Context();

        for (BillingScriptEvent event : sortedEvents) {
            Long oid = uuidMap.getOrDefault(event.uuid, 0L);
            if ("RESIZE".equals(event.eventType) || "EXTMOD".equals(event.eventType)) {
                scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("CREATE-VOL".equals(event.eventType) || "EXTMOD-VOL".equals(event.eventType)) {
                if (!event.commodities.isEmpty()) {
                    context.updateCommodities(oid, event.commodities, EntityType.VIRTUAL_VOLUME);
                }
                // Treat as a scale volume event where usage and cost go from 0 to the usage and cost of
                // the storage created.
                scaleVolumeEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("CREATE-DB".equals(event.eventType) || "EXTMOD-DB".equals(event.eventType)) {
                if (event.commodities != null && !event.commodities.isEmpty()) {
                    context.updateCommodities(oid, event.commodities, EntityType.DATABASE);
                }
                // Treat as a scale db event where usage and cost go from 0 to the usage and cost of
                // the storage created.
                scaleDbEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("RESIZE-VOL".equals(event.eventType)) {
                scaleVolumeEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                        .add(event);
            } else if ("RESIZE-DB".equals(event.eventType)) {
                scaleDbEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
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
                    revertEvent.eventType = event.eventType;
                    revertEvent.uuid = event.uuid;
                    revertEvent.sourceOnDemandRate = previousEvent.destinationOnDemandRate;
                    revertEvent.destinationOnDemandRate = previousEvent.sourceOnDemandRate;
                    scaleEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                            .add(revertEvent);
                }
            } else if ("REVERT-VOL".equals(event.eventType) || "REVERT-DB".equals(event.eventType)) {
                // Model the revert as a scale action which in the opposite direction as the previous
                // scale action.
                BillingScriptEvent previousEvent = getPreviousScaleEvent(sortedEvents, event);
                if (previousEvent == null) {
                    previousEvent = getCreateEvent(sortedEvents, event);
                }
                if (previousEvent != null) {
                    BillingScriptEvent revertEvent = new BillingScriptEvent();
                    revertEvent.timestamp = event.timestamp;
                    revertEvent.eventType = event.eventType;
                    revertEvent.uuid = event.uuid;
                    revertEvent.sourceType = previousEvent.sourceType;
                    revertEvent.destinationType = previousEvent.destinationType;
                    if (previousEvent.commodities != null) {
                        revertEvent.commodities = new ArrayList<>();
                        for (Commodity commodity : previousEvent.commodities) {
                            revertEvent.commodities.add(new Commodity(commodity.commType, 0, 0,
                                    commodity.sourceCapacity, commodity.sourceRate));
                        }
                    }
                    if ("REVERT-VOL".equals(event.eventType)) {
                        scaleVolumeEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                                .add(revertEvent);
                    } else if ("REVERT-DB".equals(event.eventType)) {
                        scaleDbEventsByEntity.computeIfAbsent(oid, r -> new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)))
                                .add(revertEvent);
                    }
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
                    riCoverageEvent.eventType = event.eventType;
                    riCoverageEvent.uuid = event.uuid;
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
                        deleteEventsByEntity.get(oid), oid, startTime, endTime, uuidMap, context, false, false)));

        scaleVolumeEventsByEntity.forEach((oid, scaleVolumeEvents) ->
                billRecords.put(oid, generateBillRecordForEntity(scaleVolumeEvents,
                        powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()),
                        deleteEventsByEntity.get(oid), oid, startTime, endTime, uuidMap, context, true, false)));

        scaleDbEventsByEntity.forEach((oid, scaleDbEvents) ->
                billRecords.put(oid, generateBillRecordForEntity(scaleDbEvents,
                        powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()),
                        deleteEventsByEntity.get(oid), oid, startTime, endTime, uuidMap, context, false, true)));

        // Generate bill records for entities that only have delete actions.
        // We don't assume that these events are for volumes at the outset (isVolume parameter is false), however delete
        // events are processed as events on volume entitytype, at the moment.
        deleteEventsByEntity.forEach((oid, event) -> {
            if (!scaleEventsByEntity.containsKey(oid) && !scaleVolumeEventsByEntity.containsKey(oid) && !scaleDbEventsByEntity.containsKey(oid)) {
                billRecords.put(oid, generateBillRecordForEntity(new TreeSet<>(Comparator.comparing(BillingScriptEvent::getTimestamp)),
                        powerEventsByEntity.getOrDefault(oid, Collections.emptyNavigableSet()), event,
                        oid, startTime, endTime, uuidMap, context, false, false));
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
        while (previousEvent != null && !"RESIZE".equals(previousEvent.eventType)
                && !"RESIZE-VOL".equals(previousEvent.eventType)) {
            previousEvent = sortedEvents.lower(previousEvent);
            BillingScriptEvent nextToPreviousEvent = sortedEvents.higher(previousEvent);
            // If the subsequent event is a revert, return the scale action previous to this
            // scale action, as this one is already reverted.
            if ("REVERT-VOL".equals(nextToPreviousEvent.eventType)) {
                previousEvent = sortedEvents.lower(previousEvent);
            }
        }
        if (previousEvent == null) {
            logger.error("Unsupported scenario: RI Coverage change action is not after a scale action.");
            return null;
        }
        return previousEvent;
    }

    @Nullable
    private static BillingScriptEvent getCreateEvent(NavigableSet<BillingScriptEvent> sortedEvents,
                                                            BillingScriptEvent event) {
        BillingScriptEvent previousEvent = sortedEvents.lower(event);
        while (previousEvent != null && !"CREATE-VOL".equals(previousEvent.eventType)) {
            previousEvent = sortedEvents.lower(previousEvent);
        }
        if (previousEvent == null) {
            logger.error("Unsupported scenario: Create Event absent for volume {}.", event.uuid);
            return null;
        }
        return previousEvent;
    }

    private static Set<BillingRecord> generateBillRecordForEntity(NavigableSet<BillingScriptEvent> scaleEvents,
            NavigableSet<BillingScriptEvent> powerEvents, @Nullable BillingScriptEvent deleteEvent,
            long entityOid, LocalDateTime startTime, LocalDateTime endTime, Map<String, Long> uuidMap,
            Context context, boolean isVolume, boolean isDb) {
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
            int entityType = EntityType.VIRTUAL_MACHINE_VALUE;
            CostCategory costCategory = CostCategory.COMPUTE_LICENSE_BUNDLE;
            int commodityType = CommodityType.UNKNOWN_VALUE;
            int providerType = EntityType.COMPUTE_TIER_VALUE;
            if (deleteEvent != null) {
                entityType = EntityType.VIRTUAL_VOLUME_VALUE;
                costCategory = CostCategory.STORAGE;
                providerType = EntityType.STORAGE_TIER_VALUE;
                commodityType = CommodityType.STORAGE_VALUE;
            } else if (isVolume) {
                entityType = EntityType.VIRTUAL_VOLUME_VALUE;
                costCategory = CostCategory.STORAGE;
                providerType = EntityType.STORAGE_TIER_VALUE;
            } else if (isDb) {
                entityType = EntityType.DATABASE_VALUE;
                costCategory = CostCategory.STORAGE;
                providerType = EntityType.DATABASE_TIER_VALUE;
            }

            List<Segment> segments = createSegments(dayStart, dayEnd, scaleEvents, deleteEvent,
                    poweredOffIntervals, uuidMap, context, EntityType.valueOf(entityType));

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
                                ? segment.durationInHours / HOURS_IN_A_MONTH : segment.durationInHours;
                        reservedSegment.durationInHours = duration * segment.expectedRICoverage;
                        providerToReservedSegment.put(segment.providerId, reservedSegment);
                    }
                }
            }

            // Create bill records for each provider for the day.
            final List<Segment> hyperScaleVCoreSegments = providerToOnDemandSegment.values()
                    .stream()
                    .filter(s -> s.destinationType != null && isHyperscaleVcoreProvider(s.destinationType))
                    .collect(Collectors.toList());
            final double totalUsageForHyperScaleSegments = getTotalUsageForHyperscale(hyperScaleVCoreSegments);
            final double totalCostforHyperScaleSegments = getTotalCostHyperscale(hyperScaleVCoreSegments);
            final double multiplierForHyperscaleSegments = totalCostforHyperScaleSegments / totalUsageForHyperScaleSegments;
            for (Segment segment : providerToOnDemandSegment.values()) {
                if (segment.durationInHours > 0) {
                    if (!isVolume && !isDb) { // VM record
                        generatedRecords.add(new Builder()
                                .cost(segment.cost)
                                .providerId(segment.providerId)
                                .sampleTime(dayStart)
                                .entityId(entityOid)
                                .entityType(entityType)
                                .priceModel(PriceModel.ON_DEMAND)
                                .costCategory(costCategory)
                                .providerType(providerType)
                                .commodityType(commodityType)
                                .usageAmount(segment.durationInHours)
                                .accountId(1L)
                                .regionId(2L)
                                .serviceProviderId(100L)
                                .build());
                    }
                    if (isDb ) { // Db compute record
                        if (isDtuProvider(segment.destinationType)) {
                            generatedRecords.add(new Builder()
                                    .cost(segment.cost)
                                    .providerId(segment.providerId)
                                    .sampleTime(dayStart)
                                    .entityId(entityOid)
                                    .entityType(entityType)
                                    .priceModel(PriceModel.ON_DEMAND)
                                    .costCategory(CostCategory.COMPUTE_LICENSE_BUNDLE)
                                    .providerType(providerType)
                                    .commodityType(commodityType)
                                    .usageAmount(segment.durationInHours)
                                    .accountId(1L)
                                    .regionId(2L)
                                    .serviceProviderId(100L)
                                    .build());
                        } else if ( isVcoreProvider(segment.destinationType)) {
                            generatedRecords.add(new Builder()
                                    .cost(isHyperscaleVcoreProvider(segment.destinationType)
                                            ? multiplierForHyperscaleSegments * segment.durationInHours * segment.numVCores
                                            : segment.cost)
                                    .providerId(segment.providerId)
                                    .sampleTime(dayStart)
                                    .entityId(entityOid)
                                    .entityType(entityType)
                                    .priceModel(PriceModel.ON_DEMAND)
                                    .costCategory(CostCategory.COMPUTE)
                                    .providerType(providerType)
                                    .commodityType(commodityType)
                                    .usageAmount(isHyperscaleVcoreProvider(segment.destinationType)
                                            ? segment.durationInHours * segment.numVCores
                                            : segment.durationInHours)
                                    .accountId(1L)
                                    .regionId(2L)
                                    .serviceProviderId(100L)
                                    .build());
                        }
                    }
                }
            }

            for (Segment segment : providerToReservedSegment.values()) {
                if (segment.durationInHours > 0 && !isVolume && !isDb) {
                    generatedRecords.add(new Builder().cost(0)
                            .providerId(segment.providerId)
                            .sampleTime(dayStart)
                            .entityId(entityOid)
                            .entityType(entityType)
                            .priceModel(PriceModel.RESERVED)
                            .costCategory(CostCategory.COMMITMENT_USAGE)
                            .providerType(providerType)
                            .commodityType(CommodityType.UNKNOWN_VALUE)
                            .usageAmount(segment.durationInHours)
                            .accountId(1L)
                            .regionId(2L)
                            .serviceProviderId(100L)
                            .build());
                }
            }

            // Billing Records based on commodities.
            final Map<Long, List<Segment>> providerIdToSegments = segments
                    .stream()
                    .collect(Collectors
                            .groupingBy(Segment::getProviderId));

            if (isVolume) {
                for (List<Segment> providerSegments : providerIdToSegments.values()) {
                    generatedRecords.addAll(generateBillRecordsBasedOnCommodities(providerSegments,
                            EntityType.VIRTUAL_VOLUME_VALUE, costCategory, entityOid, providerType, dayStart));
                }
            } else if (isDb) {
                // Excess Storage charges.
                generatedRecords.addAll(generateBillRecordsBasedOnCommodities(segments, EntityType.DATABASE_VALUE,
                        costCategory, entityOid, providerType, dayStart));
            }

            // Billing Records based on License
            final List<Segment> vCoreSegments = segments
                    .stream()
                    .filter(s -> s.destinationType != null && isVcoreProvider(s.destinationType)
                            && s.licenseCostPerHour > 0)
                    .collect(Collectors.toList());

            final double costOfLicense = getCostOfLicense(vCoreSegments);
            if (costOfLicense > 0) {
                generatedRecords.add(new Builder()
                    .cost(costOfLicense)
                    .providerId(0)
                    .sampleTime(dayStart)
                    .entityId(entityOid)
                    .entityType(entityType)
                    .priceModel(PriceModel.ON_DEMAND)
                    .costCategory(CostCategory.LICENSE)
                    .providerType(providerType)
                    .commodityType(commodityType)
                    .usageAmount(getUsageAmountOfLicense(vCoreSegments))
                    .accountId(1L)
                    .regionId(2L)
                    .serviceProviderId(100L)
                    .build());
            }

            dayStart = dayStart.plusDays(1);
        }
        return generatedRecords;
    }

    private static Set<BillingRecord> generateBillRecordsBasedOnCommodities(final List<Segment> segments,
            final int entityType, final CostCategory costCategory, final long entityOid,
            final int providerType, final LocalDateTime dayStart) {
        Set<BillingRecord> generatedRecords = new HashSet<>();
        Map<CommodityType, Double> usageAmountByCommodity = new HashMap<>();
        usageAmountByCommodity.putAll(getUsageAmountOfCommodities(segments, EntityType.valueOf(entityType)));
        Map<CommodityType, Double> costByCommodity = new HashMap<>();
        costByCommodity.putAll(getCostOfCommodities(segments, EntityType.valueOf(entityType)));
        if (!usageAmountByCommodity.isEmpty()) {
            for (CommodityType commType : usageAmountByCommodity.keySet()) {
                generatedRecords.add(new Builder()
                        .cost(costByCommodity.get(commType))
                        .providerId(entityType == EntityType.DATABASE_VALUE ? 0 : segments.get(0).providerId)
                        .sampleTime(dayStart)
                        .entityId(entityOid)
                        .entityType(entityType)
                        .priceModel(PriceModel.ON_DEMAND)
                        .costCategory(costCategory)
                        .providerType(providerType)
                        .commodityType(commType.getNumber())
                        .usageAmount(usageAmountByCommodity.get(commType))
                        .accountId(1L)
                        .regionId(2L)
                        .serviceProviderId(100L)
                        .build());
            }
        }
        return generatedRecords;
    }

    /**
     * Get Usage by Commodities.
     *
     * <p>Volumes are charged on the basis of one or more commodities</p>
     * @param segments The relevant segments for a given provider.
     * @param entityType The entity type.
     * @return Map of Commodity to usage Amount over all segments, for the provider.
     */
    private static Map<CommodityType, Double> getUsageAmountOfCommodities(final Collection<Segment> segments,
                                                                               final EntityType entityType) {
        Map<CommodityType, Double> usageAmountByCommodity = new HashMap<>();
        Double commodityUsage = 0.0;
        for (CommodityType commType : entityTypeToCommType.get(entityType)) {
            for (Segment segment : segments) {
                if ((segment.providerId == 0 && segment.destinationType == null) || segment.commodities.isEmpty()) {
                    continue;
                }
                if (!("ULTRA").equals(segment.destinationType) && !(commType.getNumber() == CommodityType.STORAGE_AMOUNT_VALUE)) {
                    continue;
                }
                Optional<Commodity> commodity = getCommodityOfType(segment.commodities, commType.name());
                if (commodity.isPresent()) {
                    commodityUsage = usageAmountByCommodity.getOrDefault(commType, 0.0);
                    usageAmountByCommodity.put(commType, commodityUsage + (segment.durationInHours * commodity.get().destinationCapacity));
                }
            }
        }
        return usageAmountByCommodity;
    }

    /**
     * Get Cost by Commodities.
     *
     * <p>Volumes are charged on the basis of one or more commodities</p>
     * @param segments The segments for a given provider.
     * @param entityType The entity type.
     * @return Map of Commodity to cost of the commodity over all segments, for the provider.
     */
    private static Map<CommodityType, Double> getCostOfCommodities(final Collection<Segment> segments,
                                                                        final EntityType entityType) {
        Map<CommodityType, Double> costByCommodity = new HashMap<>();
        Double commodityCost = 0.0;
        for (CommodityType commType : entityTypeToCommType.get(entityType)) {
            for (Segment segment : segments) {
                if ((segment.providerId == 0 && segment.destinationType == null) || segment.commodities.isEmpty()) {
                    continue;
                }
                if (!("ULTRA").equals(segment.destinationType) && !(commType.getNumber() == CommodityType.STORAGE_AMOUNT_VALUE)) {
                    continue;
                }
                Optional<Commodity> commodity = getCommodityOfType(segment.commodities, commType.name());
                if (commodity.isPresent()) {
                    commodityCost = costByCommodity.getOrDefault(commType, 0.0);
                    costByCommodity.put(commType, commodityCost + (segment.durationInHours * commodity.get().destinationRate));
                }
            }
        }
        return costByCommodity;
    }

    /**
     * Get Usage of License over all segments.
     *
     *
     * @param segments The relevant segments for a given provider.
     * @return license usage Amount over all segments, for the provider.
     */
    private static Double getUsageAmountOfLicense(final Collection<Segment> segments) {
        Double licenseUsage = 0.0;
        for (Segment segment : segments) {
            licenseUsage += segment.durationInHours * segment.numVCores;
        }
        return licenseUsage;
    }

    /**
     * Get Cost of License over all segments for a provider.
     *
     *
     * @param segments The relevant segments for a given provider.
     * @return license cost over all segments, for the provider.
     */
    private static Double getCostOfLicense(final Collection<Segment> segments) {
        Double licenseCost = 0.0;
        for (Segment segment : segments) {
            licenseCost += segment.durationInHours * segment.licenseCostPerHour;
        }
        return licenseCost;
    }

    private static Double getTotalCostHyperscale(final Collection<Segment> segments) {
        Double totalCost = 0.0;
        for (Segment segment : segments) {
            totalCost += segment.cost;
        }
        return totalCost;
    }


    private static Double getTotalUsageForHyperscale(final Collection<Segment> segments) {
        Double totalUsage = 0.0;
        for (Segment segment : segments) {
            totalUsage += (segment.durationInHours * segment.numVCores);
        }
        return totalUsage;
    }

    /**
     * Get commodity of a given CommodityType from a list of commodities.
     *
     * @param commodities The list of commodities.
     * @param commType The Commodity Type.
     * @return Commodity of type commType, if present.
     */
    private static Optional<Commodity> getCommodityOfType(final List<Commodity> commodities, final String commType) {
        return commodities.stream()
                .filter(commodity -> commodity.commType.equals(commType)).findFirst();
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
            List<Interval> poweredOffIntervals, Map<String, Long> uuidMap, Context context, EntityType entityType) {
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
                    event.commodities = Collections.EMPTY_LIST;
                } else {
                    return segments;
                }
            }

            Long oid = uuidMap.getOrDefault(event.uuid, 0L);
            List<Commodity> entityCommodities = context.entityCommoditiesMap.get(oid);
            if (event.timestamp < dayStartMillis) {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.destinationOnDemandRate,
                        event.destinationOnDemandRate != 0 ? generateProviderIdFromRate(event.destinationOnDemandRate)
                        : generateProviderIdFromType(event.destinationType),
                        entityCommodities,
                        event.expectedCloudCommitment,
                        event.destinationType,
                        getNumVcores(event.destinationType), //destinationNumVCores,
                        event.destinationLicenseRate);

                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
                if (event.destinationType != null) {
                    context.updateCommodities(oid, event.commodities, entityType);
                }
            } else {
                Segment segment = new Segment(dayStartMillis, dayEndMillis,
                        event.sourceOnDemandRate,
                        event.sourceOnDemandRate != 0 ? generateProviderIdFromRate(event.sourceOnDemandRate)
                                : generateProviderIdFromType(event.sourceType),
                        entityCommodities == null ?  Collections.emptyList() : entityCommodities,
                        0.0,
                        event.sourceType,
                        getNumVcores(event.sourceType), //event.sourceNumVCores,
                        event.sourceLicenseRate);

                // Exclude time when it is powered off.
                List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
                segments.addAll(segmentsToAdd);
                if (event.sourceType != null) {
                    context.updateCommodities(oid, event.commodities, entityType);
                }
            }
            return segments;
        }

        long segmentStart = dayStartMillis;
        long segmentEnd;
        for (BillingScriptEvent event : eventsForDay) {
            BillingScriptEvent previousEvent = scaleEvents.lower(event);
            segmentEnd = event.timestamp;
            Long oid = uuidMap.getOrDefault(event.uuid, 0L);
            List<Commodity> entityCommodities = context.entityCommoditiesMap.get(oid);
            Segment segment = new Segment(segmentStart, segmentEnd, event.sourceOnDemandRate,
                    event.sourceOnDemandRate != 0 ? generateProviderIdFromRate(event.sourceOnDemandRate)
                            : generateProviderIdFromType(event.sourceType),
                    entityCommodities == null ?  Collections.emptyList() : entityCommodities,
                    previousEvent == null ? 0.0 : previousEvent.expectedCloudCommitment,
                    event.sourceType,
                    getNumVcores(event.sourceType), //event.sourceNumVCores,
                    event.sourceLicenseRate);

            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
            segmentStart = segmentEnd;
            if (event.sourceType != null) {
                context.updateCommodities(oid, event.commodities, entityType);
            }
        }
        // last segment
        if (segmentStart < dayEndMillis && !"DELVOL".equals(eventsForDay.last().eventType)) {
            Long oid = uuidMap.getOrDefault(eventsForDay.last().uuid, 0L);
            List<Commodity> entityCommodities = context.entityCommoditiesMap.get(oid);
            Segment segment = new Segment(segmentStart, dayEndMillis,
                    eventsForDay.last().destinationOnDemandRate,
                    eventsForDay.last().destinationOnDemandRate != 0 ? generateProviderIdFromRate(eventsForDay.last().destinationOnDemandRate)
                            : generateProviderIdFromType(eventsForDay.last().destinationType),
                    entityCommodities,
                    eventsForDay.last().expectedCloudCommitment,
                    eventsForDay.last().destinationType,
                    getNumVcores(eventsForDay.last().destinationType), //eventsForDay.last().destinationNumVCores,
                    eventsForDay.last().destinationLicenseRate);

            // Exclude time when it is powered off.
            List<Segment> segmentsToAdd = segment.exclude(poweredOffIntervals);
            segments.addAll(segmentsToAdd);
            if (eventsForDay.last().destinationType != null) {
                context.updateCommodities(oid, eventsForDay.last().commodities, entityType);
            }
        }

        return segments;
    }

    @VisibleForTesting
    static void populateProviderIdFromType() {
        Long id = 0L;
        providerTypeToProviderId.put("NONE", id);
        for (String storageTierName : storageTierNames) {
            providerTypeToProviderId.put(storageTierName, ++id);
        }
        for (String dtuDbTierName : dtuTierNames) {
            providerTypeToProviderId.put(dtuDbTierName, ++id);
        }
        for (String vCoreDbTierName : vCoreTierNames) {
            providerTypeToProviderId.put(vCoreDbTierName, ++id);
        }
    }

    static long generateProviderIdFromType(final String tierType) {
        return providerTypeToProviderId.getOrDefault(tierType, 0L) * 1000;
    }

    static long generateProviderIdFromRate(double rate) {
        return (long)(rate * 10000);
    }

    static boolean isDtuProvider(@Nonnull final String tierName) {
        return dtuTierNames.contains(tierName);
    }

    static boolean isVcoreProvider(@Nonnull final String tierName) {
        return vCoreTierNames.contains(tierName);
    }

    static boolean isHyperscaleVcoreProvider(@Nonnull final String tierName) {
        return tierName != null && vCoreTierNames.contains(tierName) && tierName.indexOf("HS") == 0;
    }

    private static ActionInfo createScaleActionInfo(long entityOid, double sourceOnDemandRate, double destOnDemandRate,
            long sourceProviderId, long destProviderId, int entityType, int tierType, final List<ResizeInfo> resizeInfoList,
                                                    double expectedRiCoverage) {
        TierCostDetails.Builder destinationTierCostDetails = TierCostDetails.newBuilder()
                .setOnDemandRate(CurrencyAmount.newBuilder()
                        .setAmount(destOnDemandRate)
                        .build())
                .setOnDemandCost(CurrencyAmount.newBuilder()
                        // TODO:  should we be  multiplying by 24 instead of duration
                        // on the provider ?   Do we use the cost field in savings calculation ?
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

        Scale.Builder scaleBuilder = Scale.newBuilder();
        if (destProviderId != sourceProviderId) {
            scaleBuilder.addChanges(ChangeProvider.newBuilder()
                    .setDestination(ActionEntity.newBuilder()
                            .setId(destProviderId)
                            .setType(tierType)
                            .build())
                    .setSource(ActionEntity.newBuilder()
                            .setId(sourceProviderId)
                            .setType(tierType)
                            .build())
                    .build());
        } else {
            scaleBuilder.setPrimaryProvider(ActionEntity.newBuilder()
                    .setId(sourceProviderId)
                    .setType(tierType)
                    .build());
        }

        scaleBuilder.setCloudSavingsDetails(CloudSavingsDetails.newBuilder()
                        .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                .setOnDemandRate(CurrencyAmount.newBuilder()
                                        .setAmount(sourceOnDemandRate)
                                        .build())
                                .setOnDemandCost(CurrencyAmount.newBuilder()
                                        // TODO:  should we be  multiplying by 24 instead of duration
                                        // on the provider ?  Do we use the cost field in savings calculation ?
                                        .setAmount(sourceOnDemandRate * 24)
                                        .build())
                                .build())
                        .setProjectedTierCostDetails(destinationTierCostDetails)
                        .build())
                .setTarget(ActionEntity.newBuilder()
                        .setId(entityOid)
                        .setType(entityType)
                        .build());
        if (resizeInfoList != null && !resizeInfoList.isEmpty()) {
            scaleBuilder.addAllCommodityResizes(resizeInfoList);
        }

        return ActionInfo.newBuilder()
                .setScale(scaleBuilder.build())
                .build();
    }

    /**
     * Create the list of commodities related resize information to include in the scale action DTO.
     *
     * @param commodities The commodities being resized.
     * @return List of ResizeInfo objects.
     */
    private static List<ResizeInfo> createResizeInfoList(final List<Commodity> commodities) {
        if (commodities == null) {
            return Collections.EMPTY_LIST;
        }
        final List<ResizeInfo> resizeInfoList = new ArrayList<>();
        for (Commodity commodity : commodities) {
            resizeInfoList.add(ResizeInfo.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType
                            .valueOf(commodity.commType).getNumber()).build())
                    .setOldCapacity(commodity.commType.equals("STORAGE_AMOUNT") || commodity.commType.equals("IO_THROUGHPUT")
                            ? commodity.sourceCapacity * 1024
                            : commodity.sourceCapacity)
                    .setNewCapacity(commodity.commType.equals("STORAGE_AMOUNT") || commodity.commType.equals("IO_THROUGHPUT")
                            ? commodity.destinationCapacity * 1024
                            : commodity.destinationCapacity).build());
        }
        return resizeInfoList;
    }

    /**
     * Create ResizeInfo protobuf object.
     *
     * @param commType commodity type
     * @param oldCapacity old capacity
     * @param newCapacity new capacity
     * @return ResizeInfo object
     */
    public static ResizeInfo createResizeInfo(final CommodityType commType, final float oldCapacity, final float newCapacity) {
        return ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(commType.getNumber()).build())
                .setOldCapacity(oldCapacity)
                .setNewCapacity(newCapacity).build();
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
     * Create ExecutedActionsChangeWindow object for volume scale actions.
     *
     * @param entityOid entity OID
     * @param actionTime action time
     * @param sourceOnDemandRate source on-demand rate
     * @param destOnDemandRate destination on-demand rate
     * @param sourceProviderId source provider ID
     * @param destProviderId destination provider ID
     * @param endTime end time of the action
     * @param state the LivenessState of the executed scale action
     * @param expectedRiCoverage the expected RI coverage after the action (between 0 - 1)
     * @return ExecutedActionsChangeWindow object
     */
    public static ExecutedActionsChangeWindow createVMActionChangeWindow(long entityOid, LocalDateTime actionTime, double sourceOnDemandRate,
            double destOnDemandRate, long sourceProviderId, long destProviderId, @Nullable LocalDateTime endTime, @Nullable LivenessState state,
            double expectedRiCoverage) {
        if (state == null) {
            state = LivenessState.LIVE;
        }
        ActionInfo scaleActionInfo = createScaleActionInfo(entityOid, sourceOnDemandRate, destOnDemandRate,
                sourceProviderId, destProviderId,
                EntityType.COMPUTE_TIER_VALUE,
                EntityType.VIRTUAL_MACHINE_VALUE, new ArrayList<>(), expectedRiCoverage);

        ActionSpec actionSpec = createActionSpec(actionTime, scaleActionInfo,
                sourceOnDemandRate - destOnDemandRate);
        return createExecutedActionsChangeWindow(entityOid, actionSpec, endTime, state);
    }

    /**
     * Create ExecutedActionsChangeWindow object for volume scale actions.
     *
     * @param entityOid entity OID
     * @param actionTime action time
     * @param sourceOnDemandRate source on-demand rate
     * @param destOnDemandRate destination on-demand rate
     * @param sourceProviderId source provider ID
     * @param destProviderId destination provider ID
     * @param endTime end time of the action
     * @param state the LivenessState of the executed scale action
     * @param resizeInfoList list of commodity resize info
     * @return ExecutedActionsChangeWindow object
     */
    public static ExecutedActionsChangeWindow createVolumeActionChangeWindow(long entityOid, LocalDateTime actionTime, double sourceOnDemandRate,
            double destOnDemandRate, long sourceProviderId, long destProviderId, @Nullable LocalDateTime endTime,
            @Nullable LivenessState state, List<ResizeInfo> resizeInfoList) {
        if (state == null) {
            state = LivenessState.LIVE;
        }
        ActionInfo scaleActionInfo = createScaleActionInfo(entityOid, sourceOnDemandRate, destOnDemandRate,
                sourceProviderId, destProviderId,
                EntityType.STORAGE_TIER_VALUE,
                EntityType.VIRTUAL_VOLUME_VALUE, resizeInfoList, 0);

        ActionSpec actionSpec = createActionSpec(actionTime, scaleActionInfo,
                sourceOnDemandRate - destOnDemandRate);
        return createExecutedActionsChangeWindow(entityOid, actionSpec, endTime, state);
    }

    /**
     * Create ExecutedActionsChangeWindow object for db scale actions.
     *
     * @param entityOid entity OID
     * @param actionTime action time
     * @param sourceOnDemandRate source on-demand rate
     * @param destOnDemandRate destination on-demand rate
     * @param sourceProviderId source provider ID
     * @param destProviderId destination provider ID
     * @param endTime end time of the action
     * @param state the LivenessState of the executed scale action
     * @param resizeInfoList list of commodity resize info
     * @return ExecutedActionsChangeWindow object
     */
    public static ExecutedActionsChangeWindow createDatabaseActionChangeWindow(long entityOid, LocalDateTime actionTime, double sourceOnDemandRate,
                                                                             double destOnDemandRate, long sourceProviderId, long destProviderId, @Nullable LocalDateTime endTime,
                                                                             @Nullable LivenessState state, List<ResizeInfo> resizeInfoList) {
        if (state == null) {
            state = LivenessState.LIVE;
        }
        ActionInfo scaleActionInfo = createScaleActionInfo(entityOid, sourceOnDemandRate, destOnDemandRate,
                sourceProviderId, destProviderId,
                EntityType.DATABASE_SERVER_VALUE,
                EntityType.DATABASE_VALUE, resizeInfoList, 0);

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

    @VisibleForTesting
    static int getNumVcores(@Nonnull final String tierName) {
        if (tierName == null) {
            return 0;
        }
        final String[] parts = tierName.split("_");
        if (parts.length < 3) {
            return 1;
        }
        return Integer.parseInt(parts[2]);
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
        // Commodities involved in the scale action -- type, capacity and rate at destination.
        private List<Commodity> commodities = new ArrayList<>();
        // Cost per hour
        private final double costPerHour;
        // Cost in the segment
        private double cost;
        // Duration in hours
        private double durationInHours;
        // expected RI coverage after action
        private double expectedRICoverage;
        // destination type (ULTRA, PREMIUM, STANDARD SDD, etc .. for Volumes)
        private final String destinationType;
        // num VCores for VCORE DB.
        private final int numVCores;
        // license ratedoe VCORE DB.
        private final double licenseCostPerHour;

        Long getProviderId() {
            return providerId;
        }

        /**
         * Constructor.
         * @param start start time
         * @param end end time
         * @param costPerHour cost per hour
         * @param providerId provider ID
         * @param commodities commodities involved in the resize event
         * @param expectedRiCoverage the expected RI coverage after the action (between 0 - 1)
         * @param destinationType the destination type, if relevant (for Volumes, ULTRA, PREMIUM, STANDARD etc)
         * @param numVCores Number of vCores of the vCore tier for which this segment is being charged (relavant for VCore DBs)
         * @param licenseCostPerHour the license cost per hour (relevant for vCore DBs)
         */
        Segment(long start, long end, double costPerHour, long providerId, List<Commodity> commodities, double expectedRICoverage,
                String destinationType, int numVCores, double licenseCostPerHour) {
            super(start, end);
            this.providerId = providerId;
            this.expectedRICoverage = expectedRICoverage;
            if (commodities != null) {
                this.commodities.addAll(commodities);
            }
            this.destinationType = destinationType;
            this.costPerHour = costPerHour;
            this.numVCores = numVCores;
            this.licenseCostPerHour = licenseCostPerHour;
            updateCost();
        }

        Segment(Segment segment) {
            super(segment.start, segment.end);
            this.costPerHour = segment.costPerHour;
            this.cost = segment.cost;
            this.providerId = segment.providerId;
            this.durationInHours = segment.durationInHours;
            if (commodities != null) {
                this.commodities = segment.commodities;
            }
            this.expectedRICoverage = segment.expectedRICoverage;
            this.destinationType = segment.destinationType;
            this.numVCores = segment.numVCores;
            this.licenseCostPerHour = segment.licenseCostPerHour;
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