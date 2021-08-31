package com.vmturbo.cost.component.cca;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore.ComputeTierAllocationUpdateListener;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore.EntityComputeTierAllocationSet;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails.UnknownProvider;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ResourceCreationDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.topology.event.library.TopologyEventProvider;
import com.vmturbo.topology.event.library.TopologyEvents;

/**
 * An implementation of {@link TopologyEventProvider}, based on CCA data.
 */
public class CCATopologyEventProvider implements TopologyEventProvider, ComputeTierAllocationUpdateListener {

    private final Logger logger = LogManager.getLogger();

    private final ComputeTierAllocationStore computeTierAllocationStore;

    private final CloudScopeStore cloudScopeStore;

    private final List<TopologyEventUpdateListener> updateListeners = new ArrayList<>();

    private final Cache<Long, Instant> creationTimeCache = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofDays(1))
            .build();

    /**
     * Constructs a new CCA topology event provider instance.
     * @param computeTierAllocationStore The {@link ComputeTierAllocationStore}, containing CCA data
     *                                   for workloads buying from compute tiers (VMs).
     * @param cloudScopeStore The {@link CloudScopeStore}, used to infer the discovery time of an
     *                        entity as a stand-in for the creation time.
     */
    public CCATopologyEventProvider(@Nonnull ComputeTierAllocationStore computeTierAllocationStore,
                                    @Nonnull CloudScopeStore cloudScopeStore) {
        this.computeTierAllocationStore = Objects.requireNonNull(computeTierAllocationStore);
        this.cloudScopeStore = Objects.requireNonNull(cloudScopeStore);

        computeTierAllocationStore.registerUpdateListener(this);
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public TopologyEvents getTopologyEvents(@Nonnull final TimeInterval eventWindow,
                                            @Nonnull final TopologyEventFilter eventFilter) {

        final Set<EntityEvents> entityEventsSet = fetchComputeTopologyEvents(eventWindow, eventFilter);
        final Set<Long> entityOids = entityEventsSet.stream()
                .map(EntityEvents::getEntityOid)
                .collect(ImmutableSet.toImmutableSet());

        // We first do a batch fetch for the creation time for each of the entity OIDs. This is in order
        // to avoid individual queries for each entity that is not current cached.
        bulkUpdateCreationTimeCache(entityOids);

        return entityEventsSet.stream()
                // Adding the observed creation time assumes the creation time will have been
                // loaded as part of the bulk update above.
                .map(entityEvents -> addObservedCreationTime(entityEvents, eventWindow))
                .collect(TopologyEvents.toTopologyEvents());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void registerUpdateListener(@Nonnull final TopologyEventUpdateListener listener) {

        logger.info("Registering topology event update listener");

        updateListeners.add(listener);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void onAllocationUpdate(@Nonnull final TopologyInfo topologyInfo) {

        logger.info("Received allocation update notification (Type={}, Context ID={}, Topology ID={})",
                topologyInfo.getTopologyType(), topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId());

        // On any update notification from the computer allocation store, we notify any listeners
        // of topology event updates.
        updateListeners.forEach(TopologyEventUpdateListener::onTopologyEventUpdate);
    }

    private Set<EntityEvents> fetchComputeTopologyEvents(@Nonnull TimeInterval eventWindow,
                                                         @Nonnull TopologyEventFilter eventFilter) {

        final EntityComputeTierAllocationFilter computeAllocationFilter = EntityComputeTierAllocationFilter.builder()
                .startTimeFilter(TimeFilter.builder()
                        .time(eventWindow.endTime())
                        .comparator(TimeComparator.BEFORE_OR_EQUAL_TO)
                        .build())
                .endTimeFilter(TimeFilter.builder()
                        .time(eventWindow.startTime())
                        .comparator(TimeComparator.AFTER_OR_EQUAL_TO)
                        .build())
                .addAllEntityOids(eventFilter.entityOids())
                .addAllAccountOids(eventFilter.accountOids())
                .addAllRegionOids(eventFilter.regionOids())
                .addAllServiceProviderOids(eventFilter.serviceProviderOids())
                .build();


        // Query for all
        final EntityComputeTierAllocationSet allocationSet =
                computeTierAllocationStore.getAllocations(computeAllocationFilter);

        final Instant latestTopologyTime = allocationSet.latestTopologyInfo()
                .map(topologyInfo -> Instant.ofEpochMilli(topologyInfo.getCreationTime()))
                .orElse(Instant.MAX);

        return allocationSet.allocations().asMap().entrySet().stream()
                .map(entityAllocations -> convertComputeAllocationsToEvents(
                        entityAllocations.getKey(),
                        entityAllocations.getValue(),
                        eventWindow,
                        latestTopologyTime))
                .collect(ImmutableSet.toImmutableSet());
    }

    private EntityEvents convertComputeAllocationsToEvents(long entityOid,
                                                           @Nonnull Collection<EntityComputeTierAllocation> allocations,
                                                           @Nonnull TimeInterval eventWindow,
                                                           @Nonnull Instant latestTopologyTime) {
        final EntityEvents.Builder entityEvents = EntityEvents.newBuilder()
                .setEntityOid(entityOid);

        allocations.stream()
                // filter out allocations with start time == end time i.e. those allocations seen
                // for only a single topology broadcast.
                .filter(allocation -> !allocation.timeInterval().duration().isZero())
                .forEach(allocation -> entityEvents.addAllEvents(
                        convertAllocationToTopologyEvents(allocation, eventWindow, latestTopologyTime)));

        return entityEvents.build();
    }

    private List<TopologyEvent> convertAllocationToTopologyEvents(@Nonnull EntityComputeTierAllocation allocation,
                                                                  @Nonnull TimeInterval eventWindow,
                                                                  @Nonnull Instant latestTopologyTime) {

        final ImmutableList.Builder<TopologyEvent> entityEvents = ImmutableList.builder();

        // The search for tier allocations looks for any allocations that overlap with the event window.
        // This does not necessarily mean the start time of the allocation or the end time
        // of the allocation falls within the event window.
        final Instant allocationStartTime = allocation.timeInterval().startTime();
        if (eventWindow.contains(allocationStartTime)) {

            entityEvents.add(TopologyEvent.newBuilder()
                    .setEventTimestamp(allocationStartTime.toEpochMilli())
                    .setType(TopologyEventType.STATE_CHANGE)
                    .setEventInfo(TopologyEventInfo.newBuilder()
                            .setStateChange(EntityStateChangeDetails.newBuilder()
                                    .setSourceState(EntityState.UNKNOWN)
                                    .setDestinationState(EntityState.POWERED_ON)
                                    .build())
                            .build())
                    .build());
            entityEvents.add(TopologyEvent.newBuilder()
                    .setEventTimestamp(allocationStartTime.toEpochMilli())
                    .setType(TopologyEventType.PROVIDER_CHANGE)
                    .setEventInfo(TopologyEventInfo.newBuilder()
                            .setProviderChange(ProviderChangeDetails.newBuilder()
                                    .setUnknownSourceProvider(UnknownProvider.getDefaultInstance())
                                    .setDestinationProviderOid(allocation.cloudTierDemand().cloudTierOid())
                                    .build()))
                    .build());
        }

        final Instant allocationEndTime = allocation.timeInterval().endTime();
        if (eventWindow.contains(allocationEndTime) && !allocationEndTime.equals(latestTopologyTime)) {

            entityEvents.add(TopologyEvent.newBuilder()
                    .setEventTimestamp(allocationEndTime.toEpochMilli())
                    .setType(TopologyEventType.STATE_CHANGE)
                    .setEventInfo(TopologyEventInfo.newBuilder()
                            .setStateChange(EntityStateChangeDetails.newBuilder()
                                    .setSourceState(EntityState.POWERED_ON)
                                    .setDestinationState(EntityState.UNKNOWN)
                                    .build())
                            .build())
                    .build());
            entityEvents.add(TopologyEvent.newBuilder()
                    .setEventTimestamp(allocationEndTime.toEpochMilli())
                    .setType(TopologyEventType.PROVIDER_CHANGE)
                    .setEventInfo(TopologyEventInfo.newBuilder()
                            .setProviderChange(ProviderChangeDetails.newBuilder()
                                    .setSourceProviderOid(allocation.cloudTierDemand().cloudTierOid())
                                    .setUnknownDestinationProvider(UnknownProvider.getDefaultInstance())
                                    .build()))
                    .build());

        }

        return entityEvents.build();
    }

    private void bulkUpdateCreationTimeCache(@Nonnull Set<Long> entityOids) {

        final Map<Long, Instant> cachedCreationTimeMap = creationTimeCache.getAllPresent(entityOids);

        final Set<Long> oidsToFetch = Sets.difference(entityOids, cachedCreationTimeMap.keySet());

        final CloudScopeFilter cloudScopeFilter = CloudScopeFilter.newBuilder()
                .addAllEntityOid(oidsToFetch)
                .build();

        try (Stream<EntityCloudScope> cloudScopeStream = cloudScopeStore.streamByFilter(cloudScopeFilter)) {
            final Map<Long, Instant> fetchedCreationTimeMap = cloudScopeStream.collect(
                    ImmutableMap.toImmutableMap(
                            EntityCloudScope::entityOid,
                            EntityCloudScope::creationTime));

            final Set<Long> missingOids = Sets.difference(entityOids, fetchedCreationTimeMap.keySet());
            if (!missingOids.isEmpty()) {
                logger.warn("Missing creation time for {} entities", missingOids.size());
                if (logger.isDebugEnabled()) {
                    missingOids.forEach(missingOid -> logger.debug("Missing creation time for '{}'", missingOid));
                }
            }

            creationTimeCache.putAll(fetchedCreationTimeMap);
        }
    }

    private EntityEvents addObservedCreationTime(@Nonnull EntityEvents entityEvents,
                                                 @Nonnull TimeInterval eventWindow) {
        final Instant creationTime = creationTimeCache.getIfPresent(entityEvents.getEntityOid());

        if (creationTime != null && eventWindow.contains(creationTime)) {
            return entityEvents.toBuilder()
                    .addEvents(TopologyEvent.newBuilder()
                            .setEventTimestamp(creationTime.toEpochMilli())
                            .setEventInfo(TopologyEventInfo.newBuilder()
                                    .setResourceCreation(ResourceCreationDetails.getDefaultInstance())))
                    .build();
        }

        return entityEvents;
    }
}
