package com.vmturbo.history.stats.projected;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedCalculatedCommodity;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;

/**
 * The {@link ProjectedStatsStore} keeps track of stats from the most recent projected topology.
 * <p>
 * We currently keep these stats in memory because they are transient, and we don't need to keep
 * them over the long term. In the future it may be worth putting it in a slightly more "stable"
 * place - e.g. a KV store, or even a database table.
 */
@ThreadSafe
public class ProjectedStatsStore {

    @VisibleForTesting
    static final String PRICE_INDEX_NAME = "priceIndex";

    /**
     * The factory used to construct snapshots when a new topology comes in.
     */
    private TopologyCommoditiesSnapshotFactory snapshotFactory;

    /**
     * The commodities snapshot for the latest received topology.
     */
    @GuardedBy("topologyCommoditiesLock")
    private TopologyCommoditiesSnapshot topologyCommodities;

    private final Object topologyCommoditiesLock = new Object();

    /**
     * Map from (entity ID) -> (projected price index in the latest received topology).
     * This map is replaced, in entirety, when a new PriceIndex payload is received.
     */
    private volatile ImmutableMap<Long, Double> priceIndexMap = ImmutableMap.of();

    public ProjectedStatsStore() {
        this(TopologyCommoditiesSnapshot.newFactory());
    }

    @VisibleForTesting
    ProjectedStatsStore(@Nonnull final TopologyCommoditiesSnapshotFactory snapshotFactory) {
        this.snapshotFactory = snapshotFactory;
    }

    @VisibleForTesting
    Optional<Double> getPriceIndex(final long entityId) {
        return Optional.ofNullable(priceIndexMap.get(entityId));
    }

    /**
     * Get the snapshot representing projected stats in response to a particular request.
     * <p>
     * The search will run on the most recent available snapshot. If there is a concurrent update
     * in progress - started, but not finished - the search will run on the previous snapshot.
     * This means reliably fast searches (no blocking to wait for snapshot construction to finish)
     * at the expense of the chance for data that's one market iteration out of date.
     *
     * @param request The request coming in from the client.
     * @return An optional containing the snapshot, or an empty optional if no data is available.
     */
    public Optional<StatSnapshot> getStatSnapshot(@Nonnull final ProjectedStatsRequest request) {

        final Set<String> commodityNames = new HashSet<>(request.getCommodityNameList());
        final Set<Long> targetEntities = new HashSet<>(request.getEntitiesList());

        return getStatSnapshotForEntities(targetEntities, commodityNames);
    }

    /**
     * Get the snapshot representing projected stats for a given set of entities and commodities.
     *
     * @param targetEntities the entities to collect the stats for
     * @param commodityNames the commodities to collect for those entities
     * @return an Optional containing the snapshot, or empty optional if no data is available
     */
    public Optional<StatSnapshot> getStatSnapshotForEntities(@Nonnull final Set<Long> targetEntities,
                                                             @Nonnull final Set<String> commodityNames) {

        // capture the current topologyCommodities object; new topologies replace the entire object
        final TopologyCommoditiesSnapshot targetCommodities;
        synchronized(topologyCommoditiesLock) {
            targetCommodities = topologyCommodities;
        }
        if (targetCommodities == null) {
            return Optional.empty();
        }

        // capture the current priceIndexMap; new priceIndexMessages replace the entire map
        final Map<Long, Double> priceIndexSnapshot = priceIndexMap;

        // accumulate 'standard' and 'count' stats
        StatSnapshot.Builder builder = StatSnapshot.newBuilder();
        targetCommodities
            .getRecords(commodityNames, targetEntities)
            .forEach(builder::addStatRecords);

        // accumulate 'priceIndex' stats if requested, since they are accumulated separately
        if (commodityNames.contains(PRICE_INDEX_NAME)) {
                targetEntities.stream()
                        .filter(priceIndexSnapshot::containsKey)
                        .map(entityOid -> {
                            AccumulatedCalculatedCommodity priceIndexCommodity =
                                    new AccumulatedCalculatedCommodity(PRICE_INDEX_NAME);
                            priceIndexCommodity.recordAttributeCommodity(priceIndexSnapshot
                                    .get(entityOid)
                                    .floatValue());
                            return priceIndexCommodity.toStatRecord();
                        })
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .forEach(builder::addStatRecords);
        }
        return Optional.of(builder.build());
    }

    /**
     * Overwrite the projected topology snapshot in the store with a new set of entities.
     *
     * @param entities The {@link RemoteIterator} over the entities.
     * @return The number of entities in the updated snapshot.
     * @throws InterruptedException If the thread is interrupted while retrieving the entities.
     * @throws TimeoutException If it takes too long to get entities from the remote iterator.
     * @throws CommunicationException If there are issues connecting to the source of the entities.
     */
    public long updateProjectedTopology(@Nonnull final RemoteIterator<TopologyEntityDTO> entities)
            throws InterruptedException, TimeoutException, CommunicationException {
        final TopologyCommoditiesSnapshot newCommodities = snapshotFactory.createSnapshot(entities);
        synchronized (topologyCommoditiesLock) {
            topologyCommodities = newCommodities;
        }
        return newCommodities.getTopologySize();
    }

    /**
     * Replace the 'priceIndexMap' in its entirety when a new PriceIndexMessage payload is received.
     *
     * @param priceIndex the {@link PriceIndexMessage} with the priceIndex values for each SE OID
     */
    public void updateProjectedPriceIndex(@Nonnull final PriceIndexMessage priceIndex) {
        priceIndexMap = ImmutableMap.copyOf(priceIndex.getPayloadList().stream()
                .collect(Collectors.toMap(
                        PriceIndexMessagePayload::getOid,
                        PriceIndexMessagePayload::getPriceindexProjected)));
    }


}
