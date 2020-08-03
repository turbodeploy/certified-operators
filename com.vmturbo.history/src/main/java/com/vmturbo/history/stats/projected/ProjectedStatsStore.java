package com.vmturbo.history.stats.projected;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.ingesters.live.writers.TopologyCommoditiesProcessor;
import com.vmturbo.history.stats.projected.ProjectedPriceIndexSnapshot.PriceIndexSnapshotFactory;
import com.vmturbo.history.stats.projected.TopologyCommoditiesSnapshot.TopologyCommoditiesSnapshotFactory;

/**
 * The {@link ProjectedStatsStore} keeps track of stats from the most recent projected topology.
 * <p>
 * We currently keep these stats in memory because they are transient, and we don't need to keep
 * them over the long term. In the future it may be worth putting it in a slightly more "stable"
 * place - e.g. a KV store, or even a database table.
 */
@ThreadSafe
public class ProjectedStatsStore {

    /**
     * The factory used to construct snapshots when a new topology comes in.
     */
    private final TopologyCommoditiesSnapshotFactory topoCommSnapshotFactory;

    private final PriceIndexSnapshotFactory priceIndexSnapshotFactory;

    /**
     * The commodities snapshot for the latest received topology.
     */
    @GuardedBy("topologyCommoditiesLock")
    private TopologyCommoditiesSnapshot topologyCommodities;

    private final StatSnapshotCalculator statSnapshotCalculator;

    private final EntityStatsCalculator entityStatsCalculator;

    private final Object topologyCommoditiesLock = new Object();

    public ProjectedStatsStore() {
        this(TopologyCommoditiesSnapshot.newFactory(),
                ProjectedPriceIndexSnapshot.newFactory(),
                new StatSnapshotCalculator() {},
                new EntityStatsCalculator() {});
    }

    @VisibleForTesting
    ProjectedStatsStore(@Nonnull final TopologyCommoditiesSnapshotFactory topoCommSnapshotFactory,
                        @Nonnull final PriceIndexSnapshotFactory priceIndexSnapshotFactory,
                        @Nonnull final StatSnapshotCalculator statSnapshotCalculator,
                        @Nonnull final EntityStatsCalculator entityStatsCalculator) {
        this.topoCommSnapshotFactory = Objects.requireNonNull(topoCommSnapshotFactory);
        this.priceIndexSnapshotFactory = Objects.requireNonNull(priceIndexSnapshotFactory);
        this.statSnapshotCalculator = Objects.requireNonNull(statSnapshotCalculator);
        this.entityStatsCalculator = Objects.requireNonNull(entityStatsCalculator);
    }

    /**
     * Get a page of projected entity stats.
     *
     * @param entitiesMap The target entities. Must be non-empty. It's a mapping from seed entity
     *                    to derived entities, the derived entities may contain the seed entity
     *                    entity itself or derived entities from seed entity. Stats response will
     *                    be for each seed entity, but its value will be aggregated on derived
     *                    entities.
     * @param commodities The commodities to retrieve. Must be non-empty.
     * @param paginationParams {@link EntityStatsPaginationParams} for the page.
     * @return The {@link ProjectedEntityStatsResponse} to return to the client.
     */
    @Nonnull
    public ProjectedEntityStatsResponse getEntityStats(
            @Nonnull final Map<Long, Set<Long>> entitiesMap,
            @Nonnull final Set<String> commodities,
            @Nonnull final EntityStatsPaginationParams paginationParams) {
        if (entitiesMap.isEmpty()) {
            // For now we don't support paginating through all entities. The client is responsible
            // for providing a list of desired entities.
            // However, don't throw an exception because it's possible to request entity stats
            // for an empty set (e.g. a zero-member group) by accident.
            return ProjectedEntityStatsResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.getDefaultInstance())
                .build();
        } else if (commodities.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one commodity for " +
                "per-entity stats request.");
        }

        final TopologyCommoditiesSnapshot targetCommodities;
        synchronized(topologyCommoditiesLock) {
            targetCommodities = topologyCommodities;
        }

        if (targetCommodities == null) {
            return ProjectedEntityStatsResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.getDefaultInstance())
                .build();
        }

        return entityStatsCalculator.calculateNextPage(targetCommodities,
            statSnapshotCalculator,
            entitiesMap,
            commodities,
            paginationParams);
    }

    /**
     * Get the snapshot representing projected stats for a given set of entities and commodities.
     * <p>
     * The search will run on the most recent available snapshot. If there is a concurrent update
     * in progress - started, but not finished - the search will run on the previous snapshot.
     * This means reliably fast searches (no blocking to wait for snapshot construction to finish)
     * at the expense of the chance for data that's one market iteration out of date.
     *
     * @param targetEntities the entities to collect the stats for
     * @param commodityNames the commodities to collect for those entities
     * @return an Optional containing the snapshot, or empty optional if no data is available
     */
    @Nonnull
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

        return Optional.of(statSnapshotCalculator.buildSnapshot(targetCommodities, targetEntities, commodityNames));
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
    public long updateProjectedTopology(@Nonnull final RemoteIterator<ProjectedTopologyEntity> entities)
        throws InterruptedException, TimeoutException, CommunicationException {
        final TopologyCommoditiesSnapshot newCommodities
            = topoCommSnapshotFactory.createSnapshot(entities, priceIndexSnapshotFactory);
        return updateProjectedTopology(newCommodities);
    }

    /**
     * Overwrite the projected topology snapshot in the store with a new set of entities.
     *
     * <p>This method is used by {@link TopologyCommoditiesProcessor}, which processes projected
     * live topologies. It takes a partially-built {@link TopologyCommoditiesSnapshot} in the form
     * of a builder that is ready to build. Our priceIndexSnapshotFactory is required for the build,
     * so we finish the build here and install the reuslting snapshot.</p>
     * @param builder partially-built snapshot builder
     * @return updated project topology
     */
    public long updateProjectedTopology(@Nonnull final TopologyCommoditiesSnapshot.Builder builder) {
        return updateProjectedTopology(builder.build(priceIndexSnapshotFactory));
    }

    private long updateProjectedTopology(final TopologyCommoditiesSnapshot newCommodities) {
        synchronized (topologyCommoditiesLock) {
            topologyCommodities = newCommodities;
        }
        return newCommodities.getTopologySize();
    }

    /**
     * An interface to hide the logic of calculating the next page of entities
     * given a {@link TopologyCommoditiesSnapshot} and {@link ProjectedPriceIndexSnapshot}.
     * For now, this is only used for unit tests.
     */
    interface EntityStatsCalculator {

        @Nonnull
        default ProjectedEntityStatsResponse calculateNextPage(
                @Nonnull final TopologyCommoditiesSnapshot targetCommodities,
                @Nonnull final StatSnapshotCalculator statSnapshotCalculator,
                @Nonnull final Map<Long, Set<Long>> entitiesMap,
                @Nonnull final Set<String> commodityNames,
                @Nonnull final EntityStatsPaginationParams paginationParams) {
            // Get the entity comparator to use.
            final Comparator<Long> entityComparator = targetCommodities.getEntityComparator(
                paginationParams, entitiesMap);

            // Sort the input entity IDs using the comparator, and apply the pagination parameters
            // (i.e. limit + cursor)
            final int skipCount = paginationParams.getNextCursor().map(Integer::parseInt).orElse(0);
            final Set<Long> allRecords = entitiesMap.keySet();
            final List<Long> nextPageIds = allRecords.stream()
                    .sorted(entityComparator)
                    .skip(skipCount)
                    .limit(paginationParams.getLimit() + 1)
                    .collect(Collectors.toList());
            final ProjectedEntityStatsResponse.Builder responseBuilder =
                    ProjectedEntityStatsResponse.newBuilder();
            final PaginationResponse.Builder paginationRespBuilder = PaginationResponse.newBuilder();
            paginationRespBuilder.setTotalRecordCount(allRecords.size());
            if (nextPageIds.size() > paginationParams.getLimit()) {
                nextPageIds.remove(paginationParams.getLimit());
                paginationRespBuilder.setNextCursor(Integer.toString(skipCount + paginationParams.getLimit()));
            }
            responseBuilder.setPaginationResponse(paginationRespBuilder);

            // Get the projected stats for the next page of entities.
            nextPageIds.stream()
                    .map(entityId -> EntityStats.newBuilder()
                            .setOid(entityId)
                            .addStatSnapshots(statSnapshotCalculator.buildSnapshot(
                                targetCommodities, entitiesMap.get(entityId), commodityNames))
                            .build())
                    .forEach(responseBuilder::addEntityStats);
            return responseBuilder.build();
        }
    }

    /**
     * An interface to hide the logic of building a {@link StatSnapshot} for a set of entities
     * and commodities given a {@link TopologyCommoditiesSnapshot} and {@link ProjectedPriceIndexSnapshot}.
     * For now, this is only used for unit tests.
     */
    interface StatSnapshotCalculator {

        default StatSnapshot buildSnapshot(
                @Nonnull final TopologyCommoditiesSnapshot targetCommodities,
                @Nonnull final Set<Long> targetEntities,
                @Nonnull final Set<String> commodityNames) {
            // accumulate 'standard' and 'count' stats
            final StatSnapshot.Builder builder = StatSnapshot.newBuilder();
            targetCommodities
                    .getRecords(commodityNames, targetEntities)
                    .forEach(builder::addStatRecords);
            return builder.build();
        }
    }
}
