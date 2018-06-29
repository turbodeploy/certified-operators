package com.vmturbo.history.stats.projected;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.schema.StringConstants;

/**
 * The {@link TopologyCommoditiesSnapshot} contains information about bought and sold commodities
 * in a topology, other than the price index (which arrives separately).
 *
 * It's constructed from the projected {@link TopologyEntityDTO}s broadcast by the market,
 * and is immutable after construction.
 */
@Immutable
class TopologyCommoditiesSnapshot {

    private final SoldCommoditiesInfo soldCommoditiesInfo;
    private final BoughtCommoditiesInfo boughtCommoditiesInfo;
    private final EntityCountInfo entityCountInfo;
    private final long topologySize;

    /**
     * Create a new default factory for instances of {@link TopologyCommoditiesSnapshot}.
     *
     * @return The factory to use to create instances.
     */
    static TopologyCommoditiesSnapshotFactory newFactory() {
        return new TopologyCommoditiesSnapshotFactory() {
            @Nonnull
            @Override
            public TopologyCommoditiesSnapshot createSnapshot(
                        @Nonnull final RemoteIterator<TopologyEntityDTO> entities)
                    throws InterruptedException, TimeoutException, CommunicationException {
                return new TopologyCommoditiesSnapshot(entities);
            }
        };
    }

    @VisibleForTesting
    TopologyCommoditiesSnapshot(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo,
                                @Nonnull final BoughtCommoditiesInfo boughtCommoditiesInfo,
                                @Nonnull final EntityCountInfo entityCountInfo,
                                final long numEntities) {
        this.soldCommoditiesInfo = soldCommoditiesInfo;
        this.boughtCommoditiesInfo = boughtCommoditiesInfo;
        this.entityCountInfo = entityCountInfo;
        this.topologySize = numEntities;
    }

    private TopologyCommoditiesSnapshot(@Nonnull final RemoteIterator<TopologyEntityDTO> entities)
            throws InterruptedException, TimeoutException, CommunicationException {
        final SoldCommoditiesInfo.Builder soldCommoditiesBuilder =
                SoldCommoditiesInfo.newBuilder();
        final BoughtCommoditiesInfo.Builder boughtCommoditiesBuilder =
                BoughtCommoditiesInfo.newBuilder();
        final EntityCountInfo.Builder entityCountBuilder = EntityCountInfo.newBuilder();
        long numEntities = 0;

        while (entities.hasNext()) {
            final Collection<TopologyEntityDTO> nextChunk = entities.nextChunk();
            numEntities += nextChunk.size();
            nextChunk.forEach(entity -> {
                entityCountBuilder.addEntity(entity);
                soldCommoditiesBuilder.addEntity(entity);
                boughtCommoditiesBuilder.addEntity(entity);
            });
        }

        this.soldCommoditiesInfo = soldCommoditiesBuilder.build();
        this.boughtCommoditiesInfo = boughtCommoditiesBuilder.build(this.soldCommoditiesInfo);
        this.entityCountInfo = entityCountBuilder.build();
        this.topologySize = numEntities;
    }

    long getTopologySize() {
        return topologySize;
    }

    /**
     * Get accumulated statistics records for a set of commodities names over a set of entities
     * in the topology.
     *
     * @param commodityNames The names of the commodities - must not be empty.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return A stream of {@link StatRecord}s, with at most two {@link StatRecord}s for each
     *         commodity (bought, sold). Stream may be empty if no entities in the target set
     *         are buying or selling any of the commodities.
     */
    @Nonnull
    Stream<StatRecord> getRecords(@Nonnull final Set<String> commodityNames,
                                  @Nonnull final Set<Long> targetEntities) {
        if (commodityNames.isEmpty()) {
            throw new IllegalArgumentException("Must specify commodity names.");
        }

        return commodityNames.stream()
                .flatMap(commodityName ->
                        getCommodityRecords(commodityName, targetEntities).stream());
    }

    /**
     * Get a comparator that can be used to compare entity IDs according to the passed-in pagination
     * parameters.
     *
     * @param paginationParams The {@link EntityStatsPaginationParams} used to order entities.
     * @return A {@link Comparator} that can be used to compare entity IDs according to the
     *         {@link EntityStatsPaginationParams}. If an entity ID is not in this snapshot, or
     *         does not buy/sell the commodity, it will be considered smaller than any entity ID
     *         that is in the snapshot and does buy/sell.
     * @throws IllegalArgumentException If the sort commodity is invalid (e.g. price index, or
     *         a global count statistic like numVMs).
     */
    @Nonnull
    Comparator<Long> getEntityComparator(@Nonnull final EntityStatsPaginationParams paginationParams)
            throws IllegalArgumentException {
        final String sortCommodity = paginationParams.getSortCommodity();
        if (entityCountInfo.isCountStat(sortCommodity)) {
            throw new IllegalArgumentException("Can't order by count commodity: " +
                    sortCommodity);
        } else if (sortCommodity.equals(StringConstants.PRICE_INDEX)) {
            throw new IllegalArgumentException("Can't order by price index in commodities snapshot.");
        }

        return (id1, id2) -> {
            // For each entity, the commodity should either be sold or bought. An entity
            // shouldn't buy and sell the same commodity.
            final double id1StatValue = soldCommoditiesInfo.getValue(id1, sortCommodity) +
                    boughtCommoditiesInfo.getValue(id1, sortCommodity);
            final double id2StatValue = soldCommoditiesInfo.getValue(id2, sortCommodity) +
                    boughtCommoditiesInfo.getValue(id2, sortCommodity);
            final int valComparisonResult = paginationParams.isAscending() ?
                    Double.compare(id1StatValue, id2StatValue) :
                    Double.compare(id2StatValue, id1StatValue);
            if (valComparisonResult == 0) {
                // In order to have a stable sort, we use the entity ID as the secondary sorting
                // parameter.
                return paginationParams.isAscending() ?
                        Long.compare(id1, id2) : Long.compare(id2, id1);
            } else {
                return valComparisonResult;
            }
        };
    }

    @Nonnull
    private List<StatRecord> getCommodityRecords(@Nonnull final String commodityName,
                                                 @Nonnull final Set<Long> targetEntities) {
        if (entityCountInfo.isCountStat(commodityName)) {
            return entityCountInfo.getCountRecord(commodityName)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else if (commodityName.equals("priceIndex")) {
            // Price index requests are actually handled separately, because the price
            // index doesn't come in as part of the topology.
            // TODO: handle the Price Index in the snapshot, e.g. by creating PriceIndexInfo class
            return Collections.emptyList();
        } else {
            // This is probably a "regular" commodity.
            final List<StatRecord> retList = new ArrayList<>();

            soldCommoditiesInfo.getAccumulatedRecords(commodityName, targetEntities)
                    .ifPresent(retList::add);

            boughtCommoditiesInfo.getAccumulatedRecord(commodityName, targetEntities)
                    .ifPresent(retList::add);

            return retList;
        }
    }

    /**
     * A factory for {@link TopologyCommoditiesSnapshot}, used for dependency
     * injection for unit tests. We don't really need a factory otherwise, since
     * all of these classes are private to the {@link ProjectedStatsStore} implementation.
     */
    interface TopologyCommoditiesSnapshotFactory {

        @Nonnull
        TopologyCommoditiesSnapshot createSnapshot(final @Nonnull RemoteIterator<TopologyEntityDTO> entities)
                throws InterruptedException, TimeoutException, CommunicationException;
    }
}
