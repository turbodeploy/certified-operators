package com.vmturbo.history.stats.projected;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedCalculatedCommodity;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

/**
 * The {@link ProjectedPriceIndexSnapshot} contains information about the projected price index
 * in a topology. It's separate from the {@link TopologyCommoditiesSnapshot} because the
 * price index is broadcast separately
 *
 * It's constructed from the projected {@link PriceIndexMessage}s broadcast by the market,
 * and is immutable after construction.
 */
@Immutable
public class ProjectedPriceIndexSnapshot {

    /**
     * Map from (entity ID) -> (projected price index in the latest received topology).
     * This map is replaced, in entirety, when a new PriceIndex payload is received.
     *
     * If we want to get the top X entities by price index in the future we can keep this map
     * sorted.
     */
    private final Long2DoubleMap priceIndexMap;

    private ProjectedPriceIndexSnapshot(@Nonnull final Long2DoubleMap priceIndexByEntity) {
        priceIndexMap = priceIndexByEntity;
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
     * @throws IllegalArgumentException If the sort commodity is not the price index.
     */
    @Nonnull
    Comparator<Long> getEntityComparator(@Nonnull final EntityStatsPaginationParams paginationParams)
            throws IllegalArgumentException {
        if (!paginationParams.getSortCommodity().equals(StringConstants.PRICE_INDEX)) {
            throw new IllegalArgumentException("Price index snapshot cannot sort by: " +
                    paginationParams.getSortCommodity());
        }
        return (id1, id2) -> {
            final double id1StatValue = priceIndexMap.getOrDefault(id1.longValue(), 0.0);
            final double id2StatValue = priceIndexMap.getOrDefault(id2.longValue(), 0.0);
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

    /**
     * Get accumulated statistics records for the price index over a set of entities
     * in the topology.
     *
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return An optional containing a {@link StatRecord} if there are any matching entities
     *         with a projected price index.
     */
    @Nonnull
    public Optional<StatRecord> getRecord(@Nonnull final Set<Long> targetEntities) {
        final AccumulatedCalculatedCommodity priceIndexCommodity =
                new AccumulatedCalculatedCommodity(StringConstants.PRICE_INDEX);

        if (targetEntities.isEmpty()) {
            for (double val : priceIndexMap.values()) {
                priceIndexCommodity.recordAttributeCommodity(val);
            }
        } else {
            targetEntities.forEach(entityId -> {
                Double priceIndex = priceIndexMap.get(entityId);
                if (priceIndex != null) {
                    priceIndexCommodity.recordAttributeCommodity(priceIndex);
                }
            });
        }
        return priceIndexCommodity.toStatRecord();
    }


    /**
     * Create a new default factory for instances of {@link TopologyCommoditiesSnapshot}.
     *
     * @return The factory to use to create instances.
     */
    static PriceIndexSnapshotFactory newFactory() {
        return new PriceIndexSnapshotFactory() {
            @Nonnull
            @Override
            public ProjectedPriceIndexSnapshot createSnapshot(@Nonnull final Long2DoubleMap priceIndexByEntity) {
                if (priceIndexByEntity instanceof Long2DoubleOpenHashMap) {
                    ((Long2DoubleOpenHashMap)priceIndexByEntity).trim();
                }
                return new ProjectedPriceIndexSnapshot(priceIndexByEntity);
            }
        };
    }
    /**
     * A factory for {@link ProjectedPriceIndexSnapshot}, used for dependency
     * injection for unit tests. We don't really need a factory otherwise, since
     * all of these classes are private to the {@link ProjectedStatsStore} implementation.
     */
    interface PriceIndexSnapshotFactory {

        @Nonnull
        ProjectedPriceIndexSnapshot createSnapshot(@Nonnull Long2DoubleMap priceIndexByEntity);
    }
}
