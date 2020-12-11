package com.vmturbo.history.stats.projected;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.history.utils.HistoryStatsUtils;

/**
 * This class contains information about commodities sold by entities in a topology.
 * <p>
 * It's immutable, because for any given topology the sold commodities don't change.
 */
@Immutable
class SoldCommoditiesInfo {
    private static final Logger logger = LogManager.getLogger();

    /**
     * (commodity name) -> ( (entity id) -> (DTO describing commodity sold) )
     *
     * <p>Each entity should only have one {@link CommoditySoldDTO} for a given commodity name.
     * This may not be true in general, because the commodity name string doesn't take into
     * account the commodity spec keys. But the stats API doesn't currently support commodity
     * spec keys anyway.
     */
    private final Map<String, Long2ObjectMap<List<SoldCommodity>>> soldCommodities;

    private SoldCommoditiesInfo(
            @Nonnull final Map<String, Long2ObjectMap<List<SoldCommodity>>> soldCommodities) {
        this.soldCommodities = Collections.unmodifiableMap(soldCommodities);
    }

    @Nonnull
    static Builder newBuilder(Set<String> excludedCommodities) {
        return new Builder(excludedCommodities);
    }

    /**
     * Get the value of a particular commodity sold by a particular entity.
     *
     * @param entity The ID of the entity. It's a {@link Long} instead of a base type to avoid
     *               autoboxing.
     * @param commodityName The name of the commodity.
     * @return The average used amount of all commodities matching the name sold by the entity.
     *         This is the same formula we use to calculate "currentValue" for stat records.
     *         Returns 0 if the entity does not sell the commodity.
     */
    double getValue(final long entity,
                    @Nonnull final String commodityName) {
        final Long2ObjectMap<List<SoldCommodity>> soldByEntityId = soldCommodities.get(commodityName);
        double value = 0;
        if (soldByEntityId != null) {
            final List<SoldCommodity> soldByEntity = soldByEntityId.get(entity);
            if (CollectionUtils.isNotEmpty(soldByEntity)) {
                for (final SoldCommodity soldCommodity : soldByEntity) {
                    value += soldCommodity.getUsed();
                }
                value /= soldByEntity.size();
            }
        }
        return value;
    }

    /**
     * Get a list of {@link StatRecord}s which have accumulated information about a particular
     * commodity sold by a set of entities. The commodities with the same name but different
     * keys won't be accumulated: different {@link StatRecord} for each of them.
     *
     * @param commodityName The name of the commodity. The names are derived from
     *           {@link CommodityType}. This is not ideal - we should consider using the
     *           {@link CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return A list of the accumulated {@link StatRecord}, or an empty list
     *         if there is no information for the commodity over the target entities
     *         or this commodity is not sold.
     */
    @Nonnull
    List<StatRecord> getAccumulatedRecords(@Nonnull final String commodityName,
                                               @Nonnull final Set<Long> targetEntities) {
        Map<String, AccumulatedSoldCommodity> accumulatedSoldCommodities = new HashMap<>();
        final Long2ObjectMap<List<SoldCommodity>> soldByEntityId =
                soldCommodities.get(commodityName);
        if (soldByEntityId == null) {
            return Collections.emptyList();
        } else if (targetEntities.isEmpty()) {
            soldByEntityId.forEach(
                    (entityId, commoditySoldList) -> accumulateCommoditiesByKey(commoditySoldList,
                            commodityName, accumulatedSoldCommodities));
        } else {
            // We have some number of entities, and we accumulate the stats from all the entities.
            targetEntities.forEach(entityId -> {
                final Collection<SoldCommodity> commoditySoldDTOs = soldByEntityId.get(entityId);
                if (commoditySoldDTOs != null) {
                    accumulateCommoditiesByKey(commoditySoldDTOs, commodityName,
                            accumulatedSoldCommodities);
                } else {
                    logger.warn("Requested commodity {} not sold by entity {}", commodityName,
                            entityId);
                }
            });
        }
        return accumulatedSoldCommodities.values()
                .stream()
                .map(AccumulatedSoldCommodity::toStatRecord)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Accumulate sold commodities with the same name by key.
     *
     * @param commoditySoldList sold commodity list
     * @param commodityName commodity name
     * @param accumulatedSoldCommodities the map to collect accumulated commodities by key
     */
    private void accumulateCommoditiesByKey(Collection<SoldCommodity> commoditySoldList, String commodityName,
            Map<String, AccumulatedSoldCommodity> accumulatedSoldCommodities) {
        commoditySoldList.forEach(commoditySoldDTO -> {
            final String commodityKey = commoditySoldDTO.getCommodityType().getKey();
            AccumulatedSoldCommodity accumulatedSoldCommodity =
                    accumulatedSoldCommodities.computeIfAbsent(
                            commodityKey == null ? "empty_key" : commodityKey,
                            k -> new AccumulatedSoldCommodity(commodityName, commodityKey));
            accumulatedSoldCommodity.recordSoldCommodity(commoditySoldDTO);
        });
    }

    /**
     * Compute the total capacity over all commodities sold by a given provider.
     *
     * @param commodityName name of the commodity to average over
     * @param providerId the ID of the provider of these commodities
     * @return the total capacity over all commodities sold by this provider
     */
    @Nonnull
    public Optional<Double> getCapacity(@Nonnull final String commodityName,
                                        final long providerId) {
        return Optional.ofNullable(soldCommodities.get(commodityName))
                .map(providers -> providers.get(providerId))
                .map(commoditiesSold -> commoditiesSold.stream()
                                .mapToDouble(SoldCommodity::getCapacity).sum());
    }

    /**
     * Utility class to capture the commodity information we need for projected stats. This
     * saves a LOT of memory compared to keeping the full {@link CommodityBoughtDTO} around in
     * large topologies.
     */
    static class SoldCommodity {
        private final CommodityType commodityType;
        private final double used;
        private final double peak;
        private final double capacity;
        private final double usedPercentile;
        private final boolean hasUsedPercentile;

        SoldCommodity(CommoditySoldDTO sold) {
            this.commodityType = sold.getCommodityType();
            this.used = sold.getUsed();
            this.peak = sold.getPeak();
            this.capacity = sold.getCapacity();
            this.usedPercentile = sold.getHistoricalUsed().getPercentile();
            this.hasUsedPercentile = sold.getHistoricalUsed().hasPercentile();
        }

        public CommodityType getCommodityType() {
            return commodityType;
        }

        public double getUsed() {
            return used;
        }

        public double getPeak() {
            return peak;
        }

        public double getCapacity() {
            return capacity;
        }

        public double getPercentile() {
            return usedPercentile;
        }

        public boolean hasPercentile() {
            return hasUsedPercentile;
        }
    }

    /**
     * A builder to construct the {@link SoldCommoditiesInfo}.
     */
    static class Builder {

        private final Map<String, Long2ObjectMap<List<SoldCommodity>>> soldCommodities =
                new HashMap<>();

        /**
         * Track duplicate commodities sold by the same seller.
         * We keep this map separate from the main "soldCommodities" map because we only need
         * the {@link CommodityType} during {@link SoldCommoditiesInfo} construction.
         */
        private final Long2ObjectMap<List<CommodityType>> previouslySeenCommodities =
                new Long2ObjectOpenHashMap<>();

        private final Set<String> excludedCommodities;

        private Builder(Set<String> excludedCommodities) {
            this.excludedCommodities = excludedCommodities.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        }

        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entity.getCommoditySoldListList().forEach(commoditySold -> {
                final String commodity = HistoryStatsUtils.formatCommodityName(
                        commoditySold.getCommodityType().getType());
                // Enforce commodity exclusions.
                if (!excludedCommodities.contains(commodity.toLowerCase())) {
                    final Long2ObjectMap<List<SoldCommodity>> entitySellers =
                            soldCommodities.computeIfAbsent(commodity, k -> new Long2ObjectOpenHashMap<>());
                    saveIfNoCollision(entity, commoditySold, entitySellers);
                }
            });
            return this;
        }

        @Nonnull
        SoldCommoditiesInfo build() {
            soldCommodities.values().forEach(nestedMap -> {
                ((Long2ObjectOpenHashMap)nestedMap).trim();
                nestedMap.values().forEach(l -> ((ArrayList)l).trimToSize());
            });
            return new SoldCommoditiesInfo(soldCommodities);
        }

        /**
         * Check to see if the given commodity type (including key) for the given seller is already
         * listed in the commoditiesSoldMap.
         *
         * @param sellerEntity the ServiceEntity of the seller
         * @param commodityToAdd the new commodity to check for "already listed"
         * @param commoditiesSoldMap the map from Seller OID to Collection of Commodities
         *                           already Bought from that Seller
         */
        private void saveIfNoCollision(@Nonnull TopologyEntityDTO sellerEntity,
                                       CommoditySoldDTO commodityToAdd,
                                       Long2ObjectMap<List<SoldCommodity>> commoditiesSoldMap) {
            // check if a previous commodity for this seller has same CommodiyType (type & key)
            List<CommodityType> prevSeenSellerComms = previouslySeenCommodities.computeIfAbsent(
                    sellerEntity.getOid(), k -> new ArrayList<>());
            final boolean prevCommodityRecorded = prevSeenSellerComms.stream()
                .anyMatch(prevCommType -> prevCommType.equals(
                        commodityToAdd.getCommodityType()));
            if (prevCommodityRecorded) {
                // previous commodity with the same key; print a warning and don't save it
                logger.warn("Entity {} selling commodity type { {} } more than once.",
                        sellerEntity.getOid(),
                        commodityToAdd.getCommodityType());
            } else {
                // no previous commodity with this same key; save the new one
                prevSeenSellerComms.add(commodityToAdd.getCommodityType());
                commoditiesSoldMap.computeIfAbsent(sellerEntity.getOid(), k -> new ArrayList<>()).add(new SoldCommodity(commodityToAdd));
            }
        }
    }
}
