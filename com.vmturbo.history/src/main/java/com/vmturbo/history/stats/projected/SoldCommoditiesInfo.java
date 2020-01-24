package com.vmturbo.history.stats.projected;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import jdk.nashorn.internal.ir.annotations.Immutable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

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
     * <p>
     * Each entity should only have one {@link CommoditySoldDTO} for a given commodity name.
     * This may not be true in general, because the commodity name string doesn't take into
     * account the commodity spec keys. But the stats API doesn't currently support commodity
     * spec keys anyway.
     */
    private final Map<String, Multimap<Long, CommoditySoldDTO>> soldCommodities;

    private SoldCommoditiesInfo(
            @Nonnull final Map<String, Multimap<Long, CommoditySoldDTO>> soldCommodities) {
        this.soldCommodities = Collections.unmodifiableMap(soldCommodities);
    }

    @Nonnull
    static Builder newBuilder() {
        return new Builder();
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
    double getValue(@Nonnull final Long entity,
                    @Nonnull final String commodityName) {
        final Multimap<Long, CommoditySoldDTO> soldByEntityId = soldCommodities.get(commodityName);
        double value = 0;
        if (soldByEntityId != null) {
            final Collection<CommoditySoldDTO> soldByEntity = soldByEntityId.get(entity);
            if (CollectionUtils.isNotEmpty(soldByEntity)) {
                for (final CommoditySoldDTO soldCommodity : soldByEntity) {
                    value += soldCommodity.getUsed();
                }
                value /= soldByEntity.size();
            }
        }
        return value;
    }

    /**
     * Get the accumulated information about a particular commodity sold by a set of entities.
     *
     * @param commodityName The name of the commodity. The names are derived from
     *           {@link CommodityType}. This is not ideal - we should consider using the
     *           {@link CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return An optional containing the accumulated {@link StatRecord}, or an empty optional
     *         if there is no information for the commodity over the target entities.
     */
    @Nonnull
    Optional<StatRecord> getAccumulatedRecords(@Nonnull final String commodityName,
                                               @Nonnull final Set<Long> targetEntities) {
        final Multimap<Long, CommoditySoldDTO> soldByEntityId =
                soldCommodities.get(commodityName);
        final AccumulatedSoldCommodity overallCommoditySold =
                new AccumulatedSoldCommodity(commodityName);
        if (soldByEntityId == null) {
            // If this commodity is not sold, we don't return anything.
        } else if (targetEntities.isEmpty()) {
            soldByEntityId.asMap().forEach((entityId, commoditySoldList) ->
                    commoditySoldList.forEach(overallCommoditySold::recordSoldCommodity));
        } else {
            // We have some number of entities, and we accumulate the stats from all the entities.
            targetEntities.forEach(entityId -> {
                final Collection<CommoditySoldDTO> commoditySoldDTOs = soldByEntityId.get(entityId);
                if (commoditySoldDTOs != null) {
                    commoditySoldDTOs.forEach(overallCommoditySold::recordSoldCommodity);
                } else {
                    logger.warn("Requested commodity {} not sold by entity {}", commodityName, entityId);
                }
            });
        }
        return overallCommoditySold.toStatRecord();
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
                                        @Nonnull final Long providerId) {
        return Optional.ofNullable(soldCommodities.get(commodityName))
                .map(providers -> providers.asMap().get(providerId))
                .map(commoditiesSold -> commoditiesSold.stream()
                        .mapToDouble(CommoditySoldDTO::getCapacity).sum());
    }

    /**
     * A builder to construct the {@link SoldCommoditiesInfo}.
     */
    static class Builder {

        private final Map<String, Multimap<Long, CommoditySoldDTO>> soldCommodities =
                new HashMap<>();

        private Builder() {}

        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entity.getCommoditySoldListList().forEach(commoditySold -> {
                final String commodity = HistoryStatsUtils.formatCommodityName(
                        commoditySold.getCommodityType().getType());
                final Multimap<Long, CommoditySoldDTO> entitySellers =
                        soldCommodities.computeIfAbsent(commodity, k -> ArrayListMultimap.create());
                saveIfNoCollision(entity, commoditySold, entitySellers);
            });
            return this;
        }

        @Nonnull
        SoldCommoditiesInfo build() {
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
                                       Multimap<Long, CommoditySoldDTO> commoditiesSoldMap) {
            // check if a previous commodity for this seller has same CommodiyType (type & key)
            boolean prevCommodityRecorded = commoditiesSoldMap.get(sellerEntity.getOid()).stream()
                    .anyMatch(prevCommodityDto -> (prevCommodityDto.getCommodityType().equals(
                            commodityToAdd.getCommodityType())));
            if (prevCommodityRecorded) {
                // previous commodity with the same key; print a warning and don't save it
                logger.warn("Entity {} selling commodity type { {} } more than once.",
                        sellerEntity.getOid(),
                        commodityToAdd.getCommodityType());
            } else {
                // no previous commodity with this same key; save the new one
                commoditiesSoldMap.put(sellerEntity.getOid(), commodityToAdd);
            }
        }
    }
}
