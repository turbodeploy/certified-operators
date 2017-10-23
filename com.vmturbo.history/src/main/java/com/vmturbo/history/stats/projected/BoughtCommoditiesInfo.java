package com.vmturbo.history.stats.projected;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedBoughtCommodity;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.reports.db.CommodityTypes;

/**
 * This class contains information about commodities bought by entities in a topology.
 * <p>
 * It's immutable, because for any given topology the bought commodities don't change.
 */
@Immutable
class BoughtCommoditiesInfo {

    private static final Logger logger = LogManager.getLogger();

    /**
     * commodity name
     *   -> entity id (the buyer)
     *      -> provider id -> commodity bought from the provider by entity
     * <p>
     * Each entity should only have one {@link CommodityBoughtDTO} for a given
     * (commodity name, provider id) tuple. This may not be true in general, because the commodity
     * name string doesn't take into account the commodity spec keys. But the stats API doesn't
     * currently support commodity spec keys anyway.
     */
    private final Map<String, Map<Long, Multimap<Long, CommodityBoughtDTO>>> boughtCommodities;

    /**
     * The {@link SoldCommoditiesInfo} for the topology. This is required to look up capacities.
     * <p>
     */
    private final SoldCommoditiesInfo soldCommoditiesInfo;

    private BoughtCommoditiesInfo(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo,
            @Nonnull final Map<String, Map<Long, Multimap<Long, CommodityBoughtDTO>>> boughtCommodities) {
        this.boughtCommodities = Collections.unmodifiableMap(boughtCommodities);
        this.soldCommoditiesInfo = soldCommoditiesInfo;
    }

    @Nonnull
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Get the accumulated information about a particular commodity bought by a set of entities.
     *
     * @param commodityName The name of the commodity. The names are derived from
     *           {@link CommodityTypes}. This is not ideal - we should consider using the
     *           {@link CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return An optional containing the accumulated {@link StatRecord}, or an empty optional
     *         if there is no information for the commodity over the target entities.
     */
    Optional<StatRecord> getAccumulatedRecord(@Nonnull final String commodityName,
                                                     @Nonnull final Set<Long> targetEntities) {
        final Map<Long, Multimap<Long, CommodityBoughtDTO>> boughtByEntityId =
                boughtCommodities.get(commodityName);
        final AccumulatedBoughtCommodity overallCommoditiesBought =
                new AccumulatedBoughtCommodity(commodityName);
        if (boughtByEntityId == null) {
            // If this commodity is not bought, we don't return anything.
        } else if (targetEntities.isEmpty()) {
            // No entities = looping over all the entities.
            boughtByEntityId.forEach((entityId, boughtFromProviders) ->
                    boughtFromProviders.asMap().forEach((providerId, commoditiesBought) -> {
                        Optional<Double> capacity = (providerId != null) ?
                                soldCommoditiesInfo.getCapacity(commodityName, providerId) :
                                Optional.empty();
                        if (providerId == null || capacity.isPresent()) {
                            commoditiesBought.forEach( commodityBought ->
                                    overallCommoditiesBought.recordBoughtCommodity(
                                            commodityBought,providerId, capacity.orElse(0.0)));
                        } else {
                            logger.warn("Entity {} buying commodity {} from provider {}," +
                                            " but provider is not selling it!", entityId, commodityName,
                                    providerId);
                        }
                    }));
        } else {
            // A specific set of entities.
            targetEntities.forEach(entityId -> {
                final Multimap<Long, CommodityBoughtDTO> entitiesProviders =
                        boughtByEntityId.get(entityId);
                if (entitiesProviders == null) {
                    logger.warn("Entity {} not buying anything...");
                } else {
                    entitiesProviders.asMap().forEach((providerId, commoditiesBought) -> {
                        final Optional<Double> capacity = (providerId != null) ?
                                soldCommoditiesInfo.getCapacity(commodityName, providerId) :
                                Optional.empty();
                        if (providerId == null || capacity.isPresent()) {
                            commoditiesBought.forEach(commodityBought ->
                                    overallCommoditiesBought.recordBoughtCommodity(commodityBought,
                                            providerId, capacity.orElse(0.0)));
                        } else {
                            logger.warn("No capacity found for {} by provider {}",
                                    commodityName, providerId);
                        }
                    });
                }
            });
        }
        return overallCommoditiesBought.toStatRecord();
    }

    /**
     * A builder to construct an immutable {@link BoughtCommoditiesInfo}.
     */
    static class Builder {
        private final Map<String, Map<Long, Multimap<Long, CommodityBoughtDTO>>> boughtCommodities
                = new HashMap<>();

        private Builder() {}

        /**
         * Record the commodities bought by the given entity in the 'boughtCommoditiesMap'.
         * The first-level key is the entity's OID. The second-level key is the
         * seller's OID.
         *
         * @param entity the entity that is buying the commodities
         * @return 'this' to provide for flow-style usage
         */
        /**
         * Add entity's commodity bought into boughtCommodities map structure. If there are multiple
         * same type commodity bought from same providerId or no provider Id, we will pick the last one
         * among those commodity bought.
         *
         * @param entity project topology entity needs to process.
         * @return Builder object contains commodity bought of project topology.
         */
        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entity.getCommoditiesBoughtFromProvidersList().stream().forEach(commodityBoughtGrouping -> {
                        commodityBoughtGrouping.getCommodityBoughtList().stream()
                                .forEach(commodityBought -> {
                                    final String commodity = HistoryStatsUtils.formatCommodityName(
                                            commodityBought.getCommodityType().getType());
                                    final Long providerId = (commodityBoughtGrouping.hasProviderId()) ?
                                            commodityBoughtGrouping.getProviderId() : null;
                                    final Map<Long, Map<Long, CommodityBoughtDTO>> entityBuyers =
                                            boughtCommodities.computeIfAbsent(commodity, k -> new HashMap<>());
                                    final Map<Long, CommodityBoughtDTO> thisEntityBoughtCommodities =
                                            entityBuyers.computeIfAbsent(entity.getOid(), k -> new HashMap<>());
                                    final CommodityBoughtDTO prev =
                                            thisEntityBoughtCommodities.put(providerId, commodityBought);
                                    if (prev != null) {
                                        logger.warn("Entity {} is buying commodity {} from {} more" +
                                                        " than once. Previous: {}", entity.getOid(),
                                                commodityBought.getCommodityType(),
                                                providerId, prev.getCommodityType());
                                    }
                                });
                    }
            );
            return this;
        }

        /**
         * Construct the {@link BoughtCommoditiesInfo} once all entities have been added.
         *
         * @param soldCommoditiesInfo The {@link SoldCommoditiesInfo} constructed using the same
         *                            topology.
         * @return a new {@link BoughtCommoditiesInfo} containing the current boughCommodities
         */
        @Nonnull
        BoughtCommoditiesInfo build(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo) {
            return new BoughtCommoditiesInfo(soldCommoditiesInfo, boughtCommodities);
        }

        /**
         * Check to see if the given commodity type (including key) for the given seller is already
         * listed in the commoditiesSoldMap.
         * @param providerId the OID of the seller
         * @param commodityToAdd the new commodity to check for "already listed"
         * @param commoditiesBoughtMap the map from Seller OID to Collection of Commodities
         */
        private void saveIfNoCollision(@Nonnull Long providerId,
                                       CommodityBoughtDTO commodityToAdd,
                                       Multimap<Long, CommodityBoughtDTO> commoditiesBoughtMap) {
            // check if a previous commodity for this seller has same CommodiyType (type & key)
            boolean prevCommodityRecorded = commoditiesBoughtMap.get(providerId).stream()
                    .anyMatch(prevCommodityDto -> (prevCommodityDto.getCommodityType().equals(
                            commodityToAdd.getCommodityType())));
            if (prevCommodityRecorded) {
                // previous commodity with the same key; print a warning and don't save it
                logger.warn("Entity {} selling commodity type { {} } more than once.",
                        providerId, commodityToAdd.getCommodityType());
            } else {
                // no previous commodity with this same key; save the new one
                commoditiesBoughtMap.put(providerId, commodityToAdd);
            }
        }
    }
}
