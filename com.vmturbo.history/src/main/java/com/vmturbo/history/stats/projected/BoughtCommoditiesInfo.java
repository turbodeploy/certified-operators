package com.vmturbo.history.stats.projected;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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
     * Please note that there can be multiple {@link CommodityBoughtDTO} for a given
     * (commodity name, provider id) tuple. This is because the commodity name string doesn't take
     * into account the commodity key.
     * In addition, an entity can buy from the same provider, but in multiple separate commodities
     * sets. In this case, the same commodities will we bought by same provider.
     * Example: VM1 buys from ST1 and ST2. Market is generating a move action, from ST1 to ST2.
     * Now VM1 is buying 2 sets for commodities, both from ST2.
     * This data structure is flattening those separations, but the stats API doesn't currently support
     * commodity keys anyway.
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
        //noinspection StatementWithEmptyBody
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
                    // it will happen, for example when API try to query VCpu commodity for VM entities.
                    logger.debug("Entity {} not buying {}...", entityId, commodityName);
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
         * Record the commodities bought by the given entity in the {@link #boughtCommodities}.
         * The first-level key is the entity's OID. The second-level key is the
         * seller's OID.
         *
         * @param entity the entity that is buying the commodities
         * @return 'this' to provide for flow-style usage
         */
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {

            // iterate over the different commodities sets (comm set).
            // each one represents a Set<CommodityBoughtDTO> that the consumer needs to buy from the
            // same provider.
            // Please note that multiple comm sets can exists, and even more than one of them can
            // buy from the same provider.
            for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider : entity.getCommoditiesBoughtFromProvidersList()) {

                // get provider id for this comm set
                final Long providerId = commoditiesBoughtFromProvider.hasProviderId() ?
                        commoditiesBoughtFromProvider.getProviderId() : null;

                // set used to check if the entity is buying the same commodityType from same provider
                Set<TopologyDTO.CommodityType> commTypesAlreadySeen = new HashSet<>();

                // iterate over every commodity in this comm set
                for (CommodityBoughtDTO commodityBoughtDTO : commoditiesBoughtFromProvider.getCommodityBoughtList()) {

                    TopologyDTO.CommodityType commodityType = commodityBoughtDTO.getCommodityType();
                    final boolean notSeenBefore = commTypesAlreadySeen.add(commodityType);

                    // check if we already have this comm type in this comm set or not
                    if (notSeenBefore) {
                        // we didn't saw this comm type before, so we need to save it

                        // convert the commodity in a string format
                        final String commodityString = HistoryStatsUtils.formatCommodityName(
                                commodityType.getType());

                        // get the map for that commodity type
                        final Map<Long, Multimap<Long, CommodityBoughtDTO>> entityToCommBoughtMap =
                                boughtCommodities.computeIfAbsent(commodityString, k -> new HashMap<>());

                        // get the multimap for the current entity
                        final Multimap<Long, CommodityBoughtDTO> providerToCommBoughtMultimap =
                                entityToCommBoughtMap.computeIfAbsent(entity.getOid(),
                                        k -> ArrayListMultimap.create());

                        // for the same provider, we can have multiple commBought set, with same
                        // commodities, so in this multimap we are flattening them, and losing
                        // their separation in sets.
                        // right now this is ok, because we are not using that information after
                        // this point.
                        providerToCommBoughtMultimap.put(providerId, commodityBoughtDTO);

                    } else {
                        // in this case we are buying duplicate commodity from same provider
                        // in the same commodity set. we print a message and we don't save it
                        logger.warn("Entity {}[{}] is buying the duplicate commodity {} from the " +
                                        "provider {}. Skipping saving stats for this commodity.",
                                entity.getOid(), entity.getDisplayName(), commodityType, providerId);
                    }
                }
            }
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

    }
}
