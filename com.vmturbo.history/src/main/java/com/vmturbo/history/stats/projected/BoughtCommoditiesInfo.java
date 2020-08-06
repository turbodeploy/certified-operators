package com.vmturbo.history.stats.projected;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.protobuf.TextFormat;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedBoughtCommodity;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

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
    private final Map<String, Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>>> boughtCommodities;

    /**
     * The {@link SoldCommoditiesInfo} for the topology. This is required to look up capacities.
     */
    private final SoldCommoditiesInfo soldCommoditiesInfo;

    private BoughtCommoditiesInfo(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo,
            @Nonnull final Map<String, Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>>> boughtCommodities) {
        this.boughtCommodities = Collections.unmodifiableMap(boughtCommodities);
        this.soldCommoditiesInfo = soldCommoditiesInfo;
    }

    @Nonnull
    static Builder newBuilder(Set<String> excludedCommodityNames) {
        return new Builder(excludedCommodityNames);
    }

    /**
     * Get the value of a particular commodity bought by a particular entity.
     *
     * @param entity The ID of the entity.
     * @param commodityName The name of the commodity.
     * @return The average used amount of all commodities matching the name bought by the entity.
     *         This is the same formula we use to calculate "currentValue" for stat records.
     *         Returns 0 if the entity does not buy the commodity.
     */
    double getValue(final long entity,
                    @Nonnull final String commodityName) {
        final Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>> boughtByEntityId =
                boughtCommodities.get(commodityName);
        double value = 0;
        if (boughtByEntityId != null) {
            final Long2ObjectMap<List<BoughtCommodity>> boughtByEntity = boughtByEntityId.get(entity);
            if (boughtByEntity != null) {
                for (final List<BoughtCommodity> boughtCommodities : boughtByEntity.values()) {
                    for (final BoughtCommodity boughtCommodity : boughtCommodities) {
                        value += boughtCommodity.getUsed();
                    }
                }
                value /= boughtByEntity.size();
            }
        }
        return value;
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
        final Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>> boughtByEntityId =
                boughtCommodities.get(commodityName);
        final AccumulatedBoughtCommodity overallCommoditiesBought =
                new AccumulatedBoughtCommodity(commodityName);
        //noinspection StatementWithEmptyBody
        if (boughtByEntityId == null) {
            // If this commodity is not bought, we don't return anything.
        } else if (targetEntities.isEmpty()) {
            // No entities = looping over all the entities.
            boughtByEntityId.forEach((entityId, boughtFromProviders) ->
                    boughtFromProviders.forEach((providerId, commoditiesBought) -> {
                        Optional<Double> capacity = (providerId != TopologyCommoditiesSnapshot.NO_PROVIDER_ID)
                            ? soldCommoditiesInfo.getCapacity(commodityName, providerId)
                            : Optional.empty();
                        if (providerId == TopologyCommoditiesSnapshot.NO_PROVIDER_ID || capacity.isPresent()) {
                            commoditiesBought.forEach( commodityBought ->
                                    overallCommoditiesBought.recordBoughtCommodity(
                                            commodityBought, providerId, capacity.orElse(0.0)));
                        } else {
                            logger.warn("Entity {} buying commodity {} from provider {},"
                                + " but provider is not selling it!", entityId, commodityName,
                                providerId);
                        }
                    }));
        } else {
            // A specific set of entities.
            targetEntities.forEach(entityId -> {
                final Long2ObjectMap<List<BoughtCommodity>> entitiesProviders = boughtByEntityId.get(entityId);
                if (entitiesProviders == null) {
                    // it will happen, for example when API try to query VCpu commodity for VM entities.
                    logger.debug("Entity {} not buying {}...", entityId, commodityName);
                } else {
                    entitiesProviders.forEach((providerId, commoditiesBought) -> {
                        final Optional<Double> capacity = (providerId != TopologyCommoditiesSnapshot.NO_PROVIDER_ID)
                            ? soldCommoditiesInfo.getCapacity(commodityName, providerId)
                            : Optional.empty();
                        if (providerId == TopologyCommoditiesSnapshot.NO_PROVIDER_ID || capacity.isPresent()) {
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
     * Utility class to capture the commodity information we need for projected stats. This
     * saves a LOT of memory compared to keeping the full {@link CommodityBoughtDTO} around in
     * large topologies.
     */
    static class BoughtCommodity {
        private final double used;
        private final double peak;
        private final double usedPercentile;
        private final boolean hasUsedPercentile;

        BoughtCommodity(CommodityBoughtDTO commBought) {
            this.used = commBought.getUsed();
            this.peak = commBought.getPeak();
            this.usedPercentile = commBought.getHistoricalUsed().getPercentile();
            this.hasUsedPercentile = commBought.getHistoricalUsed().hasPercentile();
        }

        public double getUsed() {
            return used;
        }

        public double getPeak() {
            return peak;
        }

        public boolean hasPercentile() {
            return hasUsedPercentile;
        }

        public double getPercentile() {
            return usedPercentile;
        }
    }


    /**
     * A builder to construct an immutable {@link BoughtCommoditiesInfo}.
     */
    static class Builder {
        private final Map<String, Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>>> boughtCommodities
            = new HashMap<>();
        private final Map<Integer, MutableInt> duplicateCommoditiesBought
            = new HashMap();

        private final Set<String> excludedCommodityNames;

        private Builder(Set<String> excludedCommodityNames) {
            this.excludedCommodityNames = excludedCommodityNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        }

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
                final long providerId = commoditiesBoughtFromProvider.hasProviderId()
                    ? commoditiesBoughtFromProvider.getProviderId() : TopologyCommoditiesSnapshot.NO_PROVIDER_ID;

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
                        // Enforce commodity exclusion.
                        if (excludedCommodityNames.contains(commodityString.toLowerCase())) {
                            continue;
                        }

                        // get the map for that commodity type
                        final Long2ObjectMap<Long2ObjectMap<List<BoughtCommodity>>> entityToCommBoughtMap =
                                boughtCommodities.computeIfAbsent(commodityString, k -> new Long2ObjectOpenHashMap<>());

                        // get the multimap for the current entity
                        final Long2ObjectMap<List<BoughtCommodity>> providerToCommBoughtMultimap =
                                entityToCommBoughtMap.computeIfAbsent(entity.getOid(),
                                        k -> new Long2ObjectOpenHashMap<>());

                        // for the same provider, we can have multiple commBought set, with same
                        // commodities, so in this multimap we are flattening them, and losing
                        // their separation in sets.
                        // right now this is ok, because we are not using that information after
                        // this point.
                        providerToCommBoughtMultimap.computeIfAbsent(providerId, k -> new ArrayList<>())
                            .add(new BoughtCommodity(commodityBoughtDTO));
                    } else {
                        // in this case we are buying duplicate commodity from same provider
                        // in the same commodity set. we print a message and we don't save it
                        logger.debug("Entity {}[{}] is buying the duplicate commodity {} " +
                                "from the provider {}. Skipping saving stats for this commodity.",
                            () -> entity.getOid(), () -> entity.getDisplayName(),
                            () -> TextFormat.shortDebugString(commodityType), () -> providerId);
                        // count these for summary logging
                        duplicateCommoditiesBought
                            .computeIfAbsent(commodityType.getType(), MutableInt::new)
                            .increment();
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
            // provide summary logs of commodities that arose in duplicate buying scenarios
            duplicateCommoditiesBought.forEach((k,v) ->
                logger.warn("Commodity was involved in {} duplicate buying relationships: " +
                    "type {}; log@DEBUG for details",  v, k));
            boughtCommodities.values().forEach(outerMap -> {
                ((Long2ObjectOpenHashMap)outerMap).trim();
                outerMap.values().forEach(innerMap -> {
                    ((Long2ObjectOpenHashMap)innerMap).trim();
                    innerMap.values().forEach(l -> ((ArrayList)l).trimToSize());
                });
            });
            return new BoughtCommoditiesInfo(soldCommoditiesInfo, boughtCommodities);
        }

    }
}
