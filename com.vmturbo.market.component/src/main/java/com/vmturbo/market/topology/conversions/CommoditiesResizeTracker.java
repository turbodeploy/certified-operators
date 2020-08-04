package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * This class is used to store congested and underutilized commodity types of an entity.
 */
public class CommoditiesResizeTracker {

    //Store tracked resized commodities of entity and provider
    //<buyerId, sellerId> -> Set<congested commodities betwen this buyer and this seller>
    private Map<ImmutablePair<Long, Long>, Set<CommodityType>> id2CongestedCommodityTypes = Maps.newHashMap();
    private Map<ImmutablePair<Long, Long>, Set<CommodityType>> id2UnderutilizedCommodityTypes = Maps.newHashMap();

    /**
     * Store commodity entity, commodity type, congested or underutilized into store.
     * @param entityId id of entity
     * @param providerId id of provider
     * @param commodityType type of commodity
     * @param congested Whether this is a congested commodity or underutilized commodity.
     */
    public void save(@Nonnull Long entityId, @Nonnull Long providerId, @Nonnull CommodityType commodityType,
            boolean congested) {
        Map<ImmutablePair<Long, Long>, Set<CommodityType>> map = congested ? id2CongestedCommodityTypes : id2UnderutilizedCommodityTypes;
        map.computeIfAbsent(ImmutablePair.of(entityId, providerId), pair -> new HashSet<CommodityType>()).add(commodityType);
    }

    /**
     * Get congested commodity types of an entity.
     * @param entityId entity id
     * @param providerId provider id
     * @return congested commodity type set.
     */
    @Nonnull
    public Set<CommodityType> getCongestedCommodityTypes(long entityId, long providerId) {
        return id2CongestedCommodityTypes.getOrDefault(ImmutablePair.of(entityId, providerId), Collections.emptySet());
    }

    /**
     * Get underutilized commodity types of an entity.
     * @param entityId entity id
     * @param providerId provider id
     * @return underutilized commodity type set.
     */
    @Nonnull
    public Set<CommodityType> getUnderutilizedCommodityTypes(long entityId, long providerId) {
        return id2UnderutilizedCommodityTypes.getOrDefault(ImmutablePair.of(entityId, providerId), Collections.emptySet());
    }
}
