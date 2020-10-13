package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * This class is used to store congested and underutilized commodity types of an entity.
 */
public class CommoditiesResizeTracker {

    //Store tracked resized commodities of entity and provider
    //<buyerId, sellerId> -> Set<congested commodities betwen this buyer and this seller>
    private Map<ImmutablePair<Long, Long>, Set<CommodityTypeWithLookup>> id2CongestedCommodityTypes = Maps.newHashMap();
    private Map<ImmutablePair<Long, Long>, Set<CommodityTypeWithLookup>> id2UnderutilizedCommodityTypes = Maps.newHashMap();

    /**
     * Store commodity entity, commodity type, congested or underutilized into store.
     * @param entityId id of entity
     * @param providerId id of provider
     * @param commodityType type of commodity
     * @param congested Whether this is a congested commodity or underutilized commodity.
     * @param lookupType If the commodity is sold by a consumer like a VM (For ex., VMEM/VCPU),
     *                   use CommodityLookupType.CONSUMER. If the commodity is sold by a provider
     *                   like a ComputeTier (For ex., IO Troughput/Net Throughput), use
     *                   CommodityLookupType.PROVIDER.
     */
    public void save(@Nonnull Long entityId, @Nonnull Long providerId, @Nonnull CommodityType commodityType,
            boolean congested, CommodityLookupType lookupType) {
        Map<ImmutablePair<Long, Long>, Set<CommodityTypeWithLookup>> map = congested ? id2CongestedCommodityTypes : id2UnderutilizedCommodityTypes;
        map.computeIfAbsent(ImmutablePair.of(entityId, providerId), pair -> new HashSet<>())
            .add(ImmutableCommodityTypeWithLookup.builder().commodityType(commodityType).lookupType(lookupType).build());
    }

    /**
     * Get congested commodity types of an entity.
     * @param entityId entity id
     * @param providerId provider id
     * @return congested commodity type set.
     */
    @Nonnull
    public Set<CommodityTypeWithLookup> getCongestedCommodityTypes(long entityId, long providerId) {
        return id2CongestedCommodityTypes.getOrDefault(ImmutablePair.of(entityId, providerId), Collections.emptySet());
    }

    /**
     * Get underutilized commodity types of an entity.
     * @param entityId entity id
     * @param providerId provider id
     * @return underutilized commodity type set.
     */
    @Nonnull
    public Set<CommodityTypeWithLookup> getUnderutilizedCommodityTypes(long entityId, long providerId) {
        return id2UnderutilizedCommodityTypes.getOrDefault(ImmutablePair.of(entityId, providerId), Collections.emptySet());
    }

    /**
     * An enum to indicate if the commodity is sold by a consumer like a VM, or by a provider
     * like a ComputeTier.
     * When explaining an action, to check if the reason for the scale was
     * because commodity is under-utilized, we compare the capacity of the original commodity sold
     * and the capacity of the projected commodity sold, and check if it reduced. Depending on whether
     * the lookup is defined as CONSUMER/PROVIDER, we look for the commodity sold in either the consumer
     * or the provider.
     */
    public enum CommodityLookupType {
        /**
         * If the commodity is sold by a consumer like a VM (For ex., VMEM/VCPU), use
         * CommodityLookupType.CONSUMER.
         */
        CONSUMER,

        /**
         * If the commodity is sold by a provider like a ComputeTier.
         */
        PROVIDER
    }

    /**
     * Commodity type with lookup.
     */
    @Value.Immutable
    public interface CommodityTypeWithLookup {
        /**
         * The commodity type.
         * @return the commodity type
         */
        CommodityType commodityType();

        /**
         * If the commodity is sold by a consumer like a VM (For ex., VMEM/VCPU),
         * use CommodityLookupType.CONSUMER. If the commodity is sold by a provider
         * like a ComputeTier (For ex., IO Troughput/Net Throughput), use CommodityLookupType.PROVIDER.
         * @return the lokup type
         */
        CommodityLookupType lookupType();
    }
}
