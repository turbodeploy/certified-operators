package com.vmturbo.topology.processor.stitching;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Resold commodities (for further details see the "is_resold" field in CommoditySoldDTO
 * in TopologyDTO.proto) are marked as resold in the SDK in the supply chain.
 * <p/>
 * When constructing individual commodities sold, we retain a cache to provide easy lookups
 * based on the combination of TargetID, entityType, and commodityType when constructing
 * commodities sold in the {@link TopologyStitchingGraph}.
 */
public class ResoldCommodityCache {

    private final Map<Long, Long> targetIdToProbeIdMap;
    private final Map<Long, Map<EntityAndCommodityTypes, Boolean>> resoldCommodityMap;

    /**
     * Create a new cache for looking up which commodities for a target are resold
     * based on that target's supply chain.
     *
     * @param targetStore The {@link TargetStore} containing the targets.
     */
    public ResoldCommodityCache(@Nonnull final TargetStore targetStore) {
        targetIdToProbeIdMap = new HashMap<>();
        resoldCommodityMap = new HashMap<>();

        targetStore.getAll().forEach(target -> {
            targetIdToProbeIdMap.put(target.getId(), target.getProbeId());
            resoldCommodityMap.computeIfAbsent(target.getProbeId(),
                (probeId) -> buildResoldCommodities(target.getProbeInfo()));
        });
    }

    /**
     * Lookup whether a particular type of commodity is resold on a particular type of entity
     * for a target.
     *
     * @param targetId The OID of the target to look up.
     * @param entityType The numeric value of the entity type to check if the commodity
     *                   type is resold.
     * @param commodityType The numeric value of the commodity type to check if it is resold.
     * @return Returns {@link Optional#empty()} if the value cannot be looked up.
     *         Otherwise returns an optional containing the specified resold value.
     */
    public Optional<Boolean> getIsResold(final long targetId, final int entityType,
                                         final int commodityType) {
        return Optional.ofNullable(targetIdToProbeIdMap.get(targetId))
            .map(resoldCommodityMap::get)
            .map(resoldCommodities -> resoldCommodities.get(
                new EntityAndCommodityTypes(entityType, commodityType)));
    }

    @Nonnull
    private Map<EntityAndCommodityTypes, Boolean> buildResoldCommodities(
        @Nonnull final ProbeInfo probeInfo) {
        final Map<EntityAndCommodityTypes, Boolean> resoldCommodities = new HashMap<>();
        probeInfo.getSupplyChainDefinitionSetList().forEach(entity ->
            entity.getCommoditySoldList().forEach(commSold -> {
                if (commSold.hasIsResold()) {
                    resoldCommodities.put(new EntityAndCommodityTypes(
                            entity.getTemplateClass().getNumber(), commSold.getCommodityType().getNumber()),
                        commSold.getIsResold());
                }
            }));

        return resoldCommodities;
    }

    /**
     * A small helper class that combines entity and commodity types that can
     * be used as a key in a {@link HashMap}.
     */
    private static final class EntityAndCommodityTypes {
        final int entityType;
        final int commodityType;

        /**
         * Create a new {@link EntityAndCommodityTypes}.
         *
         * @param entityType The numeric type of the entity.
         * @param commodityType The numeric type of the commodity.
         */
        private EntityAndCommodityTypes(final int entityType,
                                       final int commodityType) {
            this.entityType = entityType;
            this.commodityType = commodityType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityType, commodityType);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof EntityAndCommodityTypes)) {
                return false;
            }

            final EntityAndCommodityTypes other = (EntityAndCommodityTypes)o;
            return entityType == other.entityType &&
                commodityType == other.commodityType;
        }
    }
}
