package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * This Class is used to keep track of cloud entities commodities which resize.
 */
public class CloudEntityResizeTracker {

    // Entity id - commodity type -  amount of resize
    private Map<Long, Map<CommodityType, Double>> data = Maps.newHashMap();

    public enum CommodityUsageType {
        CONGESTED,
        UNDER_UTILIZED
    }

    /**
     * Get the list of commodities which were congested, and the list of commodities which were
     * under utilized for an entity.
     *
     * @param entityId the entity id
     * @return A map of {@link CommodityUsageType} to a list of commodities
     */
    @Nonnull
    public Map<CommodityUsageType, Set<CommodityType>> getCommoditiesResizedByUsageType(@Nonnull Long entityId) {
        Map<CommodityUsageType, Set<CommodityType>> commoditiesResizedByUsageType = Maps.newHashMap();
        Set<CommodityType> congestedCommodities = new HashSet<>();
        Set<CommodityType> underUtilizedCommodities = new HashSet<>();
        Map<CommodityType, Double> commChanges = data.get(entityId);
        if (commChanges != null) {
            for(Entry<CommodityType, Double> commChange : commChanges.entrySet()) {
                if (commChange.getValue() > 0) {
                    congestedCommodities.add(commChange.getKey());
                } else if (commChange.getValue() < 0){
                    underUtilizedCommodities.add(commChange.getKey());
                }
            }
        }
        commoditiesResizedByUsageType.put(CommodityUsageType.CONGESTED, congestedCommodities);
        commoditiesResizedByUsageType.put(CommodityUsageType.UNDER_UTILIZED, underUtilizedCommodities);
        return commoditiesResizedByUsageType;
    }

    /**
     * Answers the question - did any commodity of the entity resize.
     *
     * @param entityId the entity id
     * @return true if any commodity of the entity resized. false otherwise.
     */
    public boolean didCommoditiesOfEntityResize(@Nonnull Long entityId) {
        return data.get(entityId) != null;
    }

    /**
     * Log the resize of a commodity of an entity
     * @param entityId the entity which resized
     * @param commodityType the commodityType which resized
     * @param suggestedChangeInCapacity the change in capacity
     */
    public void logCommodityResize(@Nonnull Long entityId, @Nonnull CommodityType commodityType,
                                   @Nonnull Double suggestedChangeInCapacity) {
        data.computeIfAbsent(entityId, id -> Maps.newHashMap()).computeIfAbsent(
                commodityType, commType -> suggestedChangeInCapacity);
    }
}
