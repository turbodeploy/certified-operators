package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.topology.conversions.ReservedInstanceAggregate.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class is responsible for converting all the RI related data into groups that are
 * distinguished by a unique set of keys
 *
 */
public class ReservedInstanceAggregator {

    private static final Logger logger = LogManager.getLogger();

    // contains the all the ReservedInstanceData returned by the costProbe
    CloudCostData cloudCostData;

    Map<Long, TopologyEntityDTO> topology;

    // Map of riBoughtId to ReservedInstanceData
    Map<Long, ReservedInstanceData> riDataMap = new HashMap<>();

    // This map contains a list of providers that exists in every computeTier family
    Map<String, List<TopologyEntityDTO>> familyToComputeTiers;

    // Map of Reserved instance key to {@link ReservedInstanceAggregate} objects
    Map<ReservedInstanceKey, ReservedInstanceAggregate> riAggregates;

    ReservedInstanceAggregator(@Nonnull CloudCostData cloudCostData,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        riAggregates = Maps.newHashMap();
        familyToComputeTiers = new HashMap<>();
        this.cloudCostData = cloudCostData;
        this.topology = topology;
    }

    /**
     * We aggregate RIs into distinct {@link ReservedInstanceAggregate} objects based on a set of
     * distinguishing attributes. ComputeTiers are then segregated by family and we find the largest
     * computeTier in each family for every distinct ReservedInstanceAggregate that was created.
     *
     */
    public Collection<ReservedInstanceAggregate> aggregate(@Nonnull TopologyInfo topologyInfo) {
        // aggregate RIs into distinct ReservedInstanceAggregate objects based on RIKey
        boolean success = aggregateRis(topologyInfo);

        if (success) {
            // seperate compuTiers by family
            segregateComputeTiersByFamily();

            // assign the largest computeTier for each ReservedInstanceAggregate
            updateRiAggregateWithLargestComputeTier();
        }
        return riAggregates.values();
    }

    /**
     * Iterate over {@link ReservedInstanceData} object and create {@link ReservedInstanceAggregate}
     * objects containing related set of RIs
     *
     */
    boolean aggregateRis(@Nonnull TopologyInfo topologyInfo) {
        boolean success = false;
        Collection<ReservedInstanceData> riCollection;
        if (topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo().getPlanType()
                .equals(StringConstants.OPTIMIZE_CLOUD_PLAN_TYPE)) {
            // get buy RI anf existing RI
            riCollection = cloudCostData.getAllRiBought();
        } else {
            riCollection = cloudCostData.getExistingRiBought();
        }
        for (ReservedInstanceData riData : riCollection) {
            if (riData.isValid(topology)) {
                riDataMap.put(riData.getReservedInstanceBought().getId(), riData);
                ReservedInstanceAggregate riAggregate = new ReservedInstanceAggregate(riData, topology);
                riAggregates.putIfAbsent(riAggregate.getRiKey(), riAggregate);
                riAggregate = riAggregates.get(riAggregate.getRiKey());
                riAggregate.addConstituentRi(riData);
                success = true;
            }
        }
        return success;
    }

    /**
     * Iterate over all the computeTiers in the topology sent to the market and segregate every
     * computeTier that belonged to a specific family into groups
     *
     */
    void segregateComputeTiersByFamily() {
        for (TopologyDTO.TopologyEntityDTO dto : topology.values()) {
            if (dto.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                String family = dto.getTypeSpecificInfo().getComputeTier().getFamily();
                familyToComputeTiers.putIfAbsent(family, new ArrayList<>());
                familyToComputeTiers.get(family).add(dto);
            }
        }
        // sort the entities in a family by coupon count for optimization
        CouponCountComparator comparator = new CouponCountComparator();
        for (List<TopologyEntityDTO> tiersOfFamily : familyToComputeTiers.values()) {
            Collections.sort(tiersOfFamily, comparator);
        }
    }

    /**
     * Associate every {@link ReservedInstanceAggregate} with the largest computeTier in that family
     * within the constraints of the right region or zone
     *
     */
    void updateRiAggregateWithLargestComputeTier() {
        for (ReservedInstanceAggregate riAggregate : riAggregates.values()) {
            ReservedInstanceKey key = riAggregate.getRiKey();
            List<TopologyEntityDTO> computeTiers = familyToComputeTiers.get(key.getFamily());
            if (computeTiers == null) {
                logger.error("We have an RI belonging to " + key.getFamily() + ", but no computeTiers " +
                        "belonging to the same family were in this topology");
            }
            computeTiers.stream().forEach(computeTier -> riAggregate.checkAndUpdateLargestTier(computeTier, key));
        }
    }

    /**
     * Comparator to sort computeTiers based on the number of coupons
     *
     */
    static class CouponCountComparator implements Comparator<TopologyEntityDTO> {

        @Override
        public int compare(TopologyEntityDTO tier1, TopologyEntityDTO tier2) {
            return tier2.getTypeSpecificInfo().getComputeTier().getNumCoupons() -
                    tier1.getTypeSpecificInfo().getComputeTier().getNumCoupons();
        }
    }

    /**
     * @return the mapping between the riId and the {@link ReservedInstanceData}
     *
     */
    public Map<Long, ReservedInstanceData> getRIDataMap() {
        return riDataMap;
    }
}