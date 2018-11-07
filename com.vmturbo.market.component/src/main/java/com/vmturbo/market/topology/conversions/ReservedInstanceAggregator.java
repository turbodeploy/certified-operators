package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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

    Map<Long, ReservedInstanceData> riDataMap = new HashMap<>();

    // This map contains a list of providers that exists in every computeTier family
    Map<String, List<TopologyEntityDTO>> familyToComputeTiers;

    // List of distinct {@link ReservedInstanceAggregate} objects
    List<ReservedInstanceAggregate> riAggregates;

    ReservedInstanceAggregator(@Nonnull CloudCostData cloudCostData,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        riAggregates = new ArrayList();
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
    public List<ReservedInstanceAggregate> aggregate() {
        // aggregate RIs into distinct ReservedInstanceAggregate objects based on RIKey
        boolean success = aggregateRis();

        if (success) {
            // seperate compuTiers by family
            segregateComputeTiersByFamily();

            // assign the largest computeTier for each ReservedInstanceAggregate
            updateRiAggregateWithLargestComputeTier();
        }

        return riAggregates;
    }

    /**
     * Iterate over {@link ReservedInstanceData} object and create {@link ReservedInstanceAggregate}
     * objects containing related set of RIs
     *
     */
    boolean aggregateRis() {
        boolean success = false;
        for (ReservedInstanceData riData : cloudCostData.getAllRiBought()) {
            riDataMap.put(riData.getReservedInstanceBought().getId(), riData);
            ReservedInstanceAggregate riAggregate = new ReservedInstanceAggregate(riData, topology);
            int indexOfAggregate = riAggregates.indexOf(riAggregate);
            // if riAggregate not in list of aggregates, create a new one
            if (riAggregates.indexOf(riAggregate) != -1) {
                riAggregate = riAggregates.get(indexOfAggregate);
            } else {
                riAggregates.add(riAggregate);
            }
            // add the riData in the right riAggregate
            riAggregate.addConstituentRi(riData);
            success = true;
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
    }

    /**
     * Associate every {@link ReservedInstanceAggregate} with the largest computeTier in that family
     * within the constraints of the right region or zone
     *
     */
    void updateRiAggregateWithLargestComputeTier() {
        for (ReservedInstanceAggregate riAggregate : riAggregates) {
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
     * @return the mapping between the riId and the {@link ReservedInstanceData}
     *
     */
    public Map<Long, ReservedInstanceData> getRIDataMap() {
        return riDataMap;
    }

}