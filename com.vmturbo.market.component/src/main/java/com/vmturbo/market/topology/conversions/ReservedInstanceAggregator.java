package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
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
    private CloudCostData cloudCostData;

    Map<Long, TopologyEntityDTO> topology;

    // Map of riBoughtId to ReservedInstanceData
    private Map<Long, ReservedInstanceData> riDataMap = new HashMap<>();

    // This map contains a list of providers that exists in every computeTier family
    private Map<String, List<TopologyEntityDTO>> familyToComputeTiers;

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
     * @param topologyInfo from which the RIs are to be aggregated.
     * @return collection of ReservedInstanceAggregate instances.
     */
    Collection<ReservedInstanceAggregate> aggregate(@Nonnull TopologyInfo topologyInfo) {
        // aggregate RIs into distinct ReservedInstanceAggregate objects based on RIKey
        boolean success = aggregateRis(topologyInfo);

        if (success) {
            // separate computeTiers by family
            segregateComputeTiersByFamily();

            // assign the computeTier for each ReservedInstanceAggregate
            updateRiAggregateWithComputeTier();
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
                .equals(StringConstants.OPTIMIZE_CLOUD_PLAN)) {
            // get buy RI and existing RI
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
    private void segregateComputeTiersByFamily() {
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
            tiersOfFamily.sort(comparator);
        }
    }

    /**
     * Associate every {@link ReservedInstanceAggregate} with a computeTier. For instance size
     * flexible RIs the compute tier is set to the largest one in that family
     * within the constraints of the right region or zone. For non-instance size flexible RIs, the
     * compute tier is set to the compute tier of the RI.
     */
    private void updateRiAggregateWithComputeTier() {
        for (ReservedInstanceAggregate riAggregate : riAggregates.values()) {
            findComputeTier(riAggregate).ifPresent(riAggregate::setComputeTier);
            if (riAggregate.getComputeTier() == null) {
                logger.warn("Compute Tier not set for RI {}", riAggregate.getDisplayName());
            }
        }
    }

    /**
     * Find a computeTier for the RI. If the RI is instance size flexible, then the computeTier must
     * belong to the same region as the RI. If the RI is non-instance size flexible, then the
     * computeTier must be the RI's computeTier.
     *
     * @param ri for which the computeTier is being tested for validity.
     * @return true if the computeTier is valid otherwise false.
     */
    private Optional<TopologyEntityDTO> findComputeTier(final ReservedInstanceAggregate ri) {
        return ri.getRiKey().isInstanceSizeFlexible() ? findComputeTierForSizeFlexibleRi(ri)
                : findComputeTierForNonSizeFlexibleRi(ri);
    }

    /**
     * Returns the largest computeTier that belongs to the same region and family as the RI.
     *
     * @param ri for which the computeTier is being sought.
     * @return the largest computeTier that belongs to the same region and family as the RI,
     * empty Optional if none found in the familyToComputeTiers map.
     */
    private Optional<TopologyEntityDTO> findComputeTierForSizeFlexibleRi(
            final ReservedInstanceAggregate ri) {
        // check if the computeTier is in the same region as the RI
        final ReservedInstanceKey key = ri.getRiKey();
        return Optional.ofNullable(getComputeTiersByFamily(key.getFamily()))
                .flatMap(computerTiers -> computerTiers.stream()
                        .filter(tier -> TopologyDTOUtil.areEntitiesConnected(tier,
                                key.getRegionId())).findFirst());
        // TODO(OM-50112): check if the computeTier is in the zone (if applicable)
    }

    private List<TopologyEntityDTO> getComputeTiersByFamily(final String family) {
        final List<TopologyEntityDTO> computeTiers = familyToComputeTiers.get(family);
        if (computeTiers == null) {
            logger.error("We have an RI belonging to {} but no computeTiers belonging to the " +
                    "same family were in this topology", family);
        }
        return computeTiers;
    }

    /**
     * Returns the computeTier corresponding to the RI's computeTier.
     *
     * @param ri for which the computeTier is being sought.
     * @return the computeTier corresponding to the RI's computeTier, empty Optional is none
     * found in the topology map.
     */
    private Optional<TopologyEntityDTO> findComputeTierForNonSizeFlexibleRi(
            final ReservedInstanceAggregate ri) {
        return ri.getConstituentRIs().stream().findAny()
                .map(constituentRi -> constituentRi.getReservedInstanceSpec()
                        .getReservedInstanceSpecInfo().getTierId())
                .map(this::getComputeTierById);
    }

    private TopologyEntityDTO getComputeTierById(final long computeTierId) {
        final TopologyEntityDTO computeTier = topology.get(computeTierId);
        if (computeTier == null) {
            logger.error("We have a constituent RI with computeTier id {} but it is not found in " +
                    "the topology.", computeTierId);
        }
        return computeTier;
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
    Map<Long, ReservedInstanceData> getRIDataMap() {
        return riDataMap;
    }
}