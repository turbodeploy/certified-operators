package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class is responsible for converting all the RI related data into groups that are
 * distinguished by a unique set of keys.
 *
 */
class ReservedInstanceAggregator {

    private static final Logger logger = LogManager.getLogger();

    // contains the all the ReservedInstanceData returned by the costProbe
    private final CloudCostData<TopologyEntityDTO> cloudCostData;

    private final Map<Long, TopologyEntityDTO> topology;

    // Map of riBoughtId to ReservedInstanceData
    private final Map<Long, ReservedInstanceData> riDataMap = new HashMap<>();

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    ReservedInstanceAggregator(@Nonnull CloudCostData cloudCostData,
                               @Nonnull Map<Long, TopologyEntityDTO> topology,
                               @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        this.cloudCostData = cloudCostData;
        this.topology = topology;
        this.cloudTopology = cloudTopology;
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
        final Collection<ReservedInstanceData> riCollection;
        if (topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo().getPlanType()
                .equals(StringConstants.OPTIMIZE_CLOUD_PLAN)) {
            // get buy RI and existing RI
            riCollection = cloudCostData.getAllRiBought();
        } else {
            riCollection = cloudCostData.getExistingRiBought();
        }

        // family name to list of compute tiers sorted in descending order
        final Map<String, List<TopologyEntityDTO>> familyToComputeTiers = topology.values().stream()
                .filter(dto -> dto.getEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .collect(Collectors.groupingBy(dto -> dto.getTypeSpecificInfo()
                        .getComputeTier().getFamily(), Collectors.toList()));
        familyToComputeTiers.values()
                .forEach(tiers -> tiers.sort(Comparator.comparingInt(
                        (TopologyEntityDTO t) -> t.getTypeSpecificInfo()
                                .getComputeTier()
                                .getNumCoupons()).reversed()));
        final Map<ReservedInstanceKey, ReservedInstanceAggregate> riAggregates
                = new HashMap<>();
        final Map<Long, Double> couponsUsedByRi = cloudCostData.getCurrentRiCoverage().values()
                .stream().map(Cost.EntityReservedInstanceCoverage::getCouponsCoveredByRiMap)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, Double::sum));
        logger.trace("Coupons used by RI map dump: {}", () -> couponsUsedByRi);
        for (ReservedInstanceData riData : riCollection) {
            final long businessAccountId = riData.getReservedInstanceBought()
                    .getReservedInstanceBoughtInfo().getBusinessAccountId();
            final Optional<GroupAndMembers> billingFamilyGroup =
                    cloudTopology.getBillingFamilyForEntity(businessAccountId);
            if (riData.isValid(topology)) {
                final String family = topology.get(riData.getReservedInstanceSpec()
                        .getReservedInstanceSpecInfo().getTierId()).getTypeSpecificInfo()
                        .getComputeTier().getFamily();
                final long billingFamilyId = billingFamilyGroup.map(bfg -> bfg.group().getId())
                        .orElse(businessAccountId);
                final ReservedInstanceKey riKey =
                        new ReservedInstanceKey(riData, family, billingFamilyId);
                final Optional<TopologyEntityDTO> computeTier = findComputeTier(riKey, riData,
                        familyToComputeTiers);
                if (computeTier.isPresent()) {
                    final long riBoughtId = riData.getReservedInstanceBought().getId();
                    riDataMap.put(riBoughtId, riData);
                    final ReservedInstanceAggregate riAggregate =
                            riAggregates.computeIfAbsent(riKey, key -> {
                                final ReservedInstanceScopeInfo reservedInstanceScopeInfo =
                                        riData.getReservedInstanceBought()
                                                .getReservedInstanceBoughtInfo()
                                                .getReservedInstanceScopeInfo();
                                final Set<Long> applicableBusinessAccounts = new HashSet<>();
                                if (reservedInstanceScopeInfo.getShared()) {
                                    applicableBusinessAccounts.addAll(
                                            billingFamilyGroup.map(GroupAndMembers::members)
                                                    .orElse(Collections.singleton(
                                                            businessAccountId)));
                                } else {
                                    applicableBusinessAccounts.addAll(
                                            reservedInstanceScopeInfo.getApplicableBusinessAccountIdList());
                                }
                                return new ReservedInstanceAggregate(riData, riKey,
                                        computeTier.get(), applicableBusinessAccounts);
                            });
                    final double usedCoupons = couponsUsedByRi.getOrDefault(riBoughtId, 0d);
                    logger.trace("Adding constituent RI: {} with usedCoupons: {} to RI Aggregate:" +
                                    " {}", riData::getReservedInstanceBought, () -> usedCoupons,
                            riAggregate::getDisplayName);
                    riAggregate.addConstituentRi(riData, usedCoupons);
                } else {
                    logger.warn("Compute Tier not found for RI with spec: {}, bought info: {}",
                            riData.getReservedInstanceSpec(),
                            riData.getReservedInstanceBought());
                }
            }
        }
        return riAggregates.values();
    }

    /**
     * Find a computeTier for the RI. If the RI is instance size flexible, then the computeTier must
     * belong to the same region as the RI. If the RI is non-instance size flexible, then the
     * computeTier must be the RI's computeTier.
     *
     * @param riKey of the RI for which the computeTier is being found.
     * @param riData of the RI for which the computeTier is being found.
     * @param familyToComputeTiers sorted list of compute tiers by family.
     * @return computeTier if found, otherwise empty Optional.
     */
    private Optional<TopologyEntityDTO> findComputeTier(final ReservedInstanceKey riKey,
                                                        final ReservedInstanceData riData,
                                                        final Map<String,
            List<TopologyEntityDTO>> familyToComputeTiers) {
        return riKey.isInstanceSizeFlexible() ? findComputeTierForSizeFlexibleRi(riKey,
                familyToComputeTiers) : findComputeTierForNonSizeFlexibleRi(riData);
    }

    /**
     * Returns the largest computeTier that belongs to the same region and family as the RI.
     *
     * @param riKey for which the computeTier is being sought.
     * @param familyToComputeTiers sorted list of compute tiers by family.
     * @return the largest computeTier that belongs to the same region and family as the RI,
     * empty Optional if none found in the familyToComputeTiers map.
     */
    private Optional<TopologyEntityDTO> findComputeTierForSizeFlexibleRi(
            final ReservedInstanceKey riKey,
            Map<String, List<TopologyEntityDTO>> familyToComputeTiers) {
        // check if the computeTier is in the same region as the RI
        return Optional.ofNullable(getComputeTiersByFamily(riKey.getFamily(), familyToComputeTiers))
                .flatMap(computerTiers -> computerTiers.stream()
                        .filter(tier -> TopologyDTOUtil.areEntitiesConnected(tier,
                                riKey.getRegionId())).findFirst());
        // TODO(OM-50112): check if the computeTier is in the zone (if applicable)
    }

    private List<TopologyEntityDTO> getComputeTiersByFamily(final String family, final Map<String,
            List<TopologyEntityDTO>> familyToComputeTiers) {
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
     * @param riData for which the computeTier is being sought.
     * @return the computeTier corresponding to the RI's computeTier, empty Optional is none
     * found in the topology map.
     */
    private Optional<TopologyEntityDTO> findComputeTierForNonSizeFlexibleRi(
            final ReservedInstanceData riData) {
        return Optional.ofNullable(getComputeTierById(riData.getReservedInstanceSpec()
                .getReservedInstanceSpecInfo()
                .getTierId()));
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
     * @return the mapping between the riId and the {@link ReservedInstanceData}
     */
    Map<Long, ReservedInstanceData> getRIDataMap() {
        return riDataMap;
    }
}