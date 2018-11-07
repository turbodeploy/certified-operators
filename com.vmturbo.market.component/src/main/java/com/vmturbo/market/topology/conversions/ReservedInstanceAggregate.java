package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class contains all the {@link ReservedInstanceData} objects with the same distinguishing
 * characteristics defined in the {@link ReservedInstanceKey}
 *
 */
public class ReservedInstanceAggregate {
    private static final Logger logger = LogManager.getLogger();
    private static final String SEPARATOR = "|";
    // The RIs are sorted according to the payment options
    private static Map<PaymentOption, Integer> RIPriority = ImmutableMap.of(
            PaymentOption.ALL_UPFRONT, 1,
            PaymentOption.PARTIAL_UPFRONT, 2,
            PaymentOption.NO_UPFRONT, 3);
    // the object that uniquely identifies this RIAggregate
    ReservedInstanceKey riKey;

    // list of bought RIs that have the same ReservedInstanceKey
    List<ReservedInstanceData> constituentRIs = new ArrayList<>();

    // largest compute tier for in the family that this RI belongs to
    TopologyEntityDTO largestTier = null;

    // A map of the RI Bought id to the coupon usage information
    private final Map<Long, RICouponInfo> riCouponInfoMap = Maps.newHashMap();

    public ReservedInstanceAggregate(ReservedInstanceData riData, Map<Long, TopologyEntityDTO> topology) {
        riKey = new ReservedInstanceKey(riData, topology);
    }

    public ReservedInstanceKey getRiKey() {
        return riKey;
    }

    public List<ReservedInstanceData> getConstituentRIs() {
        return constituentRIs;
    }

    void addConstituentRi(ReservedInstanceData riData) {
        constituentRIs.add(riData);
        riCouponInfoMap.put(riData.getReservedInstanceBought().getId(), new RICouponInfo(
                riData.getReservedInstanceBought()));
    }

    TopologyEntityDTO getLargestTier() {
        return largestTier;
    }

    /**
     * Checks if the computeTier is present in the zone and region that this {@link ReservedInstanceAggregate}
     * is associated with and if so, set it as the largestTier
     *
     */
    void checkAndUpdateLargestTier(TopologyEntityDTO computeTier, ReservedInstanceKey key) {
        ComputeTierInfo info = computeTier.getTypeSpecificInfo().getComputeTier();
        if (info.getFamily() == key.getFamily() &&
                // check if the computeTier is in this region
                areEntitiesConnected(computeTier, key.getRegionId()) &&
                // TODO: check if the computeTier is in this zone
                (largestTier == null
                // check if the numCoupons of this tier is larger that the current largestTier's numCoupons
                || info.getNumCoupons() > largestTier.getTypeSpecificInfo()
                       .getComputeTier().getNumCoupons())) {
            largestTier = computeTier;
        }
    }

    /**
     * Checks if the computeTier is connected to the passed entity
     *
     * @return true if connected, false otherwise
     */
    private boolean areEntitiesConnected(TopologyEntityDTO computeTier, long entityId) {
        return computeTier.getConnectedEntityListList().stream()
                .map(ConnectedEntity::getConnectedEntityId)
                .filter(id -> id == entityId)
                .count() != 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(riKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        ReservedInstanceAggregate aggregate = (ReservedInstanceAggregate)obj;
        // compare ReservedInstanceKey
        return aggregate.getRiKey().equals(this.getRiKey());
    }

    public String getDisplayName() {
        if (riKey == null) return null;
        StringBuilder displayName = new StringBuilder().append(riKey.getAccount()).append(SEPARATOR)
                .append(riKey.getFamily()).append(SEPARATOR)
                .append(riKey.getOs()).append(SEPARATOR)
                .append(riKey.getRegionId()).append(SEPARATOR)
                .append(riKey.getTenancy()).append(SEPARATOR)
                .append(riKey.getZoneId());
        return displayName.toString();
    }

    /**
     * Iterate over all the constituent RIs and sum up the coupons bought
     *
     * @return Total number of coupons bought by the RIs in this RIAggregate
     */
    public int getTotalNumberOfCouponsBought() {
        return riCouponInfoMap.values().stream().map(RICouponInfo::getTotalNumberOfCoupons)
                .mapToInt(Integer::intValue).sum();
    }

    /**
     * Iterate over all the constituent RIs and sum up the coupons used
     *
     * @return Total number of coupons used by the RIs in this RIAggregate
     */
    public float getTotalNumberOfCouponsUsed() {
        return (float)riCouponInfoMap.values().stream().map(RICouponInfo::getNumberOfCouponsUsed)
                .mapToDouble(Double::doubleValue).sum();
    }

    /**
     * Use coupons of this discounted market tier.
     * For ex. let's say we have 3 constituent RIs for this discounted market tier - 1 all upfront
     * RI (RI1), 1 partial upfront RI (RI2) and 1 no upfront RI (RI3), each with 8 coupons.
     * Total of 24 coupons. None of them are used.
     * The entity wants to use 20 coupons. So we fill the RIs in the order of all upfront, partial
     * upfront and then no upfront. So we use 8 of the all upfront, 8 of the partial upfront and 4
     * of the no upfront. So the entity reserved instance coverage returned will look like:
     * RI1 - 8, RI2 - 8, RI3 - 4.
     *
     * @param entityId The entity id which wants to use the coupons
     * @param totalNumberOfCouponsToUse the number of coupons which the entity wants to use
     * @return the entity reserved instance coverage for this
     */
    public Optional<EntityReservedInstanceCoverage> useCoupons(
            long entityId, double totalNumberOfCouponsToUse) {
        double origTotalNumberOfCouponsToUse = totalNumberOfCouponsToUse;
        List<ReservedInstanceData> sortedRis =  getConstituentRIs().stream().sorted(
                (riData1, riData2) -> RIPriority.get(riData1.getReservedInstanceSpec().getReservedInstanceSpecInfo().getType().getPaymentOption())
                        .compareTo(RIPriority.get(riData2.getReservedInstanceSpec().getReservedInstanceSpecInfo().getType().getPaymentOption())))
                .collect(Collectors.toList());
        EntityReservedInstanceCoverage.Builder riCoverageBuilder = null;
        for(ReservedInstanceData riData : sortedRis) {
            if (totalNumberOfCouponsToUse < 0) {
                logger.error("Total number of coupons to use is {} in {} for entityId {}",
                        totalNumberOfCouponsToUse, getDisplayName(), entityId);
            }
            if (totalNumberOfCouponsToUse == 0) {
                break;
            }
            ReservedInstanceBought riBought = riData.getReservedInstanceBought();
            RICouponInfo couponInfo = riCouponInfoMap.get(riBought.getId());
            double couponsOfRiUsed = couponInfo.useCoupons(totalNumberOfCouponsToUse);
            if (couponsOfRiUsed > 0) {
                if (riCoverageBuilder == null) {
                    riCoverageBuilder = EntityReservedInstanceCoverage.newBuilder();
                    riCoverageBuilder.setEntityId(entityId);
                }
                totalNumberOfCouponsToUse -= couponsOfRiUsed;
                riCoverageBuilder.putCouponsCoveredByRi(riBought.getId(), couponsOfRiUsed);
            }
        }
        if (totalNumberOfCouponsToUse > 0) {
            logger.error("Attempted to use {} coupons but could only use {} in {} for entityId {}",
                    origTotalNumberOfCouponsToUse,
                    origTotalNumberOfCouponsToUse - totalNumberOfCouponsToUse,
                    getDisplayName(), entityId);
        }
        return riCoverageBuilder == null ? Optional.empty() : Optional.of(riCoverageBuilder.build());
    }

    /**
     * Relinquish coupons of this discounted market tier used by an entity.
     * For ex. Let's say there are 2 RIs - RI1 and RI2. RI1 has total of 10 coupons and 5 are used.
     * RI2 has total of 10 coupons and 6 are used. VM1 is using 2 coupons from RI1 and 2 coupons
     * from RI2, then this method will try to relinquish it.
     * After this method call with VM1's riCoverage passed in a parameter,
     * RI1 will have a total of 10 coupons and 3 will be used.
     * RI2 will have a total of 10 coupons and 4 will be used.
     *
     * @param riCoverageToRelinquish the coupons will be relinquished according to
     *                               riCoverageToRelinquish
     */
    public void relinquishCoupons(EntityReservedInstanceCoverage riCoverageToRelinquish) {
        riCoverageToRelinquish.getCouponsCoveredByRiMap().forEach((riId, couponsToRelinquish) -> {
            double couponsRelinquished = riCouponInfoMap.get(riId).relinquishCoupons(couponsToRelinquish);
            if (couponsRelinquished < couponsToRelinquish) {
                logger.error("Wanted to relinquish {} coupons, but could only relinquish {} " +
                        "coupons in {} for {}", couponsToRelinquish, couponsRelinquished,
                        riId, riCoverageToRelinquish.getEntityId());
            }
        });
    }

    /**
     * This class is what distinguishes one {@link ReservedInstanceAggregate} from another
     *
     */
    class ReservedInstanceKey {
        Tenancy tenancy;
        OSType os;
        long regionId;
        long zoneId;
        long accountId;
        String family;

        public Tenancy getTenancy() {
            return tenancy;
        }

        public OSType getOs() {
            return os;
        }

        public long getRegionId() {
            return regionId;
        }

        public String getFamily() {
            return family;
        }

        public long getAccount() {
            return accountId;
        }

        public long getZoneId() {
            return zoneId;
        }

        private ReservedInstanceKey(ReservedInstanceData riData, Map<Long, TopologyEntityDTO> topology) {
            ReservedInstanceSpecInfo riSpec = riData.getReservedInstanceSpec().getReservedInstanceSpecInfo();
            ReservedInstanceBoughtInfo riBoughtInfo = riData.getReservedInstanceBought().getReservedInstanceBoughtInfo();
            this.tenancy = riSpec.getTenancy();
            this.os = riSpec.getOs();
            this.regionId = riSpec.getRegionId();
            this.family = topology.get(riSpec.getTierId()).getTypeSpecificInfo().getComputeTier().getFamily();
            this.zoneId = riBoughtInfo.getAvailabilityZoneId();
            this.accountId = riBoughtInfo.getBusinessAccountId();
        }

        @Override
        public int hashCode() {
            return Objects.hash(tenancy, os, regionId, family, zoneId, accountId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            // compares the tenancy, os, region, zone, family to check if 2 RIAggregates are the same
            ReservedInstanceKey other = (ReservedInstanceKey)obj;
            return this.tenancy == other.tenancy &&
                    this.os == other.os &&
                    this.regionId == other.regionId &&
                    this.family == other.family &&
                    this.zoneId == other.zoneId &&
                    this.accountId == other.accountId;
        }
    }

    /**
     * The class is used to keep track of the total coupons and coupons used per RI Bought.
     * We use this instead of directly using the RIBought because of 2 reasons:
     * 1. RIBought needs to be converted to builder objects to be able to modify the coupon
     * information which can be expensive.
     * 2. In case we want to know how many coupons of the RIBought were used prior to market
     * running, we will not lose that information
     */
    private class RICouponInfo {
        private int totalNumberOfCoupons;
        private double numberOfCouponsUsed;

        private RICouponInfo(ReservedInstanceBought riBought) {
            totalNumberOfCoupons = riBought.getReservedInstanceBoughtInfo()
                    .getReservedInstanceBoughtCoupons().getNumberOfCoupons();
            numberOfCouponsUsed = riBought.getReservedInstanceBoughtInfo()
                    .getReservedInstanceBoughtCoupons().getNumberOfCouponsUsed();
        }

        private double getRemainingCoupons() {
            return totalNumberOfCoupons - numberOfCouponsUsed;
        }

        private double getNumberOfCouponsUsed() {
            return numberOfCouponsUsed;
        }

        private int getTotalNumberOfCoupons() {
            return totalNumberOfCoupons;
        }

        /**
         * Use the coupons of this RI
         * @param numberOfCouponsToUse the number of coupons to use
         * @return the number of coupons that were used
         */
        private double useCoupons(double numberOfCouponsToUse) {
            double couponsOfRiUsed = 0;
            if (getRemainingCoupons() > 0) {
                if (getRemainingCoupons() > numberOfCouponsToUse) {
                    couponsOfRiUsed = numberOfCouponsToUse;
                } else {
                    couponsOfRiUsed = getRemainingCoupons();
                }
                if (couponsOfRiUsed > 0) {
                    numberOfCouponsUsed += couponsOfRiUsed;
                }
            }
            return couponsOfRiUsed;
        }

        /**
         * Relinquish coupons of this RI
         * @param numberOfCouponsToRelinquish the number of coupons to relinquish
         * @return the number of coupons relinquished
         */
        private double relinquishCoupons(double numberOfCouponsToRelinquish) {
            double numberOfCouponsRelinquished = 0;
            if (numberOfCouponsUsed >= numberOfCouponsToRelinquish) {
                numberOfCouponsUsed -= numberOfCouponsToRelinquish;
                numberOfCouponsRelinquished = numberOfCouponsToRelinquish;
            } else {
                numberOfCouponsRelinquished = numberOfCouponsUsed;
                numberOfCouponsUsed = 0;
            }
            return numberOfCouponsRelinquished;
        }
    }

}
