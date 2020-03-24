package com.vmturbo.market.topology.conversions;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * This class contains all the {@link ReservedInstanceData} objects with the same distinguishing
 * characteristics defined in the {@link ReservedInstanceKey}
 *
 */
public class ReservedInstanceAggregate {
    private static final Logger logger = LogManager.getLogger();
    private static final String SEPARATOR = "|";
    // The RIs are sorted according to the payment options
    private static final Map<PaymentOption, Integer> RIPriority = ImmutableMap.of(
            PaymentOption.ALL_UPFRONT, 1,
            PaymentOption.PARTIAL_UPFRONT, 2,
            PaymentOption.NO_UPFRONT, 3);
    // the object that uniquely identifies this RIAggregate
    private final ReservedInstanceKey riKey;

    // list of bought RIs that have the same ReservedInstanceKey
    private final Set<ReservedInstanceData> constituentRIs = new HashSet<>();

    // For a ReservedInstanceAggregate representing instance size flexible RIs, the largest compute
    // tier in the family that this RI belongs to. For non-instance size flexible RIs, the
    // compute tier of the RI.
    private final TopologyEntityDTO computeTier;

    // A map of the RI Bought id to the coupon usage information
    private final Map<Long, RICouponInfo> riCouponInfoMap = new HashMap<>();
    // Set of business account ids to which the RIs that belong to this RI Aggregate can be applied
    // to.
    private final Set<Long> applicableBusinessAccounts;
    private final boolean platformFlexible;

    ReservedInstanceAggregate(@Nonnull final ReservedInstanceData riData,
                              @Nonnull final ReservedInstanceKey riKey,
                              @Nonnull final TopologyEntityDTO computeTier,
                              @Nonnull final Set<Long> applicableBusinessAccounts) {
        this.riKey = Objects.requireNonNull(riKey);
        this.computeTier = Objects.requireNonNull(computeTier);
        platformFlexible = riData.getReservedInstanceSpec().getReservedInstanceSpecInfo()
                .getPlatformFlexible();
        this.applicableBusinessAccounts = applicableBusinessAccounts;
    }

    ReservedInstanceKey getRiKey() {
        return riKey;
    }

    private Set<ReservedInstanceData> getConstituentRIs() {
        return constituentRIs;
    }

    void addConstituentRi(ReservedInstanceData riData,
                          final double couponsUsed) {
        if (!riKey.isInstanceSizeFlexible() && !constituentRIs.isEmpty()) {
            logger.error("Attempting to add more than 1 constituent RI to {} which is " +
                    "not instance size flexible", getDisplayName());
            return;
        }
        constituentRIs.add(riData);
        riCouponInfoMap.put(riData.getReservedInstanceBought().getId(), new RICouponInfo(
                riData.getReservedInstanceBought(), couponsUsed));
    }

    TopologyEntityDTO getComputeTier() {
        return computeTier;
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
        if (!this.getClass().isInstance(obj)) {
            return false;
        }
        ReservedInstanceAggregate aggregate = (ReservedInstanceAggregate)obj;
        // compare ReservedInstanceKey
        return aggregate.getRiKey().equals(this.getRiKey());
    }

    public String getDisplayName() {
        if (riKey == null) return null;
        return riKey.getAccountScopeId() + SEPARATOR +
                riKey.getFamily() + SEPARATOR +
                riKey.getOs() + SEPARATOR +
                riKey.getRegionId() + SEPARATOR +
                riKey.getTenancy() + SEPARATOR +
                riKey.getZoneId();
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
     * @return mapping from RI ID -> Number of coupons used of that RI.
     */
    public Map<Long, Double> useCoupons(long entityId, double totalNumberOfCouponsToUse) {
        double origTotalNumberOfCouponsToUse = totalNumberOfCouponsToUse;
        List<ReservedInstanceData> sortedRis =  getConstituentRIs().stream().sorted(
                Comparator.comparing(riData ->
                        RIPriority.get(riData.getReservedInstanceSpec()
                                .getReservedInstanceSpecInfo().getType().getPaymentOption())))
                .collect(Collectors.toList());
        Map<Long, Double> riIdtoCouponsOfRIUsed  = new HashMap<>();
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
                totalNumberOfCouponsToUse -= couponsOfRiUsed;
                riIdtoCouponsOfRIUsed.put(riBought.getId(), couponsOfRiUsed);
            }
        }

        if (totalNumberOfCouponsToUse > 0) {
            logger.error("Attempted to use {} coupons but could only use {} in {} for entityId {}",
                    origTotalNumberOfCouponsToUse,
                    origTotalNumberOfCouponsToUse - totalNumberOfCouponsToUse,
                    getDisplayName(), entityId);
        }

        return riIdtoCouponsOfRIUsed;
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
            final RICouponInfo riCouponInfo = riCouponInfoMap.get(riId);
            if (riCouponInfo != null) {
                double couponsRelinquished = riCouponInfo.relinquishCoupons(couponsToRelinquish);
                if (couponsRelinquished < couponsToRelinquish) {
                    logger.error("Wanted to relinquish {} coupons, but could only relinquish {} " +
                                    "coupons in {} for {}", couponsToRelinquish, couponsRelinquished,
                            riId, riCoverageToRelinquish.getEntityId());
                }
            }
        });
    }

    boolean isPlatformFlexible() {
        return platformFlexible;
    }

    /**
     * The class is used to keep track of the total coupons and coupons used per RI Bought.
     * We use this instead of directly using the RIBought because of 2 reasons:
     * 1. RIBought needs to be converted to builder objects to be able to modify the coupon
     * information which can be expensive.
     * 2. In case we want to know how many coupons of the RIBought were used prior to market
     * running, we will not lose that information
     */
    private static class RICouponInfo {
        private int totalNumberOfCoupons;
        private double numberOfCouponsUsed;

        private RICouponInfo(ReservedInstanceBought riBought, double usedCoupons) {
            totalNumberOfCoupons = riBought.getReservedInstanceBoughtInfo()
                    .getReservedInstanceBoughtCoupons().getNumberOfCoupons();
            numberOfCouponsUsed = usedCoupons;
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
         * Use the coupons of this RI.
         *
         * @param numberOfCouponsToUse the number of coupons to use.
         * @return the number of coupons that were used.
         */
        private double useCoupons(double numberOfCouponsToUse) {
            double couponsOfRiUsed = 0;
            if (getRemainingCoupons() > 0) {
                couponsOfRiUsed = Math.min(getRemainingCoupons(), numberOfCouponsToUse);
                if (couponsOfRiUsed > 0) {
                    numberOfCouponsUsed += couponsOfRiUsed;
                }
            }
            return couponsOfRiUsed;
        }

        /**
         * Relinquish coupons of this RI.
         *
         * @param numberOfCouponsToRelinquish the number of coupons to relinquish.
         * @return the number of coupons relinquished.
         */
        private double relinquishCoupons(double numberOfCouponsToRelinquish) {
            double numberOfCouponsRelinquished;
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

    Set<Long> getApplicableBusinessAccount() {
        return applicableBusinessAccounts;
    }
}
