package com.vmturbo.market.topology;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.topology.conversions.ReservedInstanceAggregate;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * This is a CBTP in Classic terminology. It represents a set of Reserved instances who have common
 * tenancy, osType, regionId, zoneId, accountId, family. The RIDiscountedMarketTier consists of
 * one {@link ReservedInstanceAggregate} which in turn has constituentRIs.
 * These market tiers are converted to traders and these traders can act as suppliers for
 * shopping lists. If a cloud entity is placed on RI1, its compute shopping list will be supplied
 * by this MarketTier which will have RI1 as a constituent RI.
 */
public class RiDiscountedMarketTier implements SingleRegionMarketTier {
    private static final Logger logger = LogManager.getLogger();
    private final TopologyEntityDTO tier;
    private final TopologyEntityDTO region;
    private final ReservedInstanceAggregate riAggregate;

    public RiDiscountedMarketTier(@Nonnull TopologyEntityDTO tier,
            @Nonnull TopologyEntityDTO region,
            @Nonnull ReservedInstanceAggregate riAggregate) {
        int tierType = tier.getEntityType();
        if (!TopologyDTOUtil.isTierEntityType(tierType)
                || region.getEntityType() != EntityType.REGION_VALUE) {
            throw new IllegalArgumentException("Invalid arguments to construct RiDiscountedMarketTier" +
                    tier.getDisplayName() + ", " + region.getDisplayName());
        }
        this.tier = tier;
        this.region = region;
        this.riAggregate = riAggregate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(riAggregate);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof RiDiscountedMarketTier))
            return false;

        RiDiscountedMarketTier otherRiDiscountedMarketTier = (RiDiscountedMarketTier) other;

        return otherRiDiscountedMarketTier.getRiAggregate().equals(this.getRiAggregate());
    }

    @Override
    @Nonnull
    public TopologyEntityDTO getTier() {
        return tier;
    }

    @Override
    @Nonnull
    public TopologyEntityDTO getRegion() {
        return region;
    }

    @Nonnull
    public ReservedInstanceAggregate getRiAggregate() {
        return riAggregate;
    }

    /**
     * On demand market tier and on demand storage tier have no RI discount.
     * @return RiDiscountedMarketTier has discount
     */
    @Override
    public boolean hasRIDiscount() {
        return true;
    }

    /**
     * Iterate over all the constituent RIs and sum up the coupons bought
     *
     * @return Total number of coupons bought by the RIs in this RIAggregate
     */
    public int getTotalNumberOfCouponsBought() {
        return riAggregate.getTotalNumberOfCouponsBought();
    }

    /**
     * Iterate over all the constituent RIs and sum up the coupons used
     *
     * @return Total number of coupons used by the RIs in this RIAggregate
     */
    public float getTotalNumberOfCouponsUsed() {
        return riAggregate.getTotalNumberOfCouponsUsed();
    }

    @Override
    @Nonnull
    public String getDisplayName() {
        return EntityType.forNumber(tier.getEntityType()) + "|RiDiscountedMarketTier|"
                + tier.getDisplayName() + "|" + region.getDisplayName();
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
        return riAggregate.useCoupons(entityId, totalNumberOfCouponsToUse);
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
        riAggregate.relinquishCoupons(riCoverageToRelinquish);
    }
}
