package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * A wrapper class for initial placement finder result. This object is created per reservation entity per shopping list.
 */
public class InitialPlacementFinderResult {

    // the provider oid when placement succeeded, empty when placement failed
    private final Optional<Long> providerOid;
    // a list of failure data when placement failed
    private final List<FailureInfo> failureInfoList;

    /**
     * Constructor of InitialPlacementFinderResult.
     * @param providerOid provider oid if placement succeeded
     * @param failureInfoList failure information if placement failed
     */
    public InitialPlacementFinderResult(@Nonnull final Optional<Long> providerOid,
                                        @Nonnull List<FailureInfo> failureInfoList) {
        this.providerOid = providerOid;
        this.failureInfoList = failureInfoList;
    }

    /**
     * Returns provider oid if placement succeeded.
     *
     * @return provider oid
     */
    public Optional<Long> getProviderOid() {
        return providerOid;
    }

    /**
     * Returns a list of commodity failure data.
     *
     * @return a list containing failed commodity type, its requested amount, closest seller oid and
     * max quantity available on that seller.
     */
    public List<FailureInfo> getFailureInfoList() { return failureInfoList; }

    public static class FailureInfo {
        // commodity type causing placement failure
        private CommodityType commodityType;
        // max available quantity when placement failed
        private final double maxQuantity;
        // the closest seller oid when placement failed
        private final long closestSellerOid;
        // the requested amount
        private final double requestedAmount;

        public FailureInfo(final CommodityType commodityType,final long closestSellerOid,
                           final double maxQuantity, final double requestedAmount) {
            this.commodityType = commodityType;
            this.closestSellerOid = closestSellerOid;
            this.maxQuantity = maxQuantity;
            this.requestedAmount = requestedAmount;
        }

        /**
         * Returns the commodity type that result in unplacement.
         *
         * @return reason commodity type
         */
        public CommodityType getCommodityType() {
            return commodityType;
        }

        /**
         * Returns the closest seller oid, if placement failed.
         *
         * @return closest seller oid
         */
        public long getClosestSellerOid() {
            return closestSellerOid;
        }

        /**
         * Returns the max available quantity from all seller, if placement failed.
         *
         * @return max available quantity
         */
        public double getMaxQuantity() {
            return maxQuantity;
        }

        /**
         * Returns the requested amount from buyer, if placement failed.
         *
         * @return requested amount
         */
        public double getRequestedAmount() {
            return requestedAmount;
        }
    }
}
