package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityStats;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * A wrapper class for initial placement finder result. This object is created per reservation entity per shopping list.
 */
public class InitialPlacementFinderResult {

    // the provider oid when placement succeeded, empty when placement failed
    private final Optional<Long> providerOid;

    // the cluster commodity type if exists.
    private final Optional<CommodityType> clusterComm;

    // the commodity total used and capacity stats of the cluster sl chosen.
    private final List<CommodityStats> clusterStats;

    // a list of failure data when placement failed
    private final List<FailureInfo> failureInfoList;


    /**
     * Constructor of InitialPlacementFinderResult.
     * @param providerOid provider oid if placement succeeded
     * @param clusterComm the cluster commodity type associated with provider.
     * @param clusterStats the cluster stats
     * @param failureInfoList failure information if placement failed
     */
    public InitialPlacementFinderResult(@Nonnull final Optional<Long> providerOid,
            @Nonnull final Optional<CommodityType> clusterComm,
            @Nonnull List<CommodityStats> clusterStats,
            @Nonnull List<FailureInfo> failureInfoList) {
        this.providerOid = providerOid;
        this.clusterComm = clusterComm;
        this.clusterStats = clusterStats;
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
     * Returns cluster commodity type.
     *
     * @return cluster commodity type.
     */
    public Optional<CommodityType> getClusterComm() {
        return clusterComm;
    }

    /**
     * Returns a list of commodity failure data.
     *
     * @return a list containing failed commodity type, its requested amount, closest seller oid and
     * max quantity available on that seller.
     */
    public List<FailureInfo> getFailureInfoList() {
        return failureInfoList;
    }

    /**
     * Returns the commodity total used and capacity stats of the cluster sl chosen.
     *
     * @return the clusterStats.
     */
    public List<CommodityStats> getClusterStats() {
        return clusterStats;
    }

    /**
     * Failure information when a reservation fails.
     */
    public static class FailureInfo {
        // commodity type causing placement failure
        private CommodityType commodityType;
        // max available quantity when placement failed
        private final double maxQuantity;
        // the closest seller oid when placement failed
        private final long closestSellerOid;
        // the requested amount
        private final double requestedAmount;

        /**
         * Constructor for FailureInfo.
         * @param commodityType coomodity type of the commodity rsponsible for failure.
         * @param closestSellerOid seller with the most resources of the commodity of type commodityType.
         * @param maxQuantity quantity of commodity of type commodityType available in closestSellerOid.
         * @param requestedAmount quantity of commodity of type commodityType requested by buyer.
         */
        public FailureInfo(final CommodityType commodityType, final long closestSellerOid,
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
         * If negative return 0.
         *
         * @return max available quantity
         */
        public double getMaxQuantity() {
            return Math.max(maxQuantity, 0d);
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
