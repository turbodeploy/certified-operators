package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.market.InitialPlacement;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * A wrapper class for initial placement finder result. This object is created per reservation entity per shopping list.
 */
public class InitialPlacementFinderResult {

    // the provider oid when placement succeeded, empty when placement failed
    private final Optional<Long> providerOid;

    // the cluster commodity type if exists.
    private final Optional<CommodityType> clusterComm;

    // a list of failure data when placement failed
    private final List<FailureInfo> failureInfoList;

    private final Optional<InitialPlacement.InvalidInfo> invalidInfo;

    /**
     * Constructor of InitialPlacementFinderResult.
     * @param providerOid provider oid if placement succeeded
     * @param clusterComm the cluster commodity type associated with provider.
     * @param failureInfoList failure information if placement failed
     */
    public InitialPlacementFinderResult(@Nonnull final Optional<Long> providerOid,
                                        @Nonnull final Optional<CommodityType> clusterComm,
                                        @Nonnull List<FailureInfo> failureInfoList,
                                        @Nonnull Optional<InitialPlacement.InvalidInfo> invalidInfo) {
        this.providerOid = providerOid;
        this.clusterComm = clusterComm;
        this.failureInfoList = failureInfoList;
        this.invalidInfo = invalidInfo;
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

    public Optional<InitialPlacement.InvalidInfo> getInvalidInfo() {
        return invalidInfo;
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
        // the closest seller's cluster name
        private final String closestSellerCluster;
        // whether the failure occurs in real time cache.
        private final boolean isFailedInRealtimeCache;

        /**
         * Constructor for FailureInfo.
         * @param commodityType coomodity type of the commodity rsponsible for failure.
         * @param closestSellerOid seller with the most resources of the commodity of type commodityType.
         * @param maxQuantity quantity of commodity of type commodityType available in closestSellerOid.
         * @param requestedAmount quantity of commodity of type commodityType requested by buyer.
         * @param closestSellerCluster the key of the closest seller's cluster.
         * @param isFailedInRealtimeCache true if the failure occurs in the real time cache, false in the historical cache.
         */
        public FailureInfo(final CommodityType commodityType, final long closestSellerOid,
                           final double maxQuantity, final double requestedAmount,
                           final String closestSellerCluster,
                           final boolean isFailedInRealtimeCache) {
            this.commodityType = commodityType;
            this.closestSellerOid = closestSellerOid;
            this.maxQuantity = maxQuantity;
            this.requestedAmount = requestedAmount;
            this.closestSellerCluster = closestSellerCluster;
            this.isFailedInRealtimeCache = isFailedInRealtimeCache;
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
         * Returns the closest seller's cluster name, if placement failed.
         *
         * @return closest seller's cluster name
         */
        public String getClosestSellerCluster() {
            return closestSellerCluster;
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

        /**
         * Returns true if the failure occurs in the real time cache, false in the historical cache.
         * @return
         */
        public boolean isFailedInRealtimeCache() {
            return isFailedInRealtimeCache;
        }
    }
}
