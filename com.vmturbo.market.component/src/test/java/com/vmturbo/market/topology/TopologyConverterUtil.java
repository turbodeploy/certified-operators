package com.vmturbo.market.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcher;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter;

/**
 *  Util class that implements a builder to construct
 *  instances of Topology Converter.
 */
public class TopologyConverterUtil {
    /**
     * implement builder pattern to construct TopologyConverter
     * instances.
     */
    public static class Builder {
        private @Nonnull TopologyDTO.TopologyInfo topologyInfo;
        private boolean includeGuaranteedBuyer = TopologyConverter.INCLUDE_GUARANTEED_BUYER_DEFAULT;
        private float quoteFactor = MarketAnalysisUtils.QUOTE_FACTOR;
        private MarketMode marketMode;
        private float liveMarketMoveCostFactor = MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR;
        private float storageMoveCostFactor = MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR;
        private @Nonnull CloudRateExtractor marketCloudRateExtractor;
        private CommodityConverter incomingCommodityConverter;
        private CloudCostData cloudCostData;
        private CommodityIndexFactory commodityIndexFactory;
        private @Nonnull TierExcluderFactory tierExcluderFactory;
        private @Nonnull ConsistentScalingHelperFactory consistentScalingHelperFactory;
        private @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology;
        private @Nonnull ReversibilitySettingFetcher reversibilitySettingFetcher;
        private int licensePriceWeightScale;
        private boolean enableOP;
        private boolean useVMReservationAsUsed;
        private boolean singleVMonHost;
        private float customUtilizationThreshold;
        private FakeEntityCreator fakeEntityCreator;

        /**
         * builder setter method.
         * @param topologyInfo  Information about the topology.
         * @return Builder.
         */
        public Builder topologyInfo(@Nonnull TopologyInfo topologyInfo) {
            this.topologyInfo = Objects.requireNonNull(topologyInfo);
            return this;
        }

        /**
         * builder setter method.
         * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not.
         * @return Builder.
         */
        public Builder includeGuaranteedBuyer(boolean includeGuaranteedBuyer) {
            this.includeGuaranteedBuyer = includeGuaranteedBuyer;
            return this;
        }

        /**
         * builder setter method.
         * @param quoteFactor to be used by move recommendations.
         * @return Builder.
         */
        public Builder quoteFactor(float quoteFactor) {
            this.quoteFactor = quoteFactor;
            return this;
        }

        /**
         * builder setter method.
         * @param marketMode marketMode the market generates compute scaling action for could vms if
         *  false.The SMA (Stable Marriage Algorithm)  library generates them if true.
         * @return Builder.
         */
        public Builder marketMode(MarketMode marketMode) {
            this.marketMode = marketMode;
            return this;
        }

        /**
         * builder setter method.
         * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move
         * actions.
         * @return Builder.
         */
        public Builder liveMarketMoveCostFactor(float liveMarketMoveCostFactor) {
            this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
            return this;
        }

        /**
         * builder setter method.
         * @param storageMoveCostFactor storageMoveCostFactor used to normalize cost of storage moves.
         * @return Builder.
         */
        public Builder storageMoveCostFactor(float storageMoveCostFactor) {
            this.storageMoveCostFactor = storageMoveCostFactor;
            return this;
        }

        /**
         * builder setter method.
         * @param cloudTopology cloud cost data.
         * @return Builder.
         */
        public Builder cloudTopology(CloudTopology<TopologyEntityDTO> cloudTopology) {
            this.cloudTopology = cloudTopology;
            return this;
        }

        /**
         * builder setter method.
         * @param consistentScalingHelperFactory  CSM helper factory.
         * @return Builder.
         */
        public Builder consistentScalingHelperFactory(ConsistentScalingHelperFactory consistentScalingHelperFactory) {
            this.consistentScalingHelperFactory = consistentScalingHelperFactory;
            return this;
        }

        /**
         * builder setter method.
         * @param tierExcluderFactory  tierExcluderFactory.
         * @return Builder.
         */
        public Builder tierExcluderFactory(TierExcluderFactory tierExcluderFactory) {
            this.tierExcluderFactory = tierExcluderFactory;
            return this;
        }

        /**
         * builder setter method.
         * @param commodityIndexFactory commodityIndexFactory commodity index factory.
         * @return Builder.
         */
        public Builder commodityIndexFactory(CommodityIndexFactory commodityIndexFactory) {
            this.commodityIndexFactory = commodityIndexFactory;
            return this;
        }

        /**
         * builder setter method.
         * @param reversibilitySettingFetcher  fetcher for "Savings vs Reversibility" policy settings.
         * @return Builder.
         */
        public Builder reversibilitySettingFetcher(ReversibilitySettingFetcher reversibilitySettingFetcher) {
            this.reversibilitySettingFetcher = reversibilitySettingFetcher;
            return this;
        }

        /**
         * builder setter method.
         * @param enableOP  flag to check if to use over provisioning commodity changes.
         * @return Builder.
         */
        public Builder enableOP(boolean enableOP) {
            this.enableOP = enableOP;
            return this;
        }

        /**
         * builder setter method.
         * @param singleVMonHost  Enabling specific logic for single vm on host.
         * @return Builder.
         */
        public Builder singleVMonHost(boolean singleVMonHost) {
            this.singleVMonHost = singleVMonHost;
            return this;
        }

        /**
         * builder setter method.
         * @param customUtilizationThreshold A utilization threshold that can be used for custom logic.
         * @return Builder.
         */
        public Builder customUtilizationThreshold(float customUtilizationThreshold) {
            this.customUtilizationThreshold = customUtilizationThreshold;
            return this;
        }

        /**
         * builder setter method.
         * @param useVMReservationAsUsed  market price table.
         * @return Builder.
         */
        public Builder useVMReservationAsUsed(boolean useVMReservationAsUsed) {
            this.useVMReservationAsUsed = useVMReservationAsUsed;
            return this;
        }

        /**
         * builder setter method.
         * @param marketCloudRateExtractor market price table.
         * @return Builder.
         */
        public Builder marketCloudRateExtractor(CloudRateExtractor marketCloudRateExtractor) {
            this.marketCloudRateExtractor = marketCloudRateExtractor;
            return this;
        }

        /**
         * builder setter method.
         * @param licensePriceWeightScale licensePriceWeightScale value to scale the price weight of
         * commodities for every softwareLicenseCommodity sold by a provider.
         * @return Builder.
         */
        public Builder licensePriceWeightScale(int licensePriceWeightScale) {
            this.licensePriceWeightScale = licensePriceWeightScale;
            return this;
        }

        /**
         * builder setter method.
         * @param cloudCostData cloud cost data.
         * @return Builder.
         */
        public Builder cloudCostData(CloudCostData cloudCostData) {
            this.cloudCostData = cloudCostData;
            return this;
        }

        /**
         * builder setter method.
         * @param commodityConverter commodity index factory.
         * @return Builder.
         */
        public Builder commodityConverter(CommodityConverter commodityConverter) {
            this.incomingCommodityConverter = commodityConverter;
            return this;
        }

        /**
         * builder setter method.
         * @param fakeEntityCreator fake entity.
         * @return Builder.
         */
        public Builder fakeEntityCreator(FakeEntityCreator fakeEntityCreator) {
            this.fakeEntityCreator = fakeEntityCreator;
            return this;
        }

        /**
         * build method that returns instance of TopologyConverter.
         * @return TopologyConverter.
         */
        public TopologyConverter build() {
            return new TopologyConverter(topologyInfo, includeGuaranteedBuyer, quoteFactor, marketMode,
                    liveMarketMoveCostFactor, storageMoveCostFactor, marketCloudRateExtractor,
                    incomingCommodityConverter, cloudCostData, commodityIndexFactory, tierExcluderFactory,
                    consistentScalingHelperFactory, cloudTopology, reversibilitySettingFetcher,
                    licensePriceWeightScale, enableOP, useVMReservationAsUsed, singleVMonHost,
                    customUtilizationThreshold, fakeEntityCreator);
        }

    }
}
