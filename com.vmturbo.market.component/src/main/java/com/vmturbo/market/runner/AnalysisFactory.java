package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

/**
 * A factory class for {@link Analysis} instances.
 */
public interface AnalysisFactory {

    /**
     * Create a new {@link Analysis}.
     *
     * @param topologyInfo Information about the topology this analysis applies to.
     * @param topologyEntities The entities in the topology.
     * @param configCustomizer A {@link AnalysisConfigCustomizer} to tweak the configuration of
     *                         the analysis.
     * @return The {@link Analysis} object.
     */
    @Nonnull
    Analysis newAnalysis(@Nonnull final TopologyInfo topologyInfo,
                         @Nonnull final Set<TopologyEntityDTO> topologyEntities,
                         @Nonnull final AnalysisConfigCustomizer configCustomizer);

    /**
     * A helper function to tweak the configuration of the analysis produced by the factory.
     */
    @FunctionalInterface
    interface AnalysisConfigCustomizer {
        void customize(@Nonnull final AnalysisConfig.Builder configBuilder);
    }

    /**
     * The default implementation of {@link AnalysisFactory}.
     */
    class DefaultAnalysisFactory implements AnalysisFactory {

        private static final Logger logger = LogManager.getLogger();

        private final GroupServiceBlockingStub groupServiceClient;

        private final SettingServiceBlockingStub settingServiceClient;

        private final TierExcluderFactory tierExcluderFactory;

        private final Clock clock;

        private final MarketPriceTableFactory priceTableFactory;

        private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

        private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

        private final WastedFilesAnalysisFactory wastedFilesAnalysisFactory;

        private final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory;

        private final CloudCostDataProvider cloudCostDataProvider;

        private final AnalysisRICoverageListener listener;

        /**
         * The quote factor to use for allevate pressure plans. See {@link AnalysisConfig}.
         */
        private final float alleviatePressureQuoteFactor;

        /**
         * The quote factor for all analysis runs that are NOT alleviate pressure plans.
         * See {@link AnalysisConfig}.
         */
        private final float standardQuoteFactor;

        private final float liveMarketMoveCostFactor;

        private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

        private final ConsistentScalingHelperFactory consistentScalingHelperFactory;

        public DefaultAnalysisFactory(@Nonnull final GroupServiceBlockingStub groupServiceClient,
                  @Nonnull final SettingServiceBlockingStub settingServiceClient,
                  @Nonnull final MarketPriceTableFactory marketPriceTableFactory,
                  @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                  @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
                  @Nonnull final WastedFilesAnalysisFactory wastedFilesAnalysisFactory,
                  @Nonnull final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory,
                  @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                  @Nonnull final Clock clock,
                  final float alleviatePressureQuoteFactor,
                  final float standardQuoteFactor,
                  final float liveMarketMoveCostFactor,
                  final boolean suspensionThrottlingPerCluster,
                  @Nonnull final TierExcluderFactory tierExcluderFactory,
                  @Nonnull AnalysisRICoverageListener listener,
                  @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory) {
            Preconditions.checkArgument(alleviatePressureQuoteFactor >= 0f);
            Preconditions.checkArgument(alleviatePressureQuoteFactor <= 1.0f);
            Preconditions.checkArgument(standardQuoteFactor >= 0f);
            Preconditions.checkArgument(standardQuoteFactor <= 1.0f);

            this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
            this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
            this.priceTableFactory = Objects.requireNonNull(marketPriceTableFactory);
            this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
            this.wastedFilesAnalysisFactory = Objects.requireNonNull(wastedFilesAnalysisFactory);
            this.buyRIImpactAnalysisFactory = Objects.requireNonNull(buyRIImpactAnalysisFactory);
            this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
            this.clock = Objects.requireNonNull(clock);
            this.alleviatePressureQuoteFactor = alleviatePressureQuoteFactor;
            this.standardQuoteFactor = standardQuoteFactor;
            this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
            this.cloudCostDataProvider = cloudCostDataProvider;
            this.suspensionsThrottlingConfig = suspensionThrottlingPerCluster ?
                    SuspensionsThrottlingConfig.CLUSTER : SuspensionsThrottlingConfig.DEFAULT;
            this.tierExcluderFactory = tierExcluderFactory;
            this.listener = listener;
            this.consistentScalingHelperFactory = consistentScalingHelperFactory;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public Analysis newAnalysis(@Nonnull final TopologyInfo topologyInfo,
                                    @Nonnull final Set<TopologyEntityDTO> topologyEntities,
                                    @Nonnull final AnalysisConfigCustomizer configCustomizer) {
            final Map<String, Setting> globalSettings = retrieveSettings();
            final float quoteFactor = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo) ?
                    alleviatePressureQuoteFactor : standardQuoteFactor;
            final AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilder(quoteFactor,
                liveMarketMoveCostFactor, this.suspensionsThrottlingConfig, globalSettings);
            configCustomizer.customize(configBuilder);
            return new Analysis(topologyInfo, topologyEntities,
                groupServiceClient, clock,
                configBuilder.build(), cloudTopologyFactory,
                topologyCostCalculatorFactory, priceTableFactory, wastedFilesAnalysisFactory,
                buyRIImpactAnalysisFactory, tierExcluderFactory, listener, consistentScalingHelperFactory);
        }

        /**
         * Retrieve global settings used for analysis configuration.
         *
         * @return The map of setting values, arranged by name.
         */
        private Map<String, Setting> retrieveSettings() {

            final Map<String, Setting> settingsMap = new HashMap<>();

            // for now only interested in RateOfResize and DisableAllActions global settings.
            ImmutableList<String> inputSettings = ImmutableList.of(GlobalSettingSpecs.DisableAllActions.getSettingName(),
                    GlobalSettingSpecs.RateOfResize.getSettingName());
            final GetMultipleGlobalSettingsRequest settingRequest =
                            GetMultipleGlobalSettingsRequest.newBuilder()
                                .addAllSettingSpecName(inputSettings)
                                .build();

            try {
                settingServiceClient.getMultipleGlobalSettings(settingRequest)
                    .forEachRemaining(setting -> {
                        settingsMap.put(setting.getSettingSpecName(), setting);
                    });

                if (settingsMap.size() != inputSettings.size()) {
                    logger.error("Failed to get requested global settings from group component."
                                    + " Requested {} but received {} .",
                                    inputSettings.size(), settingsMap.size());
                }
            } catch (StatusRuntimeException e) {
                logger.error("Failed to get global settings from group component. Will run analysis " +
                        " without global settings.", e);
            }
            return settingsMap;
        }
    }

    /**
     * Configuration for an {@link Analysis}. This class provides various - often unrelated -
     * options to control how an {@link Analysis} behaves.
     */
    class AnalysisConfig {
        /**
         * The quote factor is a factor that multiplicatively adjusts the aggressiveness
         * with which the market suggests moves.
         *
         * It's a number between 0 and 1, so that Move actions are only generated if
         * best-quote < quote-factor * current-quote. That means that if we only want Moves that
         * result in at least 25% improvement we should use a quote-factor of 0.75.
         *
         * Increasing the value increases market aggressiveness. That is, the closer this value
         * is to 1, the more frequently the market will recommend moves. This value has a larger
         * effect at higher utilization values than the move-cost-factor and lower effect
         * at lower utilization. See comments on
         * https://vmturbo.atlassian.net/browse/OM-35316 for additional details.
         */
        private final float quoteFactor;

        /**
         * The move cost factor that additively controls the aggressiveness with which
         * the market recommends moves. Plan markets will use a different move cost factor
         * from this one (currently hard-coded to 0).
         *
         * Move actions are only generated if:
         * best-quote - move-cost-factor < quote-factor * current-quote.
         *
         * Decreasing the value increases market aggressiveness. That is, the closer this value
         * is to 0, the more frequently the market will recommend moves. This value has a larger
         * effect at lower utilization values than the quote-factor and lower effect
         * at higher utilization. See comments on
         * https://vmturbo.atlassian.net/browse/OM-35316 for additional details.
         */
        private final float liveMarketMoveCostFactor;

        private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

        private final Map<String, Setting> globalSettingsMap;

        /**
         * Specify whether the analysis should include guaranteed buyers in the market analysis.
         */
        private final boolean includeVDC;

        private final Optional<Integer> maxPlacementsOverride;

        /**
         * The minimum utilization threshold, if entity's utilization is below threshold,
         * Market could generate resize down action.
         */
        private final float rightsizeLowerWatermark;

        /**
         * The maximum utilization threshold, if entity's utilization is above threshold,
         * Market could generate resize up action.
         */
        private final float rightsizeUpperWatermark;

        /**
         * Use {@link AnalysisConfig#newBuilder(float, float, SuspensionsThrottlingConfig, Map)}
         */
        private AnalysisConfig(final float quoteFactor,
                              final float liveMarketMoveCostFactor,
                              final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                              final Map<String, Setting> globalSettingsMap,
                              final boolean includeVDC,
                              final Optional<Integer> maxPlacementsOverride,
                              final float rightsizeLowerWatermark,
                              final float rightsizeUpperWatermark) {
            this.quoteFactor = quoteFactor;
            this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
            this.suspensionsThrottlingConfig = suspensionsThrottlingConfig;
            this.globalSettingsMap = globalSettingsMap;
            this.includeVDC = includeVDC;
            this.maxPlacementsOverride = maxPlacementsOverride;
            this.rightsizeLowerWatermark = rightsizeLowerWatermark;
            this.rightsizeUpperWatermark = rightsizeUpperWatermark;
        }

        public float getQuoteFactor() {
            return quoteFactor;
        }

        public float getLiveMarketMoveCostFactor() {
            return liveMarketMoveCostFactor;
        }

        @Nonnull
        public SuspensionsThrottlingConfig getSuspensionsThrottlingConfig() {
            return suspensionsThrottlingConfig;
        }

        public boolean getIncludeVdc() {
            return includeVDC;
        }

        @Nonnull
        public Optional<Integer> getMaxPlacementsOverride() {
            return maxPlacementsOverride;
        }

        public float getRightsizeLowerWatermark() {
            return rightsizeLowerWatermark;
        }

        public float getRightsizeUpperWatermark() {
            return rightsizeUpperWatermark;
        }

        @Nonnull
        public Optional<Setting> getGlobalSetting(@Nonnull final GlobalSettingSpecs globalSetting) {
            return Optional.ofNullable(globalSettingsMap.get(globalSetting.getSettingName()));
        }

        @Nonnull
        public Map<String, Setting> getGlobalSettingMap() {
            return Collections.unmodifiableMap(globalSettingsMap);
        }

        /**
         * Create a builder for the {@link AnalysisConfig}. Arguments into this function represent
         * non-customizable aspects of the builder.
         *
         * Note: If the list of non-customizable aspects gets too long we can have an interface
         * that hides the non-customizable properties, and have {@link AnalysisConfigCustomizer}
         * accept that interface instead of the builder class.
         *
         * @param quoteFactor See {@link AnalysisConfig#quoteFactor}
         * @param liveMarketMoveCostFactor See {@link AnalysisConfig#liveMarketMoveCostFactor}
         * @param suspensionsThrottlingConfig See {@link AnalysisConfig#suspensionsThrottlingConfig}.
         * @param globalSettings See {@link AnalysisConfig#globalSettingsMap}
         * @return The builder, which can be further customized.
         */
        public static Builder newBuilder(final float quoteFactor, final float liveMarketMoveCostFactor,
                                         @Nonnull final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                 @Nonnull final Map<String, Setting> globalSettings) {
            return new Builder(quoteFactor, liveMarketMoveCostFactor,
                suspensionsThrottlingConfig, globalSettings);
        }

        public static class Builder {
            private final float quoteFactor;

            private final float liveMarketMoveCostFactor;

            private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

            private final Map<String, Setting> globalSettings;

            private boolean includeVDC = false;

            private Optional<Integer> maxPlacementsOverride = Optional.empty();

            private float rightsizeLowerWatermark;

            private float rightsizeUpperWatermark;

            private Builder(final float quoteFactor,
                            final float liveMarketMoveCostFactor,
                            final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                            @Nonnull final Map<String, Setting> globalSettings) {
                this.quoteFactor = quoteFactor;
                this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
                this.suspensionsThrottlingConfig = suspensionsThrottlingConfig;
                this.globalSettings = globalSettings;
            }

            /**
             * Configure whether to include guaranteed buyers (VDC, VPod, DPod) in the analysis.
             *
             * @param includeVDC true if the guaranteed buyers (VDC, VPod, DPod) should be included
             * @return this Builder to support flow style
             */
            @Nonnull
            public Builder setIncludeVDC(boolean includeVDC) {
                this.includeVDC = includeVDC;
                return this;
            }

            /**
             * If present, overrides the default number of placement rounds performed by the market during analysis.
             * If empty, uses the default value from the analysis project.
             *
             * @param maxPlacementsOverride the configuration store.
             * @return this Builder to support flow style.
             */
            @Nonnull
            public Builder setMaxPlacementsOverride(@Nonnull final Optional<Integer> maxPlacementsOverride) {
                this.maxPlacementsOverride = Objects.requireNonNull(maxPlacementsOverride);
                return this;
            }

            /**
             * Configure the minimum utilization threshold.
             *
             * @param rightsizeLowerWatermark minimum utilization threshold.
             * @return this Builder to support flow style
             */
            @Nonnull
            public Builder setRightsizeLowerWatermark(final float rightsizeLowerWatermark) {
                this.rightsizeLowerWatermark = rightsizeLowerWatermark;
                return this;
            }

            /**
             * Configure the maximum utilization threshold.
             *
             * @param rightsizeUpperWatermark maximum utilization threshold.
             * @return this Builder to support flow style
             */
            @Nonnull
            public Builder setRightsizeUpperWatermark(final float rightsizeUpperWatermark) {
                this.rightsizeUpperWatermark = rightsizeUpperWatermark;
                return this;
            }

            @Nonnull
            public AnalysisConfig build() {
                return new AnalysisConfig(quoteFactor, liveMarketMoveCostFactor,
                    suspensionsThrottlingConfig, globalSettings,
                    includeVDC, maxPlacementsOverride, rightsizeLowerWatermark,
                    rightsizeUpperWatermark);
            }
        }
    }
}
