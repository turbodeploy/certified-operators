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

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
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

        private final Clock clock;

        private final MarketPriceTableFactory priceTableFactory;

        private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

        private final TopologyCostCalculator topologyCostCalculator;

        /**
         * The quote factor to use for allevate pressure plans. See {@link AnalysisConfig}.
         */
        private final float alleviatePressureQuoteFactor;

        private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

        public DefaultAnalysisFactory(@Nonnull final GroupServiceBlockingStub groupServiceClient,
                  @Nonnull final SettingServiceBlockingStub settingServiceClient,
                  @Nonnull final MarketPriceTableFactory marketPriceTableFactory,
                  @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                  @Nonnull final TopologyCostCalculator topologyCostCalculator,
                  @Nonnull final Clock clock,
                  final float alleviatePressureQuoteFactor,
                  final boolean suspensionThrottlingPerCluster) {
            this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
            this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
            this.priceTableFactory = Objects.requireNonNull(marketPriceTableFactory);
            this.topologyCostCalculator = Objects.requireNonNull(topologyCostCalculator);
            this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
            this.clock = Objects.requireNonNull(clock);
            this.alleviatePressureQuoteFactor = alleviatePressureQuoteFactor;
            this.suspensionsThrottlingConfig = suspensionThrottlingPerCluster ?
                    SuspensionsThrottlingConfig.CLUSTER : SuspensionsThrottlingConfig.DEFAULT;
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
                    alleviatePressureQuoteFactor : AnalysisUtil.QUOTE_FACTOR;
            final AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilder(quoteFactor,
                    this.suspensionsThrottlingConfig,
                    globalSettings);
            configCustomizer.customize(configBuilder);
            return new Analysis(topologyInfo, topologyEntities,
                groupServiceClient, clock,
                configBuilder.build(), cloudTopologyFactory,
                topologyCostCalculator, priceTableFactory);
        }

        /**
         * Retrieve global settings used for analysis configuration.
         *
         * @return The map of setting values, arranged by name.
         */
        private Map<String, Setting> retrieveSettings() {

            final Map<String, Setting> settingsMap = new HashMap<>();

            // for now only interested in one global settings: RateOfResize
            final GetSingleGlobalSettingRequest settingRequest =
                    GetSingleGlobalSettingRequest.newBuilder()
                            .setSettingSpecName(GlobalSettingSpecs.RateOfResize.getSettingName())
                            .build();

            try {
                final GetGlobalSettingResponse response =
                        settingServiceClient.getGlobalSetting(settingRequest);
                if (response.hasSetting()) {
                    settingsMap.put(response.getSetting().getSettingSpecName(), response.getSetting());
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
         * The quote factor controls the aggressiveness with which the market suggests moves.
         *
         * It's a number between 0 and 1, so that Move actions are only generated if
         * best-quote < quote-factor * current-quote. That means that if we only want Moves that
         * result in at least 25% improvement we should use a quote-factor of 0.75.
         */
        private final float quoteFactor;

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
         * Use {@link AnalysisConfig#newBuilder(float, SuspensionsThrottlingConfig, Map)}.
         */
        private AnalysisConfig(final float quoteFactor,
                              final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                              final Map<String, Setting> globalSettingsMap,
                              final boolean includeVDC,
                              final Optional<Integer> maxPlacementsOverride,
                              final float rightsizeLowerWatermark,
                              final float rightsizeUpperWatermark) {
            this.quoteFactor = quoteFactor;
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
         * @param suspensionsThrottlingConfig See {@link AnalysisConfig#suspensionsThrottlingConfig}.
         * @param globalSettings See {@link AnalysisConfig#globalSettingsMap}
         * @return The builder, which can be further customized.
         */
        public static Builder newBuilder(final float quoteFactor,
                 @Nonnull final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                 @Nonnull final Map<String, Setting> globalSettings) {
            return new Builder(quoteFactor, suspensionsThrottlingConfig, globalSettings);
        }

        public static class Builder {
            private final float quoteFactor;

            private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

            private final Map<String, Setting> globalSettings;

            private boolean includeVDC = false;

            private Optional<Integer> maxPlacementsOverride = Optional.empty();

            private float rightsizeLowerWatermark;

            private float rightsizeUpperWatermark;

            private Builder(final float quoteFactor,
                            final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                            @Nonnull final Map<String, Setting> globalSettings) {
                this.quoteFactor = quoteFactor;
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
                return new AnalysisConfig(quoteFactor, suspensionsThrottlingConfig, globalSettings,
                    includeVDC, maxPlacementsOverride, rightsizeLowerWatermark,
                    rightsizeUpperWatermark);
            }
        }
    }
}
