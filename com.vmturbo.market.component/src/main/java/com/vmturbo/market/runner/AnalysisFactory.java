package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

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
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
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
     * @param initialPlacementFinder The class to perform fast reservation.
     * @return The {@link Analysis} object.
     */
    @Nonnull
    Analysis newAnalysis(@Nonnull final TopologyInfo topologyInfo,
                         @Nonnull final Set<TopologyEntityDTO> topologyEntities,
                         @Nonnull AnalysisConfigCustomizer configCustomizer,
                         @Nonnull InitialPlacementFinder initialPlacementFinder);

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

        private final GroupMemberRetriever groupMemberRetriever;

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

        private final ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory;

        /**
         * The service that will perform cloud commitment (RI) buy analysis during a migrate to cloud plan.
         */
        private final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService;

        // Determines if the market or the SMA (Stable Marriage Algorithm) library generates compute scaling action for cloud vms
        private final MarketMode marketMode;

        public DefaultAnalysisFactory(@Nonnull final GroupMemberRetriever groupMemberRetriever,
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
                                      final String marketModeName,
                                      final float liveMarketMoveCostFactor,
                                      final boolean suspensionThrottlingPerCluster,
                                      @Nonnull final TierExcluderFactory tierExcluderFactory,
                                      @Nonnull AnalysisRICoverageListener listener,
                                      @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory,
                                      @Nonnull final ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory,
                                      @Nonnull MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService) {
            Preconditions.checkArgument(alleviatePressureQuoteFactor >= 0f);
            Preconditions.checkArgument(alleviatePressureQuoteFactor <= 1.0f);
            Preconditions.checkArgument(standardQuoteFactor >= 0f);
            Preconditions.checkArgument(standardQuoteFactor <= 1.0f);

            this.groupMemberRetriever = Objects.requireNonNull(groupMemberRetriever);
            this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
            this.priceTableFactory = Objects.requireNonNull(marketPriceTableFactory);
            this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
            this.wastedFilesAnalysisFactory = Objects.requireNonNull(wastedFilesAnalysisFactory);
            this.buyRIImpactAnalysisFactory = Objects.requireNonNull(buyRIImpactAnalysisFactory);
            this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
            this.clock = Objects.requireNonNull(clock);
            this.alleviatePressureQuoteFactor = alleviatePressureQuoteFactor;
            this.standardQuoteFactor = standardQuoteFactor;
            this.marketMode = MarketMode.fromString(marketModeName);
            this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
            this.cloudCostDataProvider = cloudCostDataProvider;
            this.suspensionsThrottlingConfig = suspensionThrottlingPerCluster ?
                    SuspensionsThrottlingConfig.CLUSTER : SuspensionsThrottlingConfig.DEFAULT;
            this.tierExcluderFactory = tierExcluderFactory;
            this.listener = listener;
            this.consistentScalingHelperFactory = consistentScalingHelperFactory;
            this.reversibilitySettingFetcherFactory = reversibilitySettingFetcherFactory;
            this.migratedWorkloadCloudCommitmentAnalysisService = migratedWorkloadCloudCommitmentAnalysisService;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public Analysis newAnalysis(@Nonnull final TopologyInfo topologyInfo,
                                    @Nonnull final Set<TopologyEntityDTO> topologyEntities,
                                    @Nonnull final AnalysisConfigCustomizer configCustomizer,
                                    @Nonnull final InitialPlacementFinder initialPlacementFinder) {
            final Map<String, Setting> globalSettings = retrieveSettings();
            final float quoteFactor = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo) ?
                    alleviatePressureQuoteFactor : standardQuoteFactor;
            final AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilderWithSMA(marketMode, quoteFactor,
                liveMarketMoveCostFactor, this.suspensionsThrottlingConfig, globalSettings);
            configCustomizer.customize(configBuilder);
            return new Analysis(topologyInfo, topologyEntities,
                groupMemberRetriever, clock,
                configBuilder.build(), cloudTopologyFactory,
                topologyCostCalculatorFactory, priceTableFactory, wastedFilesAnalysisFactory,
                buyRIImpactAnalysisFactory, tierExcluderFactory, listener, consistentScalingHelperFactory,
                initialPlacementFinder, reversibilitySettingFetcherFactory, migratedWorkloadCloudCommitmentAnalysisService);
        }

        /**
         * Retrieve global settings used for analysis configuration.
         *
         * @return The map of setting values, arranged by name.
         */
        private Map<String, Setting> retrieveSettings() {

            final Map<String, Setting> settingsMap = new HashMap<>();

            // for now only interested in DisableAllActions and AllowUnlimitedHostOverprovisioning
            // global settings.
            ImmutableList<String> inputSettings = ImmutableList.of(
                    GlobalSettingSpecs.DisableAllActions.getSettingName(),
                    GlobalSettingSpecs.AllowUnlimitedHostOverprovisioning.getSettingName());
            final GetMultipleGlobalSettingsRequest settingRequest =
                GetMultipleGlobalSettingsRequest.newBuilder()
                    .addAllSettingSpecName(inputSettings)
                    .build();

            try {
                settingServiceClient.getMultipleGlobalSettings(settingRequest)
                    .forEachRemaining(setting -> settingsMap.put(setting.getSettingSpecName(), setting));

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

        // Determines if the market or the SMA  (Stable Marriage Algorithm) library generates compute scaling action for cloud vms
        private final MarketMode marketMode;

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
         * Whether quotes should be cached for reuse during SNM-enabled placement analysis.
         *
         * <p>Setting to true can improve performance in some cases. Usually those cases involve a
         * high number of biclique overlaps and volumes per VM.</p>
         */
        private final boolean useQuoteCacheDuringSNM;

        /**
         * Whether provision and activate actions should be replayed during real-time analysis.
         *
         * <p>There exists support to save provision and activate actions from one analysis cycle
         * and apply them to the next. This application is done during the 2nd analysis sub-cycle
         * and significantly reduces the time spent in the provision algorithm.</p>
         */
        private final boolean replayProvisionsForRealTime;

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
         * The maximum ratio of the on-demand cost of new template to current template that is
         * allowed for analysis engine to recommend resize up to utilize a RI. If we are sizing up
         * to use a RI, we only allow resize up to templates  that have cost less than :
         * (riCostFactor * cost at current supplier). If it is more, we prevent
         * such a resize to avoid cases like forcing small VMs to use large unused RIs.
         */
        private final float discountedComputeCostFactor;

        /**
         * Use {@link AnalysisConfig#newBuilder(float, float, SuspensionsThrottlingConfig, Map)}.
         *
         * @param marketMode              the run mode of the market?
         * @param useQuoteCacheDuringSNM Whether quotes should be cached for reuse during
         *                               SNM-enabled placement analysis.
         * @param replayProvisionsForRealTime Whether provision and activate actions should be
         *                                    replayed during real-time analysis.
         * @param rightsizeLowerWatermark the minimum utilization threshold, if entity utilization is below
         *                                it, Market could generate resize down actions.
         * @param rightsizeUpperWatermark the maximum utilization threshold, if entity utilization is above
         *                                it, Market could generate resize up actions.
         * @param discountedComputeCostFactor The maximum ratio of the on-demand cost of new
         *                                    template to current template that is allowed for
         *                                    analysis engine to recommend resize up to utilize a RI.
         */
        private AnalysisConfig(final MarketMode marketMode,
                              final float quoteFactor,
                              final float liveMarketMoveCostFactor,
                              final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                              final Map<String, Setting> globalSettingsMap,
                              final boolean includeVDC,
                              final Optional<Integer> maxPlacementsOverride,
                              final boolean useQuoteCacheDuringSNM,
                              final boolean replayProvisionsForRealTime,
                              final float rightsizeLowerWatermark,
                              final float rightsizeUpperWatermark,
                              final float discountedComputeCostFactor) {
            this.quoteFactor = quoteFactor;
            this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
            this.suspensionsThrottlingConfig = suspensionsThrottlingConfig;
            this.globalSettingsMap = globalSettingsMap;
            this.includeVDC = includeVDC;
            this.maxPlacementsOverride = maxPlacementsOverride;
            this.useQuoteCacheDuringSNM = useQuoteCacheDuringSNM;
            this.replayProvisionsForRealTime = replayProvisionsForRealTime;
            this.rightsizeLowerWatermark = rightsizeLowerWatermark;
            this.rightsizeUpperWatermark = rightsizeUpperWatermark;
            this.marketMode = marketMode;
            this.discountedComputeCostFactor = discountedComputeCostFactor;
        }

        public float getQuoteFactor() {
            return quoteFactor;
        }

        public boolean isEnableSMA() {
            return marketMode == MarketMode.SMAOnly || marketMode == MarketMode.M2withSMAActions;
        }

        public boolean isSMAOnly() {
            return marketMode == MarketMode.SMAOnly;
        }

        public boolean isM2withSMAActions() {
            return marketMode == MarketMode.M2withSMAActions;
        }

        public MarketMode getMarketMode() {
            return marketMode;
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

        /**
         * Returns whether quotes should be cached for reuse during SNM-enabled placement
         * analysis.
         *
         * <p>Setting to true can improve performance in some cases. Usually those cases involve
         * a high number of biclique overlaps and volumes per VM.</p>
         *
         * @see Builder#setUseQuoteCacheDuringSNM(boolean)
         */
        public boolean getUseQuoteCacheDuringSNM() {
            return useQuoteCacheDuringSNM;
        }

        /**
         * Whether provision and activate actions should be replayed during real-time analysis.
         *
         * <p>There exists support to save provision and activate actions from one analysis cycle
         * and apply them to the next. This application is done during the 2nd analysis sub-cycle
         * and significantly reduces the time spent in the provision algorithm.</p>
         *
         * @see Builder#setReplayProvisionsForRealTime(boolean)
         */
        public boolean getReplayProvisionsForRealTime() {
            return replayProvisionsForRealTime;
        }

        public float getRightsizeLowerWatermark() {
            return rightsizeLowerWatermark;
        }

        public float getRightsizeUpperWatermark() {
            return rightsizeUpperWatermark;
        }

        /**
         * Returns the maximum ratio of the on-demand cost of new template to current template
         * that is allowed for analysis engine to recommend resize up to utilize a RI. If we are
         * sizing up to use a RI, we only allow resize up to templates  that have cost less than :
         * (riCostFactor * cost at current supplier). If it is more, we prevent
         * such a resize to avoid cases like forcing small VMs to use large unused RIs.
         * If negative, it means that this factor is not set and this functionality is disabled.
         *
         * @return the maximum ratio of the on-demand cost of new template to current
         * template that is allowed for analysis engine to recommend resize up to utilize a RI.
         */
        public float getDiscountedComputeCostFactor() {
            return discountedComputeCostFactor;
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
            return newBuilderWithSMA(MarketMode.M2Only, quoteFactor, liveMarketMoveCostFactor,
                suspensionsThrottlingConfig, globalSettings);
        }

        /**
         * Create a builder for the {@link AnalysisConfig}. Arguments into this function represent
         * non-customizable aspects of the builder.
         * Note: If the list of non-customizable aspects gets too long we can have an interface
         * that hides the non-customizable properties, and have {@link AnalysisConfigCustomizer}
         * accept that interface instead of the builder class.
         *
         * @param marketMode if true SMA (Stable Marriage Algorithm) library generates compute scaling action for cloud vms. otherwise market generrates them.
         * @param quoteFactor See {@link AnalysisConfig#quoteFactor}
         * @param liveMarketMoveCostFactor See {@link AnalysisConfig#liveMarketMoveCostFactor}
         * @param suspensionsThrottlingConfig See {@link AnalysisConfig#suspensionsThrottlingConfig}.
         * @param globalSettings See {@link AnalysisConfig#globalSettingsMap}
         * @return The builder, which can be further customized.
         */
        public static Builder newBuilderWithSMA(final MarketMode marketMode, final float quoteFactor, final float liveMarketMoveCostFactor,
                                                @Nonnull final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                                                @Nonnull final Map<String, Setting> globalSettings) {
            return new Builder(marketMode, quoteFactor, liveMarketMoveCostFactor,
                    suspensionsThrottlingConfig, globalSettings);
        }

        public static class Builder {
            private final float quoteFactor;

            private final MarketMode marketMode;

            private final float liveMarketMoveCostFactor;

            private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;

            private final Map<String, Setting> globalSettings;

            private boolean includeVDC = false;

            private Optional<Integer> maxPlacementsOverride = Optional.empty();

            private boolean useQuoteCacheDuringSNM = false;

            private boolean replayProvisionsForRealTime = false;

            private float rightsizeLowerWatermark;

            private float rightsizeUpperWatermark;

            private float discountedComputeCostFactor;

            private Builder(final MarketMode marketMode,
                            final float quoteFactor,
                            final float liveMarketMoveCostFactor,
                            final SuspensionsThrottlingConfig suspensionsThrottlingConfig,
                            @Nonnull final Map<String, Setting> globalSettings) {
                this.quoteFactor = quoteFactor;
                this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
                this.suspensionsThrottlingConfig = suspensionsThrottlingConfig;
                this.globalSettings = globalSettings;
                this.marketMode = marketMode;
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
             * Sets the value of the <b>use quote cache during SNM</b> field.
             *
             * <p>Has no observable side-effects except setting the above field.</p>
             *
             * @param useQuoteCacheDuringSNM the new value for the field.
             * @return {@code this}
             *
             * @see AnalysisConfig#getUseQuoteCacheDuringSNM()
             */
            @NonNull
            public Builder setUseQuoteCacheDuringSNM(final boolean useQuoteCacheDuringSNM) {
                this.useQuoteCacheDuringSNM = useQuoteCacheDuringSNM;
                return this;
            }

            /**
             * Sets the value of the <b>replay provisions during real-time</b> field.
             *
             * <p>Has no observable side-effects except setting the above field.</p>
             *
             * @param replayProvisionsForRealTime the new value for the field.
             * @return {@code this}
             *
             * @see AnalysisConfig#getReplayProvisionsForRealTime()
             */
            @NonNull
            public Builder setReplayProvisionsForRealTime(boolean replayProvisionsForRealTime) {
                this.replayProvisionsForRealTime = replayProvisionsForRealTime;
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


            /**
             * Returns the maximum ratio of the on-demand cost of new template to current template
             * that is allowed for analysis engine to recommend resize up to utilize a RI. If we are
             * sizing up to use a RI, we only allow resize up to templates  that have cost less than :
             * (riCostFactor * cost at current supplier). If it is more, we prevent
             * such a resize to avoid cases like forcing small VMs to use large unused RIs.
             * If negative, it means that this factor is not set and this functionality is disabled.
             *
             * @param discountedComputeCostFactor the maximum ratio of the on-demand cost of new
             *                                    template to current template that is allowed for
             *                                    analysis engine to recommend resize up to utilize
             *                                    a RI.
             * @return this Builder to support flow style.
             */
            @Nonnull
            public Builder setDiscountedComputeCostFactor(final float discountedComputeCostFactor) {
                this.discountedComputeCostFactor = discountedComputeCostFactor;
                return this;
            }

            @Nonnull
            public AnalysisConfig build() {
                return new AnalysisConfig(marketMode, quoteFactor, liveMarketMoveCostFactor,
                    suspensionsThrottlingConfig, globalSettings, includeVDC, maxPlacementsOverride,
                    useQuoteCacheDuringSNM, replayProvisionsForRealTime, rightsizeLowerWatermark,
                    rightsizeUpperWatermark, discountedComputeCostFactor);
            }
        }
    }
}
