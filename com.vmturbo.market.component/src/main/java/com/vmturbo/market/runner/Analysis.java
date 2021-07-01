package com.vmturbo.market.runner;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.PlacementProblem;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.cloudscaling.sma.analysis.StableMarriageAlgorithm;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.diagnostics.ActionLogger;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory.DefaultAnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisMode;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis.BuyCommitmentImpactResult;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.market.runner.postprocessor.ProjectedContainerClusterPostProcessor;
import com.vmturbo.market.runner.postprocessor.ProjectedContainerSpecPostProcessor;
import com.vmturbo.market.runner.postprocessor.ProjectedEntityPostProcessor;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityAnalysisEngine;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityResults;
import com.vmturbo.market.runner.wastedfiles.WastedFilesAnalysisEngine;
import com.vmturbo.market.runner.wastedfiles.WastedFilesResults;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcher;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.SMAConverter;
import com.vmturbo.market.topology.conversions.ShoppingListInfo;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.JournalActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.JournalActionSavingsCalculatorFactory;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Analysis execution and properties. This can be for a scoped plan or for a real-time market.
 */
public class Analysis {

    private static final DataMetricSummary TOPOLOGY_CONVERT_TO_TRADER_SUMMARY = DataMetricSummary.builder()
            .withName("mkt_economy_convert_to_traders_duration_seconds")
            .withHelp("Time to convert from TopologyDTO to TraderTO before sending for analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    private static final DataMetricSummary TOPOLOGY_CONVERT_FROM_TRADER_SUMMARY = DataMetricSummary.builder()
            .withName("mkt_economy_convert_from_traders_duration_seconds")
            .withHelp("Time to convert from TraderTO back to TopologyDTO for projected topology after analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    private static final DataMetricSummary BUY_RI_IMPACT_ANALYSIS_SUMMARY = DataMetricSummary.builder()
            .withName("mkt_buy_ri_impact_analysis_duration_seconds")
            .withHelp("Time for buy RI impact analysis to run. This includes the execution of the RI coverage allocator.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 60) // 60 mins.
            .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
            .build()
            .register();

    private final Logger logger = LogManager.getLogger();

    // Analysis started (kept true also when it is completed).
    private AtomicBoolean started = new AtomicBoolean();

    // Analysis completed (successfully or not).  Default is false.
    private boolean completed = false;

    private Instant startTime = Instant.EPOCH;

    private Instant completionTime = Instant.EPOCH;

    private final Map<Long, TopologyEntityDTO> topologyDTOs;

    private final String logPrefix;

    private TopologyConverter converter;

    private SMAConverter smaConverter;

    private Map<Long, ProjectedTopologyEntity> projectedEntities = null;

    private Map<Long, CostJournal<TopologyEntityDTO>> projectedEntityCosts = null;

    private final long projectedTopologyId;

    private ActionPlan actionPlan = null;

    private String errorMsg;

    private AnalysisState state;

    private @NonNull ReplayActions realtimeReplayActions = new ReplayActions();

    private final TopologyInfo topologyInfo;

    private boolean stopAnalysis = false;

    private final SetOnce<Economy> economy = new SetOnce<>();

    /**
     * The clock used to time market analysis.
     */
    private final Clock clock;

    private final GroupMemberRetriever groupMemberRetriever;

    private final AnalysisConfig config;

    private final CloudTopology<TopologyEntityDTO> originalCloudTopology;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final TierExcluderFactory tierExcluderFactory;

    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

    private final MarketPriceTableFactory marketPriceTableFactory;

    private final WastedFilesAnalysisEngine wastedFilesAnalysisEngine;

    private final ReservedCapacityAnalysisEngine reservedCapacityAnalysisEngine
            = new ReservedCapacityAnalysisEngine();

    private final ConsistentScalingHelperFactory consistentScalingHelperFactory;

    private final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory;

    private final AnalysisRICoverageListener listener;

    private final InitialPlacementFinder initialPlacementFinder;

    private final ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory;

    private final JournalActionSavingsCalculatorFactory actionSavingsCalculatorFactory;

    /**
     * The service that will perform cloud commitment (RI) buy analysis during a migrate to cloud plan.
     */
    private final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService;

    private final CommodityIdUpdater commodityIdUpdater;

    // a set of on-prem application entity type
    private static final Set<Integer> entityTypesToSkip =
            new HashSet<>(Collections.singletonList(EntityType.BUSINESS_APPLICATION_VALUE));

    // List of post processors to update corresponding projected topology entities after analysis.
    private static final List<ProjectedEntityPostProcessor> PROJECTED_ENTITY_POST_PROCESSORS =
            Arrays.asList(new ProjectedContainerSpecPostProcessor(),
                          new ProjectedContainerClusterPostProcessor());

    /**
     * Create and execute a context for a Market Analysis given a topology, an optional 'scope' to
     * apply, and a flag determining whether guaranteed buyers (VDC, VPod, DPod) are included
     * in the market analysis or not.
     *
     * @param topologyInfo descriptive info about the topology - id, type, etc
     * @param topologyDTOs the Set of {@link TopologyEntityDTO}s that make up the topology
     * @param groupMemberRetriever used to look up group and member information
     * @param clock The clock used to time market analysis.
     * @param analysisConfig configuration for analysis
     * @param cloudTopologyFactory cloud topology factory
     * @param cloudCostCalculatorFactory cost calculation factory
     * @param priceTableFactory price table factory
     * @param wastedFilesAnalysisEngine wasted file analysis handler
     * @param buyRIImpactAnalysisFactory  buy RI impact analysis factory
     * @param tierExcluderFactory the tier excluder factory
     * @param listener that receives entity ri coverage information availability.
     * @param consistentScalingHelperFactory CSM helper factory
     * @param initialPlacementFinder the class to perform fast reservation
     * @param reversibilitySettingFetcherFactory factory for {@link ReversibilitySettingFetcher}.
     * @param migratedWorkloadCloudCommitmentAnalysisService cloud migration analysis
     * @param commodityIdUpdater commodity id updater
     */
    public Analysis(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final Collection<TopologyEntityDTO> topologyDTOs,
                    @Nonnull final GroupMemberRetriever groupMemberRetriever,
                    @Nonnull final Clock clock,
                    @Nonnull final AnalysisConfig analysisConfig,
                    @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                    @Nonnull final TopologyCostCalculatorFactory cloudCostCalculatorFactory,
                    @Nonnull final MarketPriceTableFactory priceTableFactory,
                    @Nonnull final WastedFilesAnalysisEngine wastedFilesAnalysisEngine,
                    @Nonnull final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory,
                    @Nonnull final TierExcluderFactory tierExcluderFactory,
                    @Nonnull final AnalysisRICoverageListener listener,
                    @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory,
                    @Nonnull final InitialPlacementFinder initialPlacementFinder,
                    @Nonnull final ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory,
                    @NonNull final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService,
                    @Nonnull final CommodityIdUpdater commodityIdUpdater,
                    @Nonnull final JournalActionSavingsCalculatorFactory actionSavingsCalculatorFactory) {
        this.topologyInfo = topologyInfo;
        this.topologyDTOs = topologyDTOs.stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        this.state = AnalysisState.INITIAL;
        logPrefix = topologyInfo.getTopologyType() + " Analysis " +
            topologyInfo.getTopologyContextId() + " with topology " +
            topologyInfo.getTopologyId() + " : ";
        this.projectedTopologyId = IdentityGenerator.next();
        this.groupMemberRetriever = groupMemberRetriever;
        this.clock = Objects.requireNonNull(clock);
        this.config = analysisConfig;
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.originalCloudTopology = this.cloudTopologyFactory.newCloudTopology(
                topologyDTOs.stream());
        this.wastedFilesAnalysisEngine = wastedFilesAnalysisEngine;
        this.buyRIImpactAnalysisFactory = buyRIImpactAnalysisFactory;
        this.topologyCostCalculatorFactory = cloudCostCalculatorFactory;
        this.marketPriceTableFactory = priceTableFactory;
        this.tierExcluderFactory = tierExcluderFactory;
        this.listener = listener;
        this.consistentScalingHelperFactory = consistentScalingHelperFactory;
        this.initialPlacementFinder = initialPlacementFinder;
        this.reversibilitySettingFetcherFactory = reversibilitySettingFetcherFactory;
        this.migratedWorkloadCloudCommitmentAnalysisService = migratedWorkloadCloudCommitmentAnalysisService;
        this.commodityIdUpdater = commodityIdUpdater;
        this.actionSavingsCalculatorFactory = Objects.requireNonNull(actionSavingsCalculatorFactory);
    }

    private static final DataMetricSummary RESULT_PROCESSING = DataMetricSummary.builder()
            .withName("mkt_process_result_duration_seconds")
            .withHelp("Time to process the analysis results.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 20) // 20 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A wrapper class around a {@link Topology} created from the {@link TopologyEntityDTO}s
     * associated with this analysis.
     */
    private static class ConvertedTopology {

        static final ConvertedTopology EMPTY = new ConvertedTopology(new Topology(),
                Collections.emptySet(), Collections.emptyMap());

        private final Topology topology;
        private final Set<Long> oidsToRemove;
        private final Map<Long, TopologyEntityDTO> fakeEntityDTOs;

        private ConvertedTopology(Topology topology,
                Set<Long> oidsToRemove,
                Map<Long, TopologyEntityDTO> fakeEntityDTOs) {
            this.topology = topology;
            this.oidsToRemove = oidsToRemove;
            this.fakeEntityDTOs = fakeEntityDTOs;
        }
    }

    /**
     * Creates a {@link ConvertedTopology} containing a {@link Topology} from the
     * {@link TopologyEntityDTO}s provided to the analysis constructor.
     *
     * @param enableThrottling If throttling should be enabled.
     * @return The {@link ConvertedTopology} containing the {@link Topology}, as well as some
     *         metadata.
     */
    @Nonnull
    private ConvertedTopology createM2Topology(final boolean enableThrottling) {
        final DataMetricTimer conversionTimer = TOPOLOGY_CONVERT_TO_TRADER_SUMMARY.startTimer();
        // remove any (scoped) traders that may have been flagged for removal
        // We are doing this after the convertToMarket() function because we need the original
        // traders available in the "old providers maps" so the biclique calculation can
        // preserve the original topology structure. We may refactor this in a way that lets us
        // remove the old provider map though -- see OM-26631.
        //
        // Note that although we do NOT attempt to unplace buyers of the entities being removed
        // here, the effect on the analysis is exactly equivalent if we had unplaced them
        // (as of 4/6/2016) because attempting to buy from a non-existing trader results in
        // an infinite quote which is exactly the same as not having a provider.
        final Set<Long> oidsToRemove = new HashSet<>();
        final Long2ObjectMap<TopologyEntityDTO> fakeEntityDTOs = new Long2ObjectOpenHashMap<>();
        for (TopologyEntityDTO dto : topologyDTOs.values()) {
            if (dto.hasEdit()) {
                if (dto.getEdit().hasRemoved() || dto.getEdit().hasReplaced()) {
                    oidsToRemove.add(dto.getOid());
                }
            }
        }
        final Long2ObjectMap<TraderTO> traderTOs = new Long2ObjectOpenHashMap<>();
        final Set<TraderTO> fakeTraderTOs = new HashSet<>();
        try (TracingScope scope = Tracing.trace("convert_to_market")) {

            // construct fake entities which can be used to form markets based on cluster/storage cluster
            if (enableThrottling) {
                createFakeTopologyEntityDTOsForSuspensionThrottling().forEach((id, e) -> {
                    fakeEntityDTOs.put(e.getOid(), e);
                    topologyDTOs.put(id, e);
                });
            }

            if (!stopAnalysis) {
                converter.convertToMarket(topologyDTOs, oidsToRemove).forEach(t -> {
                    // cache the traderTOs converted from fake TopologyEntityDTOs
                    if (enableThrottling && fakeEntityDTOs.containsKey(t.getOid())) {
                        fakeTraderTOs.add(t);
                    }
                    traderTOs.put(t.getOid(), t);
                });
            }
        }
        conversionTimer.observe();

        // add back fake traderTOs for suspension throttling as it may be removed due
        // to scoping
        fakeTraderTOs.forEach(t -> traderTOs.put(t.getOid(), t));

        // remove skipped entities we don't want to send to market
        converter.removeSkippedEntitiesFromTraderTOs(traderTOs);

        if (oidsToRemove.size() > 0) {
            logger.debug("Removing {} traders before analysis: ", oidsToRemove.size());
            oidsToRemove.forEach(traderTOs::remove);
        }

        logger.debug("{} Economy DTOs", traderTOs.size());
        if (logger.isTraceEnabled()) {
            traderTOs.values().stream().map(dto -> "Economy DTO: " + dto).forEach(logger::trace);
        }

        if (!stopAnalysis) {
            List<CommoditySpecification> commsToAdjustOverheadInClone =
                    createCommsToAdjustOverheadInClone();
            saveAnalysisDiags(traderTOs.values(), commsToAdjustOverheadInClone);
            Topology topology = TopologyEntitiesHandler.createTopology(traderTOs.values(),
                    topologyInfo, commsToAdjustOverheadInClone, config);
            if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                // whenever market receives entities from realtime broadcast, we update
                // cachedEconomy and also pass the commodity type to specification map associated
                // with that economy to initialPlacementFinder.
                updateReservationEconomyCache(topology.getEconomy(), true);
            }
            return new ConvertedTopology(topology, oidsToRemove, fakeEntityDTOs);
        } else {
            return ConvertedTopology.EMPTY;
        }
    }

    /**
     * Execute the analysis run. Generate the action plan, projected topology and price index message.
         * Only the first invocation of this method will actually run the analysis. Subsequent calls will
         * log an error message and immediately return {@code false}.
         *
         * @return true if this is the first invocation of this method, false otherwise.
     */
    public boolean execute() {

        if (started.getAndSet(true) || stopAnalysis) {
            logger.error(" {} Completed or Stopped or being computed", logPrefix);
            return false;
        }
        final List<AnalysisType> analysisTypeList = topologyInfo.getAnalysisTypeList();
        final boolean isBuyRIImpactAnalysis = analysisTypeList.contains(AnalysisType.BUY_RI_IMPACT_ANALYSIS);
        final boolean isMigrateToCloud = (topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo().getPlanType()
                .equals(StringConstants.CLOUD_MIGRATION_PLAN));
        final boolean isM2AnalysisEnabled = analysisTypeList.contains(AnalysisType.MARKET_ANALYSIS);
        //SMA is not supported for Migrate to Cloud plan
        final boolean isSMAEnabled = config.isEnableSMA() && !isMigrateToCloud;
        final boolean enableThrottling = topologyInfo.getTopologyType() == TopologyType.REALTIME
                && (config.getSuspensionsThrottlingConfig() == SuspensionsThrottlingConfig.CLUSTER);
        final TopologyCostCalculator topologyCostCalculator = topologyCostCalculatorFactory
                .newCalculator(topologyInfo, originalCloudTopology);
        // Use the cloud cost data we use for cost calculations for the price table.
        final CloudRateExtractor marketCloudRateExtractor = marketPriceTableFactory.newPriceTable(
                this.originalCloudTopology, topologyCostCalculator.getCloudCostData());
        final ReversibilitySettingFetcher reversibilitySettingFetcher
                = reversibilitySettingFetcherFactory.newReversibilitySettingRetriever();
        this.converter = new TopologyConverter(topologyInfo, marketCloudRateExtractor,
                null, topologyCostCalculator.getCloudCostData(),
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                originalCloudTopology, reversibilitySettingFetcher, config);
        this.smaConverter = new SMAConverter(converter);
        DataMetricTimer processResultTime = null;
        AnalysisResults results = null;
        Map<Long, List<UnplacementReason.Builder>> unplacedReasonMap = new HashMap<>();
        Map<Long, TraderTO> projTradersMap = null;

        // Don't generate actions associated with entities with these oids
        final Set<Long> suppressActionsForOids = new HashSet<>();

        // Execute wasted file analysis
        final WastedFilesResults wastedFilesAnalysis;
        if (topologyInfo.getAnalysisTypeList().contains(AnalysisType.WASTED_FILES)) {
            wastedFilesAnalysis = wastedFilesAnalysisEngine.analyzeWastedFiles(
                    topologyInfo, topologyDTOs, topologyCostCalculator, originalCloudTopology);
        } else {
            wastedFilesAnalysis = WastedFilesResults.EMPTY;
        }

        // Calculate reservedCapacity and generate resize actions
        // Do not generate reservations for cloud migration plans.
        final ReservedCapacityResults reservedCapacityResults;
        if (!isMigrateToCloud) {
            reservedCapacityResults = reservedCapacityAnalysisEngine.execute(topologyDTOs, converter.getConsistentScalingHelper());
        } else {
            reservedCapacityResults = ReservedCapacityResults.EMPTY;
        }

        ConvertedTopology convertedTopology = ConvertedTopology.EMPTY;
        if (isM2AnalysisEnabled) {
            if (topologyInfo.getTopologyType() == TopologyType.REALTIME
                    && !originalCloudTopology.getEntities().isEmpty()) {
                try {
                    // Set the cloud cost notification status. If the cloud cost notification status fails,
                    // TC will set movable to false for cloud entities
                    converter.setCostNotificationStatus(listener, this);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error(
                            String.format("Error while receiving cost notification for analysis %s",
                                    getTopologyInfo()), e);
                    return false;
                }
            }
            state = AnalysisState.IN_PROGRESS;
            startTime = clock.instant();
            logger.info("{} Started", logPrefix);

            try {
                convertedTopology = createM2Topology(enableThrottling);

                processResultTime = RESULT_PROCESSING.startTimer();

                if (!stopAnalysis) {
                    if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                        commodityIdUpdater.updateReplayActions(
                            this, converter.getCommodityConverter().getCommTypeAllocator());
                    }
                    Ede ede = new Ede();
                    results = TopologyEntitiesHandler.performAnalysis(topologyInfo,
                            config, this, convertedTopology.topology, ede);
                    // Update historical economy cache used for reservation every night. Calling the
                    // update method here will make sure the clusters in head room plan are balanced,
                    // because analysis already completed at this point.
                    if (TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo)) {
                        final UnmodifiableEconomy eco = convertedTopology.topology.getEconomy();
                        updateReservationEconomyCache(eco, false);
                    }
                    if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                        commodityIdUpdater.saveCommodityIdToCommodityType(
                            converter.getCommodityConverter().getCommTypeAllocator(), getReplayActions());
                    }
                    try {
                        Map<Trader, List<InfiniteQuoteExplanation>> infiniteQuoteTraderMap = ede
                                .getPlacementResults().getExplanations();
                        unplacedReasonMap = generateUnplacedReason(infiniteQuoteTraderMap, converter, convertedTopology.topology);
                        if (isSMAEnabled) {
                            // Cloud VM OID to a set of provider OIDs: i.e. compute tier OIDs that the VM can fit in.
                            Map<Long, Set<Long>> cloudVmOidToProvidersOIDsMap = new HashMap<>();
                            // Cloud VM OID to traderTO
                            Map<Long, Set<Long>> cloudVmOidToTraderTOs =
                                    TopologyEntitiesHandler
                                            .getProviderLists(converter.getCloudVmComputeShoppingListIDs(),
                                                    convertedTopology.topology, getCommSpecForCommodity(TopologyDTO
                                                            .CommodityType.newBuilder()
                                                            .setType(CommodityDTO.CommodityType
                                                                    .COUPON_VALUE).build()).getBaseType());
                            for (Entry<Long, Set<Long>> entry : cloudVmOidToTraderTOs.entrySet()) {
                                Set<Long> providerOIDList = new HashSet<>();
                                for (Long traderTO : entry.getValue()) {
                                    Optional<Long> computeTierID = converter.getTopologyEntityOIDForOnDemandMarketTier(traderTO);
                                    computeTierID.ifPresent(providerOIDList::add);
                                }
                                cloudVmOidToProvidersOIDsMap.put(entry.getKey(), providerOIDList);
                            }
                            boolean isOptimizeCloudPlan = (topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo().getPlanType()
                                    .equals(StringConstants.OPTIMIZE_CLOUD_PLAN));
                            boolean reduceDependency = config.isSMALite();
                            SMAInput smaInput = new SMAInput(originalCloudTopology,
                                    cloudVmOidToProvidersOIDsMap,
                                    topologyCostCalculator.getCloudCostData(), marketCloudRateExtractor, converter.getConsistentScalingHelper(), isOptimizeCloudPlan, reduceDependency);
                            saveSMADiags(smaInput);
                            smaConverter.setSmaOutput(StableMarriageAlgorithm.execute(smaInput));
                        }
                        // add shoppinglist from newly provisioned trader to shoppingListOidToInfos
                        converter.updateShoppingListMap(results.getNewShoppingListToBuyerEntryList());
                        logger.info(logPrefix + "Done performing analysis");
                    } catch (Exception e) {
                        logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                            "Analysis execute", e.getMessage(), e);
                    }
                } else {
                    logger.info(logPrefix + " Analysis Stopped");
                    state = AnalysisState.FAILED;
                }
            } catch (RuntimeException e) {
                logger.error(logPrefix + e + " Runtime exception while running M2 analysis", e);
                state = AnalysisState.FAILED;
                completionTime = clock.instant();
                errorMsg = e.toString();
                logger.info(logPrefix + "Execution time : "
                        + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
                logger.info(logPrefix + "Execution time : "
                        + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
                completed = false;
                return false;
            }
        }

        try {
            CloudTopology<TopologyEntityDTO> projectedCloudTopology = null;
            if (!stopAnalysis) {
                if (!isM2AnalysisEnabled && isBuyRIImpactAnalysis) {
                    processResultTime = RESULT_PROCESSING.startTimer();
                }

                List<TraderTO> projectedTraderDTO = new ArrayList<>();
                List<ActionTO> actionsList = new ArrayList<>();
                if (results != null) {
                    actionsList.addAll(results.getActionsList());
                }
                final List<Action.Builder> buyRIAllocateActions = new ArrayList<>();
                try {
                    try (DataMetricTimer convertFromTimer = TOPOLOGY_CONVERT_FROM_TRADER_SUMMARY.startTimer()) {
                        try (TracingScope tracingScope = Tracing.trace("convert_from_traders")) {
                            if (isM2AnalysisEnabled) { // Includes the case of both Market and BuyRiImpactAnalysis running in real-time
                                if (enableThrottling) {
                                    // remove the fake entities used in suspension throttling
                                    // we need to remove it both from the projected topology and the source topology

                                    // source, non scoped topology
                                    for (long fakeEntityOid : convertedTopology.fakeEntityDTOs.keySet()) {
                                        topologyDTOs.remove(fakeEntityOid);
                                    }

                                    // projected topology
                                    for (TraderTO projectedDTO : results.getProjectedTopoEntityTOList()) {
                                        if (!convertedTopology.fakeEntityDTOs.containsKey(projectedDTO.getOid())) {
                                            projectedTraderDTO.add(projectedDTO);
                                        }
                                    }
                                } else {
                                    projectedTraderDTO = results.getProjectedTopoEntityTOList();
                                }
                                //SMA is not supported for Migrate to Cloud plan
                                if (config.isSMAOnly() && isSMAEnabled) {
                                    // update projectedTraderTO and generate actions.
                                    smaConverter.updateWithSMAOutput(projectedTraderDTO);
                                    projectedTraderDTO = smaConverter.getProjectedTraderDTOsWithSMA();
                                }

                                // Clear the Traders and Economy to reduce memory usage in the market component before
                                // creating the Projected TopologyEntityDTOs.
                                economy.getValue().ifPresent(e -> {
                                    e.getTraders().forEach(Trader::clearShoppingAndMarketData);
                                    final Topology topology = e.getTopology();
                                    if (topology != null) {
                                        topology.clear();
                                    }
                                });

                                // results can be null if M2Analysis is not run
                                final PriceIndexMessage priceIndexMessage = results != null ?
                                        results.getPriceIndexMsg() : PriceIndexMessage.getDefaultInstance();
                                    projectedEntities = converter.convertFromMarket(
                                        projectedTraderDTO,
                                        topologyDTOs,
                                        priceIndexMessage, reservedCapacityResults,
                                        wastedFilesAnalysis);

                                if (topologyInfo.hasPlanInfo()) {
                                    attachUnplacementReasons(unplacedReasonMap, projectedEntities);

                                    // Find all VMs that are uncontrollable and
                                    // add an unplacement reason for each of them.
                                    Map<Long, List<UnplacementReason.Builder>> uncontrollableReasonMap = getUnplacementReasonForUncontrollableVm();
                                    attachUnplacementReasons(uncontrollableReasonMap,
                                            projectedEntities);
                                    unplacedReasonMap.putAll(uncontrollableReasonMap);

                                    if (isMigrateToCloud) {
                                        // detach the original provider from the migrating entities in
                                        // projected topology only in MCP.
                                        unplaceProjectedEntityWithReason(projectedEntities, unplacedReasonMap);
                                        // Do not show actions for unplaced traders in migration plans
                                        suppressActionsForOids.addAll(unplacedReasonMap.keySet());
                                    }
                                }

                                final Set<Long> wastedStorageActionsVolumeIds = wastedFilesAnalysis.getActions().stream()
                                        .map(action -> action.getInfo().getDelete().getTarget().getId())
                                        .collect(Collectors.toSet());

                                copySkippedEntitiesToProjectedTopology(
                                        wastedStorageActionsVolumeIds,
                                        convertedTopology.oidsToRemove,
                                        projectedTraderDTO,
                                        topologyDTOs,
                                        isMigrateToCloud);

                                // Map of entity type to list of projected topology entities.
                                Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities =
                                        projectedEntities.values().stream()
                                                .collect(Collectors.groupingBy(entity ->
                                                        entity.getEntity().getEntityType()));
                                // Post process projected entities.
                                for (ProjectedEntityPostProcessor postProcessor : PROJECTED_ENTITY_POST_PROCESSORS) {
                                    if (postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities)) {
                                        try (TracingScope postProcessorScope = Tracing.trace(postProcessor.getClass().getSimpleName())) {
                                            postProcessor.process(topologyInfo, projectedEntities,
                                                entityTypeToProjectedEntities, actionsList);
                                        }
                                    }
                                }

                                // Calculate the projected entity costs.
                                projectedCloudTopology =
                                    cloudTopologyFactory.newCloudTopology(projectedEntities.values().stream()
                                        .filter(ProjectedTopologyEntity::hasEntity)
                                        .filter(entity -> !entity.getDeleted())
                                        .map(ProjectedTopologyEntity::getEntity));
                            } else if (isBuyRIImpactAnalysis) { // OCP Plan Option 3 only
                                final CloudCostData cloudCostData = topologyCostCalculator.getCloudCostData();
                                projectedEntities = converter.createProjectedEntitiesAsCopyOfOriginalEntities(topologyDTOs);

                                // Calculate the projected entity costs.
                                projectedCloudTopology =
                                    cloudTopologyFactory.newCloudTopology(projectedEntities.values().stream()
                                        .filter(ProjectedTopologyEntity::hasEntity)
                                        .filter(entity -> !entity.getDeleted())
                                        .map(ProjectedTopologyEntity::getEntity));
                                converter.getProjectedRICoverageCalculator()
                                    .addRICoverageToProjectedRICoverage(cloudCostData.getCurrentRiCoverage());
                            }

                            // If this is a migrate to cloud plan, send a request to the cost component to start cloud commitment
                            // analysis (Buy RI)
                            if (isMigrateToCloud) {
                                runMigratedWorkloadCloudCommitmentAnalysis(projectedCloudTopology, projectedEntities, projectedTraderDTO);
                            }

                            // Invoke buy RI impact analysis after projected entity creation, but prior to
                            // projected cost calculations
                            // PS:  OCP Plan Option#2 (Market Only) will not be processed within runBuyRIImpactAnalysis.
                            buyRIAllocateActions.addAll(runBuyRIImpactAnalysis(projectedCloudTopology,
                                    topologyCostCalculator.getCloudCostData()));

                            // Projected RI coverage has been calculated by convertFromMarket
                            // Get it from TopologyConverter and pass it along to use for calculation of
                            // savings
                            projectedEntityCosts = topologyCostCalculator.calculateCosts(projectedCloudTopology,
                                converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage());
                        }
                    }
                } catch (Exception e) {
                    logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                        "Analysis execute", e.getMessage(), e);
                }

                // Create the action plan
                logger.info(logPrefix + "Creating action plan");
                try (TracingScope tracingScope = Tracing.trace("create_action_plan")) {
                    final ActionPlan.Builder actionPlanBuilder = ActionPlan.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setInfo(ActionPlanInfo.newBuilder()
                            .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(topologyInfo)))
                        .setAnalysisStartTimestamp(startTime.toEpochMilli());

                    final CloudActionSavingsCalculator actionSavingsCalculator = actionSavingsCalculatorFactory.newCalculator(
                            topologyDTOs, originalCloudTopology, topologyCostCalculator,
                            projectedEntities, projectedEntityCosts,
                            converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage());

                    List<Action> actions = converter.interpretAllActions(actionsList, projectedEntities,
                         originalCloudTopology, actionSavingsCalculator);

                    actions.removeIf(action -> {
                        try {
                            return suppressActionsForOids.contains(ActionDTOUtil.getPrimaryEntityId(action));
                        } catch (UnsupportedActionException e) {
                            // If it's somehow not recognized, leave the action alone
                            return false;
                        }
                    });

                    // add Buy RI impact analysis actions
                    buyRIAllocateActions.stream()
                            .map(action -> actionSavingsCalculator.calculateSavings(action)
                                    .applyToActionBuilder(action))
                            .map(Action.Builder::build)
                            .forEach(actionPlanBuilder::addAction);

                    actions.forEach(actionPlanBuilder::addAction);
                    //SMA is not supported for Migrate to Cloud plan
                    if (config.isSMAOnly() && isSMAEnabled) {
                        actions = converter.interpretAllActions(smaConverter.getSmaActions(), projectedEntities,
                         originalCloudTopology, actionSavingsCalculator);
                            actions.forEach(actionPlanBuilder::addAction);
                    }

                    if (!isMigrateToCloud) {
                        writeActionsToLog(actions, config, originalCloudTopology,
                                projectedCloudTopology, converter, topologyCostCalculator.getCloudCostData());
                    }
                    // clear the state only after interpretAllActions is called for both M2 and SMA.
                    converter.clearStateNeededForActionInterpretation();
                    // TODO move wasted files action out of main analysis once we have a framework
                    // to support multiple analyses for the same topology ID
                    actionPlanBuilder.addAllAction(wastedFilesAnalysis.getActions());
                    actionPlanBuilder.addAllAction(reservedCapacityResults.getActions());
                    logger.info(logPrefix + "Completed successfully");
                    processResultTime.observe();
                    state = AnalysisState.SUCCEEDED;
                    completionTime = clock.instant();
                    actionPlan = actionPlanBuilder.setAnalysisCompleteTimestamp(completionTime.toEpochMilli())
                        .build();
                }
            } else {
                logger.info(logPrefix + " Analysis Stopped");
                processResultTime.observe();
                state = AnalysisState.FAILED;
            }
        } catch (RuntimeException e) {
            logger.error(logPrefix + e + " Runtime exception while running analysis", e);
            state = AnalysisState.FAILED;
            completionTime = clock.instant();
            errorMsg = e.toString();
            logger.info(logPrefix + "Execution time : "
                            + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
            logger.info(logPrefix + "Execution time : "
                            + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
            completed = false;
            return false;
        }

        logger.info(logPrefix + "Execution time : "
                + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
        completed = true;
        return true;
    }

    /**
     * Call {@link InitialPlacementFinder} to trigger refresh of reservation economy caches.
     *
     * @param economy the economy associated with analysis.
     * @param isRealtime whether the analysis is for a real time topology or not.
     */
    private void updateReservationEconomyCache(final @Nonnull UnmodifiableEconomy economy,
            final boolean isRealtime) {
        if (!initialPlacementFinder.shouldConstructEconomyCache()) {
            return;
        }
        if (converter.getCommodityConverter() != null
                && converter.getCommodityConverter().getCommTypeAllocator() != null) {
            Map<TopologyDTO.CommodityType, Integer> commTypeToSpecMap = converter
                    .getCommodityConverter().getCommTypeAllocator().getReservationCommTypeToSpecMapping();
            initialPlacementFinder.updateCachedEconomy(economy, commTypeToSpecMap, isRealtime);
        }
    }

    /**
     * Detach the suppliers from {@link CommoditiesBoughtFromProvider} if there is an unplacement
     * reason associated with it.
     * NOTE: currently it is only applied for migrate to cloud plan.
     *
     * @param projectedEntities A map of oid to {@link ProjectedTopologyEntity}s.
     * @param reasonMap A map of entity oid to a list of {@link UnplacementReason.Builder}s.
     */
    private void unplaceProjectedEntityWithReason(@Nonnull Map<Long, ProjectedTopologyEntity> projectedEntities,
            @Nonnull Map<Long, List<UnplacementReason.Builder>> reasonMap) {
        for (Map.Entry<Long, List<UnplacementReason.Builder>> entry: reasonMap.entrySet()) {
            ProjectedTopologyEntity projEntity = projectedEntities.get(entry.getKey());
            List<CommoditiesBoughtFromProvider> updatedCommBoughtProviders = new ArrayList<>();
            Set<Integer> providerTypeWithReason = entry.getValue().stream()
                    .filter(r -> r.hasProviderType()).map(r -> r.getProviderType())
                    .collect(Collectors.toSet());
            if (providerTypeWithReason.isEmpty() && !entry.getValue().isEmpty()) {
                // When the UnplacementReason does not have a provider type. It can be caused by
                // reconfigure actions. Try to unplace the entire projected entity.
                projEntity.getEntity().getCommoditiesBoughtFromProvidersList().forEach(c -> {
                    updatedCommBoughtProviders.add(c.toBuilder().clearProviderId().build());
                });
            } else {
                // Iterate each CommoditiesBoughtFromProvider, clear provider for whose provider
                // type are the same as the provider type within unplacementReason.This is a
                // requirement from PM, which particularly aims at VM with several volumes. When
                // at least one volume failed to find placement, unplace all volumes of the same VM.
                for (CommoditiesBoughtFromProvider commBoughtProvider : projEntity.getEntity()
                        .getCommoditiesBoughtFromProvidersList()) {
                    if (commBoughtProvider.hasProviderEntityType() && providerTypeWithReason
                            .contains(commBoughtProvider.getProviderEntityType())) {
                        updatedCommBoughtProviders.add(commBoughtProvider.toBuilder()
                                .clearProviderId().build());
                    } else { // Keep provider of those whose provider type is not the same.
                        updatedCommBoughtProviders.add(commBoughtProvider);
                    }
                }
            }
            TopologyEntityDTO entityDTO = projEntity.getEntity().toBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    .addAllCommoditiesBoughtFromProviders(updatedCommBoughtProviders)
                    .build();
            projectedEntities.put(projEntity.getEntity().getOid(), projEntity.toBuilder()
                    .setEntity(entityDTO).build());
        }
    }

    private void saveAnalysisDiags(final Collection<TraderTO> traderTOs,
                                   final List<CommoditySpecification> commSpecsToAdjustOverhead) {
        AnalysisDiagnosticsCollectorFactory factory = new DefaultAnalysisDiagnosticsCollectorFactory();
        factory.newDiagsCollector(topologyInfo.getTopologyContextId()
                + "-" + topologyInfo.getTopologyId(), AnalysisMode.M2).ifPresent(diagsCollector -> {
            diagsCollector.saveAnalysis(traderTOs, topologyInfo, config, commSpecsToAdjustOverhead);
        });
    }

    /**
     * Populate {@link UnplacementReason.Builder}s based on {@link InfiniteQuoteExplanation}s.
     *
     * @param infiniteQuoteTraderMap A map of trader to its list of {@link InfiniteQuoteExplanation}s.
     * @param converter The topology converter.
     * @param topology The topology associated with an economy.
     * @return a map of entity oid to a list of {@link UnplacementReason.Builder}s.
     */
    private Map<Long, List<UnplacementReason.Builder>> generateUnplacedReason(
            final Map<Trader, List<InfiniteQuoteExplanation>> infiniteQuoteTraderMap,
            final TopologyConverter converter, final Topology topology) {
        Map<Long, List<UnplacementReason.Builder>> unplacementReasonMap = new HashMap<>();
        for (Entry<Trader, List<InfiniteQuoteExplanation>> entry : infiniteQuoteTraderMap.entrySet()) {
            List<InfiniteQuoteExplanation> explanations = entry.getValue();
            List<UnplacementReason.Builder> reasonList = new ArrayList<>();
            for (InfiniteQuoteExplanation explanation : explanations) {
                UnplacementReason.Builder reason = UnplacementReason.newBuilder();
                if (explanation.costUnavailable) {
                    reason.setPlacementProblem(PlacementProblem.COSTS_NOT_FOUND);
                }
                if (explanation.providerType.isPresent()) {
                    reason.setProviderType(explanation.providerType.get());
                }
                if (explanation.seller.isPresent()) {
                    long sellerOid = explanation.seller.get().getOid();
                    // the seller could be a market tier object so we need to find the
                    // topologyEntityDto asssociated with that market tier.
                    MarketTier marketTier = converter.getCloudTc()
                            .getMarketTier(explanation.seller.get().getOid());
                    if (marketTier != null) {
                        sellerOid = marketTier.getTier().getOid();
                    }
                    reason.setClosestSeller(sellerOid);
                }
                Long infiniteSlOid = topology.getShoppingListOids().get(explanation.shoppingList);
                if (infiniteSlOid != null) {
                    ShoppingListInfo slInfo = converter.getShoppingListOidToInfos().get(infiniteSlOid);
                    if (slInfo != null && slInfo.getActingId() != null) {
                        reason.setResourceOwnerOid(slInfo.getActingId());
                    }
                }
                for (InfiniteQuoteExplanation.CommodityBundle bundle : explanation.commBundle) {
                    CommodityConverter commConverter = converter.getCommodityConverter();
                    Optional<TopologyDTO.CommodityType> commType = commConverter
                            .marketToTopologyCommodity(CommoditySpecificationTO.newBuilder()
                            .setBaseType(bundle.commSpec.getBaseType())
                            .setType(bundle.commSpec.getType())
                            .build());
                    if (commType.isPresent()) {
                        FailedResources.Builder failedResources = FailedResources.newBuilder()
                                .setCommType(commType.get())
                                .setRequestedAmount(bundle.requestedAmount);
                        if (bundle.maxAvailable.isPresent()) {
                            double maxAvailable = MarketAnalysisUtils.getMaxAvailableForUnplacementReason(
                                    commType.get(), bundle);
                            failedResources.setMaxAvailable(maxAvailable);
                        }
                        reason.addFailedResources(failedResources.build());
                    }
                }
                reasonList.add(reason);
            }
            unplacementReasonMap.put(entry.getKey().getOid(), reasonList);
        }
        return unplacementReasonMap;
    }

    /**
     * Populate {@link UnplacementReason} for the {@link ProjectedTopologyEntity}.
     *
     * @param unplacedReasonMap A map of entity oid to a list of {@link UnplacementReason.Builder}.
     * @param projectedEntities A map of entity oid to {@link ProjectedTopologyEntity}.
     */
    private void attachUnplacementReasons(Map<Long, List<UnplacementReason.Builder>> unplacedReasonMap,
            Map<Long, ProjectedTopologyEntity> projectedEntities) {
        for (Map.Entry<Long, List<UnplacementReason.Builder>> e : unplacedReasonMap.entrySet()) {
            ProjectedTopologyEntity projEntity = projectedEntities.get(e.getKey());
            if (projEntity != null) {
                ProjectedTopologyEntity.Builder builder = projEntity.toBuilder();
                e.getValue().forEach( r -> builder.getEntityBuilder().addUnplacedReason(r));
                projectedEntities.put(projEntity.getEntity().getOid(), builder.build());
            }
        }
    }

    /**
     * Find all VMs that are not controllable and give them an unplacement reason (NOT_CONTROLLABLE).
     *
     * @return a map that maps VM ID to a list of unplacement reasons
     */
    private Map<Long, List<UnplacementReason.Builder>> getUnplacementReasonForUncontrollableVm() {
        Map<Long, List<UnplacementReason.Builder>> unplacedReasonMap = new HashMap<>();
        Set<Long> uncontrollableVmOids = topologyDTOs.values().stream()
                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                        && !e.getAnalysisSettings().getControllable())
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
        for (Long vmOid : uncontrollableVmOids) {
            List<UnplacementReason.Builder> reasonList = new ArrayList<>();
            reasonList.add(UnplacementReason.newBuilder()
                    .setPlacementProblem(PlacementProblem.NOT_CONTROLLABLE));
            unplacedReasonMap.put(vmOid, reasonList);
        }
        return unplacedReasonMap;
    }

    private void saveSMADiags(final SMAInput smaInput) {
        AnalysisDiagnosticsCollectorFactory factory = new DefaultAnalysisDiagnosticsCollectorFactory();
        factory.newDiagsCollector(topologyInfo.getTopologyContextId()
                + "-" + topologyInfo.getTopologyId(), AnalysisMode.SMA).ifPresent(diagsCollector -> {
            diagsCollector.saveSMAInput(smaInput, topologyInfo);
        });
    }

    /*
     * Write action to the log and if M2withSMAActions, write SMAOutput to the log.
     */
    private void writeActionsToLog(List<Action> actions, AnalysisConfig config,
                                   CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                   CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                                   TopologyConverter converter, CloudCostData cloudCostData) {

        AnalysisDiagnosticsCollectorFactory factory = new DefaultAnalysisDiagnosticsCollectorFactory();
        factory.newDiagsCollector(topologyInfo.getTopologyContextId()
                + "-" + topologyInfo.getTopologyId(), AnalysisMode.ACTIONS).ifPresent(diagsCollector -> {
            // Write actions to log file in CSV format
            ActionLogger externalize = new ActionLogger();
            List<String> actionLogs = new ArrayList<>();
            if (!config.isSMAOnly()) {
                actionLogs.addAll(externalize.logM2Actions(actions,
                        originalCloudTopology, projectedCloudTopology, cloudCostData,
                        converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage(),
                        converter.getConsistentScalingHelper()));
            }
            if (config.isEnableSMA()) {
                actionLogs.addAll(externalize.logSMAOutput(smaConverter.getSmaOutput(),
                        originalCloudTopology, projectedCloudTopology,
                        cloudCostData,
                        converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage(),
                        converter.getConsistentScalingHelper()));
            }
            diagsCollector.saveActions(actionLogs, topologyInfo);
        });
    }

    /**
     * Cancel analysis by marking this {@link Analysis} as stopped and the associated
     * Economy if one exists as stopped.
     *
     */
    public void cancelAnalysis() {
        // set the forceStop boolean to true on the economy
        stopAnalysis = true;
        getEconomy().getValue().ifPresent(e -> e.setForceStop(true));
    }

    /**
     * @return true if analysis cancellation was triggered
     *
     */
    public boolean isStopAnalysis() {
        return stopAnalysis;
    }

    /**
     * In a Cloud Migration Plan, business account {@link TopologyEntityDTO}s must have new connected
     * entities corresponding to workloads that were prviously on-prem. Here, those connections are added, and the entities
     * are returned.
     *
     * @param projectedEntitiesFromOriginalTopo entities from the original topology for which trader creation is skipped
     * @param traderTOs {@link TraderTO} analysis results
     * @param originalTopology a map of OID to {@link TopologyEntityDTO} modeling the original topology
     * @return a list of {@link TopologyEntityDTO} with businessAccount - OWNS_CONNECTION -> workload connections added
     */
    @Nonnull
    private List<TopologyEntityDTO> getNewlyConnectedProjectedEntitiesFromOriginalTopo(
            @Nonnull final List<TopologyEntityDTO> projectedEntitiesFromOriginalTopo,
            @Nonnull final List<TraderTO> traderTOs,
            @Nonnull final Map<Long, TopologyEntityDTO> originalTopology) {
        final Map<Boolean, Map<Long, TopologyEntityDTO>> isBusinessAccountToIdToTopologyEntityDTO = projectedEntitiesFromOriginalTopo.stream()
                .collect(Collectors.partitioningBy(
                        topologyEntityDTO -> topologyEntityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE,
                        Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));

        final List<TopologyEntityDTO> projectedEntitiesFromOriginalTopoNewlyConnected = Lists.newArrayList();
        if (isBusinessAccountToIdToTopologyEntityDTO.containsKey(true)) {
            Map<Long, TopologyEntityDTO> accountIdToAccountDto = isBusinessAccountToIdToTopologyEntityDTO.get(true);

            // Remove existing connections to workloads (VMs or Virtual Volumes) from all accounts.
            accountIdToAccountDto = accountIdToAccountDto.values().stream()
                    .map(a -> a.toBuilder().clearConnectedEntityList().build())
                    .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

            final Map<Long, Set<ConnectedEntity>> businessAccountsToNewlyOwnedEntities =
                    converter.getCloudTc().getBusinessAccountsToNewlyOwnedEntities(
                            traderTOs, originalTopology, accountIdToAccountDto);
            projectedEntitiesFromOriginalTopoNewlyConnected.addAll(accountIdToAccountDto.entrySet().stream()
                    .map(idToTopologyEntityDTO -> {
                        final long id = idToTopologyEntityDTO.getKey();
                        TopologyEntityDTO topologyEntityDTOWithNewConnections = idToTopologyEntityDTO.getValue();
                        if (businessAccountsToNewlyOwnedEntities.containsKey(id)) {
                            topologyEntityDTOWithNewConnections = TopologyEntityDTO.newBuilder()
                                    .addAllConnectedEntityList(businessAccountsToNewlyOwnedEntities.get(id))
                                    .mergeFrom(topologyEntityDTOWithNewConnections)
                                    .build();
                        }
                        return topologyEntityDTOWithNewConnections;
                    })
                    .collect(Collectors.toList()));
        }

        if (isBusinessAccountToIdToTopologyEntityDTO.containsKey(false)) {
            projectedEntitiesFromOriginalTopoNewlyConnected.addAll(
                    isBusinessAccountToIdToTopologyEntityDTO.get(false).values());
        }
        return projectedEntitiesFromOriginalTopoNewlyConnected;
    }


    /**
     * Copy relevant entities (entities which did not go through market conversion) from the
     * original topology to the projected topology. Skips virtual volumes from being added to
     * projected topology if they have associated wasted storage actions.
     *
     * @param wastedStorageActionsVolumeIds volumes id associated with wasted storage actions.
     * @param oidsRemoved entities removed via plan configurations.
     *                    For example, configuration changes like remove/decommission hosts etc.
     * @param traderTOs {@link TraderTO} analysis results
     * @param originalTopology the original set of {@link TopologyEntityDTO}s by OID.
     * @param isMigrateToCloud whether this is a MCP context
     */
    private void copySkippedEntitiesToProjectedTopology(
            final Set<Long> wastedStorageActionsVolumeIds,
            @Nonnull final Set<Long> oidsRemoved,
            @Nonnull final List<TraderTO> traderTOs,
            @Nonnull final Map<Long, TopologyEntityDTO> originalTopology,
            @Nonnull final boolean isMigrateToCloud) {
        final Stream<TopologyEntityDTO> projectedEntitiesFromSkippedEntities =
                converter.getSkippedEntitiesInScope(topologyDTOs.keySet()).stream();
        final List<TopologyEntityDTO> projectedEntitiesFromOriginalTopo = originalCloudTopology.getAllEntitiesOfType(
                TopologyConversionConstants.ENTITY_TYPES_TO_SKIP_TRADER_CREATION);
        final Set<ProjectedTopologyEntity> entitiesToAdd = Stream
                .concat((isMigrateToCloud
                                ? getNewlyConnectedProjectedEntitiesFromOriginalTopo(
                                        projectedEntitiesFromOriginalTopo,
                                        traderTOs,
                                        originalTopology)
                                : projectedEntitiesFromOriginalTopo).stream(),
                        projectedEntitiesFromSkippedEntities)
                // Exclude entities that were removed due to plan configurations in source topology
                .filter(entity -> !oidsRemoved.contains(entity.getOid()))
                // Exclude entities that have already been added
                .filter(entity -> !projectedEntities.containsKey(entity.getOid()))
                // Volumes with delete volume actions go into the projected topology, but get
                // marked as deleted.
                .map(entity -> Analysis.toProjectedTopologyEntity(entity, wastedStorageActionsVolumeIds.contains(entity.getOid())))
                .collect(Collectors.toSet());

        entitiesToAdd.forEach(projectedEntity -> {
            final ProjectedTopologyEntity existing =
                projectedEntities.put(projectedEntity.getEntity().getOid(), projectedEntity);
            if (existing != null && !projectedEntity.equals(existing)) {
                logger.error("Existing projected entity overwritten by entity from " +
                        "original topology. Existing (converted from market): {}\nOriginal: {}",
                    existing, projectedEntity);
            }
            logger.trace("Added entity with [name: {}, oid: {}] to projected entities.",
                    projectedEntity.getEntity().getDisplayName(),
                    projectedEntity.getEntity().getOid());
        });
    }

    private static ProjectedTopologyEntity toProjectedTopologyEntity(
            @Nonnull final TopologyEntityDTO topologyEntityDTO, final boolean deleted) {
        return ProjectedTopologyEntity.newBuilder()
                .setEntity(topologyEntityDTO)
                .setDeleted(deleted)
                .build();
    }

    private List<CommoditySpecification> createCommsToAdjustOverheadInClone() {
        return MarketAnalysisUtils.COMM_TYPES_TO_ALLOW_OVERHEAD.stream()
            .map(type -> TopologyDTO.CommodityType.newBuilder().setType(type).build())
            .map(this::getCommSpecForCommodity)
            .map(cs -> new CommoditySpecification(cs.getType(), cs.getBaseType()))
            .collect(Collectors.toList());
    }

    /**
     *
     * <p>Construct fake buying TopologyEntityDTOS to help form markets with sellers bundled by cluster/storage
     * cluster.</p>
     *
     * <p>This is to ensure each cluster/storage cluster will form a unique market regardless of
     * segmentation constraint which may divided the cluster/storage cluster.
     * </p>
     * @return a set of fake TopologyEntityDTOS with only cluster/storage cluster commodity in the
     * commodity bought list
     */
    protected Map<Long, TopologyEntityDTO> createFakeTopologyEntityDTOsForSuspensionThrottling() {
        // create fake entities to help construct markets in which sellers of a compute
        // or a storage cluster serve as market sellers
        Set<TopologyEntityDTO> fakeEntityDTOs = new HashSet<>();
        try {
            Set<TopologyEntityDTO> pmEntityDTOs = getEntityDTOsInCluster(GroupType.COMPUTE_HOST_CLUSTER);
            Set<TopologyEntityDTO> dsEntityDTOs = getEntityDTOsInCluster(GroupType.STORAGE_CLUSTER);
            Set<String> clusterCommKeySet = new HashSet<>();
            Set<String> dsClusterCommKeySet = new HashSet<>();
            pmEntityDTOs.forEach(dto -> {
                dto.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType() == CommodityType.CLUSTER_VALUE) {
                        clusterCommKeySet.add(commSold.getCommodityType().getKey());
                    }
                });
            });
            dsEntityDTOs.forEach(dto -> {
                dto.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE
                            && isRealStorageClusterCommodity(commSold)) {
                        dsClusterCommKeySet.add(commSold.getCommodityType().getKey());
                    }
                });
            });
            clusterCommKeySet.forEach(key -> fakeEntityDTOs
                .add(creatFakeDTOs(CommodityType.CLUSTER_VALUE, key)));
            dsClusterCommKeySet.forEach(key -> fakeEntityDTOs
                .add(creatFakeDTOs(CommodityType.STORAGE_CLUSTER_VALUE, key)));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get cluster members from group component due to: {}." +
                " Not creating fake entity DTOs for suspension throttling.", e.getMessage());
        }

        return fakeEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid,
                        Function.identity()));
    }

    /**
     * Runs buy RI impact analysis, if the projected topology is not empty. Currently, analysis is not
     * stitched back to cost calculation (TODO: OM-51388)
     * @param projectedCloudTopology The projected cloud topology
     * @param cloudCostData The {@link CloudCostData}, used to lookup buy RI recommendations in creating
     *                      an instance of {@link BuyRIImpactAnalysis}
     * @param isBuyRIImpactAnalysis true only if OCP Plan Option 3
     * @return  The list of allocate actions generated by the buyRI analysis.
     *          Non Empty only for OCP Plan Option 3.
.    */
    private List<Action.Builder> runBuyRIImpactAnalysis(@Nonnull CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                                                        @Nonnull CloudCostData cloudCostData) {

        final ImmutableList.Builder<Action.Builder> buyRIAllocateActions = ImmutableList.builder();

        final List<AnalysisType> analysisTypeList = topologyInfo.getAnalysisTypeList();
        if (analysisTypeList.contains(AnalysisType.BUY_RI_IMPACT_ANALYSIS) &&
                projectedCloudTopology.size() > 0) {

            try (DataMetricTimer timer = BUY_RI_IMPACT_ANALYSIS_SUMMARY.startTimer()) {
                final BuyRIImpactAnalysis buyRIImpactAnalysis = buyRIImpactAnalysisFactory
                              .createAnalysis(
                                              topologyInfo,
                                              projectedCloudTopology,
                                              cloudCostData,
                                              converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage());
                final BuyCommitmentImpactResult impactResult =
                                    buyRIImpactAnalysis.allocateCoverageFromBuyRIImpactAnalysis();

                converter.getProjectedRICoverageCalculator().addBuyRICoverageToProjectedRICoverage(
                        impactResult.buyCommitmentCoverage());

                if (!analysisTypeList.contains(AnalysisType.MARKET_ANALYSIS)) {

                    logger.info("{}Generating {} buy RI allocation actions",
                            logPrefix, impactResult.buyAllocationActions().size());

                    buyRIAllocateActions.addAll(impactResult.buyAllocationActions());
                }

            } catch (Exception e) {
                logger.error("Error executing buy RI impact analysis (Context ID={}, Topology ID={})",
                        topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId(), e);
            }
        }
        return buyRIAllocateActions.build();
    }

    /**
     * Runs the migrated workload cloud commitment analysis on the specified projected topology.
     * @param projectedCloudTopology    The projected cloud topology: used to find placed VMs
     * @param projectedEntities         A list of the projected entities generated by the market: used to lookup a
     *                                  placed VM's compute tier
     * @param projectedTraderDTO        The projected traders: used to lookup a virtual machine's region
     */
    private void runMigratedWorkloadCloudCommitmentAnalysis(@Nonnull CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                                                            @NonNull Map<Long, ProjectedTopologyEntity> projectedEntities,
                                                            @NonNull List<TraderTO> projectedTraderDTO) {
        // Define a list of all of our migrated workload placements
        final List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement> workloadPlacementList = new ArrayList<>();

        // Iterate over our traders and find virtual machines
        projectedTraderDTO.stream()
                .filter(trader -> trader.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .forEach(trader -> {
                    // Get the virtual machine
                    TopologyEntityDTO entity = projectedCloudTopology.getEntities().get(trader.getOid());

                    // Find the VM's compute tier
                    Optional<TopologyEntityDTO> computeTier = entity.getCommoditiesBoughtFromProvidersList().stream()
                            .map(c -> projectedCloudTopology.getEntities().get(c.getProviderId()))
                            .filter(provider -> provider != null && provider.getEntityType() == EntityType.COMPUTE_TIER_VALUE)
                            .findFirst();

                    // Find the region
                    Optional<TopologyEntityDTO> region = trader.getShoppingListsList().stream()
                            .filter(shoppingList -> shoppingList.hasContext() && shoppingList.getContext().hasRegionId())
                            .map(shoppingListTO -> shoppingListTO.getContext().getRegionId())
                            .map(regionId -> projectedCloudTopology.getEntities().get(regionId))
                            .findFirst();

                    // Find the VM in the original topology
                    TopologyEntityDTO originalVm = originalCloudTopology.getEntities().get(trader.getOid());

                    // Validate that we were able to find a compute tier and region
                    if (!computeTier.isPresent()) {
                        logger.warn("Could not find compute tier for workload placement for VM: {}", entity.getOid());
                    } else if (!region.isPresent()) {
                        logger.warn("Could not find region for workload placement for VM: {}, compute tier: {}", entity.getOid(), computeTier.get().getOid());
                    } else if (!isVMUsingRI(entity, computeTier.get())) {
                        // Only add the VM to the list to analyze if it has not been resized specifically to use an existing reserved instance
                        workloadPlacementList.add(Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement.newBuilder()
                                .setVirtualMachine(entity)
                                .setComputeTier(computeTier.get())
                                .setRegion(region.get())
                                .setOriginalVirtualMachine(originalVm)
                                .build());
                    }
                });

        // Get the master business account
        Optional<TopologyEntityDTO> masterBusinessAccount = Optional.empty();
        if (!workloadPlacementList.isEmpty()) {
            masterBusinessAccount = getMasterBusinessAccount(projectedCloudTopology, workloadPlacementList.get(0).getVirtualMachine().getOid());
        }

        // Validate that we actually found a master business account
        if (!masterBusinessAccount.isPresent()) {
            logger.warn("Could not find master business account in projected cloud topology");
        }

        // Send the request to start the analysis
        migratedWorkloadCloudCommitmentAnalysisService.startAnalysis(topologyInfo.getTopologyContextId(),
                masterBusinessAccount.map(TopologyEntityDTO::getOid),
                workloadPlacementList);
    }

    /**
     * Checks to see if the specified entity is using coupon commodities sold by the specified compute tier. This will tell
     * us if the market has resized the VM specifically to use a reserved instance.
     *
     * @param entity        The virtual machine entity
     * @param computeTier   The compute tier to which the virtual machine is being moved
     * @return              True if the VM is using an RI, false otherwise
     */
    private boolean isVMUsingRI(TopologyEntityDTO entity, TopologyEntityDTO computeTier) {
        List<CommoditiesBoughtFromProvider> commodities = entity.getCommoditiesBoughtFromProvidersList();
        for (CommoditiesBoughtFromProvider commodity: commodities) {
            if (commodity.getProviderId() == computeTier.getOid()) {
                // Find all coupon commodities with a used value greater than zero that this entity is buying from the computer tier
                List<CommodityBoughtDTO> couponCommodities = commodity.getCommodityBoughtList().stream()
                        .filter(c -> c.getCommodityType().getType() == CommodityType.COUPON_VALUE)
                        .filter(c -> c.getUsed() > 0)
                        .collect(Collectors.toList());
                if (!couponCommodities.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the master business account in the specified projected cloud topology for the specified VM OID.
     *
     * @param projectedCloudTopology    The projected cloud topology, generated by the market
     * @param vmOid                     The VM OID for which to find the owned business account
     * @return                          An optional wrapping the master business account, if found
     */
    Optional<TopologyEntityDTO> getMasterBusinessAccount(@Nonnull CloudTopology<TopologyEntityDTO> projectedCloudTopology, long vmOid) {
        return projectedCloudTopology.getEntities().values().stream()
                .filter(e -> e.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .filter(e -> e.getConnectedEntityListList().stream().anyMatch(ce -> ce.getConnectedEntityId() == vmOid))
                .findFirst();
    }

    /**
     * Create fake VM TopologyEntityDTOs to buy cluster/storage cluster commodity only.
     *
     * @param clusterValue cluster or storage cluster
     * @param key the commodity's key
     * @return a VM TopologyEntityDTO
     */
    private TopologyEntityDTO creatFakeDTOs(int clusterValue, String key) {
        final CommodityBoughtDTO clusterCommBought = CommodityBoughtDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(clusterValue).setKey(key).build())
                .build();
        return TopologyEntityDTO.newBuilder()
               .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
               .setOid(IdentityGenerator.next())
               .setDisplayName("Fake-" + clusterValue + key)
               .setEntityState(EntityState.POWERED_ON)
               .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false).build())
               .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                       .addCommodityBought(clusterCommBought).build())
               .build();
    }

    protected Set<TopologyEntityDTO> getEntityDTOsInCluster(GroupType groupType) {
        Set<TopologyEntityDTO> entityDTOs = new HashSet<>();
        groupMemberRetriever.getGroupsWithMembers(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(groupType))
                .build())
                .forEach(groupAndMembers -> {
                    for (long memberId : groupAndMembers.members()) {
                        if (topologyDTOs.containsKey(memberId)) {
                            entityDTOs.add(topologyDTOs.get(memberId));
                        }
                    }
                });
        return entityDTOs;
    }

    /**
     * Check if the commoditySoldDTO is a storage cluster commodity for real storage cluster.
     * <p>
     * Real storage cluster is a storage cluster that is physically exits in the data center.
     * </p>
     * @param comm commoditySoldDTO
     * @return true if it is for real cluster
     */
    private boolean isRealStorageClusterCommodity(TopologyDTO.CommoditySoldDTO comm) {
        return comm.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE
            && TopologyDTOUtil.isRealStorageClusterCommodityKey(comm.getCommodityType().getKey());
    }

    /**
     * Get the ID of the projected topology.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     *
     * @return The ID of the projected topology.
     */
    public Optional<Long> getProjectedTopologyId() {
        return completed ? Optional.of(projectedTopologyId) : Optional.empty();
    }

    /**
     * The projected topology resulted from the analysis run.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     * @return the projected topology
     */
    public Optional<Map<Long, ProjectedTopologyEntity>> getProjectedTopology() {
        return completed ? Optional.ofNullable(projectedEntities) : Optional.empty();
    }


    /**
     * Calculate and return the entity costs in the projected topology.Get the entity costs
     *
     * @return If the analysis is completed, an {@link Optional} containing the {@link CostJournal}s
     *         for the cloud entities in the projected topology. If there are no cloud entities in
     *         the projected topology, returns an empty map. If the analysis is not completed,
     *         returns an empty {@link Optional}.
     */
    public Optional<Map<Long, CostJournal<TopologyEntityDTO>>> getProjectedCosts() {
        return completed ? Optional.ofNullable(projectedEntityCosts) : Optional.empty();
    }

    /**
     * Return calculated projected reserved instance coverage per entity
     *
     * @return {@link Map} of entity id and its projected RI coverage if analysis completed, else empty
     */
    public Optional<Map<Long, EntityReservedInstanceCoverage>> getProjectedEntityRiCoverage() {
        return completed ? Optional.of(
            converter.getProjectedRICoverageCalculator().getProjectedReservedInstanceCoverage()) : Optional.empty();
    }

    /**
     * The action plan resulted from the analysis run.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     * @return the action plan
     */
    public Optional<ActionPlan> getActionPlan() {
        return completed ? Optional.ofNullable(actionPlan) : Optional.empty();
    }

    /**
     * Start time of this analysis run.
     * @return start time of this analysis run
     */
    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Completion time of this analysis run, or epoch if not yet completed.
     * @return completion time of this analysis run
     */
    public Instant getCompletionTime() {
        return completionTime;
    }

    /**
     * The topology context id of this analysis run.
     * @return the topology context id of this analysis run
     */
    public long getContextId() {
        return topologyInfo.getTopologyContextId();
    }

    /**
     * The topology id of this analysis run.
     * @return the topology id of this analysis run
     */
    public long getTopologyId() {
        return topologyInfo.getTopologyId();
    }

    /**
     * An unmodifiable view of the original topology input into the market. Whether or not the
     * analysis was scoped, this will return all the input entities (i.e. the whole topology).
     *
     * @return an unmodifiable view of the map of topology entity DTOs passed to the analysis,
     *         prior to scoping.
     */
    @Nonnull
    public Map<Long, TopologyEntityDTO> getTopology() {
        return Collections.unmodifiableMap(topologyDTOs);
    }

    @Nonnull
    public AnalysisConfig getConfig() {
        return config;
    }

    /**
     * Set the {@link #state} to {@link AnalysisState#QUEUED}.
     */
    protected void queued() {
        state = AnalysisState.QUEUED;
    }

    /**
     * The state of this analysis run.
     * @return the state of this analysis run
     */
    public AnalysisState getState() {
        return state;
    }

    /**
     * The error message reported if the state of this analysis run
     * is {@link AnalysisState#FAILED FAILED}.
     * @return the error message reported if the state of this analysis run
     * is {@link AnalysisState#FAILED FAILED}
     */
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Check if the analysis run is done (either successfully or not).
     * @return true if the state is either COMPLETED or FAILED
     */
    public boolean isDone() {
        return completed;
    }

    /**
     * Check if the analysis is running on a scoped topology and the topology is not for a cloud plan.
     *
     * @return true if the analysis is running on a scoped topology and the topology is not for a
     * cloud plan, false otherwise.
     */
    public boolean isScoped() {
        return !topologyInfo.getScopeSeedOidsList().isEmpty() && (!topologyInfo.hasPlanInfo()
                || (!topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN) &&
                        !topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.CLOUD_MIGRATION_PLAN)));
    }

    /**
     * Get the {@link TopologyInfo} of the topology this analysis is running on.
     *
     * @return The {@link TopologyInfo}.
     */
    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return topologyInfo;
    }

    /**
     * Replay actions are used only in real-time topologies.
     * @return actions (suspend/deactivate) generated in this cycle to replay.
     */
    public @NonNull ReplayActions getReplayActions() {
        return realtimeReplayActions;
    }

    /**
     * Replay actions are set only real-time topologies.
     * @param replayActions Suspend/deactivate actions from previous cycle are set to replay
     * in current analysis.
     */
    public void setReplayActions(@NonNull ReplayActions replayActions) {
        realtimeReplayActions = replayActions;
    }

    /**
     * Return a CommSpecTO for the CommodityType passed.
     * @param commType is the commType we need converted
     * @return {@link com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO}
     * equivalent of the commType.
     */
    public CommodityDTOs.CommoditySpecificationTO getCommSpecForCommodity(TopologyDTO.CommodityType commType) {
        return converter.getCommSpecForCommodity(commType);
    }

    public SetOnce<Economy> getEconomy() {
        return economy;
    }

    public void setEconomy(Economy economy) {
        this.economy.trySetValue(economy);
    }

    /**
     * Gets the original cloud topology.
     *
     * @return The original cloud topology
     */
    protected CloudTopology<TopologyEntityDTO> getOriginalCloudTopology() {
        return originalCloudTopology;
    }

}
