package com.vmturbo.market.runner;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Analysis execution and properties. This can be for a scoped plan or for a real-time market.
 */
public class Analysis {

    private static final DataMetricSummary TOPOLOGY_SCOPING_SUMMARY = DataMetricSummary.builder()
            .withName("mkt_economy_scoping_duration_seconds")
            .withHelp("Time to scope the economy for analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

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

    private Map<Long, TopologyEntityDTO> scopeEntities = Collections.emptyMap();

    private Map<Long, ProjectedTopologyEntity> projectedEntities = null;

    private Map<Long, CostJournal<TopologyEntityDTO>> projectedEntityCosts = null;

    // VM OID to a set of OIDs of computer tier that the vm can fit in.
    private Map<Long, Set<Long>> cloudVmOIDToProvidersOIDMap = new HashMap<>();

    private final long projectedTopologyId;

    private ActionPlan actionPlan = null;

    private String errorMsg;

    private AnalysisState state;

    private ReplayActions realtimeReplayActions;

    private final TopologyInfo topologyInfo;

    private boolean stopAnalysis = false;

    private final SetOnce<Economy> economy = new SetOnce<>();

    /**
     * The clock used to time market analysis.
     */
    private final Clock clock;

    private final GroupServiceBlockingStub groupServiceClient;

    private final AnalysisConfig config;

    private final CloudTopology<TopologyEntityDTO> originalCloudTopology;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final TierExcluderFactory tierExcluderFactory;

    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

    private final MarketPriceTableFactory marketPriceTableFactory;

    private final WastedFilesAnalysisFactory wastedFilesAnalysisFactory;

    private final ConsistentScalingHelperFactory consistentScalingHelperFactory;

    private final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory;

    private final AnalysisRICoverageListener listener;

    // a set of on-prem application entity type
    private static final Set<Integer> entityTypesToSkip =
            new HashSet<>(Collections.singletonList(EntityType.BUSINESS_APPLICATION_VALUE));

    /**
     * Create and execute a context for a Market Analysis given a topology, an optional 'scope' to
     * apply, and a flag determining whether guaranteed buyers (VDC, VPod, DPod) are included
     * in the market analysis or not.
     *
     * The scoping algorithm is described more completely below - {@link #scopeTopology}.
     *
     * @param topologyInfo descriptive info about the topology - id, type, etc
     * @param topologyDTOs the Set of {@link TopologyEntityDTO}s that make up the topology
     * @param groupServiceClient Used to look up groups to support suspension throttling
     * @param clock The clock used to time market analysis.
     * @param analysisConfig configuration for analysis
     * @param cloudTopologyFactory cloud topology factory
     * @param cloudCostCalculatorFactory cost calculation factory
     * @param priceTableFactory price table factory
     * @param wastedFilesAnalysisFactory wasted file analysis handler
     * @param buyRIImpactAnalysisFactory  buy RI impact analysis factory
     * @param tierExcluderFactory the tier excluder factory
     * @param listener that receives entity ri coverage information availability.
     * @param consistentScalingHelperFactory CSM helper factory
     */
    public Analysis(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                    @Nonnull final GroupServiceBlockingStub groupServiceClient,
                    @Nonnull final Clock clock,
                    @Nonnull final AnalysisConfig analysisConfig,
                    @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                    @Nonnull final TopologyCostCalculatorFactory cloudCostCalculatorFactory,
                    @Nonnull final MarketPriceTableFactory priceTableFactory,
                    @Nonnull final WastedFilesAnalysisFactory wastedFilesAnalysisFactory,
                    @Nonnull final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory,
                    @Nonnull final TierExcluderFactory tierExcluderFactory,
                    @Nonnull final AnalysisRICoverageListener listener,
                    @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory) {
        this.topologyInfo = topologyInfo;
        this.topologyDTOs = topologyDTOs.stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        this.state = AnalysisState.INITIAL;
        logPrefix = topologyInfo.getTopologyType() + " Analysis " +
            topologyInfo.getTopologyContextId() + " with topology " +
            topologyInfo.getTopologyId() + " : ";
        this.projectedTopologyId = IdentityGenerator.next();
        this.groupServiceClient = groupServiceClient;
        this.clock = Objects.requireNonNull(clock);
        this.config = analysisConfig;
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.originalCloudTopology = this.cloudTopologyFactory.newCloudTopology(topologyDTOs.stream());
        this.wastedFilesAnalysisFactory = wastedFilesAnalysisFactory;
        this.buyRIImpactAnalysisFactory = buyRIImpactAnalysisFactory;
        this.topologyCostCalculatorFactory = cloudCostCalculatorFactory;
        this.marketPriceTableFactory = priceTableFactory;
        this.tierExcluderFactory = tierExcluderFactory;
        this.listener = listener;
        this.consistentScalingHelperFactory = consistentScalingHelperFactory;
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

        if (topologyInfo.getTopologyType() == TopologyType.REALTIME
                && !originalCloudTopology.getEntities().isEmpty()) {
            final long waitStartTime = System.currentTimeMillis();
            try {
                final CostNotification notification =
                        listener.receiveCostNotification(this).get();
                final StatusUpdate statusUpdate = notification.getStatusUpdate();
                final Status status = statusUpdate.getStatus();
                if (status != Status.SUCCESS) {
                    logger.error("Cost notification reception failed for analysis with context id" +
                            " : {}, topology id: {} with status: {} and message: {}",
                            this.getContextId(), this.getTopologyId(), status,
                            statusUpdate.getStatusDescription());
                    return false;
                } else {
                    logger.debug("Cost notification with a success status received for analysis " +
                            "with context id: {}, topology id: {}", this.getContextId(),
                            this.getTopologyId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(
                        String.format("Error while receiving cost notification for analysis %s",
                                getTopologyInfo()), e);
                return false;
            } catch (ExecutionException e) {
                logger.error(
                        String.format("Error while receiving cost notification for analysis %s",
                                getTopologyInfo()), e);
                return false;
            } finally {
                final long waitEndTime = System.currentTimeMillis();
                logger.debug("Analysis with context id: {}, topology id: {} waited {} ms for the " +
                                "cost notification.", this.getContextId(), this.getTopologyId(),
                        waitEndTime - waitStartTime);
            }
        }

        final TopologyCostCalculator topologyCostCalculator = topologyCostCalculatorFactory
                .newCalculator(topologyInfo, originalCloudTopology);
        // Use the cloud cost data we use for cost calculations for the price table.
        final MarketPriceTable marketPriceTable = marketPriceTableFactory.newPriceTable(
                this.originalCloudTopology, topologyCostCalculator.getCloudCostData());
        this.converter = new TopologyConverter(topologyInfo, config.getIncludeVdc(),
                config.getQuoteFactor(), config.isEnableSMA(),
                config.getLiveMarketMoveCostFactor(),
                marketPriceTable, null, topologyCostCalculator.getCloudCostData(),
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory);
        state = AnalysisState.IN_PROGRESS;
        startTime = clock.instant();
        logger.info("{} Started", logPrefix);
        try {
            final DataMetricTimer conversionTimer = TOPOLOGY_CONVERT_TO_TRADER_SUMMARY.startTimer();
            final boolean enableThrottling = (config.getSuspensionsThrottlingConfig()
                    == SuspensionsThrottlingConfig.CLUSTER);

            // construct fake entities which can be used to form markets based on cluster/storage cluster
            final Map<Long, TopologyEntityDTO> fakeEntityDTOs;
            if (enableThrottling) {
                fakeEntityDTOs = createFakeTopologyEntityDTOsForSuspensionThrottling();
                fakeEntityDTOs.forEach(topologyDTOs::put);
            } else {
                fakeEntityDTOs = Collections.emptyMap();
            }

            Set<TraderTO> traderTOs = new HashSet<>();
            if (!stopAnalysis) {
                traderTOs.addAll(converter.convertToMarket(topologyDTOs));
            }
            // cache the traderTOs converted from fake TopologyEntityDTOs
            Set<TraderTO> fakeTraderTOs = new HashSet<>();
            if (enableThrottling) {
                for (TraderTO dto : traderTOs) {
                    if (fakeEntityDTOs.containsKey(dto.getOid())) {
                        fakeTraderTOs.add(dto);
                    }
                }
            }
            conversionTimer.observe();

            // if a scope 'seed' entity OID list is specified, then scope the topology starting with
            // the given 'seed' entities
            if (!stopAnalysis) {
                if (isScoped()) {
                    try (final DataMetricTimer scopingTimer = TOPOLOGY_SCOPING_SUMMARY.startTimer()) {
                        traderTOs = scopeTopology(traderTOs,
                            ImmutableSet.copyOf(topologyInfo.getScopeSeedOidsList()));
                        // add back fake traderTOs for suspension throttling as it may be removed due
                        // to scoping
                        traderTOs.addAll(fakeTraderTOs);
                    }

                    // save the scoped topology for later broadcast
                    scopeEntities = traderTOs.stream()
                        // convert the traders in the scope into topologyEntities
                        .map(trader -> topologyDTOs.get(trader.getOid()))
                        // remove the topologyEntities that were created as fake because of suspension throttling
                        .filter(topologyEntityDTO -> !fakeEntityDTOs.containsKey(topologyEntityDTO.getOid()))
                        // convert it to a map
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid,
                            trader -> topologyDTOs.get(trader.getOid())));
                } else {
                    scopeEntities = topologyDTOs;
                }
            }

            // remove skipped entities we don't want to send to market
            converter.removeSkippedEntitiesFromTraderTOs(traderTOs);

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
            final Set<Long> oidsToRemove = topologyDTOs.values().stream()
                    .filter(dto -> dto.hasEdit())
                    .filter(dto -> dto.getEdit().hasRemoved()
                                || dto.getEdit().hasReplaced())
                    .map(TopologyEntityDTO::getOid)
                    .collect(Collectors.toSet());
            if (oidsToRemove.size() > 0) {
                logger.debug("Removing {} traders before analysis: ", oidsToRemove.size());
                traderTOs = traderTOs.stream()
                        .filter(traderTO -> !(oidsToRemove.contains(traderTO.getOid())))
                        .collect(Collectors.toSet());
            }

            if (logger.isDebugEnabled()) {
                logger.debug(traderTOs.size() + " Economy DTOs");
            }
            if (logger.isTraceEnabled()) {
                traderTOs.stream().map(dto -> "Economy DTO: " + dto).forEach(logger::trace);
            }
            final DataMetricTimer processResultTime = RESULT_PROCESSING.startTimer();
            if (!stopAnalysis) {
                final Topology topology = TopologyEntitiesHandler.createTopology(traderTOs,
                        topologyInfo, config, this);
                final AnalysisResults results = TopologyEntitiesHandler.performAnalysis(traderTOs,
                        topologyInfo, config, this, topology);
                if (config.isEnableSMA()) {
                    Map<Long, Set<Long>> cloudVmToTpsThatFitMap =
                            TopologyEntitiesHandler
                                    .getProviderLists(converter.getCloudVmComputeShoppingListIDs(),
                                            topology);
                    for (Entry<Long, Set<Long>> cloudVmToTpsThatFitEntry : cloudVmToTpsThatFitMap.entrySet()) {
                        Set<Long> providerOIDList = new HashSet<>();
                        for (Long tpoid : cloudVmToTpsThatFitEntry.getValue()) {
                            providerOIDList.add(converter.convertTraderTOToTopologyEntityDTO(tpoid));
                        }
                        cloudVmOIDToProvidersOIDMap.put(cloudVmToTpsThatFitEntry.getKey(), providerOIDList);
                    }
                }
                // add shoppinglist from newly provisioned trader to shoppingListOidToInfos
                converter.updateShoppingListMap(results.getNewShoppingListToBuyerEntryList());
                logger.info(logPrefix + "Done performing analysis");

            // Calculate reservedCapacity and generate resize actions
            ReservedCapacityAnalysis reservedCapacityAnalysis = new ReservedCapacityAnalysis(scopeEntities);
            reservedCapacityAnalysis.execute();

            // Execute wasted file analysis
            WastedFilesAnalysis wastedFilesAnalysis = wastedFilesAnalysisFactory.newWastedFilesAnalysis(
                topologyInfo, scopeEntities, this.clock, topologyCostCalculator, originalCloudTopology);
            final Collection<Action> wastedFileActions = getWastedFilesActions(wastedFilesAnalysis);

            List<TraderTO> projectedTraderDTO = new ArrayList<>();

            try (DataMetricTimer convertFromTimer = TOPOLOGY_CONVERT_FROM_TRADER_SUMMARY.startTimer()) {
                if (enableThrottling) {
                    // remove the fake entities used in suspension throttling
                    // we need to remove it both from the projected topology and the source topology

                        // source, non scoped topology
                        for (long fakeEntityOid : fakeEntityDTOs.keySet()) {
                            topologyDTOs.remove(fakeEntityOid);
                        }

                        // projected topology
                        for(TraderTO projectedDTO : results.getProjectedTopoEntityTOList()) {
                            if (!fakeEntityDTOs.containsKey(projectedDTO.getOid())) {
                                projectedTraderDTO.add(projectedDTO);
                            }
                        }
                    } else {
                        projectedTraderDTO = results.getProjectedTopoEntityTOList();
                    }
                projectedEntities = converter.convertFromMarket(
                    projectedTraderDTO,
                    topologyDTOs,
                    results.getPriceIndexMsg(), topologyCostCalculator.getCloudCostData(),
                    reservedCapacityAnalysis,
                    wastedFilesAnalysis);
                final Set<Long> wastedStorageActionsVolumeIds = wastedFileActions.stream()
                        .map(Action::getInfo).map(ActionInfo::getDelete).map(Delete::getTarget)
                        .map(ActionEntity::getId).collect(Collectors.toSet());
                copySkippedEntitiesToProjectedTopology(wastedStorageActionsVolumeIds);

                    // Calculate the projected entity costs.
                    final CloudTopology<TopologyEntityDTO> projectedCloudTopology =
                            cloudTopologyFactory.newCloudTopology(projectedEntities.values().stream()
                                    .filter(ProjectedTopologyEntity::hasEntity)
                                    .map(ProjectedTopologyEntity::getEntity));

                    // Invoke buy RI impact analysis after projected entity creation, but prior to
                    // projected cost calculations
                    runBuyRIImpactAnalysis(
                            projectedCloudTopology,
                            topologyCostCalculator.getCloudCostData());

                    // Projected RI coverage has been calculated by convertFromMarket
                    // Get it from TopologyConverter and pass it along to use for calculation of
                    // savings
                    projectedEntityCosts = topologyCostCalculator.calculateCosts(projectedCloudTopology,
                            converter.getProjectedReservedInstanceCoverage());
                }

                // Create the action plan
                logger.info(logPrefix + "Creating action plan");
                final ActionPlan.Builder actionPlanBuilder = ActionPlan.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setInfo(ActionPlanInfo.newBuilder()
                                .setMarket(MarketActionPlanInfo.newBuilder()
                                        .setSourceTopologyInfo(topologyInfo)))
                        .setAnalysisStartTimestamp(startTime.toEpochMilli());
                converter.interpretAllActions(results.getActionsList(), projectedEntities,
                    originalCloudTopology, projectedEntityCosts, topologyCostCalculator)
                .forEach(actionPlanBuilder::addAction);

                // TODO move wasted files action out of main analysis once we have a framework
                // to support multiple analyses for the same topology ID
                actionPlanBuilder.addAllAction(wastedFileActions);
                actionPlanBuilder.addAllAction(reservedCapacityAnalysis.getActions());
                logger.info(logPrefix + "Completed successfully");
                processResultTime.observe();
                state = AnalysisState.SUCCEEDED;
                completionTime = clock.instant();
                actionPlan = actionPlanBuilder.setAnalysisCompleteTimestamp(completionTime.toEpochMilli())
                        .build();
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
     * Copy relevant entities (entities which did not go through market conversion) from the
     * original topology to the projected topology. Skips virtual volumes from being added to
     * projected topology if they have associated wasted storage actions.
     *
     * @param wastedStorageActionsVolumeIds volumes id associated with wasted storage actions.
     */
    private void copySkippedEntitiesToProjectedTopology(
            final Set<Long> wastedStorageActionsVolumeIds) {
        // Calculate set of Volumes that have been already added to projected entities. They are:
        // 1. Volumes attached to VMs
        // 2. Detached volumes that have Delete action
        final Set<Long> attachedVolumeIds = originalCloudTopology
                .getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE).stream()
                .flatMap(vm -> vm.getConnectedEntityListList().stream())
                .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                        EntityType.VIRTUAL_VOLUME_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet());
        final Set<Long> alreadyAddedVolumeIds = Sets.union(attachedVolumeIds, wastedStorageActionsVolumeIds);

        final Stream<TopologyEntityDTO> projectedEntitiesFromOriginalTopo =
                originalCloudTopology.getAllEntitiesOfType(
                        TopologyConversionConstants.ENTITY_TYPES_TO_SKIP_TRADER_CREATION).stream();
        final Stream<TopologyEntityDTO> projectedEntitiesFromSkippedEntities =
                converter.getSkippedEntitiesInScope(scopeEntities.keySet()).stream();
        final Set<ProjectedTopologyEntity> entitiesToAdd = Stream
                .concat(projectedEntitiesFromOriginalTopo, projectedEntitiesFromSkippedEntities)
                // Exclude Volumes that have been already added to projected entities
                .filter(entity -> !alreadyAddedVolumeIds.contains(entity.getOid()))
                .map(Analysis::toProjectedTopologyEntity)
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
            @Nonnull final TopologyEntityDTO topologyEntityDTO) {
        return ProjectedTopologyEntity.newBuilder().setEntity(topologyEntityDTO).build();
    }

    /**
     * Construct fake buying TopologyEntityDTOS to help form markets with sellers bundled by cluster/storage
     * cluster.
     * <p>
     * This is to ensure each cluster/storage cluster will form a unique market regardless of
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
.     */
    private void runBuyRIImpactAnalysis(@Nonnull CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                                        @Nonnull CloudCostData cloudCostData) {

        if (topologyInfo.getAnalysisTypeList().contains(AnalysisType.BUY_RI_IMPACT_ANALYSIS) &&
                projectedCloudTopology.size() > 0) {

            try (DataMetricTimer timer = BUY_RI_IMPACT_ANALYSIS_SUMMARY.startTimer()) {

                final BuyRIImpactAnalysis buyRIImpactAnalysis =
                        buyRIImpactAnalysisFactory.createAnalysis(
                                topologyInfo,
                                projectedCloudTopology,
                                cloudCostData,
                                converter.getProjectedReservedInstanceCoverage());
                final Table<Long, Long, Double> entityBuyRICoverage =
                        buyRIImpactAnalysis.allocateCoverageFromBuyRIImpactAnalysis();

                converter.addBuyRICoverageToProjectedRICoverage(entityBuyRICoverage);
            } catch (Exception e) {
                logger.error("Error executing buy RI impact analysis (Context ID={}, Topology ID={})",
                        topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId(), e);
            }
        }
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
        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                        .setGroupType(groupType))
                        .build())
            .forEachRemaining(grp -> {
                for (long i : GroupProtoUtil.getAllStaticMembers(grp.getDefinition())) {
                    if (topologyDTOs.containsKey(i)) {
                        entityDTOs.add(topologyDTOs.get(i));
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
    public Optional<Collection<ProjectedTopologyEntity>> getProjectedTopology() {
        return completed ? Optional.ofNullable(projectedEntities).map(Map::values) : Optional.empty();
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
        return completed ? Optional.ofNullable(converter.getProjectedReservedInstanceCoverage()) : Optional.empty();
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
     * An unmodifiable view of the set of topology entity DTOs that this analysis run is executed on.
     *
     * If the analysis was scoped, this will return only the entities in the scope, because those
     * were the entities that the market analysis actually ran on.
     *
     * @return an unmodifiable view of the map of topology entity DTOs that this analysis run is executed on
     */
    public Map<Long, TopologyEntityDTO> getTopology() {
        return isScoped() ? scopeEntities : Collections.unmodifiableMap(topologyDTOs);
    }

    /**
     * An unmodifiable view of the original topology input into the market. Whether or not the
     * analysis was scoped, this will return all the input entities (i.e. the whole topology).
     *
     * @return an unmodifiable view of the map of topology entity DTOs passed to the analysis,
     *         prior to scoping.
     */
    @Nonnull
    public Map<Long, TopologyEntityDTO> getOriginalInputTopology() {
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
     * Create a subset of the original topology representing the "scoped topology" given a topology
     * and a "seed scope" set of SE's.
     *
     * <p>Any unplaced service entities are automatically included in the "scoped topology"
     *
     * <p>Starting with the "seed", follow the "buys-from" relationships "going up" and elements to
     * the "scoped topology". Along the way construct a set of traders at the "top", i.e. that do
     * not buy from any other trader. Then, from the "top" elements, follow relationships "going down"
     * and add Traders that "may sell" to the given "top" elements based on the commodities each is
     * shopping for. Recursively follow the "may sell" relationships down to the bottom, adding those
     * elements to the "scoped topology" as well.
     *
     * <p>Setting up the initial market, specifically the populateMarketsWthSellers() call on T traders
     * with M markets runs worst case O(M*T) - see the comments on that method for further details.
     *
     * <p>Once the market is set up, lookups for traders and markets are each in constant time,
     * and each trader is examined at most once, adding at worst O(T).
     *
     * @param traderTOs the topology to be scoped
     * @param seedOids the OIDs of the ServiceEntities that constitute the scope 'seed'
     * @return Set of {@link TraderTO} objects
     **/
    @VisibleForTesting
    Set<TraderTO> scopeTopology(@Nonnull Set<TraderTO> traderTOs, @Nonnull Set<Long> seedOids) {
        // the resulting scoped topology - contains at least the seed OIDs
        final Set<Long> scopedTopologyOIDs = Sets.newHashSet(seedOids);

        // holder for the scoped topology
        final Set<TraderTO> traderTOsInScopedTopology = Sets.newHashSet();

        // create a Topology and associated Economy representing the source topology
        final Topology topology = new Topology();
        for (final TraderTO traderTO : traderTOs) {
            // include all "uplaced" traders in the final scoped topology
            if (traderIsUnplaced(traderTO)) {
                traderTOsInScopedTopology.add(traderTO);
            } else {
                // include all "placed" traders in the market for calculating the scope
                ProtobufToAnalysis.addTrader(topology, traderTO);
            }
        }

        // this call 'finalizes' the topology, calculating the inverted maps in the 'economy'
        // and makes the following code run more efficiently
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();

        // a "work queue" of entities to expand; any given OID is only ever added once -
        // if already in 'scopedTopologyOIDs' it has been considered and won't be re-expanded
        Queue<Long> suppliersToExpand = Lists.newLinkedList(seedOids);

        // the queue of entities to expand "downwards"
        Queue<Long> buyersToSatisfy = Lists.newLinkedList();
        Set<Long> visited = new HashSet<>();

        // starting with the seed, expand "up"
        while (!suppliersToExpand.isEmpty()) {
            long traderOid = suppliersToExpand.remove();

            if (!topology.getTraderOids().containsValue(traderOid)) {
                // not all entities are guaranteed to be in the traders set -- the
                // market will exclude entities based on factors such as entitystate, for example.
                // If we encounter an entity that is not in the market, don't expand it any further.
                logger.debug("Skipping OID {}, as it is not in the market.", traderOid);
                continue;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("expand OID {}: {}", traderOid, topology.getTraderOids().inverse()
                        .get(traderOid).getDebugInfoNeverUseInCode());
            }
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);
            // add OIDs of traders THAT buy from this entity which we have not already added
            final List<Long> customerOids = thisTrader.getUniqueCustomers().stream()
                    .map(trader -> topology.getTraderOids().get(trader))
                    .filter(customerOid -> !scopedTopologyOIDs.contains(customerOid) &&
                            !suppliersToExpand.contains(customerOid))
                    .collect(Collectors.toList());
            if (customerOids.size() == 0) {
                // if no customers, then "start downwards" from here
                if (!visited.contains(traderOid) &&
                        // skip BusinessApplications if it is not a seed
                        (seedOids.contains(traderOid) || !entityTypesToSkip.contains(thisTrader.getType()))) {
                    buyersToSatisfy.add(traderOid);
                    visited.add(traderOid);
                }
            } else {
                // otherwise keep expanding upwards
                suppliersToExpand.addAll(customerOids);
                if (logger.isTraceEnabled()) {
                    logger.trace("add supplier oids ");
                    customerOids.forEach(oid -> logger.trace("{}: {}", oid, topology.getTraderOids()
                            .inverse().get(oid).getDebugInfoNeverUseInCode()));
                }
            }
        }
        logger.trace("top OIDs: {}", buyersToSatisfy);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        Set<Long> providersExpanded = new HashSet<>();
        // starting with buyersToSatisfy, expand "downwards"
        while (!buyersToSatisfy.isEmpty()) {
            long traderOid = buyersToSatisfy.remove();
            providersExpanded.add(traderOid);
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            // build list of sellers of markets this Trader buys from; omit Traders already expanded
            Set<Trader> potentialSellers = topology.getEconomy().getPotentialSellers(thisTrader);
            List<Long> sellersOids = potentialSellers.stream()
                            .map(trader -> topology.getTraderOids().get(trader))
                            .filter(buyerOid -> !providersExpanded.contains(buyerOid))
                            .collect(Collectors.toList());
            scopedTopologyOIDs.addAll(sellersOids);
            for (Long buyerOid : sellersOids) {
                if (visited.contains(buyerOid)) {
                    continue;
                }
                visited.add(buyerOid);
                buyersToSatisfy.add(buyerOid);
            }

            if (logger.isTraceEnabled()) {
                if (sellersOids.size() > 0) {
                    logger.trace("add buyer oids: ");
                    sellersOids.forEach(oid -> logger.trace("{}: {}", oid, topology.getTraderOids()
                            .inverse().get(oid).getDebugInfoNeverUseInCode()));
                }
            }
        }
        // return the subset of the original TraderTOs that correspond to the scoped topology
        // TODO: improve the speed of this operation by iterating over the scopedTopologyIds instead
        // of the full topology - OM-27745
        traderTOs.stream()
                .filter(traderTO -> scopedTopologyOIDs.contains(traderTO.getOid()))
                .forEach(traderTOsInScopedTopology::add);
        return traderTOsInScopedTopology;
    }

    /**
     * Determine if a TraderTO is unplaced. It is considered unplaced if any of the commodities
     * bought do not have a supplier.
     *
     * @param traderTO the TraderTO to test for suppliers
     * @return true iff any of the commodities bought have no supplier
     */
    private boolean traderIsUnplaced(@Nonnull final TraderTO traderTO) {
        return traderTO.getShoppingListsList().stream()
                .anyMatch(shoppingListTO -> !shoppingListTO.hasSupplier() ||
                        shoppingListTO.getSupplier() <= 0);
    }

    /**
     * Replay actions are used only in real-time topologies.
     * @return actions (suspend/deactivate) generated in this cycle to replay.
     */
    public ReplayActions getReplayActions() {
        return realtimeReplayActions;
    }

    /**
     * Replay actions are set only real-time topologies.
     * @param replayActions Suspend/deactivate actions from previous cycle are set to replay
     * in current analysis.
     */
    public void setReplayActions(ReplayActions replayActions) {
        this.realtimeReplayActions = replayActions;
    }

    /**
     * Get the WastedFilesAnalysis associated with this Analysis.
     * @param wastedFilesAnalysis analysis to get wasted files actions.
     * @return {@link Collection} of actions representing the wasted files or volumes.
     */
    private Collection<Action> getWastedFilesActions(WastedFilesAnalysis wastedFilesAnalysis) {
        if (topologyInfo.getAnalysisTypeList().contains(AnalysisType.WASTED_FILES)) {
            wastedFilesAnalysis.execute();
            logger.debug("Getting wasted files actions.");
            if (wastedFilesAnalysis.getState() == AnalysisState.SUCCEEDED) {
                return wastedFilesAnalysis.getActions();
            }
        }
        return Collections.emptyList();
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
}
