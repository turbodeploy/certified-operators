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
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start.SkippedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Analysis execution and properties. This can be for a scoped plan or for a real-time market.
 */
public class Analysis {
    private static final String STORAGE_CLUSTER_WITH_GROUP = "group";
    private static final String STORAGE_CLUSTER_ISO = "iso-";
    private static final String FREE_STORAGE_CLUSTER = "free_storage_cluster";

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

    private final Logger logger = LogManager.getLogger();

    // Analysis started (kept true also when it is completed).
    private AtomicBoolean started = new AtomicBoolean();

    // Analysis completed (successfully or not).
    private boolean completed = false;

    private Instant startTime = Instant.EPOCH;

    private Instant completionTime = Instant.EPOCH;

    private final Map<Long, TopologyEntityDTO> topologyDTOs;

    private final String logPrefix;

    private final TopologyConverter converter;

    private Map<Long, TopologyEntityDTO> scopeEntities = Collections.emptyMap();

    private Map<Long, ProjectedTopologyEntity> projectedEntities = null;

    private Map<Long, CostJournal<TopologyEntityDTO>> projectedEntityCosts = null;

    private final long projectedTopologyId;

    private ActionPlan actionPlan = null;

    private String errorMsg;

    private AnalysisState state;

    private ReplayActions realtimeReplayActions;

    private final TopologyInfo topologyInfo;

    /**
     * The clock used to time market analysis.
     */
    private final Clock clock;

    private final GroupServiceBlockingStub groupServiceClient;

    private final AnalysisConfig config;

    private final CloudTopology<TopologyEntityDTO> originalCloudTopology;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final TopologyCostCalculator topologyCostCalculator;

    private final MarketPriceTable marketPriceTable;

    private final WastedFilesAnalysis wastedFilesAnalysis;

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
     */
    public Analysis(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                    @Nonnull final GroupServiceBlockingStub groupServiceClient,
                    @Nonnull final Clock clock,
                    @Nonnull final AnalysisConfig analysisConfig,
                    @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                    @Nonnull final TopologyCostCalculatorFactory cloudCostCalculatorFactory,
                    @Nonnull final MarketPriceTableFactory priceTableFactory,
                    @Nonnull final WastedFilesAnalysisFactory wastedFilesAnalysisFactory) {
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
        this.topologyCostCalculator = cloudCostCalculatorFactory.newCalculator(topologyInfo);
        this.originalCloudTopology = this.cloudTopologyFactory.newCloudTopology(topologyDTOs.stream());

        // Use the cloud cost data we use for cost calculations for the price table.
        this.marketPriceTable = priceTableFactory.newPriceTable(
                this.originalCloudTopology, this.topologyCostCalculator.getCloudCostData());
        this.converter = new TopologyConverter(topologyInfo,
            analysisConfig.getIncludeVdc(), analysisConfig.getQuoteFactor(),
            analysisConfig.getLiveMarketMoveCostFactor(),
            this.marketPriceTable,
            null,
            this.topologyCostCalculator.getCloudCostData(),
            CommodityIndex.newFactory());
        this.wastedFilesAnalysis = wastedFilesAnalysisFactory.newWastedFilesAnalysis(topologyInfo,
            topologyDTOs, this.clock, topologyCostCalculator, marketPriceTable);

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
        if (started.getAndSet(true)) {
            logger.error(" {} Completed or being computed", logPrefix);
            return false;
        }
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

            Set<TraderTO> traderTOs = converter.convertToMarket(topologyDTOs);
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
            }

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

            final AnalysisResults results = TopologyEntitiesHandler.performAnalysis(traderTOs,
                topologyInfo, config, this);

            final DataMetricTimer processResultTime = RESULT_PROCESSING.startTimer();
            // add shoppinglist from newly provisioned trader to shoppingListOidToInfos
            converter.updateShoppingListMap(results.getNewShoppingListToBuyerEntryList());
            logger.info(logPrefix + "Done performing analysis");

            List<TraderTO> projectedTraderDTO = new ArrayList<>();
            // retrieve entities which were not converted so that they can be added to the projected
            // topology
            List<ProjectedTopologyEntity> projectedEntitiesFromOriginalTopo =
                    originalCloudTopology.getAllEntitesOfTypes(
                            TopologyConversionConstants.STATIC_INFRASTRUCTURE)
                            .stream().map(p -> ProjectedTopologyEntity.newBuilder()
                                    .setEntity(p).build()).collect(Collectors.toList());
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
                    results.getPriceIndexMsg(), topologyCostCalculator.getCloudCostData());
                projectedEntitiesFromOriginalTopo.forEach(projectedEntity -> {
                    final ProjectedTopologyEntity existing =
                        projectedEntities.put(projectedEntity.getEntity().getOid(), projectedEntity);
                    if (existing != null && !projectedEntity.equals(existing)) {
                        logger.error("Existing projected entity overwritten by entity from " +
                            "original topology. Existing (converted from market): {}\nOriginal: {}",
                            existing, projectedEntity);
                    }
                });

                // Calculate the projected entity costs.
                final CloudTopology<TopologyEntityDTO> projectedCloudTopology =
                        cloudTopologyFactory.newCloudTopology(projectedEntities.values().stream()
                                .filter(ProjectedTopologyEntity::hasEntity)
                                .map(ProjectedTopologyEntity::getEntity));
                // Projected RI coverage has been calculated by convertFromMarket
                // Get it from TopologyCoverter and pass it along to use for calculation of savings
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
            results.getActionsList().stream()
                    .map(action -> converter.interpretAction(action, projectedEntities,
                            this.originalCloudTopology, projectedEntityCosts,
                            topologyCostCalculator))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(actionPlanBuilder::addAction);
            // TODO move wasted files action out of main analysis once we have a framework
            // to support multiple analyses for the same topology ID
            actionPlanBuilder.addAllAction(getWastedFilesActions());
            logger.info(logPrefix + "Completed successfully");
            processResultTime.observe();
            state = AnalysisState.SUCCEEDED;

            completionTime = clock.instant();
            actionPlan = actionPlanBuilder.setAnalysisCompleteTimestamp(completionTime.toEpochMilli())
                .build();
        } catch (RuntimeException e) {
            logger.error(logPrefix + e + " while running analysis", e);
            state = AnalysisState.FAILED;
            completionTime = clock.instant();
            errorMsg = e.toString();
        }

        logger.info(logPrefix + "Execution time : "
                + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
        completed = true;
        return true;
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
            Set<TopologyEntityDTO> pmEntityDTOs = getEntityDTOsInCluster(ClusterInfo.Type.COMPUTE);
            Set<TopologyEntityDTO> dsEntityDTOs = getEntityDTOsInCluster(ClusterInfo.Type.STORAGE);
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

    protected Set<TopologyEntityDTO> getEntityDTOsInCluster(ClusterInfo.Type clusterInfo) {
        Set<TopologyEntityDTO> entityDTOs = new HashSet<>();
        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .addTypeFilter(Type.CLUSTER)
            .setClusterFilter(ClusterFilter.newBuilder()
                .setTypeFilter(clusterInfo).build())
            .build())
            .forEachRemaining(grp -> {
                for (long i : grp.getCluster().getMembers().getStaticMemberOidsList()) {
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
        if (comm.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE) {
            return !comm.getCommodityType().getKey().toLowerCase()
                    .startsWith(STORAGE_CLUSTER_WITH_GROUP) && !comm.getCommodityType().getKey()
                    .toLowerCase().startsWith(STORAGE_CLUSTER_ISO) && !comm.getCommodityType()
                    .getKey().toLowerCase().equals(FREE_STORAGE_CLUSTER);
        }
        return false;
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
                || (!topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN_TYPE) &&
                        !topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.CLOUD_MIGRATION_PLAN_TYPE)));
    }

    /**
     * Get the OIDs of entities skipped during conversion of {@link TopologyEntityDTO}s to
     * {@link TraderTO}s.
     *
     * @return A set of the OIDS of entities skipped during conversion.
     */
    @Nonnull
    public Set<SkippedEntity> getSkippedEntities() {
        if (!isDone()) {
            throw new IllegalStateException("Attempting to get skipped entities before analysis is done.");
        }
        return converter.getSkippedEntities().stream()
            .map(entity -> SkippedEntity.newBuilder()
                .setOid(entity.getOid())
                .setEntityType(entity.getEntityType())
                .setEnvironmentType(entity.getEnvironmentType())
                .build())
            .collect(Collectors.toSet());
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
     * The state of an analysis run.
     *
     * <p>An analysis run starts in the {@link #INITIAL} state when it is created. If it then gets
     * executed via the {@link MarketRunner} then it transitions to {@link #QUEUED} when it is
     * placed in the queue for execution. When the {@link Analysis#execute} method is invoked it
     * goes into {@link #IN_PROGRESS}, and when the run completes it goes into {@link #SUCCEEDED}
     * if it completed successfully, or to {@link #FAILED} if it completed with an exception.
     */
    public enum AnalysisState {
        /**
         * The analysis object was created, but not yet queued or started.
         */
        INITIAL,
        /**
         * The analysis is queued for execution.
         */
        QUEUED,
        /**
         * The analysis was removed from the queue and is currently running.
         */
        IN_PROGRESS,
        /**
         * Analysis completed successfully.
         */
        SUCCEEDED,
        /**
         * Analysis completed with an exception.
         */
        FAILED
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
        topology.populateMarketsWithSellers();

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
                if (!visited.contains(traderOid)) {
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
     *
     * @return {@link Collection} of actions representing the wasted files or volumes.
     */
    private Collection<Action> getWastedFilesActions() {
        // only generate wasted files actions for the realtime market
        if (!topologyInfo.hasPlanInfo() &&
            topologyInfo.getAnalysisTypeList().contains(AnalysisType.WASTED_FILES)) {
            wastedFilesAnalysis.execute();
            logger.debug("Getting wasted files actions.");
            if (wastedFilesAnalysis.getState() == AnalysisState.SUCCEEDED) {
                return wastedFilesAnalysis.getActions();
            }
        };
        return Collections.emptyList();
    }
}
