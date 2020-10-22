package com.vmturbo.market.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.javatuples.Triplet;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.CommodityResizeDependencyMap;
import com.vmturbo.commons.analysis.RawMaterialsMap;
import com.vmturbo.commons.analysis.UpdateFunction;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.tracing.ClassicTracer;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.ProvisionBase;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.DoubleTernaryOperator;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;


/**
 * Handle the entities received by the listener.
 */
public class TopologyEntitiesHandler {

    private TopologyEntitiesHandler() {
        // Utilities class.
    }

    private static final Logger logger = LogManager.getLogger();
    private static final String SCOPED_ANALYSIS_LABEL = "scoped";
    private static final String GLOBAL_ANALYSIS_LABEL = "global";
    public static final String PLAN_CONTEXT_TYPE_LABEL = "plan";
    public static final String LIVE_CONTEXT_TYPE_LABEL = "live";

    private static final DataMetricSummary ECONOMY_BUILD = DataMetricSummary.builder()
            .withName("mkt_economy_build_duration_seconds")
            .withHelp("Time to construct the economy.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 20) // 20 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    private static final DataMetricSummary ANALYSIS_RUNTIME = DataMetricSummary.builder()
            .withName("mkt_analysis_duration_seconds")
            .withHelp("Time to run the analysis.")
            .withLabelNames("scope_type")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 20) // 20 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    private static final DataMetricSummary ANALYSIS_ECONOMY_SIZE = DataMetricSummary.builder()
        .withName("mkt_analysis_economy_size")
        .withHelp("Number of traders in the economy for each market analysis.")
        .withLabelNames("scope_type", "context_type")
        .build()
        .register();

    /**
     * For each shoppinglist in cloudVmComputeShoppingList we compute the
     * list of providers that has enough capacity for all commodities.
     * @param computeCloudShoppingListIds compute shopping list ids of all cloud vms
     * @param topology the current topology
     * @param couponCommodityBaseType base type of coupon commodity so that it is not included in quote.
     * @return a map from the oid of the vm associated with the shoppingList to a set of
     * oids of the providers that can fit the vm.
     */

    public static Map<Long, Set<Long>> getProviderLists(Set<Long> computeCloudShoppingListIds,
                                                        Topology topology, int couponCommodityBaseType) {
        final Ede ede = new Ede();
        final Economy economy = (Economy)topology.getEconomy();
        Set<ShoppingList> cloudVmComputeShoppingList = new HashSet<>();
        BiMap<Long, ShoppingList> shoppingListBiMapInverse = topology
                .getShoppingListOids().inverse();
        for (Long shoppingListID : computeCloudShoppingListIds) {

            final ShoppingList shopList = shoppingListBiMapInverse.get(shoppingListID);
            if (shopList == null) {
                logger.error("Cannot find shopping list for shoppingListID: {}", shoppingListID);
            } else {
                cloudVmComputeShoppingList.add(shopList);
            }
        }
        return ede.getProviderLists(cloudVmComputeShoppingList, economy,
                couponCommodityBaseType);
    }

    /**
     * Create an {@link Topology} from a set of {@link TraderTO}s
     * @param traderTOs A set of trader TOs.
     * @param topologyInfo Information about the topology, including parameters for the analysis.
     * @param commsToAdjustOverheadInClone commodities to adjust overhead in clones.
     * @return The newly created topology.
     */
    public static Topology createTopology(Collection<TraderTO> traderTOs,
                                          @Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                          final List<CommoditySpecification> commsToAdjustOverheadInClone) {
        try (TracingScope scope = Tracing.trace("create_market_traders")) {
            // Sort the traderTOs based on their oids so that the input into analysis is consistent every cycle
            logger.info("Received TOs from marketComponent. Starting sorting of traderTOs.");
            final long sortStart = System.currentTimeMillis();
            SortedMap<Long, TraderTO> sortedTraderTOs = traderTOs.stream().collect(Collectors.toMap(
                TraderTO::getOid, Function.identity(), (oldTrader, newTrader) -> newTrader, TreeMap::new));
            final long sortEnd = System.currentTimeMillis();
            logger.info("Completed sorting of traderTOs. Time taken = {} seconds", ((double)(sortEnd - sortStart)) / 1000);
            logger.info("Starting economy creation on {} traders", sortedTraderTOs.size());
            final Topology topology = new Topology();
            for (final TraderTO traderTO : sortedTraderTOs.values()) {
                // If it's a trader that's added specifically for headroom calculation, don't add
                // it to the topology along with the other traders. Add it to a separate list,
                // and the market will use that list to help calculate headroom.
                if (traderTO.getTemplateForHeadroom()) {
                    topology.addTradersForHeadroom(traderTO);
                } else {
                    ProtobufToAnalysis.addTrader(topology, traderTO);
                }
            }
            // The markets in the economy must be populated with their sellers after all traders have been
            // added. Map creation is not dependent on it, but for clarity it makes sense to add all
            // sellers into markets after traders have been added.
            topology.populateMarketsWithSellersAndMergeConsumerCoverage();
            topology.setTopologyId(topologyInfo.getTopologyId());

            populateCommodityResizeDependencyMap(topology);
            if (!topologyInfo.hasPlanInfo()) {
                populateHistoryBasedResizeDependencyMap(topology);
            }
            populateProducesDependencyMap(topology);
            populateRawMaterialsMap(topology);
            commsToAdjustOverheadInClone.forEach(topology::addCommsToAdjustOverhead);
            logger.info("Created economy with " + topology.getEconomy().getMarkets().size() + " markets");
            return topology;
        }
    }

    /**
     * Create an {@link Economy} from a set of {@link TraderTO}s
     * and return the list of {@link Action}s for those TOs.
     * @param traderTOs A set of trader TOs.
     * @param topologyInfo Information about the topology, including parameters for the analysis.
     * @param analysisConfig has information about this round of analysis
     * @param analysis containing reference for replay actions.
     * @param topology the corresponding topology
     * @param ede the economy engine
     * @return The list of actions for the TOs.
     */
    public static AnalysisResults performAnalysis(Collection<TraderTO> traderTOs,
                                                  @Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                                  final AnalysisConfig analysisConfig,
                                                  final Analysis analysis,
                                                  final Topology topology,
                                                  @Nonnull final Ede ede) {
        try (TracingScope scope = Tracing.trace("perform_analysis")) {
            final long start = System.nanoTime();
            final DataMetricTimer buildTimer = ECONOMY_BUILD.startTimer();
            final Economy economy = (Economy)topology.getEconomy();
            analysis.setEconomy(economy);
            economy.setForceStop(analysis.isStopAnalysis());
            // enable estimates
            setEconomySettings(economy.getSettings(), analysisConfig);
            // compute startPriceIndex
            final PriceStatement startPriceStatement = new PriceStatement();
            startPriceStatement.computePriceIndex(economy);
            buildTimer.observe();

            final ClassicTracer classicTracer = new ClassicTracer();
            final boolean isRealtime = topologyInfo.getTopologyType() == TopologyType.REALTIME;
            final String scopeType = topologyInfo.getScopeSeedOidsCount() > 0 ?
                SCOPED_ANALYSIS_LABEL :
                GLOBAL_ANALYSIS_LABEL;
            final DataMetricTimer runTimer = ANALYSIS_RUNTIME
                .labels(scopeType)
                .startTimer();
            AnalysisResults results;

            // Generate actions
            final String marketId = topologyInfo.getTopologyType() + "-"
                + Long.toString(topologyInfo.getTopologyContextId()) + "-"
                + Long.toString(topologyInfo.getTopologyId());
            // Set replay actions.
            final @NonNull ReplayActions seedActions = isRealtime ? analysis.getReplayActions()
                : new ReplayActions();

            boolean isCloudMigrationPlan = TopologyDTOUtil.isCloudMigrationPlan(topologyInfo);
            // Set isResize to false for migration to cloud use case. Set isResize to true otherwise.
            boolean isResize = !isCloudMigrationPlan;
            // trigger suspension throttling in XL
            List<Action> actions;
            try (TracingScope ignored = Tracing.trace("first_round_analysis")) {
                actions = ede.generateActions(economy, true, true, true, isResize,
                    true, seedActions, marketId, isRealtime,
                    isRealtime ? analysisConfig.getSuspensionsThrottlingConfig() : SuspensionsThrottlingConfig.DEFAULT,
                    Optional.of(classicTracer));
            }
            final long stop = System.nanoTime();
            results = AnalysisToProtobuf.analysisResults(actions,
                topology.getShoppingListOids(), stop - start,
                topology, startPriceStatement);
            if (isRealtime) {
                // run another round of analysis on the new state of the economy with provisions enabled
                // and resize disabled. We add only the provision recommendations to the list of actions generated.
                // We neglect suspensions since there might be associated moves that we dont want to include
                //
                // This is done because in a real-time scenario, we assume that provision actions cannot be
                // automated and in order to be executed manually require a physical hardware purchase (this
                // seems like a bad assumption to hardcode for an entire category of actions rather than
                // provide a mechanism to convey the information on a per-entity basis). Given this assumption,
                // we want to do the best job of getting the customer's environment to a desired state WITHOUT
                // provision actions (market subcycle 1) and then if there are still insufficient resources
                // to meet demand, add any necessary provision actions on top of the recommendations without
                // provisions (market subcycle 2).
                AnalysisResults.Builder builder = results.toBuilder();
                economy.getSettings().setResizeDependentCommodities(false);

                // Make sure clones and only clones are suspendable. Currently suspend actions generated
                // in the second sub-cycle are discarded and only useful when collapsed with a provision
                // or activate action. Since we don't support collapsing of suspends of non-clone and
                // clone traders, there is no point in spending time to suspend the former. When the
                // corresponding functionality is implemented we should remove this loop.
                // Also, it should be fine at the time of this writing to just set suspendable to false
                // as there shouldn't be any clones in the economy at this point.
                for (Trader trader : economy.getTraders()) {
                    trader.getSettings().setSuspendable(trader.isClone());
                }
                // This is a HACK first implemented by the market for OM-31510 in legacy which subsequently
                // caused OM-33185 in XL. Because we don't want the provision actions to affect the projected topology
                // price statements given the assumption above that for real-time, provision actions take a long
                // time, and the user probably is more interested in the desired state of their topology if they
                // execute the actions that are possible to execute in the short term, we need to exclude the
                // IMPACT of the provision actions from the AnalysisResults even though we include the provision
                // actions themselves in the results. Note that if we are to ever include any of the move/start
                // actions on the newly provisioned entities, excluding the provisioned entities will cause those
                // actions to reference entities not actually in the projected topology.
                @NonNull List<Action> secondRoundActions;
                try (TracingScope unused = Tracing.trace("second_round_analysis")) {
                    secondRoundActions = ede.generateActions(economy, true, true,
                        true, false, true, false,
                        analysisConfig.getReplayProvisionsForRealTime() ? seedActions : new ReplayActions(),
                        marketId, SuspensionsThrottlingConfig.DEFAULT, Optional.of(classicTracer)).stream()
                        .filter(action -> (action instanceof ProvisionByDemand
                            || action instanceof ProvisionBySupply
                            || action instanceof Activate)
                            // Extract resize actions that explicitly set extractAction
                            // to true as part of resizeThroughSupplier
                            // provision actions.
                            || action instanceof Resize && action.isExtractAction())
                        .collect(Collectors.toList());
                }
                List<Trader> provisionedTraders = Lists.newArrayList();
                Set<Trader> resizeThroughSuppliers = Sets.newHashSet();
                for (Action action : secondRoundActions) {
                    final ActionTO actionTO = AnalysisToProtobuf.actionTO(
                        action, topology.getShoppingListOids(), topology);
                    if (actionTO != null) {
                        builder.addActions(actionTO);
                        // After action is added, find the provisioned trader
                        // to be added later in analysis results
                        if (action instanceof ProvisionBase) {
                            provisionedTraders.add(((ProvisionBase)action).getProvisionedSeller());
                        } else if (action instanceof Activate) {
                            /** Update state of traderTO that was already created
                             * because this is an existing entity.
                             * We are relying on index of economy and corresponding entry
                             * in the projected TraderTO in the builder.
                             * If someone skips some Trader in economy for converting it
                             * to TraderTO, this assumption will break.
                             */
                            builder.getProjectedTopoEntityTOBuilder(
                                action.getActionTarget().getEconomyIndex())
                                .setState(TraderStateTO.ACTIVE);
                        } else if (action instanceof Resize && action.getActionTarget().getSettings()
                            .isResizeThroughSupplier()) {
                            resizeThroughSuppliers.add(action.getActionTarget());
                        }
                    }
                }

                // Before building the results, generate traderTOs for provisioned traders from second round
                // If Action DTO is added, check if we need to add provisioned traderTO as well
                addProvisionedTraderToBuilder(builder, provisionedTraders, economy, topology);

                // Recreate Resize Through Supplier Trader in order to have the updated impact from the
                // Second round of analysis actions.
                resizeThroughSuppliers.forEach(t -> {
                    builder.setProjectedTopoEntityTO(t.getEconomyIndex(),
                        AnalysisToProtobuf.traderTO(economy, t, topology.getShoppingListOids(),
                            economy.getPreferentialShoppingLists()
                                .stream().map(sl -> sl.getBuyer()).collect(Collectors.toSet())));
                });
                results = builder.build();

                // Update replay actions
                analysis.setReplayActions(new ReplayActions(
                    secondRoundActions.stream()
                        .filter(action -> action instanceof ProvisionBySupply
                            || action instanceof Activate)
                        .collect(Collectors.toList()), // porting ProvisionByDemand not supported yet!
                    actions.stream()
                        .filter(action -> action instanceof Deactivate)
                        .map(action -> (Deactivate)action)
                        .collect(Collectors.toList()),
                    topology
                ));
            }

            runTimer.observe();

            // Capture a metric about the size of the economy analyzed
            final String contextType = topologyInfo.hasPlanInfo() ? PLAN_CONTEXT_TYPE_LABEL : LIVE_CONTEXT_TYPE_LABEL;
            ANALYSIS_ECONOMY_SIZE
                .labels(scopeType, contextType)
                .observe((double)traderTOs.size());

            logger.info("Completed analysis, with {} actions, and a projected topology of {} traders",
                results.getActionsCount(), results.getProjectedTopoEntityTOCount());
            return results;
        }
    }

    /**
     * Create TraderTOs from provisioned traders from second round
     * of real time analysis
     * @param analysisResultsBuilder    Analysis results builder to be updated
     * @param provisionedTraders        {@link List} of traders provisioned by market
     * @param economy                   {@link Economy}
     * @param topology                  {@link Topology}
     */
    private static void addProvisionedTraderToBuilder(
                    AnalysisResults.Builder analysisResultsBuilder, List<Trader> provisionedTraders,
                    Economy economy, Topology topology) {
        for (Trader provisionedTrader : provisionedTraders) {
            TraderTO pTraderTO = AnalysisToProtobuf.traderTO(economy, provisionedTrader,
                            topology.getShoppingListOids(),
                            Collections.emptySet());
            if (pTraderTO != null) {
                analysisResultsBuilder.addProjectedTopoEntityTO(pTraderTO);
                logger.trace("Provisioned trader {}, added to returned traderTOs from market",
                                provisionedTrader.getDebugInfoNeverUseInCode());
            }
        }
    }

    /**
     * Convert an update function (increment or decrement) from the commons project
     * representation to the analysis project representation.
     *
     * @param func an update function in the commons project representation
     * @return an update function in the analysis project representation
     */
    private static DoubleTernaryOperator convertUpdateFunction(UpdateFunction func) {
        UpdatingFunctionTO funcTO = CommodityResizeDependencyMap.updatingFunctionTO(func);
        return ProtobufToAnalysis.updatingFunction(funcTO);
    }

    /**
     * Convert a {@link CommodityResizeSpecification} to
     * {@link CommodityResizeSpecification}.
     *
     * @param spec a commodity resize specification from the commons package
     * @return a commodity resize specification in the analysis project
     */
    private static CommodityResizeSpecification specToSpec(
                    CommodityResizeDependencyMap.CommodityResizeDependencySpec spec) {
        return new CommodityResizeSpecification(spec.getCommodityType(),
            convertUpdateFunction(spec.getIncrementFunction()),
            convertUpdateFunction(spec.getDecrementFunction()));
    }

    /**
     * Obtain the commodity resize produces-sdependency map from the commons package, convert it and
     * put it in the topology.
     *
     * @param topology where to place the map
     */
    private static void populateProducesDependencyMap(Topology topology) {
        CommodityResizeDependencyMap.commodityResizeProducesMap.forEach((k, v) -> {
                topology.addToModifiableCommodityProducesDependencyMap(k, v);
            }
        );
    }

    /**
     * Obtain the commodity resize dependency map from the commons package, convert it and
     * put it in the topology.
     *
     * @param topology where to place the map
     */
    private static void populateCommodityResizeDependencyMap(Topology topology) {
        Map<Integer, List<CommodityResizeDependencyMap.CommodityResizeDependencySpec>> commonMap =
            CommodityResizeDependencyMap.commodityResizeDependencyMap;

        Map<Integer, List<CommodityResizeSpecification>> resizeDependencyMap
            = topology.getModifiableCommodityResizeDependencyMap();

        commonMap.forEach((k, v) ->
            resizeDependencyMap.put(k, v.stream()
                    .map(TopologyEntitiesHandler::specToSpec)
                    .collect(Collectors.toList())));
    }

    /**
     * Populates the commodity history based resize dependency skip map of a {@link Topology}.
     *
     * @param topology where to place the map
     */
    public static void populateHistoryBasedResizeDependencyMap(final @NonNull Topology topology) {
        topology.getModifiableHistoryBasedResizeSkipDependency()
            .putAll(MarketAnalysisUtils.HISTORY_BASED_RESIZE_DEPENDENCY_SKIP_MAP);
    }

    /**
     * Obtain the raw materials map and put it in the topology.
     * No conversion required.
     *
     * @param topology where to place the map
     */
    public static void populateRawMaterialsMap(Topology topology) {
        for (Map.Entry<Integer, List<Triplet<Integer, Boolean, Boolean>>> entry : RawMaterialsMap.rawMaterialsMap.entrySet()) {
            topology.getModifiableRawCommodityMap().put(entry.getKey(), new RawMaterials(entry.getValue()));
        }
    }

    private static void setEconomySettings(@Nonnull EconomySettings economySettings,
                                           @Nonnull final AnalysisConfig analysisConfig) {

        economySettings.setEstimatesEnabled(false);
        economySettings.setUseQuoteCacheDuringSNM(analysisConfig.getUseQuoteCacheDuringSNM());
        economySettings.setRightSizeLower(analysisConfig.getRightsizeLowerWatermark());
        economySettings.setRightSizeUpper(analysisConfig.getRightsizeUpperWatermark());
        if (analysisConfig.getDiscountedComputeCostFactor() > 0) {
            economySettings.setDiscountedComputeCostFactor(analysisConfig.getDiscountedComputeCostFactor());
            logger.info("Setting discounted compute cost factor with value : {}",
                analysisConfig.getDiscountedComputeCostFactor());
        }

        analysisConfig.getMaxPlacementsOverride().ifPresent(maxPlacementIterations -> {
            logger.info("Overriding economy setting max placement iterations with value: {}",
                maxPlacementIterations);
            economySettings.setMaxPlacementIterations(maxPlacementIterations);
        });
    }
}
