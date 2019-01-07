package com.vmturbo.market.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.CommodityResizeDependencyMap;
import com.vmturbo.commons.analysis.RawMaterialsMap;
import com.vmturbo.commons.analysis.UpdateFunction;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
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
     * Create an {@link Economy} from a set of {@link TraderTO}s
     * and return the list of {@link Action}s for those TOs.
     * @param traderTOs A set of trader TOs.
     * @param topologyInfo Information about the topology, including parameters for the analysis.
     * @param analysis containing reference for replay actions.
     * @return The list of actions for the TOs.
     */
    public static AnalysisResults performAnalysis(Set<TraderTO> traderTOs,
                                                  @Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                                  final AnalysisConfig analysisConfig,
                                                  final Analysis analysis) {
        logger.info("Received TOs from marketComponent. Starting economy creation on {} traders",
                traderTOs.size());
        final long start = System.nanoTime();
        final DataMetricTimer buildTimer = ECONOMY_BUILD.startTimer();
        final Topology topology = new Topology();
        for (final TraderTO traderTO : traderTOs) {
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
        topology.populateMarketsWithSellers();

        populateCommodityResizeDependencyMap(topology);
        populateRawMaterialsMap(topology);
        commToAllowOverheadInClone(topology);

        final Economy economy = (Economy)topology.getEconomy();
        // enable estimates
        setEconomySettings(economy.getSettings(), analysisConfig);
        // compute startPriceIndex
        final PriceStatement startPriceStatement = new PriceStatement();
        startPriceStatement.computePriceIndex(economy);
        final Ede ede = new Ede();
        buildTimer.observe();

        final boolean isRealtime = topologyInfo.getTopologyType() == TopologyType.REALTIME;
        final String scopeType = topologyInfo.getScopeSeedOidsCount() > 0 ?
            SCOPED_ANALYSIS_LABEL :
            GLOBAL_ANALYSIS_LABEL;
        final DataMetricTimer runTimer = ANALYSIS_RUNTIME
            .labels(scopeType)
            .startTimer();
        final List<Action> actions;
        AnalysisResults results;

        // Generate actions
        final String marketId = topologyInfo.getTopologyType() + "-"
                + Long.toString(topologyInfo.getTopologyContextId()) + "-"
                + Long.toString(topologyInfo.getTopologyId());
        // Set replay actions.
        if (isRealtime) {
            ede.setReplayActions(analysis.getReplayActions() == null ? new ReplayActions()
                            : analysis.getReplayActions());
        }
        // trigger suspension throttling in XL
        actions = ede.generateActions(economy, true, true, true, true,
                true, marketId, isRealtime,
                isRealtime ? analysisConfig.getSuspensionsThrottlingConfig() : SuspensionsThrottlingConfig.DEFAULT);
        final long stop = System.nanoTime();

        results = AnalysisToProtobuf.analysisResults(actions, topology.getTraderOids(),
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
            @NonNull List<Action> secondRoundActions = new ArrayList<>();
            AnalysisResults.Builder builder = results.toBuilder();
            economy.getSettings().setResizeDependentCommodities(false);
            // This is a HACK first implemented by the market for OM-31510 in legacy which subsequently
            // caused OM-33185 in XL. Because we don't want the provision actions to affect the projected topology
            // price statements given the assumption above that for real-time, provision actions take a long
            // time, and the user probably is more interested in the desired state of their topology if they
            // execute the actions that are possible to execute in the short term, we need to exclude the
            // IMPACT of the provision actions from the AnalysisResults even though we include the provision
            // actions themselves in the results. Note that if we are to ever include any of the move/start
            // actions on the newly provisioned entities, excluding the provisioned entities will cause those
            // actions to reference entities not actually in the projected topology.
            secondRoundActions.addAll(ede.generateActions(economy, true, true,
                true, false, true, false, marketId).stream()
                .filter(action -> (action instanceof ProvisionByDemand ||
                    action instanceof ProvisionBySupply ||
                    action instanceof Activate))
                .collect(Collectors.toList()));
            for (Action action : secondRoundActions) {
                final ActionTO actionTO = AnalysisToProtobuf.actionTO(
                    action, topology.getTraderOids(), topology.getShoppingListOids(), topology);
                if (actionTO != null) {
                    builder.addActions(actionTO);
                }
            }
            results = builder.build();

            // Update replay actions
            ReplayActions newReplayActions = ede.getReplayActions();
            newReplayActions.setTraderOids(topology.getTraderOids());
            newReplayActions.setActions(actions.stream()
                            .filter(action -> action.getType().equals(ActionType.DEACTIVATE))
                            .collect(Collectors.toList()));
            analysis.setReplayActions(newReplayActions);
        }

        runTimer.observe();

        // Capture a metric about the size of the economy analyzed
        final String contextType = topologyInfo.hasPlanInfo() ? PLAN_CONTEXT_TYPE_LABEL : LIVE_CONTEXT_TYPE_LABEL;
        ANALYSIS_ECONOMY_SIZE
            .labels(scopeType, contextType)
            .observe((double) traderTOs.size());

        logger.info("Completed analysis, with {} actions, and a projected topology of {} traders",
                results.getActionsCount(), results.getProjectedTopoEntityTOCount());
        return results;
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
     * Convert a {@link CommodityResizeDependencyMap.CommodityResizeDependencySpec} to
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
     * Obtain the raw materials map and put it in the topology.
     * No conversion required.
     *
     * @param topology where to place the map
     */
    private static void populateRawMaterialsMap(Topology topology) {
        topology.getModifiableRawCommodityMap().putAll(RawMaterialsMap.rawMaterialsMap);
    }

    private static void commToAllowOverheadInClone(Topology topology) {
        AnalysisUtil.COMM_TYPES_TO_ALLOW_OVERHEAD.stream()
            .map(CommoditySpecification::new)
            .forEach(topology::addCommsToAdjustOverhead);
    }

    private static void setEconomySettings(@Nonnull EconomySettings economySettings,
                                           @Nonnull final AnalysisConfig analysisConfig) {

        economySettings.setEstimatesEnabled(false);
        economySettings.setRightSizeLower(analysisConfig.getRightsizeLowerWatermark());
        economySettings.setRightSizeUpper(analysisConfig.getRightsizeUpperWatermark());

        analysisConfig.getGlobalSetting(GlobalSettingSpecs.RateOfResize).ifPresent(rateOfResize -> {
            if (rateOfResize.hasNumericSettingValue()) {
                economySettings.setRateOfResize(rateOfResize.getNumericSettingValue().getValue());
            }
        });

        analysisConfig.getMaxPlacementsOverride().ifPresent(maxPlacementIterations -> {
            logger.info("Overriding economy setting max placement iterations with value: {}",
                maxPlacementIterations);
            economySettings.setMaxPlacementIterations(maxPlacementIterations);
        });
    }
}
