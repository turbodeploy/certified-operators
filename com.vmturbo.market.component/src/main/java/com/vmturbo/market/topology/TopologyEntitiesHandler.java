package com.vmturbo.market.topology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.CommodityResizeDependencyMap;
import com.vmturbo.commons.analysis.RawMaterialsMap;
import com.vmturbo.commons.analysis.UpdateFunction;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
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
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 20) // 20 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * Create an {@link Economy} from a set of {@link TraderTO}s
     * and return the list of {@link Action}s for those TOs.
     * @param traderTOs A set of trader TOs.
     * @param topologyInfo Information about the topology, including parameters for the analysis.
     * @return The list of actions for the TOs.
     */
    public static AnalysisResults performAnalysis(Set<TraderTO> traderTOs,
                    @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
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
        economy.getSettings().setEstimatesEnabled(true);
        // compute startPriceIndex
        final PriceStatement startPriceStatement = new PriceStatement();
        startPriceStatement.computePriceIndex(economy);
        final Ede ede = new Ede();
        buildTimer.observe();

        final boolean isRealtime = topologyInfo.getTopologyType() == TopologyType.REALTIME;
        final DataMetricTimer runTimer = ANALYSIS_RUNTIME.startTimer();
        final List<Action> actions;

        // The current implementation of headroom plans in M2 requires a different method call
        // to calculate headroom actions. When/if we switch to the FMO implementation (calculating
        // headroom based on the projected topology output of a regular plan) we can get rid
        // of this.
        if (!economy.getTradersForHeadroom().isEmpty()) {
            // Sanity check that the conditions we expect are correct.
            if (isRealtime ||
                    topologyInfo.getPlanInfo().getPlanType() != PlanProjectType.CLUSTER_HEADROOM) {
                throw new IllegalStateException("Attempting to generate headroom actions for a " +
                        "non-headroom analysis request! Topology info: " + topologyInfo);
            }
            actions = ede.generateHeadroomActions(economy, false, false, false, true);
        } else {
            // Generate actions
            final String marketId = topologyInfo.getTopologyType() + "-"
                    + Long.toString(topologyInfo.getTopologyContextId()) + "-"
                    + Long.toString(topologyInfo.getTopologyId());
            actions = ede.generateActions(economy, true,
                    true, true, true, true, marketId, isRealtime);
        }
        final long stop = System.nanoTime();

        AnalysisResults results = AnalysisToProtobuf.analysisResults(actions, topology.getTraderOids(),
                topology.getShoppingListOids(), stop - start,
                topology, startPriceStatement);
        runTimer.observe();

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
}
