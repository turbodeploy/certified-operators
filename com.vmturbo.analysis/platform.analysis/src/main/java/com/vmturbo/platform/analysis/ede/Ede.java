package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionCollapse;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.ActionStats;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation.CommodityBundle;
import com.vmturbo.platform.analysis.utilities.M2Utils;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.analysis.utilities.PlacementStats;
import com.vmturbo.platform.analysis.utilities.QuoteTracker;
import com.vmturbo.platform.analysis.utilities.StatsManager;
import com.vmturbo.platform.analysis.utilities.StatsUtils;
import com.vmturbo.platform.analysis.utilities.StatsWriter;

/**
 *
 * The Economic Decisions Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public final class Ede {

    private PlacementResults edePlacementResults;

    // Constructor

    /**
     * Constructs the Economic Decisions Engine, with an empty initial State
     */
    public Ede() {}

    static final Logger logger = LogManager.getLogger(Ede.class);

    /**
     * For each shopping list in shoppingLists we compute the
     * list of providers that has enough capacity for all commodities.
     * @param shoppingLists the shopping list set of interest
     * @param economy The snapshot of the economy
     * @param couponCommodityBaseType base type of coupon commodity so that it is not included in quote.
     * @return a map from a VM's OID associated with the shopping list to a set of
     * provider OIDs that the VM can fit into.
     */
    public @NonNull Map<Long, Set<Long>> getProviderLists(
            Set<ShoppingList> shoppingLists, Economy economy, int couponCommodityBaseType) {
        Map<Long, Set<Long>>  providerListMap = new HashMap<>();
        for (ShoppingList shoppingList : shoppingLists) {
            Set<Long> providerList = new HashSet<>();
            for (Trader trader : economy.getMarket(shoppingList)
                    .getActiveSellersAvailableForPlacement()) {
                if (CostFunctionFactory.insufficientCommodityWithinSellerCapacityQuote(shoppingList, trader, couponCommodityBaseType).isFinite()) {
                    providerList.add(trader.getOid());
                }
            }
            Long buyerID = shoppingList.getBuyer().getOid();
            providerListMap.put(buyerID, providerList);
        }
        return providerListMap;
    }

    /**
     * Returns a new set of actions for a snapshot of the economy.
     *
     * <p>This is an overloaded version providing default values for a number of arguments.</p>
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param mktName contains the market name and any stats to be written to the row
     *          delimited by "|"
     *
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, ReplayActions, String)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
            boolean classifyActions, boolean isProvision, boolean isSuspension, boolean isResize,
            String mktName) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, new ReplayActions(), mktName,
                               SuspensionsThrottlingConfig.DEFAULT);
    }

    /**
     * Returns a new set of actions for a snapshot of the economy.
     *
     * <p>This is an overloaded version providing default values for a number of arguments.</p>
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     *
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, ReplayActions, String)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
            boolean classifyActions, boolean isProvision, boolean isSuspension, boolean isResize) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, new ReplayActions(), "unspecified|",
                               SuspensionsThrottlingConfig.DEFAULT);
    }

    /**
     * Returns a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @param isReplay True if we want to run replay algorithm and false otherwise
     * @param seedActions Actions that should be used to "seed" various analysis algorithms. This
     *                    can be for stability or performance. e.g. to deactivate the same traders
     *                    as last cycle or to start with more supply to spend less time in provision
     *                    algorithm.
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @param suspensionsThrottlingConfig level of Suspension throttling.
     *         CLUSTER: Make co sellers of suspended seller suspendable false.
     *         DEFAULT: Unlimited suspensions.
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                    boolean classifyActions, boolean isProvision, boolean isSuspension,
                    boolean isResize, boolean collapse, boolean isReplay,
                    @NonNull ReplayActions seedActions, String mktData,
                    SuspensionsThrottlingConfig suspensionsThrottlingConfig) {
        String analysisLabel = "Analysis ";
        logger.info(analysisLabel + "Started.");
        @NonNull List<Action> actions = new ArrayList<>();
        ActionStats actionStats = new ActionStats(actions, M2Utils.getTopologyId(economy));
        ActionClassifier classifier = null;
        if (classifyActions) {
            try {
                classifier = new ActionClassifier(economy);
            }
            catch (ClassNotFoundException | IOException e) {
                logger.error(analysisLabel + "Could not copy economy.", e);
                return actions;
            }
        }
        //Parse market stats data coming from Market1, add prepare to write to stats file.
        String data[] = StatsUtils.getTokens(mktData, "|");
        if (data.length <= 1) {
            // XL file name is in contextid-topologyid format
            data = StatsUtils.getTokens(mktData, "-");
            if (data.length < 1 || !data[0].equals("777777")) { // Real market context id
                data = new String[1];
                data[0] = "plan";
            }
        }
        StatsUtils statsUtils = new StatsUtils("m2stats-" + data[0], false);
        if (data.length == 3) {
            statsUtils.append(data[1]); // date, Time, topo name , topo send time from M1
            statsUtils.after(Instant.parse(data[2]));
        } else {
            statsUtils.appendHeader("Date, Time,Topology,Topo Send Time,"
                                    + "Bootstrap Time, Initial Place Time,Resize Time,"
                                    + "Placement Time,Provisioning Time,Suspension Time,"
                                    + "Total Plan Time,Total Actions");
            //currently 4 columns for topology related data
            statsUtils.appendDate(true, false);
            statsUtils.appendTime(false, false);
            statsUtils.append(mktData); // if not in expected parse format, should contain
                                        // contextid-topologyid, different for each plan
            statsUtils.append("NA"); // topology send time if unavailable
        }
        // Sort the buyers of each market based on the current quote (on-prem) or
        // current cost (cloud) sorted high to low.
        if (economy.getSettings().getSortShoppingLists()) {
            economy.sortBuyersofMarket();
        }

        // create a subset list of markets that have at least one buyer that can move
        economy.composeMarketSubsetForPlacement();

        Ledger ledger = new Ledger(economy);

        logBasketSoldOfTradersWithDebugEnabled(economy.getTraders());

        if (logger.isTraceEnabled()) {
            logger.trace("PSL relinquished VMs:");
            for(ShoppingList shoppingList : economy.getPreferentialShoppingLists()){
                logger.trace("PSL: " + shoppingList.toString());
            }
        }

        //sort the PreferentialShoppingList
        long start = System.currentTimeMillis();
        if (economy.getSettings().getSortShoppingLists()) {
            economy.sortPreferentialShoppingLists();
        }
        long end = System.currentTimeMillis();
        if (logger.isTraceEnabled()) {
            logger.trace("PSL Execution time of sortShoppingLists of size: "
                    + economy.getPreferentialShoppingLists().size() + " is: " + (end - start) + " milliseconds");
        }

        // generate moves for preferential shoppingLists
        List<Action> preferentialActions = new ArrayList<>(Placement.prefPlacementDecisions(economy,
                economy.getPreferentialShoppingLists()).getActions());

        if (logger.isTraceEnabled()) {
            logger.trace("PSL Actions: ");
            preferentialActions.stream()
                .flatMap(action -> {
                    if (action.getType() == ActionType.MOVE) {
                        return Stream.of(action);
                    } else if (action.getType() == ActionType.COMPOUND_MOVE) {
                        return ((CompoundMove) action).getConstituentMoves().stream();
                    }
                    return Stream.empty();
                })
                .forEach(action -> {
                    Move moveAction = (Move) action;
                    logger.trace("PSL Action: Destination is " +
                                    moveAction.getDestination() +
                                    " for the VM: " +
                                    moveAction.getTarget());
                });
        }

        actions.addAll(preferentialActions);
        logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "inactive/idle Trader placement");

        // Start by provisioning enough traders to satisfy all the demand
        // Save first call to before() to calculate total plan time
        Instant begin = statsUtils.before();
        if (isProvision) {
            actions.addAll(seedActions.replayActions(economy));
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "provision replay");
            // time to run provision replay
            statsUtils.after();

            statsUtils.before();
            actions.addAll(BootstrapSupply.bootstrapSupplyDecisions(economy));
            ledger = new Ledger(economy);
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "bootstrap");
            // time to run bootstrap
            statsUtils.after();

            statsUtils.before();
            // run placement algorithm to balance the environment
            actions.addAll(Placement.runPlacementsTillConverge(
                economy, ledger, EconomyConstants.PLACEMENT_PHASE).getActions());
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "placement");
            // time to run initial placement
            statsUtils.after();
        }

        statsUtils.before();
        if (isResize) {
            actions.addAll(Resizer.resizeDecisions(economy, ledger));
        }
        logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "resizing");
        // resize time
        statsUtils.after();

        if (isReplay) {
            actions.addAll(seedActions.tryReplayDeactivateActions(economy, ledger, suspensionsThrottlingConfig));
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "replaying");
        }

        statsUtils.before();
        PlacementResults placementResults = Placement.runPlacementsTillConverge(economy, ledger,
            EconomyConstants.PLACEMENT_PHASE);
        actions.addAll(placementResults.getActions());
        logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "placement");
        // placement time
        statsUtils.after();

        statsUtils.before();
        // trigger provision, suspension and resize algorithm only when needed
        int oldActionCount = actions.size();
        if (isProvision) {
            actions.addAll(Provision.provisionDecisions(economy, ledger));
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "provisioning");
            // if provision generated some actions, run placements
            if (actions.size() > oldActionCount) {
                placementResults = Placement.runPlacementsTillConverge(economy, ledger,
                    EconomyConstants.PLACEMENT_PHASE);
                actions.addAll(placementResults.getActions());
                logger.info(actionStats.phaseLogEntry("post provisioning placement"));
            }
        }
        // provisioning time
        statsUtils.after();

        statsUtils.before();
        if (isSuspension) {
            Suspension suspension = new Suspension(suspensionsThrottlingConfig);
            // find if any seller is the sole provider in any market, if so, it should not
            // be considered as suspension candidate
            suspension.findSoleProviders(economy);
            actions.addAll(suspension.suspensionDecisions(economy, ledger));
            logPhaseAndClearPlacementStats(actionStats, economy.getPlacementStats(), "suspending");
        }
        // suspension time
        statsUtils.after();

        if (collapse && !economy.getForceStop()) {
            List<@NonNull Action> collapsed = ActionCollapse.collapsed(actions);
            // Reorder actions by type.
            actions = ActionCollapse.groupActionsByTypeAndReorderBeforeSending(collapsed);
            actionStats.setActionsList(actions);
        }

        // mark non-executable actions
        if (classifyActions && classifier != null) {
            classifier.classify(actions);
        }
        logger.info(actionStats.finalLogEntry());
        // total time to run plan
        statsUtils.after(begin);
        // file total actions
        statsUtils.append(actions.size(), false, false);
        StatsWriter statsWriter = StatsManager.getInstance().init();
        statsWriter.add(statsUtils);

        if (logger.isDebugEnabled()) {
            // Log number of actions by type
            actions.stream()
                            .collect(Collectors.groupingBy(Action::getClass))
                            .forEach((k, v) -> logger
                                            .debug("    " + k.getSimpleName() + " : " + v.size()));
        }
        // Reconfigures generated within Placement or Bootstrap do not have a quoteTracker.
        // We can not create quoteTrackers at the moment when reconfigure was created, especially in
        // the Bootstrap reconfigure case, because PlacementResults is not available. In order to
        // keep quoteTrackers in PlacementResults for reconfigure traders, at the end of Ede, we
        // call addQuoteTrackerForEmptyReconfigures.
        placementResults.createQuoteTrackerForReconfigures(actions);
        placementResults.populateExplanationForInfinityQuoteTraders();
        logInfiniteQuoteTraders(placementResults);
        this.edePlacementResults = placementResults;
        return actions;
    }

    /**
     * Create a new set of actions for a realtime-snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @param seedActions Actions that should be used to "seed" various analysis algorithms.
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @param isRealTime True for analysis of a realtime topology
     * @param suspensionsThrottlingConfig level of Suspension throttling.
     *         CLUSTER: Make co sellers of suspended seller suspendable false.
     *         DEFAULT: Unlimited suspensions.
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
            boolean classifyActions, boolean isProvision, boolean isSuspension, boolean isResize,
            boolean collapse, @NonNull ReplayActions seedActions, String mktData,
            boolean isRealTime, SuspensionsThrottlingConfig suspensionsThrottlingConfig) {
        @NonNull List<Action> actions = new ArrayList<>();
        // only run replay in first sub round for realTime market.
        if (isRealTime) {
            economy.getSettings().setResizeDependentCommodities(false);
            // run a round of analysis without provisions.
            actions.addAll(generateActions(economy, classifyActions, false, isSuspension,
                            isResize, collapse, true, seedActions, mktData,
                            suspensionsThrottlingConfig));
        } else {
            actions.addAll(generateActions(economy, classifyActions, isProvision,
                            isSuspension, isResize, collapse, false, new ReplayActions(), mktData,
                            suspensionsThrottlingConfig));
        }

        return actions;
    }

    /**
     * Run a capacity plan using the economic scheduling engine and generate a new set of actions.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @return A list of actions suggested after running a capacityPlan using
     *          the the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateHeadroomActions(@NonNull Economy economy,
                    boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        // create a subset list of markets that have at least one buyer that can move
        economy.composeMarketSubsetForPlacement();
        @NonNull List<Action> actions = new ArrayList<>();
        // balance entities in plan
        actions.addAll(generateActions(economy, false, isProvision, isSuspension, isResize));
        logger.info("Plan completed initial placement with " + actions.size() + " actions.");
        int oldIndex = economy.getTraders().size();
        // add clones of sampleSE to the economy
        economy.getTradersForHeadroom().forEach(template -> ProtobufToAnalysis.addTrader(
                                        economy.getTopology(), template));

        Set<Market> newMarkets = new HashSet<>();
        for (int i=oldIndex; i<economy.getTraders().size(); i++) {
            newMarkets.addAll(economy.getMarketsAsBuyer(economy.getTraders().get(i)).values());
        }

        for (Market market : newMarkets) {
            economy.populateMarketWithSellers(market);
        }

        int oldActionCount = actions.size();

        actions.addAll(generateActions(economy, false, isProvision, isSuspension, isResize));
        logger.info("Plan completed placement after adding demand with " + (actions.size() - oldActionCount)
                    + " actions.");
        return actions;
    }

    /**
     * Log basket sold of traders for which debug has been enabled from advanced tab.
     */
    private void logBasketSoldOfTradersWithDebugEnabled(@NonNull final
                                                        List<@NonNull @ReadOnly Trader> traders) {
        traders.stream().filter(trader -> trader.isDebugEnabled()).forEach(trader ->
                logger.debug("Basket sold by {}:\n{}", trader.getDebugInfoNeverUseInCode(),
                        trader.getBasketSold().toDebugString()));
    }

    /**
     * Log messages to describe why traders could not be placed.
     *
     * @param placementResults The results describing why traders could not be placed.
     */
    private void logInfiniteQuoteTraders(@NonNull final PlacementResults placementResults) {
        try { // As we will print out log messages in the market component for best explanations
              // for infinity trader, change this to debug level as it will list all sellers giving
              // infinity.
            if (logger.isDebugEnabled()) {
                placementResults.getInfinityQuoteTraders().forEach((trader, quoteTrackers) ->
                        logger.debug("Infinity quote trader " + trader + " due to:\n\t"
                                + quoteTrackers.stream()
                                        .filter(QuoteTracker::hasQuotesToExplain)
                                        .map(QuoteTracker::explainAllInfiniteQuotes)
                                        .flatMap(List::stream)
                                        .map(ex -> printExplanation(ex))
                                        .collect(Collectors.toList())));
            }
        } catch (Exception e) {
            logger.error("Error during unplaced trader logging: ", e);
        }
    }

    /**
     * A string explanation for a given {@link InfiniteQuoteExplanation}. This method is only for
     * logging purpose in analytics. {@link InfiniteQuoteExplanation} object is the one sent out
     * from analysis library.
     *
     * @param ex The given {@link InfiniteQuoteExplanation}.
     * @return a readable string.
     */
    private String printExplanation(InfiniteQuoteExplanation ex) {
        StringBuilder sb = new StringBuilder();
        sb.append("shopping list");
        if (ex.providerType.isPresent()) {
            sb.append(" looking for provider type ").append(ex.providerType.get());
        }
        if (ex.costUnavailable) {
            sb.append(" missing cost data");
            return sb.toString();
        }
        if (ex.seller.isPresent()) {
            sb.append(" from seller ").append(ex.seller.get().getDebugInfoNeverUseInCode());
        }
        if (!ex.commBundle.isEmpty()) {
            sb.append(" asking for commodity ");
        }
        for (CommodityBundle c : ex.commBundle) {
            sb.append(" { ").append(c.commSpec.getDebugInfoNeverUseInCode())
                    .append(" with requested amount ").append(c.requestedAmount);
            if (c.maxAvailable.isPresent()) {
                sb.append(" and available amount is ").append(c.maxAvailable.get());
            }
            sb.append(" } ");
        }
        return sb.toString();
    }

    /**
     * Create a log entry for the phase with the given name and clear placement stats after logging them.
     *
     * @param actionStats Statistics for actions for the phase.
     * @param placementStats Statistics for placements performed during the phase.
     * @param phaseName The name of the phase.
     */
    private void logPhaseAndClearPlacementStats(@NonNull final ActionStats actionStats,
                                                @NonNull final PlacementStats placementStats,
                                                @NonNull final String phaseName) {
        logger.info(actionStats.phaseLogEntry(phaseName, placementStats));
        placementStats.clear();
    }

    /**
     * Returns the {@link PlacementResults}.
     *
     * @return PlacementResults
     */
    public PlacementResults getPlacementResults() {
        return this.edePlacementResults;
    }

}
