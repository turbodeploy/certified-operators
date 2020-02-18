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
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.ActionStats;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.M2Utils;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.analysis.utilities.PlacementStats;
import com.vmturbo.platform.analysis.utilities.ProvisionUtils;
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

    // Fields
    private transient @NonNull ReplayActions replayActions_;

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
     * @return a map from a VM's OID associated with the shopping list to a set of
     * provider OIDs that the VM can fit into.
     */
    public @NonNull Map<Long, Set<Long>> getProviderLists(
            Set<ShoppingList> shoppingLists, Economy economy) {
        Map<Long, Set<Long>>  providerListMap = new HashMap<>();
        for (ShoppingList shoppingList : shoppingLists) {
            Set<Long> providerList = new HashSet<>();
            for (Trader trader : economy.getMarket(shoppingList)
                    .getActiveSellersAvailableForPlacement()) {
                Trader tp = AnalysisToProtobuf.replaceNewSupplier(shoppingList, economy, trader);
                // if  trader is a cbtp convert it to tp
                if (tp != null) {
                    trader = tp;
                }
                if (CostFunctionFactory.insufficientCommodityWithinSellerCapacityQuote(shoppingList, trader, -1).isFinite()) {
                    providerList.add( economy.getTopology().getTraderOids().get(trader));
                }
            }
            Long buyerID = economy.getTopology().getTraderOids().get(shoppingList.getBuyer());
            providerListMap.put(buyerID, providerList);
        }
        return providerListMap;
    }

    /**
     * Generate Actions.
     *
     * Add 'collapse' argument
     * @see #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, String)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, String mktName) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, mktName);
    }

    /** Generate Actions.
     *
     * Add 'collapse' and 'replay' argument
     * @see #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, String)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision,
                                                          boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, collapse, false, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @param isReplay True if we want to run replay algorithm and false otherwise
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                    boolean classifyActions, boolean isProvision, boolean isSuspension,
                    boolean isResize, boolean collapse, boolean isReplay, String mktData) {
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

        if (isReplay && getReplayActions() != null) {
            getReplayActions().replayActions(economy, ledger);
            actions.addAll(getReplayActions().getActions());
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
            actions.addAll(Provision.provisionDecisions(economy, ledger, this));
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
            Suspension suspension = new Suspension();
            if (getReplayActions() != null) {
                // initialize the rolled back trader list from last run
                getReplayActions().translateRolledbackTraders(economy, economy.getTopology());
                suspension.setRolledBack(getReplayActions().getRolledBackSuspensionCandidates());
            }
            // find if any seller is the sole provider in any market, if so, it should not
            // be considered as suspension candidate
            suspension.findSoleProviders(economy);
            actions.addAll(suspension.suspensionDecisions(economy, ledger, this));
            if (getReplayActions() != null) {
                getReplayActions().getRolledBackSuspensionCandidates().addAll(suspension.getRolledBack());
            }
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
        addUnplacementsForEmptyReconfigures(economy, actions, placementResults);
        logUnplacedTraders(placementResults);
        createUnplacedExplanations(placementResults);
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
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @param isRealTime True for analysis of a realtime topology
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see ActionCollapse#collapsed(List)
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse,
                                                          String mktData, boolean isRealTime,
                                                          SuspensionsThrottlingConfig suspensionsThrottlingConfig) {
        Suspension.setSuspensionsThrottlingConfig(suspensionsThrottlingConfig);
        @NonNull List<Action> actions = new ArrayList<>();
        // only run replay in first sub round for realTime market.
        boolean isReplay = true;
        if (isRealTime) {
            economy.getSettings().setResizeDependentCommodities(false);
            // run a round of analysis without provisions.
            actions.addAll(generateActions(economy, classifyActions, false, isSuspension,
                            isResize, collapse, isReplay, mktData));
        } else {
            actions.addAll(generateActions(economy, classifyActions, isProvision,
                            isSuspension, isResize, collapse, !isReplay, mktData));
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
     * save the {@link ReplayActions} associated with this {@link Ede}
     */
    public Ede setReplayActions(ReplayActions state) {
        replayActions_ = state;
        return this;
    }

    /**
     * @return return the {@link ReplayActions} associated with this {@link Ede}
     */
     public ReplayActions getReplayActions() {
         return replayActions_;
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
    private void logUnplacedTraders(@NonNull final PlacementResults placementResults) {
        try {
            placementResults.getUnplacedTraders().forEach((trader, quoteTrackers) ->
                logger.info("Unable to place trader " + trader + " due to:\n\t" +
                    quoteTrackers.stream()
                        .filter(QuoteTracker::hasQuotesToExplain)
                        .map(QuoteTracker::explainInterestingSellers)
                        .collect(Collectors.joining("\n\t"))));
        } catch (Exception e) {
            logger.error("Error during unplaced trader logging: ", e);
        }
    }

    /**
     * Create the explanation describing why the trader could not be placed.
     *
     * This is used to show the causes of placement failure.
     *
     * @param placementResults The results describing why traders could not be placed.
     */
    private void createUnplacedExplanations(@NonNull final PlacementResults placementResults) {
        try {
            placementResults.getUnplacedTraders().forEach((trader, quoteTrackers) -> {
                String explanation = quoteTrackers.stream()
                                .filter(QuoteTracker::hasQuotesToExplain)
                                .map(QuoteTracker::explainInterestingSellers)
                                .collect(Collectors.joining("\n\t"));
                trader.setUnplacedExplanation(explanation);
            });
        }
        catch (Exception e) {
            logger.error("Error during unplaced explanation creation: ", e);
        }
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
     * When a shopping list is buying from a market with no sellers, we create a reconfigure with no
     * explaining commodity. These reconfigures are not very helpful, but they do indicate that the
     * shopping list could not be placed. When we generate these actions, create unplacement information
     * for them so that they can be logged or sent along.
     *
     * TODO: Hopefully one day we will do something other than generate a reconfigure with no commodity
     *       and we can get rid of this specialized logic.
     *
     * @param placementResults The actions and unplaced VMs that resulted from running analysis.
     */
    private void addUnplacementsForEmptyReconfigures(@NonNull final Economy economy,
                                                     @NonNull final List<Action> actions,
                                                     @NonNull final PlacementResults placementResults) {
        Map<Trader, List<ShoppingList>> shoppingListsForMarketsWithNoSellers = actions.stream()
            .filter(action -> action instanceof Reconfigure)
            .map(action -> (Reconfigure) action)
            .filter(reconfigure -> economy.getMarket(reconfigure.getTarget()).getActiveSellers().isEmpty())
            .map(reconfigure -> reconfigure.getTarget())
            .collect(Collectors.groupingBy(ShoppingList::getBuyer));

        shoppingListsForMarketsWithNoSellers.forEach((trader, shoppingLists) ->
            placementResults.addResultsForMarketsWithNoSuppliers(
                trader, shoppingLists, economy));
    }
}
