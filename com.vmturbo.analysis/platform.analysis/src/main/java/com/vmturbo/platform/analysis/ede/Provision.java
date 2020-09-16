package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.ProvisionBase;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.ledger.Ledger.MostExpensiveCommodityDetails;
import com.vmturbo.platform.analysis.utilities.ProvisionUtils;

/*
 * This class is contains the implementation for generating the provision actions on an economy
 * For generating the provisions we first check the engagement criteria on the market,
 * if this passes, we clone the best seller after which we run placements. This is followed
 * by the acceptance criteria. If the acceptance criteria evaluates to be true, we retain the
 * provisioned trader. We do this for every market in the economy.
 *
 * @author shravan
 *
 */
public class Provision {

    static final Logger logger = LogManager.getLogger();

    /*
     * The bundle contains information about the mostProfitableTrader, most profitable commodity and
     * the revenue of this commodity.
     */
    private static class MostProfitableBundle {

        Trader mostProfitableTrader_;
        double mostProfitableCommRev_ = 0;
        CommoditySpecification mostProfitableComm_;

        public MostProfitableBundle(Trader mostProfitableTrader, double mostProfitableCommRev,
                                    CommoditySpecification mostProfitableComm) {
            super();
            mostProfitableTrader_ = mostProfitableTrader;
            mostProfitableCommRev_ = mostProfitableCommRev;
            mostProfitableComm_ = mostProfitableComm;
        }

        public Trader getMostProfitableTrader() {
            return mostProfitableTrader_;
        }

        public double getMostProfitableCommRev() {
            return mostProfitableCommRev_;
        }

        public CommoditySpecification getMostProfitableCommoditySpecification() {
            return mostProfitableComm_;
        }
    }

    /**
     * Return a list of recommendations to optimize the cloning of all eligible traders in the
     * economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose traders we want to clone if profitable while remaining
     *                  in the desired state
     * @param ledger - the class that contains exp/rev about all the traders and commodities in
     *                  the economy
     * @return list of provision and move actions
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy,
                                                                    @NonNull Ledger ledger) {

        List<@NonNull Action> allActions = new ArrayList<>();
        if (economy.getSettings().isEstimatesEnabled()) {
            EstimateSupply es = new EstimateSupply(economy, ledger, true);

            allActions.addAll(es.getActions());
            allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                    EconomyConstants.PROVISION_PHASE).getActions());
        }
        // copy the markets from economy and use the copy to iterate, because in
        // the provision logic, we may add new basket which result in new market
        List<Market> orignalMkts = new ArrayList<>();
        orignalMkts.addAll(economy.getMarkets());
        for (Market market : orignalMkts) {
            // if the traders in the market are not eligible for provision, skip this market
            if (!canMarketProvisionSellers(market)) {
                continue;
            }
            for(;;) {
                if (economy.getForceStop()) {
                    return allActions;
                }
                List<@NonNull Action> actions = new ArrayList<>();

                ledger.calculateExpAndRevForSellersInMarket(economy, market);
                // break if there is no seller that is eligible for cloning in the market
                MostProfitableBundle pb = findBestTraderToEngage(market, ledger, economy);
                Trader mostProfitableTrader = pb.getMostProfitableTrader();
                if (mostProfitableTrader == null) {
                    break;
                }
                boolean isDebugMostProfitableTrader = mostProfitableTrader.isDebugEnabled();
                String mostProfitableTraderDebugInfo =
                        mostProfitableTrader.getDebugInfoNeverUseInCode();
                Action provisionAction = null;
                double origRoI = ledger.getTraderIncomeStatements().get(
                        mostProfitableTrader.getEconomyIndex()).getROI();
                double oldRevenue = ledger.getTraderIncomeStatements().get(
                        mostProfitableTrader.getEconomyIndex()).getRevenues();

                Trader provisionedTrader = null;
                boolean isDebugProvisionedTrader = false;
                boolean successfulEvaluation = false;
                if (!market.getInactiveSellers().isEmpty()) {
                    // TODO: pick a trader that is closest to the mostProfitableTrader to activate
                    // reactivate a suspended seller
                    List<Trader> copiedInactiveSellers = new ArrayList<>(market.getInactiveSellers());
                    for (Trader seller : copiedInactiveSellers) {
                        if (isEligibleForActivation(seller, mostProfitableTrader, economy, market)) {
                            provisionAction = new Activate(economy, seller, market.getBasket(),
                                    mostProfitableTrader,
                                    pb.getMostProfitableCommoditySpecification());
                            actions.add(provisionAction.take());
                            provisionedTrader = ((Activate)provisionAction).getTarget();
                            isDebugProvisionedTrader = provisionedTrader.isDebugEnabled();
                            String provisionedTraderDebugInfo =
                                    provisionedTrader.getDebugInfoNeverUseInCode();

                            if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                logger.info("Activate " + provisionedTraderDebugInfo
                                        + " to reduce ROI of " + mostProfitableTraderDebugInfo
                                        + ". Its original ROI is " + origRoI
                                        + " and its max desired ROI is "
                                        + ledger.getTraderIncomeStatements()
                                        .get(mostProfitableTrader.getEconomyIndex())
                                        .getMaxDesiredROI() + ".");
                            }

                            actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));

                            if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader,
                                    provisionedTrader, pb.getMostProfitableCommRev())) {
                                if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                    logger.info("Roll back activation of " + provisionedTraderDebugInfo
                                            + ", because it does not reduce ROI of "
                                            + mostProfitableTraderDebugInfo + ".");
                                }
                                // remove IncomeStatement from ledger and rollback actions
                                rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                                actions.clear();
                                continue;
                            }
                            successfulEvaluation = true;
                            break;
                        }
                    }
                }
                if (!successfulEvaluation) {
                    // provision a new trader
                    provisionAction = new ProvisionBySupply(economy,
                            mostProfitableTrader, pb.getMostProfitableCommoditySpecification());
                    actions.add(provisionAction.take());
                    provisionedTrader = ((ProvisionBySupply)provisionAction).getProvisionedSeller();
                    isDebugProvisionedTrader = provisionedTrader.isDebugEnabled();
                    String provisionedTraderDebugInfo =
                            provisionedTrader.getDebugInfoNeverUseInCode();

                    if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                        logger.info("Provision " + provisionedTraderDebugInfo
                                + " to reduce ROI of " + mostProfitableTraderDebugInfo
                                + ". Its original ROI is " + origRoI
                                + " and its max desired ROI is "
                                + ledger.getTraderIncomeStatements()
                                .get(mostProfitableTrader.getEconomyIndex())
                                .getMaxDesiredROI() + ".");
                    }

                    List<Action> subActions = ((ProvisionBySupply)provisionAction)
                            .getSubsequentActions();
                    actions.addAll(subActions);
                    ledger.addTraderIncomeStatement(provisionedTrader);
                    subActions.forEach(action -> {
                        if (action instanceof ProvisionBase) {
                            ledger.addTraderIncomeStatement(((ProvisionBase)action)
                                    .getProvisionedSeller());
                        }
                    });

                    actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));
                    if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader,
                            provisionedTrader, pb.getMostProfitableCommRev())) {
                        if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                            logger.info("Roll back provision of " + provisionedTraderDebugInfo
                                    + ", because it does not reduce ROI of "
                                    + mostProfitableTraderDebugInfo + ".");
                        }
                        // Because if we roll back original action, subsequent actions will roll back too.
                        actions.removeAll(subActions);
                        // remove IncomeStatement from ledger and rollback actions
                        rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                        break;
                    }
                }
                logger.info(mostProfitableTrader.getDebugInfoNeverUseInCode() + " triggered " +
                        ((provisionAction instanceof Activate) ? "ACTIVATION of " : "PROVISION of ")
                        + provisionedTrader.getDebugInfoNeverUseInCode()
                        + " due to commodity : "
                        + pb.getMostProfitableCommoditySpecification().getDebugInfoNeverUseInCode());
                ((ActionImpl)provisionAction).setImportance(oldRevenue - ledger
                        .getTraderIncomeStatements().get(mostProfitableTrader
                                .getEconomyIndex()).getRevenues());

                if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                    logger.info("New ROI of " + mostProfitableTraderDebugInfo + " is "
                            + ledger.getTraderIncomeStatements()
                            .get(mostProfitableTrader.getEconomyIndex()).getMaxDesiredROI()
                            + ".");
                }

                allActions.addAll(actions);
            }
        }
        return allActions;
    }

    /**
     * Return a list of move actions to optimize the placement of the traders in a
     * market, after a provision or activation of a seller.
     *
     * @param economy - the economy where the market exist
     * @param market - the market whose traders we move
     * @param mostProfitableTrader - the most profitable trader of the market
     *
     * @return list of move actions
     */
    public static @NonNull List<@NonNull Action> placementAfterProvisionAction(@NonNull Economy economy
            , @NonNull Market market
            , @NonNull Trader mostProfitableTrader) {
        List<@NonNull Action> actions = new ArrayList<>();
        actions.addAll(Placement.prefPlacementDecisions(economy,
                new ArrayList<>(mostProfitableTrader.getCustomers())).getActions());
        // Allow all buyers in markets where mostProfitableTrader is a seller place again so they
        // can re-balance with the added resources in case these buyers are not part of the
        // current market.
        for (Market m : economy.getMarketsAsSeller(mostProfitableTrader)) {
            actions.addAll(Placement.prefPlacementDecisions(economy, m.getBuyers()).getActions());
        }
        return actions;
    }

    /**
     * returns true if the traders in the market are in the right conditions for them to be
     * considered for cloning
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that implies
     * eligibility to clone
     *
     * @return true if the sellers in the market are eligible for cloning and false otherwise
     */
    private static boolean canMarketProvisionSellers(Market market) {

        // do not consider cloning in this market if there are no active sellers
        // available for placement
        if (market.getActiveSellersAvailableForPlacement().isEmpty()) {
            return false;
        }

        List<ShoppingList> buyers = market.getBuyers();
        // there is no point in cloning in a market with a single buyer, and the single buyer
        // is not a guaranteed buyer
        if (buyers.size() == 1 && !buyers.get(0).getBuyer().getSettings().isGuaranteedBuyer()) {
            return false;
        }

        // if none of the buyers in this market are movable and the immovable buyer is not a
        // guaranteedbuyer, provisioning a seller is not beneficial
        if (buyers.stream().allMatch(shoppingList -> !shoppingList.isMovable()
                && !shoppingList.getBuyer().getSettings().isGuaranteedBuyer())) {
            return false;
        }

        return true;
    }

    /**
     * returns best trader to clone after checking the engagement criteria for all traders of a
     * particular market
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that
     * implies eligibility to clone
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param economy - that the market is a part of
     *
     * @return the {@link MostProfitableBundle} containing the mostProfitableTrader if there is one that
     * can clone and NULL otherwise
     */
    public static MostProfitableBundle findBestTraderToEngage(Market market, Ledger ledger, Economy economy) {

        Trader mostProfitableTrader = null;
        double roiOfRichestTrader = 0;
        double mostProfitableCommRev = 0;
        CommoditySpecification commSpec = null;
        // consider only sellers available for placements. Considering a seller with cloneable false
        // is going to fail acceptanceCriteria since none its customers will move
        for (Trader seller : market.getActiveSellersAvailableForPlacement()) {
            boolean isDebugTrader = seller.isDebugEnabled();
            String traderDebugInfo = seller.getDebugInfoNeverUseInCode();
            MostExpensiveCommodityDetails mostExpensiveCommodity = ledger.calculateExpRevForTraderAndGetTopRevenue(economy, seller);
            if (seller.getSettings().isCloneable()) {
                IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller
                        .getEconomyIndex());
                // return the most profitable trader
                double roiOfTrader = traderIS.getROI();
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} trader ROI: " + roiOfTrader
                            + ", max desired ROI: " + traderIS.getMaxDesiredROI() + ".");
                }
                // The seller should have at least 1 customer which is participating in the market
                // so that this customer can be moved out when processing this market.
                Set<ShoppingList> sellerCustomersInCurrentMarket = seller.getCustomers(market);
                // TODO: evaluate if checking for movable customers earlier is beneficial
                // clone candidate should either have at least one customer is movable or
                // all customers that are from guaranteed buyer
                if (sellerCustomersInCurrentMarket.size() > 0
                        && (roiOfTrader > traderIS.getMaxDesiredROI())
                        && (roiOfTrader > roiOfRichestTrader)
                        && (seller.getCustomers().stream().anyMatch(sl -> sl.isMovable())
                        || (seller.getCustomers().stream().allMatch(sl ->
                        sl.getBuyer().getSettings().isGuaranteedBuyer())))) {
                    mostProfitableTrader = seller;
                    roiOfRichestTrader = roiOfTrader;
                    mostProfitableCommRev = mostExpensiveCommodity.getRevenues();
                    commSpec = mostExpensiveCommodity.getCommoditySpecification();
                } else {
                    if (logger.isTraceEnabled() || isDebugTrader) {
                        if (roiOfTrader <= traderIS.getMaxDesiredROI()) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because its ROI (" + roiOfTrader + ") is not"
                                    + " bigger than its max desired ROI ("
                                    + traderIS.getMaxDesiredROI() + ").");
                        }
                        if (roiOfTrader <= roiOfRichestTrader) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because its ROI (" + roiOfTrader + ") is not"
                                    + " bigger than the ROI od the richest trader so far ("
                                    + traderIS.getMaxDesiredROI() + ").");
                        }
                        if (seller.getCustomers().stream().allMatch(sl -> !sl.isMovable())) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because it has no movable customer.");
                        }
                        if (seller.getCustomers().stream().anyMatch(sl ->
                                !sl.getBuyer().getSettings().isGuaranteedBuyer())) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because there is one customer which is not a"
                                    + " guaranteed buyer.");
                        }
                    }
                }
            } else {
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} is not clonable.");

                }
            }
        }
        return new MostProfitableBundle(mostProfitableTrader, mostProfitableCommRev, commSpec);
    }

    /**
     * Calculate the current ROI of the  <b>mostProfitableTrader</b>. Return true if this has
     * decreased compared to <b>origROI</b>
     *
     * @param economy - the {@link Economy} where <b>mostProfitableTrader</b> participates in
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param origRoI - the RoI of the mostProfitableTrader before placements
     * @param mostProfitableTrader - {@link Trader} that had the highest RoI and was selected to be
     * cloned
     * @param provisionedTrader - {@link Trader} that has been provisioned in this economy
     * @param origMostProfitableCommRev - is the highest revenue generated by a single revenue
     *
     * @return true - if (a) the current ROI of the <b>mostProfitableTrader</b> is less than or
     * equal to the <b>origROI</b> and (b) and the newly provisioned trader has customers
     *
     */
    public static boolean evaluateAcceptanceCriteria(Economy economy, Ledger ledger, double origRoI
            , Trader mostProfitableTrader
            , Trader provisionedTrader
            , double origMostProfitableCommRev) {

        double newMostProfitableCommRev = ledger
                .calculateExpRevForTraderAndGetTopRevenue(economy, mostProfitableTrader)
                .getRevenues();
        // check if the RoI of the mostProfitableTrader after cloning is less than before cloning
        // and that at least one nonGuaranteedBuyer has moved into the new host
        return ledger.getTraderIncomeStatements().get(mostProfitableTrader.getEconomyIndex())
                .getROI() < origRoI && !provisionedTrader.getCustomers().isEmpty() &&
                origMostProfitableCommRev > newMostProfitableCommRev;
    }

    /**
     * Remove {@link IncomeStatement} of a trader and rollback action after acceptanceCriteria fails
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader that is being removed
     * @param provisionedTrader - {@link Trader} that was cloned
     * @param actions - bunch of actions that were generated after passing the acceptanceCriteria
     *                  that need to be rolledBack
     *
     */
    public static void rollBackActionAndUpdateLedger(Ledger ledger,
                                                     Trader provisionedTrader, List<@NonNull Action> actions, Action provisionAction) {
        // remove IncomeStatement from ledger and rollback actions
        if (provisionAction instanceof ProvisionBySupply) {
            Lists.reverse(((ProvisionBySupply)provisionAction).getSubsequentActions()).forEach(action -> {
                if (action instanceof ProvisionBase) {
                    ledger.removeTraderIncomeStatement(((ProvisionBase)action).getProvisionedSeller());
                }
            });
            ledger.removeTraderIncomeStatement(provisionedTrader);
        }
        Lists.reverse(actions).forEach(axn -> axn.rollback());
    }

    /**
     * Checks if the trader is eligible for activation. It checks if any of the traders on the
     * mostProfitableTrader satisfies 2 conditions:
     * 1. does it have cliques in common with inactive trader and
     * 2. can it fit in the inactive trader
     * If yes, then the inactiveTrader is eligible for activation
     *
     * @param inactiveTrader the inactive trader which is a candidate for activation
     * @param mostProfitableTrader the mostProfitableTrader
     * @param economy the economy
     * @param m the market under process
     * @return true if the trader is eligible for activation, false otherwise
     */
    @VisibleForTesting
    protected static boolean isEligibleForActivation(
            Trader inactiveTrader, Trader mostProfitableTrader, Economy economy, Market m) {
        Set<ShoppingList> slsOnMostProfitableTrader = mostProfitableTrader.getCustomers(m);
        for (ShoppingList sl : slsOnMostProfitableTrader) {
            Trader customerOnMostProfitableTrader = sl.getBuyer();
            if ((!customerOnMostProfitableTrader.getSettings().isShopTogether() ||
                    doBuyerAndSellerShareCliques(customerOnMostProfitableTrader, inactiveTrader, economy))
                    && ProvisionUtils.canBuyerFitInSeller(sl, inactiveTrader, economy)) {
                return true;
            }
        }
        if (logger.isTraceEnabled() || inactiveTrader.isDebugEnabled()) {
            logger.debug(inactiveTrader.getDebugInfoNeverUseInCode() + " is not eligible for " +
                    "activation because none of the customers of " +
                    mostProfitableTrader.getDebugInfoNeverUseInCode() + " can be placed on it.");
        }
        return false;
    }

    /**
     * Checks if the buyer's common cliques and seller's cliques have anything in common.
     *
     * @param buyer the Trader which is a buyer
     * @param seller the Trader which is a seller
     * @param economy the economy
     * @return true if buyer's common cliques and seller's cliques have anything in common.
     */
    private static boolean doBuyerAndSellerShareCliques (
            Trader buyer, Trader seller , Economy economy) {
        // economy.getCommonCliques(buyer) returns com.google.common.collect.Sets$SetView,
        // which doesn't support retainAll.
        Set<Long> commonCliquesOfCustomer = new HashSet<>(economy.getCommonCliques(buyer));
        Set<Long> inactiveTraderCliques = seller.getCliques();
        commonCliquesOfCustomer.retainAll(inactiveTraderCliques);
        return !commonCliquesOfCustomer.isEmpty();
    }
}
