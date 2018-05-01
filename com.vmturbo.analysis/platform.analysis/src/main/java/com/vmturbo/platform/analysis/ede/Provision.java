package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.ledger.Ledger.MostExpensiveCommodityDetails;

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
     *                  the ecomomy
     * @param isShopTogether - flag specifies if we want to use SNM or normal placement between
     *                  provisions
     *
     * @return list of provision and move actions
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy
                                                                    , @NonNull Ledger ledger
                                                                    , Ede ede) {

        List<@NonNull Action> allActions = new ArrayList<>();
        if (economy.getSettings().isEstimatesEnabled()) {
            EstimateSupply es = new EstimateSupply(economy, ledger, true);

            allActions.addAll(es.getActions());
            allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                            EconomyConstants.PROVISION_PHASE));
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

                // run placement on the current buyers
                allActions.addAll(Placement.prefPlacementDecisions(economy,
                                new ArrayList<ShoppingList>(market.getBuyers())));

                ledger.calculateExpAndRevForSellersInMarket(economy, market);
                // break if there is no seller that is eligible for cloning in the market
                MostProfitableBundle pb = findBestTraderToEngage(market, ledger, economy);
                Trader mostProfitableTrader = pb.getMostProfitableTrader();
                if (mostProfitableTrader == null) {
                    break;
                }
                Action provisionAction = null;
                double origRoI = ledger.getTraderIncomeStatements().get(
                                mostProfitableTrader.getEconomyIndex()).getROI();
                double oldRevenue = ledger.getTraderIncomeStatements().get(
                                mostProfitableTrader.getEconomyIndex()).getRevenues();

                Trader provisionedTrader = null;
                boolean successfulEvaluation = false;
                if (!market.getInactiveSellers().isEmpty()) {
                    // TODO: pick a trader that is closest to the mostProfitableTrader to activate
                    // reactivate a suspended seller
                    List<Trader> copiedInactiveSellers = new ArrayList<>(market.getInactiveSellers());
                    for (Trader seller : copiedInactiveSellers) {
                        provisionAction = new Activate(economy, seller, market,
                                        mostProfitableTrader,
                                        pb.getMostProfitableCommoditySpecification());
                        actions.add(provisionAction.take());
                        provisionedTrader = ((Activate)provisionAction).getTarget();

                        actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));

                        if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader,
                                        provisionedTrader, pb.getMostProfitableCommRev())) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("rollback activation of "
                                            + mostProfitableTrader.getDebugInfoNeverUseInCode()
                                            + " as the RoI of the modelSeller does not go down");
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
                if (!successfulEvaluation) {
                    // provision a new trader
                    provisionAction = new ProvisionBySupply(economy,
                                    mostProfitableTrader, pb.getMostProfitableCommoditySpecification());
                    actions.add(provisionAction.take());
                    provisionedTrader = ((ProvisionBySupply)provisionAction).getProvisionedSeller();
                    List<Action> subActions = ((ProvisionBySupply)provisionAction)
                                    .getSubsequentActions();
                    actions.addAll(subActions);
                    ledger.addTraderIncomeStatement(provisionedTrader);
                    subActions.forEach(action -> {
                        if (action instanceof ProvisionBySupply) {
                            ledger.addTraderIncomeStatement(((ProvisionBySupply)action)
                                                            .getProvisionedSeller());
                        } else if (action instanceof ProvisionByDemand) {
                            ledger.addTraderIncomeStatement(((ProvisionByDemand)action)
                                                            .getProvisionedSeller());
                        }
                    });

                    actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));
                    if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader,
                                    provisionedTrader, pb.getMostProfitableCommRev())) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("rollback cloning of "
                                        + mostProfitableTrader.getDebugInfoNeverUseInCode()
                                        + " as the RoI of the modelSeller does not go down");
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
                        new ArrayList<ShoppingList>(mostProfitableTrader.getCustomers())));
        actions.addAll(Placement.prefPlacementDecisions(economy, market.getBuyers()));
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
        // is not a guaranteedbuyer
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
        // consider only sellers available for placements. Considering a seller with clonable false
        // is going to fail acceptanceCriteria since none its customers will move
        for (Trader seller : market.getActiveSellersAvailableForPlacement()) {
            MostExpensiveCommodityDetails mostExpensiveCommodity = ledger.calculateExpRevForTraderAndGetTopRevenue(economy, seller);
            if (seller.getSettings().isCloneable()) {
                IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller
                                .getEconomyIndex());
                // return the most profitable trader
                double roiOfTrader = traderIS.getROI();
                // TODO: evaluate if checking for movable customers earlier is beneficial
                // clone candidate should either have at least one customer is movable or
                // all customers that are from guaranteed buyer
                if ((roiOfTrader > traderIS.getMaxDesiredROI()) && (roiOfTrader > roiOfRichestTrader)
                                && (seller.getCustomers().stream().anyMatch(sl -> sl.isMovable())
                                                || (seller.getCustomers().stream().allMatch(sl ->
                                                sl.getBuyer().getSettings().isGuaranteedBuyer())))) {
                    mostProfitableTrader = seller;
                    roiOfRichestTrader = roiOfTrader;
                    mostProfitableCommRev = mostExpensiveCommodity.getRevenues();
                    commSpec = mostExpensiveCommodity.getCommoditySpecification();
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
     * remove {@Link incomeStatement} of a trader and rollback action after acceptanceCriteria fails
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
                if (action instanceof ProvisionBySupply) {
                    ledger.removeTraderIncomeStatement(((ProvisionBySupply)action).getProvisionedSeller());
                } else if (action instanceof ProvisionByDemand) {
                    ledger.removeTraderIncomeStatement(((ProvisionByDemand)action).getProvisionedSeller());
                }
            });
            ledger.removeTraderIncomeStatement(provisionedTrader);
        }
        Lists.reverse(actions).forEach(axn -> axn.rollback());
    }
}
