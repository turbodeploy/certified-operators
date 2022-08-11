package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.GuaranteedBuyerHelper;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

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

    static final Logger logger = Logger.getLogger(Provision.class);

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
                                                                    , boolean isShopTogether
                                                                    , Ede ede) {

        List<@NonNull Action> allActions = new ArrayList<>();
        for (Market market : economy.getMarkets()) {
            // if the traders in the market are not eligible for provision, skip this market
            if (!canMarketProvisionSellers(market)) {
                continue;
            }
            for(;;) {
                List<@NonNull Action> actions = new ArrayList<>();
                ledger.calculateExpAndRevForSellersInMarket(economy, market);
                // break if there is no seller that is eligible for cloning in the market
                Trader mostProfitableTrader = findBestTraderToEngage(market, ledger);
                if (mostProfitableTrader == null) {
                    break;
                }
                Action provisionAction = null;
                double origRoI = ledger.getTraderIncomeStatements().get(
                                mostProfitableTrader.getEconomyIndex()).getROI();
                double oldRevenue = ledger.getTraderIncomeStatements().get(
                                mostProfitableTrader.getEconomyIndex()).getRevenues();

                Trader provisionedTrader = null;
                if (!market.getInactiveSellers().isEmpty()) {
                    // TODO: pick a trader that is closest to the mostProfitableTrader to activate
                    // reactivate a suspended seller
                    provisionAction = new Activate(economy, market.getInactiveSellers().
                                    get(0), market, mostProfitableTrader);
                    actions.add(provisionAction.take());
                    provisionedTrader = ((Activate)provisionAction).getTarget();
                } else {
                    // provision a new trader
                    provisionAction = new ProvisionBySupply(economy,
                                    mostProfitableTrader);
                    actions.add(provisionAction.take());
                    provisionedTrader = ((ProvisionBySupply)provisionAction).getProvisionedSeller();
                }

                // run placement after adding a new seller to the economy
                // TODO: run placement within a market
                boolean keepRunning = true;
                while (keepRunning) {
                    List<Action> placeActions;
                    if (isShopTogether) {
                        placeActions = ede.breakDownCompoundMove(Placement
                                        .shopTogetherDecisions(economy, new ArrayList<ShoppingList>
                                        (mostProfitableTrader.getCustomers())));
                    } else {
                        placeActions = Placement.placementDecisions(economy,
                                        new ArrayList<ShoppingList>(mostProfitableTrader
                                                        .getCustomers()));
                    }
                    keepRunning = !(placeActions.isEmpty() || placeActions.stream().allMatch(a ->
                                                a instanceof Reconfigure));
                    actions.addAll(placeActions);
                }
                ledger.addTraderIncomeStatement(provisionedTrader);

                if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader,
                                provisionedTrader)) {
                    logger.error("rollback provisioning of a new trader if the RoI of the "
                                    + "modelSeller does not go down");
                    // remove IncomeStatement from ledger and rollback actions
                    rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions);
                    break;
                }
                ((ActionImpl)provisionAction).setImportance(oldRevenue - ledger
                                .getTraderIncomeStatements().get(mostProfitableTrader
                                                .getEconomyIndex()).getRevenues());
                allActions.addAll(actions);
            }
        }
        GuaranteedBuyerHelper.processGuaranteedbuyerInfo(economy);
        return allActions;
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
        if (market.getActiveSellers().isEmpty()) {
            return false;
        }

        List<ShoppingList> buyers = market.getBuyers();
        // there is no point in cloning in a market with a single buyer
        if (buyers.size() == 1) {
            return false;
        }

        // if all buyers in this market are guaranteedBuyers, it should not cause a clone to happen
        if (buyers.stream().allMatch(sl -> sl.getBuyer().getSettings().isGuaranteedBuyer())) {
            return false;
        }

        // FALSE if there is no seller with greater than 1 customer shopping in this market who is
        // not a guaranteedBuyer
        boolean multipleBuyersInSupplier = false;
        for (Trader seller : market.getActiveSellers()) {
            if (seller.getCustomers().stream().filter(sl -> buyers.contains(sl) && !sl.getBuyer()
                            .getSettings().isGuaranteedBuyer()).count() > 1) {
                multipleBuyersInSupplier = true;
                break;
            }
        }
        if (!multipleBuyersInSupplier)
            return false;

        // if none of the buyers in this market are movable, provisioning a seller is not beneficial
        if (!buyers.stream().anyMatch(shoppingList -> shoppingList.isMovable())) {
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
     *
     * @return the mostProfitableTrader if there is one that can clone and NULL otherwise
     */
    public static Trader findBestTraderToEngage(Market market, Ledger ledger) {

        Trader mostProfitableTrader = null;
        double roiOfRichestTrader = 0;
        List<ShoppingList> buyers = market.getBuyers();
        for (Trader seller : market.getActiveSellers()) {
            // consider only sellers that have more than 1 movable non-guaranteedBuyer
            if (seller.getSettings().isCloneable() && seller.getCustomers().stream().filter(sl ->
                    buyers.contains(sl) && !sl.getBuyer().getSettings().isGuaranteedBuyer())
                        .count() > 1) {
                IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller
                                .getEconomyIndex());
                // return the most profitable trader
                double roiOfTrader = traderIS.getROI();
                // TODO: evaluate if checking for movable customers earlier is beneficial
                if ((roiOfTrader > traderIS.getMaxDesiredROI()) && (roiOfTrader > roiOfRichestTrader)
                                && seller.getCustomers().stream().anyMatch(sl -> sl.isMovable())) {
                    mostProfitableTrader = seller;
                    roiOfRichestTrader = roiOfTrader;
                }
            }
        }
        return mostProfitableTrader;
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
     *
     * @return true - if (a) the current ROI of the <b>mostProfitableTrader</b> is less than or
     * equal to the <b>origROI</b> and (b) and the newly provisioned trader has customers
     *
     */
    public static boolean evaluateAcceptanceCriteria(Economy economy, Ledger ledger, double origRoI
                                                                , Trader mostProfitableTrader
                                                                , Trader provisionedTrader ) {

        ledger.calculateExpRevForSeller(economy, mostProfitableTrader);
        // check if the RoI of the mostProfitableTrader after cloning is less or equal to that
        // before cloning, and that at least one nonGuaranteedBuyer has moved into the new host
        return ledger.getTraderIncomeStatements().get(mostProfitableTrader.getEconomyIndex())
                        .getROI() <= origRoI && !provisionedTrader.getCustomers().isEmpty() &&
                        provisionedTrader.getCustomers().stream().anyMatch(sl -> !sl.getBuyer()
                                        .getSettings().isGuaranteedBuyer());
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
                    Trader provisionedTrader, List<@NonNull Action> actions) {
        // remove IncomeStatement from ledger and rollback actions
        ledger.removeTraderIncomeStatement(provisionedTrader);
        Lists.reverse(actions).forEach(axn -> axn.rollback());
    }
}
