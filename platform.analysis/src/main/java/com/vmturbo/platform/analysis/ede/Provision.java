package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
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

                double origRoI = ledger.getTraderIncomeStatements().get(
                                mostProfitableTrader.getEconomyIndex()).getROI();
                // TODO: we could reactivate a suspended seller, currently I just clone most profitable seller
                ProvisionBySupply provisionAction = new ProvisionBySupply(economy, mostProfitableTrader);
                actions.add(provisionAction.take());

                Trader provisionedTrader = provisionAction.getProvisionedSeller();
                // run placement after adding a new seller to the economy
                // TODO: run placement within a market
                boolean keepRunning = true;
                while (keepRunning) {
                    List<Action> placeActions;
                    if (isShopTogether) {
                        placeActions = ede.breakDownCompoundMove(Placement.shopTogetherDecisions(economy));
                    } else {
                        placeActions = Placement.placementDecisions(economy);
                    }
                    keepRunning = !placeActions.isEmpty();
                    actions.addAll(placeActions);
                }
                ledger.addTraderIncomeStatement(provisionedTrader);

                if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, mostProfitableTrader)) {
                    logger.error("Provisioning of a new trader fif not cause the RoI of the "
                                    + "modelSeller to go down");
                    // remove IncomeStatement from ledger and rollback actions
                    ledger.removeTraderIncomeStatement(provisionedTrader);
                    Lists.reverse(actions).forEach(axn -> axn.rollback());
                    break;
                }
                allActions.addAll(actions);
            }
        }

        return allActions;
    }

    /**
     * returns true if the traders in the market are in the right conditions for them to be considered for cloning
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that implies eligibility to clone
     *
     * @return true if the sellers in the market are eligible for cloning and false otherwise
     */
    private static boolean canMarketProvisionSellers(Market market) {

        // do not consider cloning in this market if there are no active sellers
        if (market.getActiveSellers().isEmpty()) {
            return false;
        }

        // there is no point in cloning in a market with a single buyer
        if (market.getBuyers().size() == 1) {
            return false;
        }

        // if none of the buyers in this market are movable, provisioning a seller is not beneficial
        if (!market.getBuyers().stream().anyMatch(shoppingList -> shoppingList.isMovable())) {
            return false;
        }

        return true;
    }

    /**
     * returns best trader to clone after checking the engagement criteria for all traders of a particular market
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that implies eligibility to clone
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     *
     * @return the mostProfitableTrader if there is one that can clone and NULL otherwise
     */
    public static Trader findBestTraderToEngage(Market market, Ledger ledger) {

        Trader mostProfitableTrader = null;
        double roiOfRichestTrader = 0;
        for (Trader seller : market.getActiveSellers()) {
            // consider only sellers that have more than 1 movable buyer
            if (seller.getSettings().isCloneable() && seller.getCustomers().size() > 1) {
                IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
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
     * @param mostProfitableTrader - {@link Trader} that had the highest RoI and was selected to be cloned
     *
     * @return true - if the current ROI of the <b>mostProfitableTrader</b> is less than the <b>origROI</b>
     */
    public static boolean evaluateAcceptanceCriteria(Economy economy, Ledger ledger, double origRoI
                                                                , Trader mostProfitableTrader) {

        ledger.calculateExpRevForSeller(economy, mostProfitableTrader);
        // check if the RoI of the mostProfitableTrader has decreased after cloning
        return ledger.getTraderIncomeStatements().get(mostProfitableTrader.getEconomyIndex())
                        .getROI() < origRoI;
    }

}
