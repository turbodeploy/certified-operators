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
     *                 in the desired state
     * @param ledger - the class that contains exp/rev about all the traders and commodities in
     *                 the ecomomy
     *
     * @return list of provision and move actions
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy,
                                                                    @NonNull Ledger ledger) {

        List<@NonNull Action> allActions = new ArrayList<>();
        List<@NonNull Action> actions = new ArrayList<>();
        for (Market market : economy.getMarkets()) {
            for(;;) {
                // if there are no sellers in the market, the buyer is misconfigured
                actions.clear();
                if (market.getActiveSellers().isEmpty()) {
                    break;
                }

                ledger.calculateExpAndRevForSellersInMarket(economy, market);
                // continue if there is no seller that is eligible for cloning in the market
                Trader mostProfitableTrader = findProfitableTraderToEngage(market, ledger);
                if (mostProfitableTrader == null) {
                    break;
                }

                // TODO: we could reactivate a suspended seller, currently I just clone most profitable seller
                ProvisionBySupply provisionAction = new ProvisionBySupply(economy, mostProfitableTrader);
                actions.add(provisionAction.take());

                Trader provisionedTrader = provisionAction.getProvisionedSeller();
                // run placement after adding a new seller to the economy
                actions.addAll(Placement.placementDecisions(economy));
                ledger.addTraderIncomeStatement(provisionedTrader);

                ledger.calculateExpAndRevForSellersInMarket(economy, market);
                if (!evalAcceptanceCriteriaForMarket(market, ledger)) {
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
     * returns best trader to clone after checking the engagement criteria for all traders of a particular market
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that implies eligibility to clone
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     *
     * @return the mostProfitableTrader if there is one that can clone and NULL otherwise
     */
    public static Trader findProfitableTraderToEngage(Market market, Ledger ledger) {

        Trader mostProfitableTrader = null;
        double roiOfMostProfitableTrader = 0;
        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            // return the most profitable trader
            double roiOfTrader = traderIS.getROI();
            if (seller.getSettings().isCloneable() && (roiOfTrader > traderIS.getMaxDesiredROI())
                                                   && (roiOfTrader > roiOfMostProfitableTrader)) {
                mostProfitableTrader = seller;
                roiOfMostProfitableTrader = roiOfTrader;
            }
        }
        return mostProfitableTrader;
    }

    /**
     * returns true/false after checking the acceptance criteria for a particular market
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param market - the market whose seller ROIs are checked to verify profitability after a clone was added to the market
     *
     * @return true - if this trader is not at loss and false otherwise
     */
    public static boolean evalAcceptanceCriteriaForMarket(Market market, Ledger ledger) {

        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            if (traderIS.getROI() < traderIS.getMinDesiredROI()) {
                return false;
            }
        }
        return true;
    }

}
