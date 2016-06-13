package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

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
     * Return a list of recommendations to optimize the cloning of all eligible traders in the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy) {

        @NonNull List<Action> allActions = new ArrayList<>();

        for (Market market : economy.getMarkets()) {
            @NonNull List<Action> actions = new ArrayList<>();
            // if there are no sellers in the market, the buyer is misconfigured
            List<@NonNull Trader> sellers = new ArrayList<>();
            sellers.addAll(market.getActiveSellers());
            if (sellers.isEmpty()) {
                continue;
            }

            Ledger ledger = new Ledger(economy);
            ledger.calculateAllTraderExpensesAndRevenues(economy);
            // continue if there is no seller that is eligible for cloning in the market
            if (!checkEngageCriteriaForMarket(ledger, market)) {
                continue;
            }

            Trader mostProfitableTrader = null;
            double roiOfMostProfitableTrader = 0;
            for (Trader seller : sellers) {
                // check engagementCriteria on every activeTrader
                if (seller.getSettings().isCloneable()) {
                    if (ledger.getTraderIncomeStatements().get(seller.getEconomyIndex()).getROI() > roiOfMostProfitableTrader) {
                        mostProfitableTrader = seller;
                    }
                }
            }

            // we could reactivate a suspended seller, currently i just clone most profitable seller
            ProvisionBySupply provisionAction = null;
            if (mostProfitableTrader != null) {
                provisionAction = new ProvisionBySupply(economy, mostProfitableTrader);
                actions.add(provisionAction.take());
            } else {
                continue;
            }

            // TODO: check if a market specific placement is beneficial?
            Trader provisionedTrader = provisionAction.getProvisionedSeller();
            // run placement after adding a new seller to the economy
            actions.addAll(Placement.placementDecisions(economy));
            sellers.add(provisionedTrader);
            Ledger newLedger = new Ledger(economy);
            // TODO: change re-creation and computation of expRev to just updating the ledger
            newLedger.calculateAllTraderExpensesAndRevenues(economy);
            if (!checkAcceptanceCriteriaForMarket(newLedger, market)) {
                // remove IncomeStatement from ledger, trader from economy and the provision action
                newLedger.getTraderIncomeStatements().remove(provisionedTrader.getEconomyIndex());
                actions.forEach(axn -> axn.rollback());
                actions.clear();
                break;
            }
            allActions.addAll(actions);
        }

        return allActions;
    }

    /**
     * returns true/false after checking the engagement criteria for a particular trader
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param market - the market whose seller ROIs are checked to verify profitability that implies eligibility to clone
     *
     * @return true - if there is even 1 profitable trader and is capable of cloning and false otherwise
     */
    public static boolean checkEngageCriteriaForMarket(Ledger ledger, Market market) {

        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            // return true if there is even 1 profitable trader
            if (traderIS.getROI() > traderIS.getMaxDesiredROI()) {
                return true;
            }
        }
        logger.warn("There is no profitable seller to clone in market " + market.toString());
        return false;
    }

    /**
     * returns true/false after checking the acceptance criteria for a particular trader
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param market - the market whose seller ROIs are checked to verify profitability after a clone was added to the market
     *
     * @return true - if this trader is not at loss and false otherwise
     */
    public static boolean checkAcceptanceCriteriaForMarket(Ledger ledger, Market market) {

        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            if (traderIS.getROI() < traderIS.getMinDesiredROI()) {
                return false;
            }
        }
        return true;
    }

}
