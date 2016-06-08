package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class Provision {

    /**
     * Return a list of recommendations to optimize the cloning of all eligible traders in the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param state - the state capturing all previous decisions of the economic decisions engine
     * @param timeMiliSec - time in Mili Seconds the placement decision algorithm is invoked
     *                      (typically since Jan. 1, 1970)
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy,
                    @NonNull List<@NonNull StateItem> state, long timeMiliSec) {

        @NonNull List<Action> actions = new ArrayList<>();

        for (Market market : economy.getMarkets()) {
            // if there are no sellers in the market, the buyer is misconfigured
            List<@NonNull Trader> sellers = market.getActiveSellers();
            if (sellers.isEmpty()) {
                continue;
            }

            Ledger ledger = new Ledger(economy);
            ledger.calculateAllTraderExpensesAndRevenues(economy);
            Trader mostProfitableTrader = null;
            double roiOfMostProfitableTrader = 0;
            for (Trader seller : sellers) {
                // check engagementCriteria on every activeTrader
                if (seller.getSettings().isCloneable() && checkEngageCriteriaForTrader(ledger, seller)) {
                    if (ledger.getTraderIncomeStatements().get(seller.getEconomyIndex()).getROI() > roiOfMostProfitableTrader) {
                        mostProfitableTrader = seller;
                    }
                }
            }

            // we could reactivate a suspended seller, currently i just clone most profitable seller
            ProvisionBySupply provisionAction = null;
            if (mostProfitableTrader != null) {
                provisionAction = new ProvisionBySupply(economy, mostProfitableTrader);
                actions.add(provisionAction);
            }

            // TODO: check if a market specific placement is beneficial?
            if (provisionAction == null) {
                continue;
            } else {
                Trader provisionedTrader = provisionAction.getProvisionedSeller();
                // run placement after adding a new seller to the economy
                actions.addAll(Placement.placementDecisions(economy, state, timeMiliSec));
                sellers.add(provisionedTrader);
                Ledger newLedger = new Ledger(economy);
                // TODO: change re-creation and computation of expRev to just updation of the values
                newLedger.calculateAllTraderExpensesAndRevenues(economy);
                List<@NonNull Trader> sellers_ = market.getActiveSellers();
                for (Trader seller : sellers_) {
                    if (!checkAcceptanceCriteriaForTrader(newLedger, seller)) {
                        // remove IncomeStatement from ledger, trader from economy and the provision action
                        newLedger.getTraderIncomeStatements().remove(provisionedTrader.getEconomyIndex());
                        economy.removeTrader(provisionedTrader);
                        actions.remove(provisionAction);
                        // after removing trader run placements again
                        actions.addAll(Placement.placementDecisions(economy, state, timeMiliSec));
                        break;
                    }
                }
            }
        }

        return actions;
    }

    /**
     * returns true/false after checking the engagement criteria for a particular trader
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param trader - the trader whose ROI is checked to verify profitability that implies eligibility to clone
     *
     * @return true - if this trader is profitable and is capable of cloning and false otherwise
     */
    public static boolean checkEngageCriteriaForTrader(Ledger ledger, Trader trader) {

        IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(trader.getEconomyIndex());
        return traderIS.getROI() > traderIS.getMaxDesiredROI();
    }

    /**
     * returns true/false after checking the acceptance criteria for a particular trader
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param trader - the trader whose ROI is checked to verify profitability after a clone was added to the market
     *
     * @return true - if this trader is not at loss and false otherwise
     */
    public static boolean checkAcceptanceCriteriaForTrader(Ledger ledger, Trader trader) {

        IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(trader.getEconomyIndex());
        return traderIS.getROI() >= traderIS.getMinDesiredROI();
    }

}
