package com.vmturbo.platform.analysis.ede;

import java.util.List;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.collect.Lists;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class Suspension extends Supply {

    private static final Logger logger = Logger.getLogger(ProvisionBySupply.class);

    @Override
    public Trader findTheBestTraderToEngage(Market market, Ledger ledger) {
        // if there is only on active seller and it has customers, we should not consider suspending it
        if (market.getActiveSellers().size() == 1 && !market.getBuyers().isEmpty()) {
            return null;
        }
        Trader leastProfitableTrader = null;
        double roiOfLeastProfitableTrader = Double.MAX_VALUE;
        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS =
                            ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            double roiOfTrader = traderIS.getROI();
            if (seller.getSettings().isSuspendable() && (roiOfTrader < traderIS.getMinDesiredROI())
                            && (roiOfTrader < roiOfLeastProfitableTrader)) {
                leastProfitableTrader = seller;
                roiOfLeastProfitableTrader = roiOfTrader;
            }
        }
        return leastProfitableTrader;
    }

    @Override
    public boolean evalAcceptanceCriteriaForMarket(Market market, Ledger ledger) {

        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS =
                            ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            if (traderIS.getROI() > traderIS.getMaxDesiredROI()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void takeActionAndUpdateLedger(Economy economy, Market market, Ledger ledger,
                    Trader bestTraderToEngage, List<@NonNull Action> actions) {
        Deactivate deactivateAction = new Deactivate(economy, bestTraderToEngage, market);
        actions.add(deactivateAction.take());
        return;
    }

    @Override
    public void rollBackActionAndUpdateLedger(Ledger ledger, Trader provisionedTrader,
                    List<@NonNull Action> actions) {
        if (provisionedTrader != null) {
            // this is the roll back for suspension so provisionedTrader should always be null
            logger.error("ProvisionedTrader is not null when rolling back a suspension action!");
        }
        Lists.reverse(actions).forEach(axn -> axn.rollback());
        return;
    }

}
