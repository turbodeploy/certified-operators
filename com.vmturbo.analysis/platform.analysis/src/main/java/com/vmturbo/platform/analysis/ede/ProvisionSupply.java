package com.vmturbo.platform.analysis.ede;

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

public class ProvisionSupply extends Supply {

    private static final Logger logger = Logger.getLogger(ProvisionBySupply.class);

    @Override
    public Trader findTheBestTraderToEngage(Market market, Ledger ledger) {

        Trader mostProfitableTrader = null;
        double roiOfMostProfitableTrader = 0;
        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS =
                            ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
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

    @Override
    public void takeActionAndUpdateLedger(Economy economy, Market market, Ledger ledger,
                    Trader bestTraderToEngage, List<@NonNull Action> actions) {
        // TODO: we could reactivate a suspended seller, currently I just clone most
        // profitable seller
        ProvisionBySupply provisionBySupplyAction =
                        new ProvisionBySupply(economy, bestTraderToEngage);
        actions.add(provisionBySupplyAction.take());
        // get the newly added seller and create an income statement for it in ledger
        Trader provisionedTrader = provisionBySupplyAction.getProvisionedSeller();
        ledger.addTraderIncomeStatement(provisionedTrader);
        return;
    }

    @Override
    public boolean evalAcceptanceCriteriaForMarket(Market market, Ledger ledger,
                    Trader provisionedTrader) {

        for (Trader seller : market.getActiveSellers()) {
            IncomeStatement traderIS =
                            ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            if (traderIS.getROI() < traderIS.getMinDesiredROI()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void rollBackActionAndUpdateLedger(Ledger ledger,
                    Trader provisionedTrader, List<@NonNull Action> actions) {
        // remove IncomeStatement from ledger and rollback actions
        ledger.removeTraderIncomeStatement(provisionedTrader);
        Lists.reverse(actions).forEach(axn -> axn.rollback());
        return;
    }
}
