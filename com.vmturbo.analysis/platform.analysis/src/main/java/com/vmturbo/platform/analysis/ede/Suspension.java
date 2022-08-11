package com.vmturbo.platform.analysis.ede;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.collect.Lists;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class Suspension extends Supply {

    private static final Logger logger = Logger.getLogger(Suspension.class);

    // a set to keep all traders that is the sole seller in any market.
    private @NonNull Set<@NonNull Trader> soleProviders = new HashSet<@NonNull Trader>();
    // a map to keep unprofitable sellers that should not be considered as suspension candidate
    // in a particular market. In general, those sellers have gone through the process in which it
    // was selected to deactivate, however, after deactivating it and run placement desicisions,
    // some customers on this trader can not move out of it. So it should not be selected again
    // because we there will always be some customers staying on it.
    private @NonNull Set<@NonNull Trader> unprofitableSellersCouldNotSuspend =
                    new HashSet<@NonNull Trader>();

    @Override
    public Trader findTheBestTraderToEngage(Market market, Ledger ledger) {
        Trader leastProfitableTrader = null;
        double roiOfLeastProfitableTrader = Double.MAX_VALUE;
        for (Trader seller : market.getActiveSellers()) {
            // we should not consider sole providers or the sellers that have been selected
            // as suspension candidate once but failed to move customers out of itself
            if (soleProviders.contains(seller) || (unprofitableSellersCouldNotSuspend
                            .contains(seller))) {
                continue;
            }
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
    public boolean evalAcceptanceCriteriaForMarket(Market market, Ledger ledger,
                    Trader suspensionCandidate) {
        // if any customer still present on the suspension candidate, cancel suspension
        // and put this candidate into unprofitableSellersCouldNotSuspend so that it would
        // not be considered again next round
        if (!suspensionCandidate.getCustomers().isEmpty()) {
            unprofitableSellersCouldNotSuspend.add(suspensionCandidate);
            return false;
        }
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

    /**
     * Construct the set which keeps traders that are the sole seller in any market.
     * @param economy The {@link Economy} in which suspension would take place.
     */
    public void findSoleProviders(Economy economy) {
        for (Trader trader : economy.getTraders()) {
            List<Market> marketsAsSeller = economy.getMarketsAsSeller(trader);
            // being the sole provider means the seller is the only active seller in a market
            // and it has some customers which are not the shoppinglists from guaranteed buyers
            if (marketsAsSeller.stream().anyMatch((m) -> m.getActiveSellers().size() == 1 && m
                            .getBuyers().stream()
                            .anyMatch(sl -> !sl.getBuyer().getSettings().isGuaranteedBuyer()))) {
                soleProviders.add(trader);
            }
        }
    }
}
