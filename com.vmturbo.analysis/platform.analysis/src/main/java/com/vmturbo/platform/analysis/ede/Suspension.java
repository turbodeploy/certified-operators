package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class Suspension {

    // a set to keep all traders that is the sole seller in any market.
    private @NonNull Set<@NonNull Trader> soleProviders = new HashSet<@NonNull Trader>();
    // a map to keep unprofitable sellers that should not be considered as suspension candidate in
    // any market. In general, those sellers have gone through the process in which it was selected
    // to deactivate, however, after deactivating it and run placement decisions, some customers on
    // this trader can not move out of it. So it should not be selected again because there will
    // always be some customers staying on it.
    private @NonNull Set<@NonNull Trader> unprofitableSellersCouldNotSuspend =
                    new HashSet<@NonNull Trader>();
    private Ledger ledger_;

    private PriorityQueue<Trader> suspensionCandidateHeap_ = new PriorityQueue<>((t1, t2) -> {
        IncomeStatement is1 = ledger_.getTraderIncomeStatements().get(t1.getEconomyIndex());
        IncomeStatement is2 = ledger_.getTraderIncomeStatements().get(t2.getEconomyIndex());
        double c1 = is1.getROI()/is1.getMinDesiredROI();
        double c2 = is2.getROI()/is2.getMinDesiredROI();
        return c1 > c2 ? 1 : c1 == c2 ? 0 : -1;
    });

    /**
     * Return a list of recommendations to optimize the suspension of all eligible traders in the
     * economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose non-profitable traders we want to suspend while remaining
     *                  in the desired state
     * @param ledger - the class that contains exp/rev about all the traders and commodities in
     *                  the ecomomy
     * @param isShopTogether - flag specifies if we want to use SNM or normal placement between
     *                  suspensions
     *
     * @return list of deactivate and move actions
     */
    public @NonNull List<@NonNull Action> suspensionDecisions(@NonNull Economy economy,
                    @NonNull Ledger ledger, Ede ede, boolean isShopTogether) {
        List<@NonNull Action> allActions = new ArrayList<>();
        List<@NonNull Action> actions = new ArrayList<>();
        int round=0;
        // run suspension for 2 rounds
        while(round < 2) {
            ledger.calculateExpRevForTradersInEconomy(economy);
            adjustUtilThreshold(economy, true);
            ledger_ = ledger;

            for (Market market : economy.getMarkets()) {
                if (economy.getForceStop()) {
                    return allActions;
                }
                // skip markets that don't have suspendable active sellers
                if (market.getActiveSellers().isEmpty() || !market.getActiveSellers().stream().anyMatch(
                                t -> t.getSettings().isSuspendable())) {
                    continue;
                }
                List<Trader> suspensionCandidates = new ArrayList<>();
                suspensionCandidates.addAll(market.getActiveSellers().stream().filter(
                                t -> t.getSettings().isSuspendable()).collect(Collectors.toList()));
                for (Trader seller : suspensionCandidates) {
                    if (seller.getCustomers().isEmpty()) {
                        suspendTrader(economy, market, seller, allActions);
                        continue;
                    }
                    IncomeStatement incomeStmt = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
                    if (!suspensionCandidateHeap_.contains(seller) && !soleProviders.contains(seller)
                                    && (incomeStmt.getROI() < (incomeStmt.getMinDesiredROI() +
                                                    incomeStmt.getMaxDesiredROI())/2)) {
                        suspensionCandidateHeap_.offer(seller);
                    }
                }
            }

            Trader trader;
            while ((trader = suspensionCandidateHeap_.poll()) != null) {
                actions.clear();
                Market sampleMarket = economy.getMarketsAsSeller(trader).get(0);
                List<ShoppingList> sls = new ArrayList<>();
                for (Market mkt : economy.getMarketsAsSeller(trader)) {
                    sls.addAll(mkt.getBuyers());
                }
                suspendTrader(economy, sampleMarket, trader, actions);
                // perform placement on just the buyers of the markets that the trader sells in
                actions.addAll(Placement.runPlacementsTillConverge(economy, sls, ledger,
                                isShopTogether, true, "SUSPENSION"));

                // rollback actions if the trader still has customers
                if (!trader.getCustomers().isEmpty()) {
                    Lists.reverse(actions).forEach(axn -> axn.rollback());
                } else {
                    allActions.addAll(actions);
                }
            }
            // reset threshold
            adjustUtilThreshold(economy, false);

            // run economy wide placements after every round of suspension
            allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether, "SUPPLY"));
            round++;
        }
        return allActions;
    }

    /**
     * Adjust the utilThreshold of {@link CommoditySold} by {@link Trader}s to maxDesiredUtil
     *
     * @param economy - the {@link Economy} which is being evaluated for suspension
     * @param update - set threshold to maxDesiredUtil if true or reset to original value if false
     */
    @VisibleForTesting
    void adjustUtilThreshold (Economy economy, boolean update) {
        if (update) {
            for (Trader seller : economy.getTraders()) {
                double util = seller.getSettings().getMaxDesiredUtil();
                for (CommoditySold cs : seller.getCommoditiesSold()) {
                    // skip if step priceFn
                    if (cs.getSettings().getPriceFunction().unitPrice(util) == 1) {
                        continue;
                    } else {
                        cs.getSettings().setUtilizationUpperBound(util);
                    }
                }
            }
        } else {
            for (Trader seller : economy.getTraders()) {
                for (CommoditySold cs : seller.getCommoditiesSold()) {
                    CommoditySoldSettings csSett = cs.getSettings();
                    csSett.setUtilizationUpperBound(csSett.getOrigUtilizationUpperBound());
                }
            }
        }
    }

    /**
     * Suspend the <code>bestTraderToEngage</code> and add the action to the <code>actions</code> list
     *
     * @param economy - the {@link Economy} in which the suspend action is performed
     * @param market - the {@link Market} in which the suspend action takes place
     * @param traderToSuspend - the trader that satisfies the engagement criteria best
     * @param actions - a list that the suspend action would be added to
     */
    public void suspendTrader(Economy economy, Market market, Trader traderToSuspend,
                        List<@NonNull Action> actions) {
        Deactivate deactivateAction = new Deactivate(economy, traderToSuspend, market);
        actions.add(deactivateAction.take());
        return;
    }

    /**
     * Construct the set which keeps traders that are the sole seller in any market.
     *
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

    public void setRolledBack(@NonNull Set<@NonNull Trader> rolledBackTraders) {
        unprofitableSellersCouldNotSuspend = rolledBackTraders;
    }

    public @NonNull Set<@NonNull Trader> getRolledBack() {
        return unprofitableSellersCouldNotSuspend;
    }

}
