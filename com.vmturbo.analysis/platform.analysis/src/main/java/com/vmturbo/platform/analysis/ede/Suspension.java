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
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

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
        double c1 = is1.getROI() / is1.getMinDesiredROI();
        double c2 = is2.getROI() / is2.getMinDesiredROI();
        return c1 > c2 ? 1 : c1 == c2 ? 0 : -1;
    });

    private static SuspensionsThrottlingConfig suspensionsThrottlingConfig =
                                                                                 SuspensionsThrottlingConfig.DEFAULT;

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
     *                  the economy
     * @return list of deactivate and move actions
     */
    public @NonNull List<@NonNull Action> suspensionDecisions(@NonNull Economy economy,
                                                              @NonNull Ledger ledger, Ede ede) {
        List<@NonNull Action> allActions = new ArrayList<>();
        int round = 0;
        // suspend entities that arent sellers in any market
        for (Trader seller : economy.getTraders()) {
            if (seller.getSettings().isSuspendable() && seller.getState().isActive()
                &&
                seller.getCustomers().isEmpty()
                &&
                economy.getMarketsAsSeller(seller).isEmpty()) {
                suspendTrader(economy, null, seller, allActions);
            }
        }
        // run suspension for 3 rounds. We can have scenarios where, there are VMs that can move
        // when buyers in a different market make room. In order to enable this, we retry suspensions
        // after a round of economy-wide placements. We do this a third time for better packing as
        // placements is the only expense here
        while (round < 3) {
            ledger.calculateExpRevForTradersInEconomy(economy);
            // adjust utilThreshold to maxDesiredUtil*utilTh of the seller. Thereby preventing moves
            // that force utilization to exceed maxDesiredUtil*utilTh
            adjustUtilThreshold(economy, true);
            ledger_ = ledger;

            for (Market market : economy.getMarkets()) {
                if (economy.getForceStop()) {
                    return allActions;
                }
                // skip markets that don't have suspendable active sellers. We consider only
                // activeSellersAvailableForPlacement as entities should not move out of the ones
                // not available for placement
                if (market.getActiveSellersAvailableForPlacement().isEmpty() ||
                                !market.getActiveSellersAvailableForPlacement().stream().anyMatch(
                                                t -> t.getSettings().isSuspendable())) {
                    continue;
                }
                List<Trader> suspensionCandidates = new ArrayList<>();
                // suspensionCandidates can be only activeSellers that canAcceptNewCustomers that are suspendable
                suspensionCandidates.addAll(market.getActiveSellersAvailableForPlacement().stream()
                                            .filter(t -> t.getSettings().isSuspendable())
                                                .collect(Collectors.toList()));
                for (Trader seller : suspensionCandidates) {
                    if (seller.getCustomers().isEmpty()) {
                        suspendTrader(economy, market, seller, allActions);
                        continue;
                    }
                    IncomeStatement incomeStmt = ledger.getTraderIncomeStatements()
                                    .get(seller.getEconomyIndex());
                    if (!suspensionCandidateHeap_.contains(seller)
                        && !soleProviders.contains(seller)
                        && (incomeStmt.getROI() < (incomeStmt.getMinDesiredROI() +
                                                   incomeStmt.getMaxDesiredROI())
                                                  / 2)) {
                        suspensionCandidateHeap_.offer(seller);
                    }
                }
            }

            Trader trader;
            while ((trader = suspensionCandidateHeap_.poll()) != null) {
                allActions.addAll(deactivateTraderIfPossible(trader, economy, ledger));
            }

            // reset threshold
            adjustUtilThreshold(economy, false);

            // run economy wide placements after every round of suspension
            allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                                                                  EconomyConstants.SUPPLY_PHASE));
            round++;
        }
        return allActions;
    }

    /**
     * If a trader can be suspended, adds to allActions list .Tries to move all customers
     * can move out of current trader and if its customer list is empty the trader is suspended.
     *
     * @param trader The {@link Trader} we try to suspend.
     * @param economy the {@link Economy} which is being evaluated for suspension.
     * @param ledger The {@link Ledger} related to current {@link Economy}
     * @return action list related to suspension of trader.
     */
    public List<Action> deactivateTraderIfPossible(Trader trader, Economy economy, Ledger ledger) {
        List<Market> markets = economy.getMarketsAsSeller(trader);
        Market market = (markets == null || markets.isEmpty()) ? null : markets.get(0);

        List<@NonNull Action> suspendActions = new ArrayList<>();
        if (!trader.getSettings().isSuspendable()) {
            return suspendActions;
        }
        List<ShoppingList> customersOfSuspCandidate = new ArrayList<>();
        customersOfSuspCandidate.addAll(trader.getCustomers());

        suspendTrader(economy, market, trader, suspendActions);

        if (market != null) {
            // perform placement on just the customers on the suspensionCandidate
            suspendActions.addAll(
                                  Placement.runPlacementsTillConverge(economy,
                                                                      customersOfSuspCandidate,
                                                                      ledger, true,
                                                                      EconomyConstants.SUSPENSION_PHASE));
        }

        // rollback actions if the trader still has customers
        if (!trader.getCustomers().isEmpty()) {
            Lists.reverse(suspendActions).forEach(axn -> axn.rollback());
            return new ArrayList<>();
        } else {
            if (suspensionsThrottlingConfig == SuspensionsThrottlingConfig.CLUSTER) {
                makeCoSellersNonSuspendable(economy, trader);
            }
        }
        return suspendActions;
    }

    /**
     * Adjust the utilThreshold of {@link CommoditySold} by {@link Trader}s to maxDesiredUtil
     *
     * @param economy - the {@link Economy} which is being evaluated for suspension
     * @param update - set threshold to maxDesiredUtil*utilThreshold if true or reset to original value if false
     */
    @VisibleForTesting
    void adjustUtilThreshold(Economy economy, boolean update) {
        if (update) {
            for (Trader seller : economy.getTraders()) {
                double util = seller.getSettings().getMaxDesiredUtil();
                for (CommoditySold cs : seller.getCommoditiesSold()) {
                    double utilThreshold = cs.getSettings().getUtilizationUpperBound();
                    PriceFunction pf = cs.getSettings().getPriceFunction();
                    double priceAtMaxUtil = pf.unitPrice(util * utilThreshold, null, seller, cs, economy);
                    // skip if step and constant priceFns
                    if (!((priceAtMaxUtil == pf.unitPrice(0.0, null, seller, cs, economy)) ||
                          (priceAtMaxUtil == pf.unitPrice(1.0, null, seller, cs, economy)))) {
                        cs.getSettings().setUtilizationUpperBound(util * utilThreshold);
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
            if (marketsAsSeller.stream().anyMatch((m) -> m.getActiveSellersAvailableForPlacement()
                            .size() == 1 && m.getBuyers().stream().anyMatch(
                                                                            sl -> !sl.getBuyer()
                                                                                            .getSettings()
                                                                                            .isGuaranteedBuyer()))) {
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

    @VisibleForTesting
    public Set<Trader> getSoleProviders() {
        return soleProviders;
    }

    /**
     * Make co-sellers of suspension candidate inactive.
     * get Markets susp candidate sells in, although INACTIVE
     * disable suspension of all other traders in markets where deactivated trader
     * is a seller including inactive sellers as they may have been picked in the
     * previous round
     * @param economy The economy trader participates in.
     * @param trader The trader, which is the suspension candidate picked.
     */
    protected static void makeCoSellersNonSuspendable(Economy economy, Trader trader) {
        final Trader picked = trader;
        for (Market mktAsSeller : economy.getMarketsAsSeller(trader)) {
            mktAsSeller.getActiveSellers().stream().filter(seller -> seller != picked)
                            .forEach(t -> t.getSettings().setSuspendable(false));
            mktAsSeller.getInactiveSellers().stream().filter(seller -> seller != picked)
                            .forEach(t -> t.getSettings().setSuspendable(false));
        }
    }

    public static SuspensionsThrottlingConfig getSuspensionsthrottlingconfig() {
        return suspensionsThrottlingConfig;
    }

    public static void setSuspensionsThrottlingConfig(SuspensionsThrottlingConfig suspensionsThrottligConfig) {
        suspensionsThrottlingConfig = suspensionsThrottligConfig;
    }
}
