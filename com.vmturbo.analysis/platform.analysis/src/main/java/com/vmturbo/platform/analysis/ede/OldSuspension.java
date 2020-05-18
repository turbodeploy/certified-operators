package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class OldSuspension {

    private static final String SUSPENSION_PHASE = "Suspension Phase";
    // a set to keep all traders that is the sole seller in any market.
    private @NonNull Set<@NonNull Trader> soleProviders = new HashSet<@NonNull Trader>();
    // a map to keep unprofitable sellers that should not be considered as suspension candidate
    // in a particular market. In general, those sellers have gone through the process in which it
    // was selected to deactivate, however, after deactivating it and run placement decisions,
    // some customers on this trader can not move out of it. So it should not be selected again
    // because we there will always be some customers staying on it.
    private @NonNull Set<@NonNull Trader> unprofitableSellersCouldNotSuspend =
                    new HashSet<@NonNull Trader>();

    /**
     * Return a list of actions to suspend unneeded supply in the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the {@link Economy} whose traders we may clone or suspend to bring in
     *                  the desired state
     * @param ledger - the {@link Ledger}} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param ede - the {@link Ede} which contains the utility method to break down the compound
     *        move actions
     * @param isShopTogether - the boolean to indicate if shopTogether should be used to make
     *        placement decisions or not
     * @return a list of actions with regard to supply change and the move actions after applying
     *         the supply change actions
     */
    public @NonNull List<@NonNull Action> suspensionDecisions(@NonNull Economy economy,
                    @NonNull Ledger ledger, Ede ede, boolean isShopTogether) {
        List<@NonNull Action> allActions = new ArrayList<>();
        List<@NonNull Action> actions = new ArrayList<>();
        EstimateSupply es = null;
        if (economy.getSettings().isEstimatesEnabled()) {
            es = new EstimateSupply(economy, ledger, false);
        }
        for (Market market : economy.getMarkets()) {
            // consider only sellers that canAcceptNewCustomers for suspension
            if (market.getActiveSellersAvailableForPlacement().stream()
                            .allMatch(s -> !s.getSettings().isSuspendable())) {
                continue;
            }
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
            // break if forceStop or if market has no activeSellers or suspCandidates or leastProfitableTrader
            // or if we rollback because there are customers that cant move outside this leastProfitableTraders
            for (;;) {
                if (economy.getForceStop()) {
                    return allActions;
                }
                // skip markets that don't have suspendable active sellers
                actions.clear();
                if (market.getActiveSellers().isEmpty()
                                || (es != null && es.getSuspensionCandidates(market) == null)) {
                    break;
                }
                List<Trader> suspensionCandidates = es != null
                                ? (@NonNull @ReadOnly List<@NonNull Trader>)es
                                                .getSuspensionCandidates(market).stream()
                                                .filter(t -> t.getState().isActive())
                                                .collect(Collectors.toList())
                                : market.getActiveSellers();
                Trader leastProfitableTrader = findTheBestTraderToEngage(suspensionCandidates, ledger);
                // break if there is no seller that satisfies the engagement criteria in the market
                if (leastProfitableTrader == null) {
                    break;
                }
                double oldRevenue = ledger.getTraderIncomeStatements()
                                .get(leastProfitableTrader.getEconomyIndex()).getRevenues();
                takeActionAndUpdateLedger(economy, market.getBasket(), ledger,
                                            leastProfitableTrader, actions);

                List<@NonNull Action> placementActions = Placement.runPlacementsTillConverge(
                                economy, ledger, SUSPENSION_PHASE).getActions();
                actions.addAll(placementActions);
                // keep a set of traders whose ROI will be affected as a result of placement after
                // the suspension action.
                // we need to check if the placement decision made after the suspension will result in
                // any trader that goes outside the desire state, the traders may be in other markets
                Set<Trader> affectedTraders = new HashSet<Trader>();
                for (Action a : placementActions) {
                    if (a instanceof CompoundMove) {
                        for (Move move : ((CompoundMove)a).getConstituentMoves()) {
                            affectedTraders.add(move.getDestination());
                        }
                    } else if (a instanceof Move) {
                        affectedTraders.add(((Move)a).getDestination());
                    }
                }
                // if any non-guaranteedBuyer is still present on the suspension candidate,
                boolean hasCustomerPresent = leastProfitableTrader.getCustomers().stream().filter(
                                sl -> !sl.getBuyer().getSettings().isGuaranteedBuyer()).count() != 0;
                // if we find the least profitable trader who has customers that can not move out
                // of it after placement, we continue to the second least profitable trader, if the
                // second least profitable trader has same issue, we go to the third least profitable, etc.
                if (!evalAcceptanceCriteriaForMarket(economy, market, ledger, leastProfitableTrader,
                                placementActions, affectedTraders)) {
                    rollBackActionAndUpdateLedger(economy, market, ledger, actions, affectedTraders);
                    if (hasCustomerPresent) {
                        continue;
                    } else {
                        break;
                    }
                }
                if (es != null) {
                    es.getSuspensionCandidates(market).remove(leastProfitableTrader);
                }
                ((ActionImpl)actions.get(0)).setImportance(-oldRevenue);
                allActions.addAll(actions);
            }
        }

        return allActions;
    }

    /**
     * Return the best trader to suspend after checking the engagement criteria for all
     * traders of a particular market
     *
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the sellers considered
     * @return the best trader satisfy the engagement criteria if there is any, otherwise NULL
     */
    public Trader findTheBestTraderToEngage(List<Trader> candidates, Ledger ledger) {
        Trader leastProfitableTrader = null;
        double roiOfLeastProfitableTrader = Double.MAX_VALUE;
        for (Trader seller : candidates) {
            if (!seller.getSettings().isSuspendable()) {
                continue;
            }
            if (seller.getCustomers().isEmpty()) {
                return seller;
            }
            // we should not consider sole providers or the sellers that have been selected
            // as suspension candidate once but failed to move customers out of itself
            if (soleProviders.contains(seller) || (unprofitableSellersCouldNotSuspend
                            .contains(seller))) {
                continue;
            }
            IncomeStatement traderIS =
                            ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());
            double roiOfTrader = traderIS.getROI();
            if ((roiOfTrader < traderIS.getMinDesiredROI())
                            && (roiOfTrader < roiOfLeastProfitableTrader)) {
                leastProfitableTrader = seller;
                roiOfLeastProfitableTrader = roiOfTrader;
            }
        }
        return leastProfitableTrader;
    }

    /**
     * Return true/false after checking the acceptance criteria for a particular market
     *
     * @param economy - the {@link Economy} in which the suspend action is taken place
     * @param market - the {@link Market} whose sellers are considered to verify profitability that
     *                 implies eligibility to suspend
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the sellers considered
     * @param suspensionCandidate - the {@link Trader} that is suspended
     * @param actions - a list of placement actions generated after the supply change action
     * @param affectedTraders the traders that are the destinations of placement actions
     * @return true - if the acceptance criteria is met by every trader in market
     */
    public boolean evalAcceptanceCriteriaForMarket(Economy economy, Market market, Ledger ledger,
                    Trader suspensionCandidate, List<@NonNull Action> actions,
                    Set<Trader> affectedTraders) {
        // if any non-guaranteedBuyer is still present on the suspension candidate, cancel
        // suspension and put this candidate into unprofitableSellersCouldNotSuspend so that it
        // would not be considered again next round
        if (suspensionCandidate.getCustomers().stream()
                        .filter(sl -> !sl.getBuyer().getSettings().isGuaranteedBuyer())
                        .count() != 0) {
            unprofitableSellersCouldNotSuspend.add(suspensionCandidate);
            return false;
        }
        for (Trader t : affectedTraders) {
            // calculate the income statement for affected sellers, they may not in the current market
            ledger.calculateExpRevForTraderAndGetTopRevenue(economy, t);
            IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(t.getEconomyIndex());
            if (traderIS.getROI() > traderIS.getMaxDesiredROI()) {
                // since the suspending the candidate leads to undesirable ROI for some traders
                // after running full economy placement, we should not consider it as suspension
                // candidate again
                unprofitableSellersCouldNotSuspend.add(suspensionCandidate);
                return false;
            }
        }
        return true;
    }

    /**
     * Return a list of actions which contains the suspend action. The particular action would be
     * created, executed and added to the list returned.
     *
     * @param economy - the {@link Economy} in which the suspend action is taken place
     * @param triggeringBasket - The {@link Basket} of the {@link Market} in which the suspend
     *                         action takes place
     * @param ledger -  the {@link Ledger} that holds the incomeStatement of the economy
     * @param bestTraderToEngage - the trader that satisfies the engagement criteria best
     * @param actions - a list that the suspend action would be added to
     */
    public void takeActionAndUpdateLedger(Economy economy, @NonNull Basket triggeringBasket,
              Ledger ledger, Trader bestTraderToEngage, List<@NonNull Action> actions) {
        Deactivate deactivateAction = new Deactivate(economy, bestTraderToEngage, triggeringBasket);
        actions.add(deactivateAction.take());
    }

    /**
     * Rolling back the actions in a reverse order.
     * <p>
     *  Rolling back the actions must be in reverse order, otherwise we may encounter issues.
     *  e.g: action1 is trader1 move from A to B, action2 is trader1 move from B to C
     *  rolling back should start from action2 as trader1 is at C now
     * </p>
     *
     * @param economy - the {@link Economy} in which the suspend action is taken place
     * @param market - the {@link Market} in which the suspend action takes place
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the economy
     * @param actions - a list of actions to be rolled back
     * @param affectedTraders the traders that are the destinations of placement actions
     */
    public void rollBackActionAndUpdateLedger(Economy economy, Market market, Ledger ledger,
                    List<@NonNull Action> actions, Set<Trader> affectedTraders) {
        // first roll back all the move actions
        Lists.reverse(actions).forEach(axn -> axn.rollback());
        // then calculate the expense and revenues for action affected sellers in current market
        // as for affected sellers outside market, we will calculate the expense and revenue once
        // running suspension on their market
        for (Trader t : affectedTraders) {
            if (market.getActiveSellersAvailableForPlacement().contains(t)) {
                ledger.calculateExpRevForTraderAndGetTopRevenue(economy, t);
            }
        }
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
            // and it has some customers which are not the shopping lists from guaranteed buyers
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
