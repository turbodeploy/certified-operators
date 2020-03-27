package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * This class keeps actions from an analysis run and metadata needed to replay them
 * in a subsequent run.
 */
public class ReplayActions {

    static final Logger logger = LogManager.getLogger(ReplayActions.class);

    // The actions that are to be replayed
    private @NonNull List<Action> actions_ = new LinkedList<>();
    // The traders which could not be suspended
    private @NonNull Set<Trader> rolledBackSuspensionCandidates_ = new HashSet<>();
    // The trader to OID map needed for translating traders between economies
    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids_ = HashBiMap.create();

    public List<Action> getActions() {
        return actions_;
    }
    public void setActions(List<Action> actions) {
        actions_ = actions;
    }
    public Set<Trader> getRolledBackSuspensionCandidates() {
        return rolledBackSuspensionCandidates_;
    }
    public void setRolledBackSuspensionCandidates(Set<Trader> rolledBackSuspensionCandidates) {
        rolledBackSuspensionCandidates_ = rolledBackSuspensionCandidates;
    }
    public BiMap<Trader, Long> getTraderOids() {
        return traderOids_;
    }
    public void setTraderOids(BiMap<Trader, Long> traderOids) {
        traderOids_ = traderOids;
    }

    /**
     * Replay Actions from earlier run on the new {@link Economy}
     *
     * @param economy The {@link Economy} in which actions are to be replayed
     */
    public void replayActions(Economy economy, Ledger ledger) {
        List<Action> deactivateActions = actions_.stream()
                        .filter(action -> action.getType() == ActionType.DEACTIVATE)
                        .collect(Collectors.toList());
        LinkedList<Action> actions = new LinkedList<>(tryReplayDeactivateActions(deactivateActions, economy, ledger));
        actions_.removeAll(deactivateActions);

        for (Action action : actions_) {
            try {
                Function<@NonNull Trader, @NonNull Trader> traderMap = oldTrader -> {
                    Long oid = action.getEconomy().getTopology().getTraderOids().get(oldTrader);
                    Trader newTrader = economy.getTopology().getTraderOids().inverse().get(oid);

                    if (newTrader == null) {
                        throw new NoSuchElementException("Could not find trader with oid " + oid
                            + " " + oldTrader.getDebugInfoNeverUseInCode() + " in new economy");
                    }

                    return newTrader;
                };

                Function<@NonNull ShoppingList, @NonNull ShoppingList> shoppingListMap = oldSl -> {
                    Long oid = action.getEconomy().getTopology().getShoppingListOids().get(oldSl);
                    ShoppingList newSl =
                                    economy.getTopology().getShoppingListOids().inverse().get(oid);

                    if (newSl == null) {
                        throw new NoSuchElementException("Could not find shopping list with oid "
                            + oid + " " + oldSl.getDebugInfoNeverUseInCode() + " in new economy");
                    }

                    return newSl;
                };

                Action ported = action.port(economy, traderMap, shoppingListMap);
                if (ported.isValid()) {
                    actions.add(ported.take());
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("replayed " + action.toString());
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not replay " + action.toString(), e);
                }
            }
        } // end for each action

        actions_ = actions;
    } // end replayActions

    /**
     * Try deactivate actions and replay only if we are able to move all customers
     * out of current trader.
     *
     * @param deactivateActions List of potential deactivate actions.
     * @param economy The {@link Economy} in which actions are to be replayed.
     * @param ledger The {@link Ledger} related to current {@link Economy}.
     * @return action list related to suspension of trader.
     */
    private List<Action> tryReplayDeactivateActions(List<Action> deactivateActions, Economy economy,
                    Ledger ledger) {
        List<@NonNull Action> suspendActions = new ArrayList<>();
        if (deactivateActions.isEmpty()) {
            return suspendActions;
        }
        Suspension suspensionInstance = new Suspension();
        // adjust utilThreshold of the seller to maxDesiredUtil*utilTh. Thereby preventing moves
        // that force utilization to exceed maxDesiredUtil*utilTh.
        suspensionInstance.adjustUtilThreshold(economy, true);
        for (Action deactivateAction : deactivateActions) {
            Deactivate oldAction = (Deactivate) deactivateAction;
            Trader newTrader = translateTrader(oldAction.getTarget(), economy, "Deactivate");
            if (isEligibleforSuspensionReplay(newTrader, economy)) {
                if (Suspension.getSuspensionsthrottlingconfig() == SuspensionsThrottlingConfig.CLUSTER) {
                    Suspension.makeCoSellersNonSuspendable(economy, newTrader);
                }
                if (newTrader.getSettings().isControllable()) {
                    suspendActions.addAll(suspensionInstance.deactivateTraderIfPossible(newTrader, economy,
                                    ledger, true));
                } else {
                    // If controllable is false, deactivate the trader without checking criteria
                    // as entities may not be able to move out of the trader with controllable false.
                    List<Market> marketsAsSeller = economy.getMarketsAsSeller(newTrader);
                    Deactivate replayedSuspension = new Deactivate(economy, newTrader,
                        !marketsAsSeller.isEmpty() ? marketsAsSeller.get(0).getBasket()
                                                   : newTrader.getBasketSold());
                    suspendActions.add(replayedSuspension.take());
                    suspendActions.addAll(replayedSuspension.getSubsequentActions());
                }
            }
        }
        //reset the above set utilThreshold.
        suspensionInstance.adjustUtilThreshold(economy, false);
        return suspendActions;
    }

    /**
     * Check for conditions that qualify trader (like trader state, sole provider etc.)
     * for replay of suspension.
     * @param trader to replay suspension for.
     * @param economy to which trader belongs.
     * @return true, if trader qualifies for replay of suspension.
     */
    private boolean isEligibleforSuspensionReplay(Trader trader, Economy economy) {
        return trader != null
            && trader.getSettings().isSuspendable()
            && trader.getState().isActive()
            // If trader is sole provider in any market, we don't want to replay the suspension.
            // Consider scenario where we replay suspension for PM1 but there was a new placement policy
            // for VM1 (currently on PM2) to move on PM1. We end recommending a reconfigure action because
            // PM1 becomes inactive due to replay of suspension.
            && economy.getMarketsAsSeller(trader).stream()
                .noneMatch(market -> market.getActiveSellers().size() == 1);
    }

    /**
     * Translate the list of rolled-back suspension candidate traders to the given {@link Economy}
     *
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param newTopology The {@link Topology} for the given {@link Economy}
     */
    public void translateRolledbackTraders(Economy newEconomy, Topology newTopology) {
        Set<Trader> newTraders = new HashSet<>();
        rolledBackSuspensionCandidates_.forEach(t -> {
            Trader newTrader = translateTrader(t, newEconomy, "translateTraders");
            if (newTrader != null) {
                newTraders.add(newTrader);
            }
        });
        rolledBackSuspensionCandidates_ = newTraders;
    }

    /**
     * Translate the given trader to the one in new {@link Economy}
     *
     * @param trader The trader for which we want to find the corresponding trader
     *               in new {@link Economy}
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param callerName A tag used by caller, useful in logging
     * @return Trader in new Economy or null if it fails to translate it
     */
    public @Nullable Trader translateTrader(Trader trader, Economy newEconomy,
                                            String callerName) {
        Topology newTopology = newEconomy.getTopology();
        Long oid = traderOids_.get(trader);
        Trader newTrader = newTopology.getTraderOids().inverse().get(oid);
        if (newTrader == null) {
            logger.info("Could not find trader with oid " + oid + " " + callerName + " " +
                         ((trader != null) ? trader.getDebugInfoNeverUseInCode() : "nullTrader"));
        }
        return newTrader;
    }

}
