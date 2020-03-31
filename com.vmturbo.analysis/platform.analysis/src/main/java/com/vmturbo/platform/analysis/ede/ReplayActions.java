package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Economy;
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
    // The topology that contains the above traders and targets of above actions.
    // It is needed to map traders from old economy to new using OIDs.
    private @NonNull Topology topology_ = new Topology();

    public List<Action> getActions() {
        return actions_;
    }
    public void setActions(List<Action> actions) {
        actions_ = actions;
    }

    public @NonNull Topology getTopology() {
        return topology_;
    }

    public void setTopology(@NonNull Topology topology) {
        topology_ = topology;
    }

    /**
     * Replay Actions from earlier run on the new {@link Economy}
     *
     * @param economy The {@link Economy} in which actions are to be replayed
     */
    public void replayActions(Economy economy, Ledger ledger) {
        List<Deactivate> deactivateActions = actions_.stream()
                        .filter(action -> action instanceof Deactivate)
                        .map(action -> (Deactivate)action)
                        .collect(Collectors.toList());
        LinkedList<Action> actions =
            new LinkedList<>(tryReplayDeactivateActions(deactivateActions, economy, ledger));
        actions_.removeAll(deactivateActions);

        for (Action action : actions_) {
            try {
                Action ported = action.port(economy,
                    oldTrader -> mapTrader(oldTrader, getTopology(), economy.getTopology()),
                    oldSl -> mapShoppingList(oldSl, getTopology(), economy.getTopology())
                );
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
    private List<Action> tryReplayDeactivateActions(List<Deactivate> deactivateActions, Economy economy,
                    Ledger ledger) {
        List<@NonNull Action> suspendActions = new ArrayList<>();
        if (deactivateActions.isEmpty()) {
            return suspendActions;
        }
        Suspension suspensionInstance = new Suspension();
        // adjust utilThreshold of the seller to maxDesiredUtil*utilTh. Thereby preventing moves
        // that force utilization to exceed maxDesiredUtil*utilTh.
        suspensionInstance.adjustUtilThreshold(economy, true);
        for (Deactivate deactivateAction : deactivateActions) {
            try {
                @NonNull Deactivate ported = deactivateAction.port(economy,
                    oldTrader -> mapTrader(oldTrader, getTopology(), economy.getTopology()),
                    oldSl -> mapShoppingList(oldSl, getTopology(), economy.getTopology())
                );
                @NonNull Trader newTrader = ported.getTarget();
                if (ported.isValid() && isEligibleforSuspensionReplay(newTrader, economy)) {
                    if (Suspension.getSuspensionsthrottlingconfig()
                            == SuspensionsThrottlingConfig.CLUSTER) {
                        Suspension.makeCoSellersNonSuspendable(economy, newTrader);
                    }
                    if (newTrader.getSettings().isControllable()) {
                        suspendActions.addAll(
                            suspensionInstance.deactivateTraderIfPossible(newTrader, economy,
                                                                            ledger, true));
                    } else {
                        // If controllable is false, deactivate the trader without checking criteria
                        // as entities may not be able to move out of the trader with controllable
                        // false.
                        suspendActions.add(ported.take());
                        suspendActions.addAll(ported.getSubsequentActions());
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not replay " + deactivateAction.toString(), e);
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
    private boolean isEligibleforSuspensionReplay(@NonNull Trader trader, Economy economy) {
        // If trader is sole provider in any market, we don't want to replay the suspension.
        // Consider scenario where we replay suspension for PM1 but there was a new placement policy
        // for VM1 (currently on PM2) to move on PM1. We end recommending a reconfigure action because
        // PM1 becomes inactive due to replay of suspension.
        return economy.getMarketsAsSeller(trader).stream()
                .noneMatch(market -> market.getActiveSellers().size() == 1);
    }

    /**
     * Maps a {@link Trader} to another one with the same OID in another topology.
     *
     * @param oldTrader The trader that is going to be mapped.
     * @param oldTopology The topology that contains <b>oldTrader</b>.
     * @param newTopology The topology in which to search for a trader with OID equal to the
     *                    <b>oldTrader</b>'s one.
     * @return The trader in <b>newTopology</b> that has the same OID as <b>oldTrader</b> had in
     *         <b>oldTopology</b> iff one exists.
     * @throws NoSuchElementException Iff no such trader exists in <b>newTopology</b>.
     */
    static @NonNull Trader mapTrader(@NonNull Trader oldTrader,
                                    @NonNull Topology oldTopology, @NonNull Topology newTopology) {
        Long oid = oldTopology.getTraderOids().get(oldTrader);
        Trader newTrader = newTopology.getTraderOids().inverse().get(oid);

        if (newTrader == null) {
            throw new NoSuchElementException("Could not find trader with oid " + oid
                + " " + oldTrader.getDebugInfoNeverUseInCode() + " in new topology");
        }

        return newTrader;
    }

    /**
     * Maps a {@link ShoppingList} to another one with the same OID in another topology.
     *
     * @param oldShoppingList The shopping list that is going to be mapped.
     * @param oldTopology The topology that contains <b>oldShoppingList</b>.
     * @param newTopology The topology in which to search for a shopping list with OID equal to the
     *                    <b>oldShoppingList</b>'s one.
     * @return The shopping list in <b>newTopology</b> that has the same OID as
     *         <b>oldShoppingList</b> had in <b>oldTopology</b> iff one exists.
     * @throws NoSuchElementException Iff no such shopping list exists in <b>newTopology</b>.
     */
    static @NonNull ShoppingList mapShoppingList(@NonNull ShoppingList oldShoppingList,
                                    @NonNull Topology oldTopology, @NonNull Topology newTopology) {
        Long oid = oldTopology.getShoppingListOids().get(oldShoppingList);
        ShoppingList newShoppingList = newTopology.getShoppingListOids().inverse().get(oid);

        if (newShoppingList == null) {
            throw new NoSuchElementException("Could not find shopping list with oid "
                + oid + " " + oldShoppingList.getDebugInfoNeverUseInCode() + " in new topology");
        }

        return newShoppingList;
    }

}
