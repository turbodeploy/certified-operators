package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * This class keeps actions from an analysis run and metadata needed to replay them in a subsequent
 * run.
 *
 * <p>{@link ReplayActions} objects should be considered read-only, although currently there is no
 * way to force that the encapsulated topology won't be changed through the getter...</p>
 */
public class ReplayActions {
    // Fields
    static final Logger logger = LogManager.getLogger(ReplayActions.class);

    // The actions to be replayed during the 2nd analysis sub-cycle.
    private final @NonNull ImmutableList<@NonNull Action> actions_;
    // The deactivate actions to be replayed during the 1st analysis sub-cycle.
    private final @NonNull ImmutableList<@NonNull Deactivate> deactivateActions_;
    // The topology that contains the above traders and targets of above actions.
    // It is needed to map traders from old economy to new using OIDs.
    private final @NonNull Topology topology_;

    // Constructors

    /**
     * Constructs an empty {@link ReplayActions} object.
     */
    public ReplayActions() {
        actions_ = ImmutableList.of();
        deactivateActions_ = ImmutableList.of();
        topology_ = new Topology();
    }

    /**
     * Constructs a {@link ReplayActions} object with the given contents.
     *
     * @param actions The list of actions {@code this} object will attempt to replay. It may be
     *                copied internally as needed to ensure that the internal list wont be modified.
     * @param deactivateActions The list of deactivate actions {@code this} object will attempt to
     *                          replay. It may be copied internally as needed to ensure that the
     *                          internal list wont be modified.
     * @param topology The topology with which the above actions are associated with.
     */
    public ReplayActions(@NonNull List<@NonNull Action> actions,
                         @NonNull List<@NonNull Deactivate> deactivateActions,
                         @NonNull Topology topology) {
        actions_ = ImmutableList.copyOf(actions);
        deactivateActions_ = ImmutableList.copyOf(deactivateActions);
        topology_ = topology;
    }

    // Methods

    /**
     * Returns an immutable list of actions {@code this} object is responsible for replaying.
     */
    @Pure
    public @NonNull ImmutableList<@NonNull Action> getActions(@ReadOnly ReplayActions this) {
        return actions_;
    }

    /**
     * Returns an immutable list of deactivate actions {@code this} object is responsible for
     * replaying.
     */
    @Pure
    public @NonNull ImmutableList<@NonNull Deactivate>
                                                getDeactivateActions(@ReadOnly ReplayActions this) {
        return deactivateActions_;
    }

    /**
     * Returns the topology associated with the {@link #getActions() actions} {@code this} object is
     * responsible for replaying.
     *
     * <p>It should not be modified.</p>
     *
     * @see #getActions()
     */
    @Pure
    public @NonNull Topology getTopology(@ReadOnly ReplayActions this) {
        return topology_;
    }

    /**
     * Replays encapsulated {@link #getActions() actions} on the given {@link Economy}.
     *
     * @param economy The {@link Economy} on which the actions are to be replayed.
     * @return The list of actions that were taken on <b>economy</b>.
     */
    public @NonNull List<@NonNull Action> replayActions(Economy economy) {
        List<Action> actions = new ArrayList<>();

        for (Action action : getActions()) {
            try {
                Action ported = action.port(economy,
                    oldTrader -> mapTrader(oldTrader, getTopology(), economy.getTopology()),
                    oldSl -> mapShoppingList(oldSl, getTopology(), economy.getTopology())
                );

                if (ported.isValid()) {
                    actions.add(ported.take());
                    List<Action> subActions = ported.getSubsequentActions();
                    actions.addAll(subActions);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Replayed " + action.toString());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Attempted to replay " + action.toString()
                                    + ", but it was no longer valid");
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not replay " + action.toString(), e);
                }
            }
        } // end for each action

        return actions;
    } // end replayActions

    /**
     * Tries to replay {@link #getDeactivateActions() deactivate actions} only if we are able to
     * move all customers out of current trader.
     *
     * @param economy The {@link Economy} in which actions are to be replayed.
     * @param ledger The {@link Ledger} related to current {@link Economy}.
     * @return action list related to suspension of trader.
     */
    public @NonNull List<@NonNull Action> tryReplayDeactivateActions(@NonNull Economy economy,
                                                                     Ledger ledger) {
        @NonNull List<@NonNull Action> suspendActions = new ArrayList<>();
        if (getDeactivateActions().isEmpty()) {
            return suspendActions;
        }
        Suspension suspensionInstance = new Suspension();
        // adjust utilThreshold of the seller to maxDesiredUtil*utilTh. Thereby preventing moves
        // that force utilization to exceed maxDesiredUtil*utilTh.
        suspensionInstance.adjustUtilThreshold(economy, true);
        for (Deactivate deactivateAction : getDeactivateActions()) {
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
