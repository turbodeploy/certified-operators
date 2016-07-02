package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;

/**
 *
 * The Economic Decisions Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public final class Ede {

    // Fields

    // Constructor

    /**
     * Constructs the Economic Decisions Engine, with an empty initial State
     */
    public Ede() {}

    // Methods

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy - the snapshot of the economy which we analyze and take decisions
     * @return A list of actions suggested by the economic decisions engine
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy) {
        @NonNull List<Action> actions = new ArrayList<>();
        // generate placement actions
        boolean keepRunning = true;
        while (keepRunning) {
            List<Action> placeActions = Placement.placementDecisions(economy);
            keepRunning = !placeActions.isEmpty();
            actions.addAll(placeActions);
        }
        Ledger ledger = new Ledger(economy);
        // generate provision actions
        actions.addAll(Provision.provisionDecisions(economy, ledger));
        actions.addAll(new Suspension().supplyDecisions(economy, ledger, false));
        actions.addAll(Resize.resizeDecisions(economy));
        return actions;
    }

    /**
     * A helper method to break down the compoundMove to individual move so that legacy UI can
     * assimilate it. This method should be called only when legacy UI is used!
     *
     * @param compoundMoves a list of CompoundMove actions to be broken down into individual
     * Move actions
     * @return a list of moves that constitute the compoundMove
     */
    public static List<Action> breakDownCompoundMove(List<Action> compoundMoves) {
        // break down the compound move to individual moves so that legacy UI can assimilate it.
        // TODO: if new UI can support compoundMove, we do not need this break down
        List<Action> moveActions = new ArrayList<Action>();
        compoundMoves.forEach(a -> {
            if (a instanceof CompoundMove) {
                moveActions.addAll(((CompoundMove)a).getConstituentMoves());
            }
        });
        return moveActions;
    }
}
