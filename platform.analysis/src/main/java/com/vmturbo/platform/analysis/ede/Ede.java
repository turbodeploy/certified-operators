package com.vmturbo.platform.analysis.ede;

import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

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
        return Placement.placementDecisions(economy);
    }

}
