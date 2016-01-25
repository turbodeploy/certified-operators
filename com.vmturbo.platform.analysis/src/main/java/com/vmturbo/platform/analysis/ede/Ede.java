package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 *
 * The Economic Decisions Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public final class Ede {

    // Fields

    // The state reflecting the decisions the economic decisions engine takes for Traders
    // Each StateItem refers to a particular trader. Different StateItems refer to different traders.
    private final @NonNull List<@NonNull StateItem> eseState = new ArrayList<>();

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
        // update the state: add/remove state items for traders added/removed since last invocation
        updateState(economy);

        // return  a new set of recommendations - currently only placement
        long timeMiliSec = new Date().getTime(); // number of milliseconds since Jan. 1, 1970
        return Placement.placementDecisions(economy, eseState, timeMiliSec);
    }

    /**
     * Add/remove state items for traders added/removed since last invocation
     *
     * The correctness of this synchronization algorithm depends on the fact that traders are always
     * added to the end of the traders list in the Economy, i.e., the trader index for a particular
     * trader never increases.
     *
     * @param economy - the snapshot of the economy
     */
    private void updateState(@NonNull UnmodifiableEconomy economy) {
        int ecoIndex = 0;
        int stateIndex = 0;
        // remove state items for traders that are no longer there
        while (ecoIndex < economy.getTraders().size() && stateIndex < eseState.size()) {
            while (stateIndex < eseState.size()
                            && eseState.get(stateIndex).getTrader() != economy.getTraders().get(ecoIndex)) {
                // TODO: remove has a linear cost. Change it to pay the linear cost once for all removes
                eseState.remove(stateIndex);
            }
            ecoIndex++;
            stateIndex++;
        }
        // add state items for new traders since last invocation
        for (ecoIndex = eseState.size(); ecoIndex < economy.getTraders().size(); ecoIndex++) {
            eseState.add(new StateItem(economy.getTraders().get(ecoIndex)));
        }
    }
}
