package com.vmturbo.platform.analysis.ese;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

/**
 *
 * The Economic Scheduling Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public class Ese {

    // Fields

    // The state reflecting the decisions the economic scheduling engine takes for Traders
    private List<StateItem> eseState = new ArrayList<>();

    // Constructor

    /**
     * Constructs the Economic Scheduling Engine.
     */
    public Ese() {}

    // Methods

    /**
     * Create a new set of recommendations for a snapshot of the economy.
     *
     * @param economy - the snapshot of the economy which we analyze and take decisions
     * @return A list of recommendations suggested by the economic scheduling engine
     */
    public List<RecommendationItem> createRecommendations(Economy economy) {
        List<RecommendationItem> recommendations = new ArrayList<RecommendationItem>();
        long time = new Date().getTime();

        // update the state
        int ecoIndex = 0;
        int stateIndex = 0;
        // remove state items for traders that are no longer there
        while (ecoIndex < economy.getTraders().size() && stateIndex < eseState.size()) {
            while (!eseState.get(stateIndex).getTrader().equals(economy.getTraders().get(ecoIndex))) {
                eseState.remove(stateIndex);
            }
            ecoIndex++;
            stateIndex++;
        }
        // add state items for new traders since last invocation
        for (ecoIndex = eseState.size(); ecoIndex < economy.getTraders().size(); ecoIndex++) {
            eseState.add(new StateItem(economy.getTraders().get(ecoIndex)));
        }

        // create a new set of recommendations
        recommendations.addAll(Placement.placementDecisions(economy, eseState, time));

        return recommendations;
    }
}
