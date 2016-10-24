package com.vmturbo.platform.analysis.ede;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
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

    static final Logger logger = Logger.getLogger(Ede.class);

    // Methods

    /**
     * Add 'collapse' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
            boolean isShopTogether, boolean isProvision, boolean isSuspension,
            boolean isResize) {
        return generateActions(economy, isShopTogether, isProvision, isSuspension, isResize, false);
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param isShopTogether True if we want to enable SNM and false otherwise.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see {@link Action#collapsed(List)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                    boolean isShopTogether, boolean isProvision, boolean isSuspension,
                    boolean isResize, boolean collapse) {
        logger.info("Plan Started.");
        // Start by provisioning enough traders to satisfy all the demand
        @NonNull List<Action> actions = BootstrapSupply.bootstrapSupplyDecisions(economy);
        logger.info("Plan completed bootstrap with " + actions.size() + " actions.");
        int oldActionCount = actions.size();
        // run placement algorithm to balance the environment
        actions.addAll(Placement.runPlacementsTillConverge(economy, isShopTogether));
        logger.info("Plan completed initial placement with " + (actions.size() - oldActionCount) + " actions.");
        oldActionCount = actions.size();
        Ledger ledger = new Ledger(economy);
        // trigger provision, suspension and resize algorithm only when needed
        if (isProvision) {
            actions.addAll(Provision.provisionDecisions(economy, ledger, isShopTogether, this));
        }
        logger.info("Plan completed provisioning with " + (actions.size() - oldActionCount) + " actions.");
        oldActionCount = actions.size();
        if (isSuspension) {
            Suspension suspension = new Suspension();
            // find if any seller is the sole provider in any market, if so, it should not
            // be considered as suspension candidate
            suspension.findSoleProviders(economy);
            actions.addAll(suspension.supplyDecisions(economy, ledger, this, isShopTogether,
                            false));
        }
        logger.info("Plan completed suspending with " + (actions.size() - oldActionCount) + " actions.");
        oldActionCount = actions.size();
//        if (isResize) {
//            actions.addAll(Resizer.resizeDecisions(economy, ledger));
//        }
//        logger.info("Plan completed resizing with " + (actions.size() - oldActionCount) + " actions.");
        if (collapse) {
            // TODO: All of this should be done in the collapse method
            // Don't collapse Moves that have a 'null' source (no-source moves)
            List<Action> noSourceMoves = actions.stream()
                    .filter(m -> m instanceof Move && ((Move)m).getSource() == null)
                    .collect(Collectors.toList());
            actions.removeAll(noSourceMoves);
            // Now collapse all actions except for those no-source moves
            List<@NonNull Action> collapsed = Action.collapsed(actions);
            // Reorder actions by type. Include the no-source moves.
            actions = Action.groupActionsByTypeAndReorderBeforeSending(noSourceMoves, collapsed);
        }
        logger.info("Plan completed with " + actions.size() + " actions.");
        if (logger.isDebugEnabled()) {
        	// Log number of actions by type
        	actions.stream()
        	    .collect(Collectors.groupingBy(Action::getClass))
        	    .forEach((k, v) -> logger.debug("    " + k.getSimpleName() + " : " + v.size()));
        }
        return actions;
    }

}
