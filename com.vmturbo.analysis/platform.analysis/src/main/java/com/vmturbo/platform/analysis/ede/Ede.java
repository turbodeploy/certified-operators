package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.utilities.StatsManager;
import com.vmturbo.platform.analysis.utilities.StatsUtils;
import com.vmturbo.platform.analysis.utilities.StatsWriter;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;

/**
 *
 * The Economic Decisions Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public final class Ede {

    // Fields

    // Constructor

    private static final String PLACEMENT_PHASE = "Placement Phase";

    /**
     * Constructs the Economic Decisions Engine, with an empty initial State
     */
    public Ede() {}

    static final Logger logger = Logger.getLogger(Ede.class);

    /**
     * Generate Actions.
     *
     * Add 'collapse' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, String mktName) {
        return generateActions(economy, isShopTogether, isProvision, isSuspension, isResize, false,
                               mktName);
    }

    /** Generate Actions.
     *
     * Add 'collapse' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, String)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize) {
        return generateActions(economy, isShopTogether, isProvision, isSuspension, isResize, false, "unspecified|");

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
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        return generateActions(economy, isShopTogether, isProvision, isSuspension, isResize,
                               collapse, "unspecified|");

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
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see {@link Action#collapsed(List)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse, String mktData) {
        logger.info("Plan Started.");
        @NonNull List<Action> actions = new ArrayList<>();
        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
        }
        catch (ClassNotFoundException|IOException e) {
            logger.error("Could not copy economy.", e);
            return actions;
        }

        //Parse market stats data coming from Market1, add prepare to write to stats file.
        String data[] = StatsUtils.getTokens(mktData, "|");
        StatsUtils statsUtils = new StatsUtils("m2stats-" + data[0], true);
        if (data.length == 3) {
            statsUtils.append(data[1]);
            statsUtils.after(Instant.parse(data[2]));
        } else {
            statsUtils.append("NA,NA,NA,NA"); //currently 4 columns for topology related  data
        }

        // create a subset list of markets that have atleast one buyer that can move
        economy.composeMarketSubsetForPlacement();
        // generate moves for IDLE VMs
        actions.addAll(Placement.prefPlacementDecisions(economy, economy.getIdleVms()));
        int oldActionCount = actions.size();
        logger.info("Plan completed idleVM placement with " + oldActionCount + " actions.");

        // Start by provisioning enough traders to satisfy all the demand
        // Save first call to before() to calculate total plan time
        Instant begin = statsUtils.before();
        if (isProvision) {
            actions.addAll(BootstrapSupply.bootstrapSupplyDecisions(economy));
        }
        logger.info("Plan completed bootstrap with " + (actions.size() - oldActionCount)
                    + " actions.");
        // time to run bootstrap
        statsUtils.after();

        oldActionCount = actions.size();
        statsUtils.before();
        Ledger ledger = new Ledger(economy);
        // run placement algorithm to balance the environment
        actions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether,
                                                           PLACEMENT_PHASE));
        logger.info("Plan completed initial placement with " + (actions.size() - oldActionCount)
                    + " actions.");
        // time to run initial placement
        statsUtils.after();

        oldActionCount = actions.size();
        statsUtils.before();
        if (isResize) {
            actions.addAll(Resizer.resizeDecisions(economy, ledger));
        }
        logger.info("Plan completed resizing with " + (actions.size() - oldActionCount)
                    + " actions.");
        // resize time
        statsUtils.after();

        oldActionCount = actions.size();
        statsUtils.before();
        actions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether,
                                                           PLACEMENT_PHASE));
        logger.info("Plan completed placement with " + (actions.size() - oldActionCount)
                    + " actions.");
        // placement time
        statsUtils.after();

        oldActionCount = actions.size();
        statsUtils.before();
        // trigger provision, suspension and resize algorithm only when needed
        if (isProvision) {
            actions.addAll(Provision.provisionDecisions(economy, ledger, isShopTogether, this));
        }
        logger.info("Plan completed provisioning with " + (actions.size() - oldActionCount)
                    + " actions.");
        // provisioning time
        statsUtils.after();

        oldActionCount = actions.size();
        statsUtils.before();
        if (isSuspension) {
            Suspension suspension = new Suspension();
            // find if any seller is the sole provider in any market, if so, it should not
            // be considered as suspension candidate
            suspension.findSoleProviders(economy);
            actions.addAll(suspension.supplyDecisions(economy, ledger, this, isShopTogether,
                                                      false));
        }
        logger.info("Plan completed suspending with " + (actions.size() - oldActionCount)
                    + " actions.");
        // suspension time
        statsUtils.after();

        if (collapse && !economy.getForceStop()) {
            List<@NonNull Action> collapsed = Action.collapsed(actions);
            // Reorder actions by type.
            actions = Action.groupActionsByTypeAndReorderBeforeSending(collapsed);
        }
        // mark non-executable actions
        classifier.classify(actions);
        logger.info("Plan completed with " + actions.size() + " actions.");
        // total time to run plan
        statsUtils.after(begin);
        // file total actions
        statsUtils.append(actions.size(), false, false);
        StatsWriter statsWriter = StatsManager.getInstance().init("m2stats-" + data[0]);
        statsWriter.add(statsUtils.getdata());

        if (logger.isDebugEnabled()) {
            // Log number of actions by type
            actions.stream()
                            .collect(Collectors.groupingBy(Action::getClass))
                            .forEach((k, v) -> logger
                                            .debug("    " + k.getSimpleName() + " : " + v.size()));
        }
        return actions;
    }

    /**
     * Run a capacity plan using the economic scheduling engine and generate a new set of actions.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param isShopTogether True if we want to enable SNM and false otherwise.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @return A list of actions suggested after running a capacityPlan using
     *          the the economic decisions engine.
     *
     * @see {@link Action#collapsed(List)}
     */
    public @NonNull List<@NonNull Action> generateHeadroomActions(@NonNull Economy economy,
                                                          boolean isShopTogether, boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        // create a subset list of markets that have at least one buyer that can move
        economy.composeMarketSubsetForPlacement();
        @NonNull List<Action> actions = new ArrayList<>();
        // balance entities in plan
        actions.addAll(generateActions(economy, isShopTogether, isProvision, isSuspension, isResize));
        logger.info("Plan completed initial placement with " + actions.size() + " actions.");
        // add clones of sampleSE to the economy
        economy.getTradersForHeadroom().forEach(template -> ProtobufToAnalysis.addTrader(
                                        economy.getTopology(), template));
        int oldActionCount = actions.size();
        actions.addAll(generateActions(economy, isShopTogether, isProvision, isSuspension, isResize));
        logger.info("Plan completed placement after adding demand with " + (actions.size() - oldActionCount)
                    + " actions.");
        return actions;
    }
}