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
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.ActionStats;
import com.vmturbo.platform.analysis.utilities.StatsManager;
import com.vmturbo.platform.analysis.utilities.StatsUtils;
import com.vmturbo.platform.analysis.utilities.StatsWriter;

/**
 *
 * The Economic Decisions Engine creates recommendations for an {@link Economy economy} based on
 * the state it maintains for decisions it previously took for each {@link Trader}.
 *
 */
public final class Ede {

    // Fields
    private transient @NonNull ReplayActions replayActions_;

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
                                                          boolean classifyActions,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, String mktName) {
        return generateActions(economy, classifyActions, isShopTogether, isProvision, isSuspension,
                               isResize, false, mktName);
    }

    /** Generate Actions.
     *
     * Add 'collapse' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, String)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize) {
        return generateActions(economy, classifyActions, isShopTogether, isProvision, isSuspension,
                               isResize, false, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
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
                                                          boolean classifyActions,
                                                          boolean isShopTogether,
                                                          boolean isProvision,
                                                          boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        return generateActions(economy, classifyActions, isShopTogether, isProvision, isSuspension,
                               isResize, collapse, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
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
                                                          boolean classifyActions,
                                                          boolean isShopTogether,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse, String mktData) {
        String analysisLabel = "Analysis ";
        logger.info(analysisLabel + "Started.");
        @NonNull List<Action> actions = new ArrayList<>();
        ActionStats actionStats = new ActionStats((ArrayList<Action>)actions);
        ActionClassifier classifier = null;
        if (classifyActions) {
            try {
                classifier = new ActionClassifier(economy);
            }
            catch (ClassNotFoundException | IOException e) {
                logger.error(analysisLabel + "Could not copy economy.", e);
                return actions;
            }
        }
        if (getReplayActions() != null) {
            getReplayActions().replayActions(economy);
            actions.addAll(getReplayActions().getActions());
            logger.info(actionStats.phaseLogEntry("replaying"));
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
        actions.addAll(Placement.prefPlacementDecisions(economy, economy.getInactiveOrIdleTraders()));
        logger.info(actionStats.phaseLogEntry("inactive/idle Trader placement"));

        // Start by provisioning enough traders to satisfy all the demand
        // Save first call to before() to calculate total plan time
        Instant begin = statsUtils.before();
        if (isProvision) {
            actions.addAll(BootstrapSupply.bootstrapSupplyDecisions(economy));
        }
        logger.info(actionStats.phaseLogEntry("bootstrap"));
        // time to run bootstrap
        statsUtils.after();

        statsUtils.before();
        Ledger ledger = new Ledger(economy);
        // run placement algorithm to balance the environment
        actions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether,
                                                           PLACEMENT_PHASE));
        logger.info(actionStats.phaseLogEntry("placement"));
        // time to run initial placement
        statsUtils.after();

        statsUtils.before();
        if (isResize) {
            actions.addAll(Resizer.resizeDecisions(economy, ledger));
        }
        logger.info(actionStats.phaseLogEntry("resizing"));
        // resize time
        statsUtils.after();

        statsUtils.before();
        actions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether,
                                                           PLACEMENT_PHASE));
        logger.info(actionStats.phaseLogEntry("placement"));
        // placement time
        statsUtils.after();

        statsUtils.before();
        // trigger provision, suspension and resize algorithm only when needed
        int oldActionCount = actions.size();
        if (isProvision) {
            actions.addAll(Provision.provisionDecisions(economy, ledger, isShopTogether, this));
            logger.info(actionStats.phaseLogEntry("provisioning"));
            // if provision generated some actions, run placements
            if (actions.size() > oldActionCount) {
                actions.addAll(Placement.runPlacementsTillConverge(economy, ledger, isShopTogether,
                                                                   PLACEMENT_PHASE));
                logger.info(actionStats.phaseLogEntry("post provisioning placement"));
            }
        }
        // provisioning time
        statsUtils.after();

        statsUtils.before();
        if (isSuspension) {
            Suspension suspension = new Suspension();
            if (getReplayActions() != null) {
                // initialize the rolled back trader list from last run
                getReplayActions().translateRolledbackTraders(economy, economy.getTopology());
                suspension.setRolledBack(getReplayActions().getRolledBackSuspensionCandidates());
            }
            // find if any seller is the sole provider in any market, if so, it should not
            // be considered as suspension candidate
            suspension.findSoleProviders(economy);
            actions.addAll(suspension.suspensionDecisions(economy, ledger, this, isShopTogether));
            if (getReplayActions() != null) {
                getReplayActions().getRolledBackSuspensionCandidates().addAll(suspension.getRolledBack());
            }
        }
        logger.info(actionStats.phaseLogEntry("suspending"));
        // suspension time
        statsUtils.after();

        if (collapse && !economy.getForceStop()) {
            List<@NonNull Action> collapsed = Action.collapsed(actions);
            // Reorder actions by type.
            actions = Action.groupActionsByTypeAndReorderBeforeSending(collapsed);
        }

        // mark non-executable actions
        if (classifyActions && classifier != null) {
            classifier.classify(actions);
        }
        logger.info(actionStats.finalLogEntry());
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
        boolean isPlan = true;
        // create a subset list of markets that have at least one buyer that can move
        economy.composeMarketSubsetForPlacement();
        @NonNull List<Action> actions = new ArrayList<>();
        // balance entities in plan
        actions.addAll(generateActions(economy, isPlan, isShopTogether, isProvision, isSuspension, isResize));
        logger.info("Plan completed initial placement with " + actions.size() + " actions.");
        // add clones of sampleSE to the economy
        economy.getTradersForHeadroom().forEach(template -> ProtobufToAnalysis.addTrader(
                                        economy.getTopology(), template));
        int oldActionCount = actions.size();
        actions.addAll(generateActions(economy, isPlan, isShopTogether, isProvision, isSuspension, isResize));
        logger.info("Plan completed placement after adding demand with " + (actions.size() - oldActionCount)
                    + " actions.");
        return actions;
    }

    /**
     * save the {@link ReplayActions} associated with this {@link Ede}
     */
    public Ede setReplayActions(ReplayActions state) {
        replayActions_ = state;
        return this;
    }

    /**
     * @return return the {@link ReplayActions} associated with this {@link Ede}
     */
     public ReplayActions getReplayActions() {
         return replayActions_;
     }
}