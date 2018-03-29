package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionCollapse;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
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

    /**
     * Constructs the Economic Decisions Engine, with an empty initial State
     */
    public Ede() {}

    static final Logger logger = LogManager.getLogger(Ede.class);

    /**
     * Generate Actions.
     *
     * Add 'collapse' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, String)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, String mktName) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, mktName);
    }

    /** Generate Actions.
     *
     * Add 'collapse' and 'replay' argument
     * @see {@link #generateActions(Economy, boolean, boolean, boolean, boolean, boolean, boolean, String)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, false, false, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
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
                                                          boolean isProvision,
                                                          boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        return generateActions(economy, classifyActions, isProvision, isSuspension,
                               isResize, collapse, false, "unspecified|");
    }

    /**
     * Create a new set of actions for a snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @param isReplay True if we want to run replay algorithm and false otherwise
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see {@link Action#collapsed(List)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                    boolean classifyActions, boolean isProvision, boolean isSuspension,
                    boolean isResize, boolean collapse, boolean isReplay, String mktData) {
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
        //Parse market stats data coming from Market1, add prepare to write to stats file.
        String data[] = StatsUtils.getTokens(mktData, "|");
        if (data.length <= 1) {
            // XL file name is in contextid-topologyid format
            data = StatsUtils.getTokens(mktData, "-");
            if (data.length < 1 || !data[0].equals("777777")) { // Real market context id
                data = new String[1];
                data[0] = "plan";
            }
        }
        StatsUtils statsUtils = new StatsUtils("m2stats-" + data[0], false);
        if (data.length == 3) {
            statsUtils.append(data[1]); // date, Time, topo name , topo send time from M1
            statsUtils.after(Instant.parse(data[2]));
        } else {
            statsUtils.appendHeader("Date, Time,Topology,Topo Send Time,"
                                    + "Bootstrap Time, Initial Place Time,Resize Time,"
                                    + "Placement Time,Provisioning Time,Suspension Time,"
                                    + "Total Plan Time,Total Actions");
            //currently 4 columns for topology related data
            statsUtils.appendDate(true, false);
            statsUtils.appendTime(false, false);
            statsUtils.append(mktData); // if not in expected parse format, should contain
                                        // contextid-topologyid, different for each plan
            statsUtils.append("NA"); // topology send time if unavailable
        }

        // create a subset list of markets that have atleast one buyer that can move
        economy.composeMarketSubsetForPlacement();

        Ledger ledger = new Ledger(economy);

        if (isReplay && getReplayActions() != null) {
            getReplayActions().replayActions(economy, ledger);
            actions.addAll(getReplayActions().getActions());
            logger.info(actionStats.phaseLogEntry("replaying"));
        }

        // generate moves for IDLE VMs
        actions.addAll(Placement.prefPlacementDecisions(economy, economy.getInactiveOrIdleTraders()));
        logger.info(actionStats.phaseLogEntry("inactive/idle Trader placement"));

        // Start by provisioning enough traders to satisfy all the demand
        // Save first call to before() to calculate total plan time
        Instant begin = statsUtils.before();
        if (isProvision) {
            actions.addAll(BootstrapSupply.bootstrapSupplyDecisions(economy));
            ledger = new Ledger(economy);
            logger.info(actionStats.phaseLogEntry("bootstrap"));
            // time to run bootstrap
            statsUtils.after();

            statsUtils.before();
            // run placement algorithm to balance the environment
            actions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                                                               EconomyConstants.PLACEMENT_PHASE));
            logger.info(actionStats.phaseLogEntry("placement"));
            // time to run initial placement
            statsUtils.after();
        }

        statsUtils.before();
        if (isResize) {
            actions.addAll(Resizer.resizeDecisions(economy, ledger));
        }
        logger.info(actionStats.phaseLogEntry("resizing"));
        // resize time
        statsUtils.after();

        statsUtils.before();
        actions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                        EconomyConstants.PLACEMENT_PHASE));
        logger.info(actionStats.phaseLogEntry("placement"));
        // placement time
        statsUtils.after();

        statsUtils.before();
        // trigger provision, suspension and resize algorithm only when needed
        int oldActionCount = actions.size();
        if (isProvision) {
            actions.addAll(Provision.provisionDecisions(economy, ledger, this));
            logger.info(actionStats.phaseLogEntry("provisioning"));
            // if provision generated some actions, run placements
            if (actions.size() > oldActionCount) {
                actions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                                EconomyConstants.PLACEMENT_PHASE));
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
            actions.addAll(suspension.suspensionDecisions(economy, ledger, this));
            if (getReplayActions() != null) {
                getReplayActions().getRolledBackSuspensionCandidates().addAll(suspension.getRolledBack());
            }
        }
        logger.info(actionStats.phaseLogEntry("suspending"));
        // suspension time
        statsUtils.after();

        if (collapse && !economy.getForceStop()) {
            List<@NonNull Action> collapsed = ActionCollapse.collapsed(actions);
            // Reorder actions by type.
            actions = ActionCollapse.groupActionsByTypeAndReorderBeforeSending(collapsed);
            actionStats.setActionsList((ArrayList<Action>)actions);
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
        StatsWriter statsWriter = StatsManager.getInstance().init();
        statsWriter.add(statsUtils);

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
     * Create a new set of actions for a realtime-snapshot of the economy.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
     * @param classifyActions True if we we want to classify actions into non-executable.
     * @param isProvision True if we need to trigger provision algorithm and false otherwise
     * @param isSuspension True if we need to trigger suspension algorithm and false otherwise
     * @param isResize True if we need to trigger resize algorithm and false otherwise
     * @param collapse whether to collapse the returned list of actions.
     * @param mktData contains the market name and any stats to be written to the row
     *          delimited by "|"
     * @param isRealTime True for analysis of a realtime topology
     * @return A list of actions suggested by the economic decisions engine.
     *
     * @see {@link Action#collapsed(List)}
     */
    public @NonNull List<@NonNull Action> generateActions(@NonNull Economy economy,
                                                          boolean classifyActions,
                                                          boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse,
                                                          String mktData, boolean isRealTime,
                                                          SuspensionsThrottlingConfig suspensionsThrottlingConfig) {
        Suspension.setSuspensionsThrottlingConfig(suspensionsThrottlingConfig);
        @NonNull List<Action> actions = new ArrayList<>();
        // only run replay in first sub round for realTime market.
        boolean isReplay = true;
        if (isRealTime) {
            economy.getSettings().setResizeDependentCommodities(false);
            // run a round of analysis without provisions.
            actions.addAll(generateActions(economy, classifyActions, false, isSuspension,
                            isResize, collapse, isReplay, mktData));
        } else {
            actions.addAll(generateActions(economy, classifyActions, isProvision,
                            isSuspension, isResize, collapse, !isReplay, mktData));
        }

        return actions;
    }

    /**
     * Run a capacity plan using the economic scheduling engine and generate a new set of actions.
     *
     * @param economy The snapshot of the economy which we analyze and take decisions.
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
                    boolean isProvision, boolean isSuspension,
                                                          boolean isResize, boolean collapse) {
        // create a subset list of markets that have at least one buyer that can move
        economy.composeMarketSubsetForPlacement();
        @NonNull List<Action> actions = new ArrayList<>();
        // balance entities in plan
        actions.addAll(generateActions(economy, false, isProvision, isSuspension, isResize));
        logger.info("Plan completed initial placement with " + actions.size() + " actions.");
        int oldIndex = economy.getTraders().size();
        // add clones of sampleSE to the economy
        economy.getTradersForHeadroom().forEach(template -> ProtobufToAnalysis.addTrader(
                                        economy.getTopology(), template));

        Set<Market> newMarkets = new HashSet<>();
        for (int i=oldIndex; i<economy.getTraders().size(); i++) {
            newMarkets.addAll(economy.getMarketsAsBuyer(economy.getTraders().get(i)).values());
        }

        for (Market market : newMarkets) {
            economy.populateMarketWithSellers(market);
        }

        int oldActionCount = actions.size();

        actions.addAll(generateActions(economy, false, isProvision, isSuspension, isResize));
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