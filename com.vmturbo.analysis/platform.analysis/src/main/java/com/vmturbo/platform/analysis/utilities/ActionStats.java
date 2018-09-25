package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;

/**
 * This class tracks action statistics through various phases. The supported usage is:<br>
 * <code>
 *     ActionStats actionStats = new ActionStats(ArrayList<Action>);<br>
 *     actionStats.phaseLogEntry("Phase1");<br>
 *     actionStats.phaseLogEntry("Phase2");<br>
 *     actionStats.phaseLogEntry("Phase3");<br>
 *     actionStats.finalLogEntry();
 * </code>
 *
 */
public class ActionStats {

    private static final String ST = ", ST:";
    private static final String PM = " (PM:";
    private static final String PHASE_FINAL = "final";
    private static final String V_MEM = "VMem";
    private static final String V_CPU = "VCPU";
    private static final String STORAGE = "Storage";
    private static final String PHYSICAL_MACHINE = "PhysicalMachine";
    private static final String VIRTUAL_MACHINE = "VirtualMachine";

    static final Logger logger = LogManager.getLogger(ActionStats.class);

    /**
     * Data object to hold the counts for various action types.
     *
     */
    private static class ActionStatsData {
        long numProvisions = 0;
        long numSuspensions = 0;
        long numActivations = 0;
        long numResizes = 0;
        long numMoves = 0;

        long numHostProvisions = 0;
        long numHostSuspensions = 0;
        long numHostActivations = 0;
        long numCpuResizes = 0;
        long numHostMoves = 0;

        long numStorageProvisions = 0;
        long numStorageSuspensions = 0;
        long numStorageActivations = 0;
        long numMemResizes = 0;
        long numStorageMoves = 0;
    }


    private @NonNull List<@NonNull Action> actions_;
    private int size_;
    private Instant begin_;
    private Instant lastPhaseEnd_;
    private boolean closed_;
    private final long analysisId;

    public ActionStats(List<@NonNull Action> actions, final long analysisId) {
        actions_ = actions;
        size_ = actions.size();
        begin_ = Instant.now();
        lastPhaseEnd_ = begin_;
        this.analysisId = analysisId;
    }

    /**
     * Update the actions list associated with this instance of {@link ActionStats}.
     *
     * @param actions New actions array list
     */
    public void setActionsList(List<@NonNull Action> actions) {
        actions_ = actions;
        size_ = actions.size();
    }

    /**
     * Generates the log message with counts of various actions. It is called after
     * a phase completes.
     *
     * @param phaseName The phase string containing placement, resize, provision, suspension
     * @param placementStats The placement stats for placements performed during the phase.
     * @return log string
     */
    public String phaseLogEntry(@NonNull final String phaseName,
                                @NonNull final PlacementStats placementStats) {
        final String phaseString = phaseLogEntry(phaseName);
        return phaseString + " " + placementStats.logMessage();
    }

    /**
     * Generates the log message with counts of various actions. It is called after
     * a phase completes.
     *
     * @param phase The phase string containing placement, resize, provision, suspension
     * @return log string
     */
    public String phaseLogEntry(String phase) {
        checkArgument(!closed_, "ActionStats closed");

        ActionStatsData data = new ActionStatsData();
        count(size_, data);

        size_ = actions_.size();
        Instant end = Instant.now();
        long took = end.getEpochSecond() - lastPhaseEnd_.getEpochSecond();
        lastPhaseEnd_ = end;
        StringBuilder sb = new StringBuilder();
        sb.append("Analysis completed ").append(phase).append(" in ")
            .append(took).append(" sec");
        if (isThereData(data)) {
            sb.append(" with");
            body(sb, data, phase);
        }
        sb.append(" [analysisId=").append(analysisId).append("]");
        return sb.toString();
    }

    /**
     * Generates the log message with counts of various actions. It is called after collapsing.
     *
     * @return log string
     */
    public String finalLogEntry() {
        checkArgument(!closed_, "ActionStats closed");

        Instant end = Instant.now();
        long took = end.getEpochSecond() - begin_.getEpochSecond();
        ActionStatsData data = new ActionStatsData();
        count(0, data);

        StringBuilder sb = new StringBuilder();
        sb.append("Analysis completed in ").append(took).append(" sec");
        if (isThereData(data)) {
            sb.append(" with");
            body(sb, data, PHASE_FINAL);
        }
        sb.append(" [analysisId=").append(analysisId).append("]");

        closed_ = true;
        return sb.toString();
    }

    private boolean isThereData(ActionStatsData data) {
        return data.numMoves > 0 || data.numResizes > 0 ||
               data.numProvisions > 0 || data.numSuspensions > 0;
    }

    private boolean isThereNonResizeData(ActionStatsData data) {
        return data.numMoves > 0 || data.numProvisions > 0 || data.numSuspensions > 0;
    }

    /**
     * Append counts to the StringBuilder
     *
     * @param stringBuilder The StringBuilder which is appended to
     * @param data The object containing the counts
     * @param phase The phase for which the log is being generated
     */
    private void body(StringBuilder stringBuilder, ActionStatsData data, String phase) {
        if (data.numResizes > 0) {
            stringBuilder.append(" ").append(data.numResizes).append(" resizes").append(" (VCPU:")
            .append(data.numCpuResizes).append(", VMem:")
            .append(data.numMemResizes).append(")");
        }
        if (isThereNonResizeData(data)) {
            stringBuilder.append(",");
        }
        if (data.numProvisions > 0) {
           stringBuilder
            //" XXX provisions (PM:XX1, ST:XX2),")
            .append(" ").append(data.numProvisions).append(" provisions").append(PM)
                .append(data.numHostProvisions).append(ST)
                .append(data.numStorageProvisions).append("),");
        }
        if (data.numSuspensions > 0) {
            stringBuilder
            //" YYY suspensions (PM:YY1, ST:YY2), and")
            .append(" ").append(data.numSuspensions).append(" suspensions").append(PM)
                .append(data.numHostSuspensions)
                .append(ST).append(data.numStorageSuspensions).append("),");
        }
        if (data.numActivations > 0) {
            stringBuilder
            //" WWW activations (PM:YY1, ST:YY2), and")
            .append(" ").append(data.numActivations).append(" activations").append(PM)
                .append(data.numHostActivations)
                .append(ST).append(data.numStorageActivations).append("),");
        }
        if (data.numMoves > 0) {
            stringBuilder
            //" ZZZ moves");
            .append(" ").append(data.numMoves).append(" moves").append(PM)
                .append(data.numHostMoves)
                .append(ST).append(data.numStorageMoves).append(")");
        }
    }

    /**
     * Counts the various types of actions
     *
     * @param offset The offset in Actions list to start the count from
     * @param actionStatsData The data object where the counts are accumulated
     */
    private void count(int offset, ActionStatsData actionStatsData) {
        for (int i = offset; i < actions_.size(); i++) {
            Action action = actions_.get(i);
            switch(action.getType()) {
                case MOVE :
                    incrementMoves(actionStatsData, (Move) action);
                    break;
                case COMPOUND_MOVE :
                    for (Move m : ((CompoundMove) action).getConstituentMoves()) {
                        incrementMoves(actionStatsData, m);
                    }
                    break;
                case RESIZE :
                    actionStatsData.numResizes++;
                    Resize resize = (Resize) action;
                    String resizeTrader = resize.getSellingTrader()
                                    .getDebugInfoNeverUseInCode();
                    String resizeCommodity = resize.getResizedCommoditySpec()
                                    .getDebugInfoNeverUseInCode();
                    if (resizeTrader.startsWith(VIRTUAL_MACHINE) &&
                                    resizeCommodity.startsWith(V_CPU)) {
                        actionStatsData.numCpuResizes++;
                    } else if (resizeTrader.startsWith(VIRTUAL_MACHINE)  &&
                                    resizeCommodity.startsWith(V_MEM)) {
                        actionStatsData.numMemResizes++;
                    }
                    break;
                case PROVISION_BY_DEMAND :
                    actionStatsData.numProvisions++;
                    ProvisionByDemand provisionByDemand = (ProvisionByDemand) action;
                    String demandDebug = provisionByDemand.getModelSeller()
                                    .getDebugInfoNeverUseInCode();
                    if (demandDebug.startsWith(PHYSICAL_MACHINE)) {
                        actionStatsData.numHostProvisions++;
                    } else if (demandDebug.startsWith(STORAGE)) {
                        actionStatsData.numStorageProvisions++;
                    }
                    break;
                case PROVISION_BY_SUPPLY :
                    actionStatsData.numProvisions++;
                    ProvisionBySupply provisionBySupply = (ProvisionBySupply) action;
                    String supplyDebug = provisionBySupply.getModelSeller()
                                    .getDebugInfoNeverUseInCode();
                    if (supplyDebug.startsWith(PHYSICAL_MACHINE)) {
                        actionStatsData.numHostProvisions++;
                    } else if (supplyDebug.startsWith(STORAGE)) {
                        actionStatsData.numStorageProvisions++;
                    }
                    break;
                case ACTIVATE :
                    Activate activate = (Activate) action;
                    actionStatsData.numActivations++;
                    String activateTarget = activate.getActionTarget().getDebugInfoNeverUseInCode();
                    if (activateTarget.startsWith(PHYSICAL_MACHINE)) {
                        actionStatsData.numHostActivations++;
                    } else if (activateTarget.startsWith(STORAGE)) {
                        actionStatsData.numStorageActivations++;
                    }
                    break;
                case DEACTIVATE :
                    actionStatsData.numSuspensions++;
                    Deactivate deactivate = (Deactivate) action;
                    String deactTarget = deactivate.getActionTarget().getDebugInfoNeverUseInCode();
                    if (deactTarget.startsWith(PHYSICAL_MACHINE)) {
                        actionStatsData.numHostSuspensions++;
                    } else if (deactTarget.startsWith(STORAGE)) {
                        actionStatsData.numStorageSuspensions++;
                    }
                    break;
                case RECONFIGURE :
                    Reconfigure reconfigure = (Reconfigure) action;
                    // count Reconfigurations??
                    break;
                case UNKNOWN :
            }
        }
    }

    /**
     * Increment counts for moves
     *
     * @param actionStatsData The data object where the counts are accumulated
     * @param move The {@link Move} action
     */
    private void incrementMoves(ActionStatsData actionStatsData, Move move) {
        if (move == null || actionStatsData == null) {
            return;
        }
        if (move.getDestination() == null) {
            logger.error("Move action with a null destination. Move target: "
                            + (move.getActionTarget() == null ? "nullTrader":
                               move.getActionTarget().getDebugInfoNeverUseInCode()));
            return;
        }
        actionStatsData.numMoves++;
        String destinationDebug = move.getDestination().getDebugInfoNeverUseInCode();
        if (destinationDebug.startsWith(PHYSICAL_MACHINE)) {
            actionStatsData.numHostMoves++;
        } else if (destinationDebug.startsWith(STORAGE)) {
            actionStatsData.numStorageMoves++;
        }
    }
}
