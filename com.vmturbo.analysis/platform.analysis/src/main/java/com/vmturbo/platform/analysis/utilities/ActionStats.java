package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Resize;

/**
 * This class tracks action statistics through various phases. The supported usage is:<br>
 * <code>
 * ActionStats actionStats = new ActionStats(ArrayList<Action>);<br>
 * actionStats.phaseLogEntry("Phase1");<br>
 * actionStats.phaseLogEntry("Phase2");<br>
 * actionStats.phaseLogEntry("Phase3");<br>
 * actionStats.finalLogEntry();
 * </code>
 */
public class ActionStats {

    private static final String PHASE_FINAL = "final";

    static final Logger logger = LogManager.getLogger(ActionStats.class);

    /**
     * Data object to hold the counts for various action types, over a specific phase.
     */
    private static class ActionStatsData {

        // map<actionType, map<entityType, count>>
        final Map<ActionType, Map<String, Long>> statsMap = new HashMap<>();
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
        return !data.statsMap.isEmpty();
    }

    /**
     * Append counts to the StringBuilder
     *
     * @param stringBuilder The StringBuilder which is appended to
     * @param data          The object containing the counts
     * @param phase         The phase for which the log is being generated
     */
    private void body(StringBuilder stringBuilder, ActionStatsData data, String phase) {

        // final string example:
        // " 34 MOVE (PhysicalMachine:10, Storage:24), 5 PROVISION_BY_DEMAND (PhysicalMachine:4, Storage:1)"
        data.statsMap.forEach((actionType, entityMap) -> {

            if (!entityMap.isEmpty()) {

                //Example: " 34 MOVE ("
                Long globalCount = entityMap.values().stream().collect(Collectors.summingLong(Long::longValue));
                String statsDescBegin = String.format(" %d %s (", globalCount, actionType);
                stringBuilder.append(statsDescBegin);

                entityMap.forEach((entityType, count) -> {

                    //Example: "PhysicalMachine:10, "
                    String entityStatsDesc = String.format("%s:%d, ", entityType, count);
                    stringBuilder.append(entityStatsDesc);

                });

                // trim the last 2 chars
                stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
                stringBuilder.append("),");

            }
        });

        // trim the last char
        stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
    }

    /**
     * Counts the various types of actions
     *
     * @param offset          The offset in Actions list to start the count from
     * @param actionStatsData The data object where the counts are accumulated
     */
    private void count(int offset, ActionStatsData actionStatsData) {
        for (int i = offset; i < actions_.size(); i++) {
            Action action = actions_.get(i);
            switch (action.getType()) {
                case MOVE:
                    incrementMoves(actionStatsData, (Move) action);
                    break;
                case COMPOUND_MOVE:
                    increaseEntityCount(actionStatsData, action.getType(),
                            extractEntityType(action.getActionTarget().getDebugInfoNeverUseInCode()));
                    break;
                case RESIZE:
                    Resize resize = (Resize) action;
                    String resizeTrader = resize.getSellingTrader()
                        .getDebugInfoNeverUseInCode();
                    String resizeCommodity = resize.getResizedCommoditySpec()
                        .getDebugInfoNeverUseInCode();

                    String entityType = extractEntityType(resizeTrader);
                    String commType = extractEntityType(resizeCommodity);

                    // use the encoding entityType|commodityType to store it in the map
                    increaseEntityCount(actionStatsData, action.getType(), entityType + "|" + commType);
                    break;
                case PROVISION_BY_DEMAND:
                    ProvisionByDemand provisionByDemand = (ProvisionByDemand) action;
                    String demandDebug = provisionByDemand.getModelSeller()
                        .getDebugInfoNeverUseInCode();

                    increaseEntityCount(actionStatsData, action.getType(), extractEntityType(demandDebug));
                    break;
                case PROVISION_BY_SUPPLY:
                    ProvisionBySupply provisionBySupply = (ProvisionBySupply) action;
                    String supplyDebug = provisionBySupply.getModelSeller()
                        .getDebugInfoNeverUseInCode();

                    increaseEntityCount(actionStatsData, action.getType(), extractEntityType(supplyDebug));
                    break;
                case ACTIVATE:
                case DEACTIVATE:
                case RECONFIGURE_CONSUMER:
                case RECONFIGURE_PROVIDER_ADDITION:
                case RECONFIGURE_PROVIDER_REMOVAL:
                    String target = action.getActionTarget().getDebugInfoNeverUseInCode();
                    increaseEntityCount(actionStatsData, action.getType(), extractEntityType(target));
                    break;
                case UNKNOWN:
            }
        }
    }

    /**
     * Increment counts for moves
     *
     * @param move The {@link Move} action
     */
    private void incrementMoves(ActionStatsData actionStatsData, Move move) {
        if (move == null) {
            return;
        }
        if (move.getDestination() == null) {
            logger.error("Move action with a null destination. Move target: "
                + (move.getActionTarget() == null ? "nullTrader" :
                move.getActionTarget().getDebugInfoNeverUseInCode()));
            return;
        }

        String entityType = extractEntityType(move.getDestination().getDebugInfoNeverUseInCode());
        increaseEntityCount(actionStatsData, ActionType.MOVE, entityType);
    }

    private void increaseEntityCount(ActionStatsData actionStatsData, ActionType actionType, String entityType) {
        actionStatsData.statsMap.computeIfAbsent(actionType, k -> new HashMap<>())
            .merge(entityType, 1L, Long::sum);
    }

    // this is based on the convention that the debug string is in the format: EntityType|OID|DisplayName
    private String extractEntityType(String debugString) {
        String entityType = "";
        String[] strings = debugString.split("\\|");
        entityType = strings[0];
        return entityType;
    }
}
