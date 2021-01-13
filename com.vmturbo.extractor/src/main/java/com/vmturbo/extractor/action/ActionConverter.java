package com.vmturbo.extractor.action;

import java.sql.Timestamp;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;

/**
 * Responsible for converting {@link ActionSpec}s coming from the action orchestrator to the
 * appropriate action spec and action {@link Record}s that can be written to the database.
 */
public class ActionConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Action type enum mappings. We make these explicit, instead of relying on name equivalence,
     * to make it harder to accidentally break things.
     */
    private static final BiMap<ActionDTO.ActionType, ActionType> ACTION_TYPE_MAP =
            ImmutableBiMap.<ActionDTO.ActionType, ActionType>builder()
                    .put(ActionDTO.ActionType.START, ActionType.START)
                    .put(ActionDTO.ActionType.MOVE, ActionType.MOVE)
                    .put(ActionDTO.ActionType.SUSPEND, ActionType.SUSPEND)
                    .put(ActionDTO.ActionType.PROVISION, ActionType.PROVISION)
                    .put(ActionDTO.ActionType.RECONFIGURE, ActionType.RECONFIGURE)
                    .put(ActionDTO.ActionType.RESIZE, ActionType.RESIZE)
                    .put(ActionDTO.ActionType.ACTIVATE, ActionType.ACTIVATE)
                    .put(ActionDTO.ActionType.DEACTIVATE, ActionType.DEACTIVATE)
                    .put(ActionDTO.ActionType.DELETE, ActionType.DELETE)
                    .put(ActionDTO.ActionType.BUY_RI, ActionType.BUY_RI)
                    .put(ActionDTO.ActionType.SCALE, ActionType.SCALE)
                    .put(ActionDTO.ActionType.ALLOCATE, ActionType.ALLOCATE)
                    .build();

    /**
     * Action severity enum mappings. We make these explicit, instead of relying on name equivalence,
     * to make it harder to accidentally break things.
     */
    private static final BiMap<ActionDTO.Severity, Severity> SEVERITY_MAP =
            ImmutableBiMap.<ActionDTO.Severity, Severity>builder()
                    .put(ActionDTO.Severity.NORMAL, Severity.NORMAL)
                    .put(ActionDTO.Severity.MINOR, Severity.MINOR)
                    .put(ActionDTO.Severity.MAJOR, Severity.MAJOR)
                    .put(ActionDTO.Severity.CRITICAL, Severity.CRITICAL)
                    .build();

    /**
     * Action state enum mappings. We make these explicit, instead of relying on name equivalence,
     * to make it harder to accidentally break things.
     */
    private static final BiMap<ActionDTO.ActionState, TerminalState> STATE_MAP =
            ImmutableBiMap.<ActionDTO.ActionState, TerminalState>builder()
                    .put(ActionDTO.ActionState.SUCCEEDED, TerminalState.SUCCEEDED)
                    .put(ActionDTO.ActionState.FAILED, TerminalState.FAILED)
                    .build();


    /**
     * Action category enum mappings. We make these explicit, instead of relying on name equivalence,
     * to make it harder to accidentally break things.
     */
    private static final BiMap<ActionDTO.ActionCategory, ActionCategory> ACTION_CATEGORY_MAP =
            ImmutableBiMap.<ActionDTO.ActionCategory, ActionCategory>builder()
                    .put(ActionDTO.ActionCategory.PERFORMANCE_ASSURANCE, ActionCategory.PERFORMANCE_ASSURANCE)
                    .put(ActionDTO.ActionCategory.EFFICIENCY_IMPROVEMENT, ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .put(ActionDTO.ActionCategory.PREVENTION, ActionCategory.PREVENTION)
                    .put(ActionDTO.ActionCategory.COMPLIANCE, ActionCategory.COMPLIANCE)
                    .build();

    @Nullable
    Record makeExecutedActionSpec(ActionSpec actionSpec, String message) {
        final Record executedActionRecord = new Record(CompletedAction.TABLE);
        try {
            final long primaryEntityId = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()).getId();
            executedActionRecord.set(CompletedAction.RECOMMENDATION_TIME,
                    new Timestamp(actionSpec.getRecommendationTime()));

            TerminalState state = STATE_MAP.get(actionSpec.getActionState());
            if (state == null) {
                // Only succeeded/failed actions get mapped successfully.
                return null;
            }

            if (!actionSpec.hasDecision() || !actionSpec.hasExecutionStep()) {
                // Something is wrong. A completed action should have a decision and execution step.
                logger.error("Completed action {} (id: {}) does not have a decision and execution step.",
                        actionSpec.getDescription(), actionSpec.getRecommendation().getId());
                return null;
            } else {
                final ActionDecision decision = actionSpec.getDecision();
                // The last execution step's completion time is the completion time of the whole
                // action.
                final ExecutionStep executionStep = actionSpec.getExecutionStep();
                executedActionRecord.set(CompletedAction.ACCEPTANCE_TIME,
                        new Timestamp(decision.getDecisionTime()));
                executedActionRecord.set(CompletedAction.COMPLETION_TIME,
                        new Timestamp(executionStep.getCompletionTime()));
            }
            executedActionRecord.set(CompletedAction.ACTION_OID, actionSpec.getRecommendation().getId());
            executedActionRecord.set(CompletedAction.TYPE, extractType(actionSpec));
            executedActionRecord.set(CompletedAction.CATEGORY, extractCategory(actionSpec));
            executedActionRecord.set(CompletedAction.SEVERITY, extractSeverity(actionSpec));
            executedActionRecord.set(CompletedAction.TARGET_ENTITY, primaryEntityId);
            executedActionRecord.set(CompletedAction.INVOLVED_ENTITIES,
                    ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation())
                            .toArray(new Long[0]));
            executedActionRecord.set(CompletedAction.DESCRIPTION, actionSpec.getDescription());
            executedActionRecord.set(CompletedAction.SAVINGS,
                    actionSpec.getRecommendation().getSavingsPerHour().getAmount());


            executedActionRecord.set(CompletedAction.FINAL_STATE, state);
            executedActionRecord.set(CompletedAction.FINAL_MESSAGE, message);


            // We don't set the hash here. We set it when we write the data.

            return executedActionRecord;
        } catch (UnsupportedActionException e) {
            return null;
        }

    }

    /**
     * Create a record for the pending action table from a particular {@link ActionSpec}.
     *
     * @param actionSpec The {@link ActionSpec} from the action orchestrator.
     * @return The database {@link Record}, or null if the action is unsupported.
     */
    @Nullable
    Record makePendingActionRecord(ActionSpec actionSpec) {
        final Record pendingActionRecord = new Record(ActionModel.PendingAction.TABLE);
        try {
            final long primaryEntityId = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()).getId();
            pendingActionRecord.set(ActionModel.PendingAction.RECOMMENDATION_TIME,
                    new Timestamp(actionSpec.getRecommendationTime()));
            pendingActionRecord.set(ActionModel.PendingAction.ACTION_OID, actionSpec.getRecommendation().getId());
            pendingActionRecord.set(ActionModel.PendingAction.TYPE, extractType(actionSpec));
            pendingActionRecord.set(ActionModel.PendingAction.CATEGORY, extractCategory(actionSpec));
            pendingActionRecord.set(ActionModel.PendingAction.SEVERITY, extractSeverity(actionSpec));
            pendingActionRecord.set(ActionModel.PendingAction.TARGET_ENTITY, primaryEntityId);
            pendingActionRecord.set(ActionModel.PendingAction.INVOLVED_ENTITIES,
                    ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation())
                            .toArray(new Long[0]));
            pendingActionRecord.set(ActionModel.PendingAction.DESCRIPTION, actionSpec.getDescription());
            pendingActionRecord.set(ActionModel.PendingAction.SAVINGS,
                    actionSpec.getRecommendation().getSavingsPerHour().getAmount());

            // We don't set the hash here. We set it when we write the data.

            return pendingActionRecord;
        } catch (UnsupportedActionException e) {
            return null;
        }
    }

    private ActionCategory extractCategory(ActionSpec spec) {
        return ACTION_CATEGORY_MAP.getOrDefault(spec.getCategory(), ActionCategory.UNKNOWN);
    }

    private ActionType extractType(ActionSpec spec) {
        ActionDTO.ActionType type = ActionDTOUtil.getActionInfoActionType(spec.getRecommendation());
        return ACTION_TYPE_MAP.getOrDefault(type, ActionType.NONE);
    }

    private Severity extractSeverity(ActionSpec spec) {
        return SEVERITY_MAP.getOrDefault(spec.getSeverity(), Severity.NORMAL);
    }
}
