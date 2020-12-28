package com.vmturbo.extractor.action;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;

/**
 * Responsible for converting {@link ActionSpec}s coming from the action orchestrator to the
 * appropriate action spec and action {@link Record}s that can be written to the database.
 */
public class ActionConverter {

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
    private static final BiMap<ActionDTO.ActionState, ActionState> STATE_MAP =
            ImmutableBiMap.<ActionDTO.ActionState, ActionState>builder()
                    .put(ActionDTO.ActionState.READY, ActionState.READY)
                    .put(ActionDTO.ActionState.CLEARED, ActionState.CLEARED)
                    .put(ActionDTO.ActionState.REJECTED, ActionState.REJECTED)
                    .put(ActionDTO.ActionState.ACCEPTED, ActionState.ACCEPTED)
                    .put(ActionDTO.ActionState.QUEUED, ActionState.QUEUED)
                    .put(ActionDTO.ActionState.IN_PROGRESS, ActionState.IN_PROGRESS)
                    .put(ActionDTO.ActionState.SUCCEEDED, ActionState.SUCCEEDED)
                    .put(ActionDTO.ActionState.FAILED, ActionState.FAILED)
                    .put(ActionDTO.ActionState.PRE_IN_PROGRESS, ActionState.PRE_IN_PROGRESS)
                    .put(ActionDTO.ActionState.POST_IN_PROGRESS, ActionState.POST_IN_PROGRESS)
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

    /**
     * Create a record for the pending action table from a particular {@link ActionSpec}.
     *
     * @param actionSpec The {@link ActionSpec} from the action orchestrator.
     * @return The database {@link Record}, or null if the action is unsupported.
     */
    @Nullable
    Record makePendingActionRecord(ActionSpec actionSpec) {
        final Record actionSpecRecord = new Record(ActionModel.PendingAction.TABLE);
        try {
            final long primaryEntityId = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()).getId();
            actionSpecRecord.set(ActionModel.PendingAction.ACTION_OID, actionSpec.getRecommendation().getId());
            actionSpecRecord.set(ActionModel.PendingAction.TYPE, extractType(actionSpec));
            actionSpecRecord.set(ActionModel.PendingAction.CATEGORY, extractCategory(actionSpec));
            actionSpecRecord.set(ActionModel.PendingAction.SEVERITY, extractSeverity(actionSpec));
            actionSpecRecord.set(ActionModel.PendingAction.TARGET_ENTITY, primaryEntityId);
            actionSpecRecord.set(ActionModel.PendingAction.INVOLVED_ENTITIES,
                    ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation())
                            .toArray(new Long[0]));
            actionSpecRecord.set(ActionModel.PendingAction.DESCRIPTION, actionSpec.getDescription());
            actionSpecRecord.set(ActionModel.PendingAction.SAVINGS,
                    actionSpec.getRecommendation().getSavingsPerHour().getAmount());

            // We don't set the hash here. We set it when we write the data.

            return actionSpecRecord;
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

    private ActionState mapState(ActionDTO.ActionState state) {
        return STATE_MAP.getOrDefault(state, ActionState.READY);
    }

    private Severity extractSeverity(ActionSpec spec) {
        return SEVERITY_MAP.getOrDefault(spec.getSeverity(), Severity.NORMAL);
    }
}
