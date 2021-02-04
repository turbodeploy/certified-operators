package com.vmturbo.extractor.action;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ActionSavings;
import com.vmturbo.extractor.schema.json.reporting.ActionAttributes;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.graph.TopologyGraph;

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

    private final ActionAttributeExtractor actionAttributeExtractor;

    private final ObjectMapper objectMapper;

    /**
     * Create a new instance of the converter.
     *
     * @param actionAttributeExtractor The {@link ActionAttributeExtractor} used to extract
     *                                 type-specific action attributes.
     * @param objectMapper The {@link ObjectMapper} used to serialize JSON.
     */
    public ActionConverter(@Nonnull final ActionAttributeExtractor actionAttributeExtractor,
                           @Nonnull final ObjectMapper objectMapper) {
        this.actionAttributeExtractor = actionAttributeExtractor;
        this.objectMapper = objectMapper;
    }

    @Nullable
    Record makeExecutedActionSpec(ActionSpec actionSpec, String message,
            TopologyGraph<SupplyChainEntity> topologyGraph) {
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
            executedActionRecord.set(CompletedAction.ATTRS, extractAttrs(actionSpec, topologyGraph));
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
     * @param topologyGraph The topology graph, used to help find additional entity information
     *                      (e.g. display names) for action attributes.
     * @return The database {@link Record}, or null if the action is unsupported.
     */
    @Nullable
    Record makePendingActionRecord(ActionSpec actionSpec, TopologyGraph<SupplyChainEntity> topologyGraph) {
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
            pendingActionRecord.set(ActionModel.PendingAction.ATTRS, extractAttrs(actionSpec,
                    topologyGraph));

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

    @Nullable
    private JsonString extractAttrs(@Nonnull final ActionSpec actionSpec,
            TopologyGraph<SupplyChainEntity> topologyGraph) {
        ActionAttributes actionAttributes = actionAttributeExtractor.extractAttributes(actionSpec, topologyGraph);
        try {
            return new JsonString(objectMapper.writeValueAsString(actionAttributes));
        } catch (JsonProcessingException e) {
            logger.error("Failed to convert action to JSON.", e);
            return null;
        }
    }

    /**
     * Create action to be exported based on given action spec and topology info.
     *
     * @param actionSpec the action from AO
     * @param topologyGraph the graph containing the topology
     * @param policyById map of policies by id
     * @param relatedEntitiesExtractor used to extract related entities
     * @return {@link Action}
     */
    @Nonnull
    public Action makeExportedAction(@Nonnull ActionSpec actionSpec,
            @Nonnull TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull Map<Long, Policy> policyById,
            @Nonnull Optional<RelatedEntitiesExtractor> relatedEntitiesExtractor) {
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        final Action action = new Action();
        action.setOid(recommendation.getId());
        action.setCreationTime(ExportUtils.getFormattedDate(actionSpec.getRecommendationTime()));
        action.setState(actionSpec.getActionState().name());
        action.setCategory(actionSpec.getCategory().name());
        action.setMode(actionSpec.getActionMode().name());
        action.setDescription(actionSpec.getDescription());
        action.setSeverity(actionSpec.getSeverity().name());

        // set risk description
        try {
            final String riskDescription = RiskUtil.createRiskDescription(actionSpec, policyById::get,
                    entityId -> topologyGraph.getEntity(entityId)
                            .map(SupplyChainEntity::getDisplayName)
                            .orElse(null));
            action.setExplanation(riskDescription);
        } catch (UnsupportedActionException e) {
            logger.error("Cannot calculate risk for unsupported action {}", actionSpec, e);
        }

        // set target and related
        try {
            ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(recommendation, true);
            action.setTarget(ActionAttributeExtractor.getActionEntityWithType(primaryEntity, topologyGraph));
            relatedEntitiesExtractor.ifPresent(extractor ->
                    action.setRelated(extractor.extractRelatedEntities(primaryEntity.getId())));
        } catch (UnsupportedActionException e) {
            // this should not happen
            logger.error("Unable to get primary entity for unsupported action {}", actionSpec, e);
        }

        // set action savings if available
        if (recommendation.hasSavingsPerHour()) {
            CurrencyAmount savingsPerHour = recommendation.getSavingsPerHour();
            ActionSavings actionSavings = new ActionSavings();
            actionSavings.setUnit(CostProtoUtil.getCurrencyUnit(savingsPerHour));
            actionSavings.setAmount(savingsPerHour.getAmount());
            action.setSavings(actionSavings);
        }

        final ActionDTO.ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
        action.setType(actionType.name());

        // Add type-specific attributes to the action.
        actionAttributeExtractor.populateActionAttributes(actionSpec, topologyGraph, action);

        return action;
    }
}
