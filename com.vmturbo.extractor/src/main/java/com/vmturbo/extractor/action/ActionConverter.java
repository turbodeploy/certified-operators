package com.vmturbo.extractor.action;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionMode;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;
import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.CostAmount;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
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
    private static final BiMap<ActionDTO.ActionState, TerminalState> TERMINAL_STATE_MAP =
            ImmutableBiMap.<ActionDTO.ActionState, TerminalState>builder()
                    .put(ActionDTO.ActionState.SUCCEEDED, TerminalState.SUCCEEDED)
                    .put(ActionDTO.ActionState.FAILED, TerminalState.FAILED)
                    .build();

    /**
     * Action state mappings.
     */
    private static final BiMap<ActionDTO.ActionState, ActionState> STATE_MAP =
            ImmutableBiMap.<ActionDTO.ActionState, ActionState>builder()
                    .put(ActionDTO.ActionState.READY, ActionState.READY)
                    .put(ActionDTO.ActionState.CLEARED, ActionState.CLEARED)
                    .put(ActionDTO.ActionState.ACCEPTED, ActionState.ACCEPTED)
                    .put(ActionDTO.ActionState.REJECTED, ActionState.REJECTED)
                    .put(ActionDTO.ActionState.QUEUED, ActionState.QUEUED)
                    .put(ActionDTO.ActionState.IN_PROGRESS, ActionState.IN_PROGRESS)
                    .put(ActionDTO.ActionState.SUCCEEDED, ActionState.SUCCEEDED)
                    .put(ActionDTO.ActionState.FAILED, ActionState.FAILED)
                    .put(ActionDTO.ActionState.PRE_IN_PROGRESS, ActionState.PRE_IN_PROGRESS)
                    .put(ActionDTO.ActionState.POST_IN_PROGRESS, ActionState.POST_IN_PROGRESS)
                    .put(ActionDTO.ActionState.FAILING, ActionState.FAILING)
                    .build();

    /**
     * Action mode mappings.
     */
    private static final BiMap<ActionDTO.ActionMode, ActionMode> MODE_MAP =
            ImmutableBiMap.<ActionDTO.ActionMode, ActionMode>builder()
                    .put(ActionDTO.ActionMode.AUTOMATIC, ActionMode.AUTOMATIC)
                    .put(ActionDTO.ActionMode.DISABLED, ActionMode.DISABLED)
                    .put(ActionDTO.ActionMode.EXTERNAL_APPROVAL, ActionMode.EXTERNAL_APPROVAL)
                    .put(ActionDTO.ActionMode.MANUAL, ActionMode.MANUAL)
                    .put(ActionDTO.ActionMode.RECOMMEND, ActionMode.RECOMMEND)
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
     * Related entities to include for storage as target entity in action.
     */
    private static final Set<Integer> RELATED_ENTITY_TYPES_FOR_STORAGE = ImmutableSet.of(
            EntityDTO.EntityType.STORAGE_VALUE,
            EntityDTO.EntityType.DISK_ARRAY_VALUE,
            EntityDTO.EntityType.LOGICAL_POOL_VALUE,
            EntityDTO.EntityType.STORAGE_CONTROLLER_VALUE,
            EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE,
            EntityDTO.EntityType.DATACENTER_VALUE
    );

    /**
     * Related entities filter for different type of target entity in action.
     */
    public static final Map<Integer, Predicate<Integer>> RELATED_ENTITY_FILTER_FOR_ACTION_TARGET_ENTITY =
            ImmutableMap.of(
                    EntityDTO.EntityType.STORAGE_VALUE, RELATED_ENTITY_TYPES_FOR_STORAGE::contains
            );

    private final ActionAttributeExtractor actionAttributeExtractor;

    private final CachingPolicyFetcher cachingPolicyFetcher;

    private final DataProvider dataProvider;

    private final ObjectMapper objectMapper;

    private final DataExtractionFactory dataExtractionFactory;

    /**
     * Create a new instance of the converter.
     *
     * @param actionAttributeExtractor The {@link ActionAttributeExtractor} used to extract
     *                                 type-specific action attributes.
     * @param cachingPolicyFetcher Used to fetch policies. Policy names are required to compose descriptions.
     * @param dataProvider Used to get the latest topology graph.
     * @param dataExtractionFactory Used to get information about related entities for data extraction.
     * @param objectMapper The {@link ObjectMapper} used to serialize JSON.
     */
    public ActionConverter(@Nonnull final ActionAttributeExtractor actionAttributeExtractor,
                           @Nonnull final CachingPolicyFetcher cachingPolicyFetcher,
                           @Nonnull final DataProvider dataProvider,
                           @Nonnull final DataExtractionFactory dataExtractionFactory,
                           @Nonnull final ObjectMapper objectMapper) {
        this.actionAttributeExtractor = actionAttributeExtractor;
        this.cachingPolicyFetcher = cachingPolicyFetcher;
        this.dataProvider = dataProvider;
        this.dataExtractionFactory = dataExtractionFactory;
        this.objectMapper = objectMapper;
    }

    @Nonnull
    List<Record> makeExecutedActionSpec(List<ExecutedAction> executedActions) {
        final TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topologyGraph == null) {
            // This should not happen, because we check for the topology graph earlier.
            logger.error("No topology graph found. Cannot create executed action records.");
            return Collections.emptyList();
        }
        final Long2ObjectMap<ReportingActionAttributes> attributes =
                actionAttributeExtractor.extractReportingAttributes(executedActions.stream()
                    .map(ExecutedAction::getActionSpec)
                    .collect(Collectors.toList()), topologyGraph);

        final List<Record> retList = new ArrayList<>(executedActions.size());
        executedActions.forEach(action -> {
            final ActionSpec actionSpec = action.getActionSpec();
            final long actionId = actionSpec.getRecommendation().getId();
            final Record executedActionRecord = new Record(CompletedAction.TABLE);
            try {
                final long primaryEntityId = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()).getId();
                executedActionRecord.set(CompletedAction.RECOMMENDATION_TIME,
                        new Timestamp(actionSpec.getRecommendationTime()));

                TerminalState state = TERMINAL_STATE_MAP.get(actionSpec.getActionState());
                if (state == null) {
                    // Only succeeded/failed actions get mapped successfully.
                    logger.error("Completed action {} (id: {}) has non-final state: {}",
                            actionSpec.getDescription(), actionId, actionSpec.getActionState());
                    return;
                }

                if (!actionSpec.hasDecision() || !actionSpec.hasExecutionStep()) {
                    // Something is wrong. A completed action should have a decision and execution step.
                    logger.error("Completed action {} (id: {}) does not have a decision and execution step.",
                            actionSpec.getDescription(), actionId);
                    return;
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
                executedActionRecord.set(CompletedAction.ACTION_OID, actionId);
                executedActionRecord.set(CompletedAction.TYPE, extractType(actionSpec));
                executedActionRecord.set(CompletedAction.CATEGORY, extractCategory(actionSpec));
                executedActionRecord.set(CompletedAction.SEVERITY, extractSeverity(actionSpec));
                executedActionRecord.set(CompletedAction.TARGET_ENTITY, primaryEntityId);
                executedActionRecord.set(CompletedAction.INVOLVED_ENTITIES,
                        ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation())
                                .toArray(new Long[0]));
                executedActionRecord.set(CompletedAction.ATTRS, toJsonString(attributes.get(actionId)));
                executedActionRecord.set(CompletedAction.DESCRIPTION, actionSpec.getDescription());
                executedActionRecord.set(CompletedAction.SAVINGS,
                        actionSpec.getRecommendation().getSavingsPerHour().getAmount());


                executedActionRecord.set(CompletedAction.FINAL_STATE, state);
                executedActionRecord.set(CompletedAction.FINAL_MESSAGE, action.getMessage());


                // We don't set the hash here. We set it when we write the data.

                retList.add(executedActionRecord);
            } catch (UnsupportedActionException e) {
                // Should not happen.
            }
        });
        return retList;
    }

    /**
     * Create a record for the pending action table from a particular {@link ActionSpec}.
     *
     * @param actionSpecs The {@link ActionSpec}s from the action orchestrator, arranged by id.
     * @return The database {@link Record}s the actions map to, arranged by id.
     */
    @Nonnull
    List<Record> makePendingActionRecords(List<ActionSpec> actionSpecs) {
        final TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topologyGraph == null) {
            // This should not happen, because we check for the topology graph before
            // creating the writer for pending actions.
            logger.error("No topology graph found. Cannot create pending action records.");
            return Collections.emptyList();
        }
        final Long2ObjectMap<ReportingActionAttributes> attributes = actionAttributeExtractor.extractReportingAttributes(actionSpecs, topologyGraph);
        final List<Record> retList = new ArrayList<>(actionSpecs.size());
        actionSpecs.forEach(actionSpec -> {
            final Record pendingActionRecord = new Record(ActionModel.PendingAction.TABLE);
            final long actionId = actionSpec.getRecommendation().getId();
            try {
                final long primaryEntityId = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()).getId();
                pendingActionRecord.set(ActionModel.PendingAction.RECOMMENDATION_TIME,
                        new Timestamp(actionSpec.getRecommendationTime()));
                pendingActionRecord.set(ActionModel.PendingAction.ACTION_OID, actionId);
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
                pendingActionRecord.set(ActionModel.PendingAction.ATTRS, toJsonString(attributes.get(actionId)));

                retList.add(pendingActionRecord);
            } catch (UnsupportedActionException e) {
                // Shouldn't happen.
            }
        });
        return retList;
    }

    private ActionCategory extractCategory(ActionSpec spec) {
        return ACTION_CATEGORY_MAP.getOrDefault(spec.getCategory(), ActionCategory.UNKNOWN);
    }

    /**
     * Map action category from protobuf to db schema.
     *
     * @param actionCategory the protobuf action category
     * @return the db schema action category
     */
    public static ActionCategory extractActionCategory(ActionDTO.ActionCategory actionCategory) {
        return ACTION_CATEGORY_MAP.getOrDefault(actionCategory, ActionCategory.UNKNOWN);
    }

    private ActionType extractType(ActionSpec spec) {
        ActionDTO.ActionType type = ActionDTOUtil.getActionInfoActionType(spec.getRecommendation());
        return ACTION_TYPE_MAP.getOrDefault(type, ActionType.NONE);
    }

    /**
     * Map action mode from protobuf to db enum.
     *
     * @param actionMode The {@link ActionDTO.ActionMode}.
     * @return The {@link ActionMode}.
     */
    @Nonnull
    public static ActionMode extractActionMode(final ActionDTO.ActionMode actionMode) {
        // Default just for safety; we shouldn't need it.
        return MODE_MAP.getOrDefault(actionMode, ActionMode.MANUAL);
    }

    /**
     * Map action state from protobuf to db enum.
     *
     * @param actionState The {@link ActionDTO.ActionState}.
     * @return The {@link ActionState}.
     */
    @Nonnull
    public static ActionState extractActionState(final ActionDTO.ActionState actionState) {
        // Default just for safety.
        return STATE_MAP.getOrDefault(actionState, ActionState.READY);
    }


    /**
     * Map action type from protobuf to db schema.
     *
     * @param actionType the protobuf action type
     * @return the db schema action type
     */
    public static ActionType extractActionType(ActionDTO.ActionType actionType) {
        return ACTION_TYPE_MAP.getOrDefault(actionType, ActionType.NONE);
    }

    private Severity extractSeverity(ActionSpec spec) {
        return SEVERITY_MAP.getOrDefault(spec.getSeverity(), Severity.NORMAL);
    }

    /**
     * Map action severity from protobuf to db schema.
     *
     * @param severity the protobuf action severity
     * @return the db schema action severity
     */
    public static Severity extractActionSeverity(ActionDTO.Severity severity) {
        return SEVERITY_MAP.getOrDefault(severity, Severity.NORMAL);
    }

    @Nullable
    private JsonString toJsonString(@Nullable final ActionAttributes actionAttributes) {
        if (actionAttributes == null) {
            return null;
        }

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
     * @param actionSpecs the action from AO, arranged by id.
     * @return {@link Action}
     */
    @Nonnull
    public Collection<Action> makeExportedActions(@Nonnull List<ActionSpec> actionSpecs) {
        final Map<Long, Policy> policyById = cachingPolicyFetcher.getOrFetchPolicies();
        final TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topologyGraph == null) {
            // This should not happen, because we check for the topology graph before
            // creating the writer for exported actions.
            logger.error("No topology graph present in extractor component. Cannot export actions.");
            return Collections.emptyList();
        }
        final Optional<RelatedEntitiesExtractor> relatedEntitiesExtractor =
                dataExtractionFactory.newRelatedEntitiesExtractor();
        final TargetsExtractor targetsExtractor = dataExtractionFactory.newTargetsExtractor();

        final Long2ObjectMap<Action> retActions = new Long2ObjectOpenHashMap<>(actionSpecs.size());
        actionSpecs.forEach(actionSpec -> {
            final ActionDTO.Action recommendation = actionSpec.getRecommendation();
            final Action action = new Action();
            action.setOid(recommendation.getId());
            action.setCreationTime(ExportUtils.getFormattedDate(actionSpec.getRecommendationTime()));
            action.setState(extractActionState(actionSpec.getActionState()).getLiteral());
            action.setCategory(extractCategory(actionSpec).getLiteral());
            action.setMode(extractActionMode(actionSpec.getActionMode()).getLiteral());
            action.setDescription(actionSpec.getDescription());
            action.setSeverity(extractSeverity(actionSpec).getLiteral());

            // set risk description
            final Function<Long, String> policyNameDisplayNameRetriever = policyId -> {
                final PolicyDTO.Policy policy = policyById.get(policyId);
                if (policy != null) {
                    return policy.getPolicyInfo().getDisplayName();
                }
                return null;
            };
            try {
                final String riskDescription = RiskUtil.createRiskDescription(actionSpec,
                        policyNameDisplayNameRetriever,
                        entityId -> topologyGraph.getEntity(entityId)
                                .map(SupplyChainEntity::getDisplayName)
                                .orElse(null));
                action.setExplanation(riskDescription);
            } catch (UnsupportedActionException e) {
                logger.error("Cannot calculate risk for unsupported action {}", actionSpec, e);
            }

            // set related entities
            try {
                ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(recommendation);
                relatedEntitiesExtractor.ifPresent(extractor -> {
                    final Predicate<Integer> relatedEntityFilter = RELATED_ENTITY_FILTER_FOR_ACTION_TARGET_ENTITY.getOrDefault(
                            primaryEntity.getType(), RelatedEntitiesExtractor.INCLUDE_ALL_RELATED_ENTITY_TYPES);
                    action.setRelated(extractor.extractRelatedEntities(primaryEntity.getId(), relatedEntityFilter));
                });
            } catch (UnsupportedActionException e) {
                // this should not happen
                logger.error("Unable to get primary entity for unsupported action {}", actionSpec, e);
            }

            // set action savings if available
            if (recommendation.hasSavingsPerHour()) {
                action.setSavings(CostAmount.newAmount(recommendation.getSavingsPerHour()));
            }

            action.setType(extractType(actionSpec).getLiteral());

            retActions.put(actionSpec.getRecommendation().getId(), action);
        });

        // populate attributes as a last step, since a single action (like atomic resize) may be
        // flattened into multiple actions and it requires all other fields to create copy of an action
        List<Action> actions = actionAttributeExtractor.populateExporterAttributes(
                actionSpecs, topologyGraph, retActions);

        // The attribute extractor sets the target entity.
        actions.forEach(action -> {
            ActionImpactedEntity target = action.getTarget();
            if (target != null && targetsExtractor != null) {
                List<Target> targets = targetsExtractor.extractTargets(target.getOid());
                if (targets != null) {
                    target.setAttrs(Collections.singletonMap(ExportUtils.TARGETS_JSON_KEY_NAME, targets));
                }
            }
        });
        return actions;
    }
}
