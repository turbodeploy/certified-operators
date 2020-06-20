package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowProperty;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.ActionConversions;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

/**
 * A super-class for action execution context implementations.
 * Contains logic common to all action execution contexts.
 */
public abstract class AbstractActionExecutionContext implements ActionExecutionContext {

    /**
     * Token to use generating error logging during entity lookup. This corresponds to the
     * main entity in any action, the entity being acted upon.
     */
    private static final String TARGET_LOOKUP_TOKEN = "primary";

    /**
     * The id of this action, as sent from Action Orchestrator
     */
    private final long actionId;

    /**
     * The id of the target on which to execute this action, as sent from Action Orchestrator
     */
    private final long targetId;

    /**
     * Workflow, if any.
     * Workflows allow actions to be executed through a third party action orchestrator
     */
    private final Workflow workflow;

    /**
     * The action type-specific data associated with this action, as sent from Action Orchestrator
     */
    private final ActionInfo actionInfo;

    /**
     * Provides additional data for handling action execution special cases (i.e. complex actions)
     */
    private final ActionDataManager dataManager;

    /**
     * Cache a reference to the entityStore to retrieve target info about entities involved in the
     * action.
     */
    private final EntityStore entityStore;

    /**
     * Retrieves and converts an entity in order to provide the full entity data for action execution.
     */
    protected final EntityRetriever entityRetriever;

    /**
     * A list of {@link ActionItemDTO} to send to the probe for action execution.
     * This is the main carrier of data to the probes when executing an action.
     * By convention, the first ActionItem in the list will declare the overarching type of the
     *   action being executed, as well as include any additional ContextData needed to execute
     *   the action.
     */
    protected List<ActionItemDTO> actionItems;

    /**
     * The SDK (probe-facing) type that will be sent to the probes.
     */
    protected ActionItemDTO.ActionType SDKActionType;

    protected ActionDTO.ActionType actionType;

    @Nullable
    protected final String explanation;

    protected AbstractActionExecutionContext(@Nonnull final ExecuteActionRequest request,
                                             @Nonnull final ActionDataManager dataManager,
                                             @Nonnull final EntityStore entityStore,
                                             @Nonnull final EntityRetriever entityRetriever) {
        Objects.requireNonNull(request);
        this.actionId = request.getActionId();
        this.targetId = request.getTargetId();
        this.workflow = request.hasWorkflowInfo() ? buildWorkflow(request.getWorkflowInfo()) : null;
        this.actionInfo = Objects.requireNonNull(request.getActionInfo());
        this.dataManager = Objects.requireNonNull(dataManager);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.entityRetriever = Objects.requireNonNull(entityRetriever);
        this.actionType = Objects.requireNonNull(request.getActionType());
        this.explanation = request.getExplanation();
    }

    @Nonnull
    @Override
    public ActionItemDTO.ActionType getSDKActionType() {
        if (SDKActionType == null) {
            SDKActionType = calculateSDKActionType(actionType);
        }
        return SDKActionType;
    }

    /**
     * Calculates the {@link ActionItemDTO.ActionType} that will be sent to probes from the one
     * received from Action Orchestrator {@link ActionDTO.ActionType}
     *
     * @param actionType The {@link ActionDTO.ActionType} received from AO
     * @return The {@link ActionItemDTO.ActionType} that will be sent to the probes
     */
    protected ActionItemDTO.ActionType calculateSDKActionType(@Nonnull final ActionDTO.ActionType actionType) {
        return ActionConversions.convertActionType(actionType);
    }

    private void buildActionItems() throws ContextCreationException {
        // Build the action items, the primary data carrier to the probe for action execution
        List<ActionItemDTO.Builder> actionItemBuilders = initActionItemBuilders();
        actionItems = actionItemBuilders.stream()
                .map(Builder::build)
                .collect(Collectors.toList());
    }

    /**
     * The id of the overarching action. This is the ID that gets assigned by the Action Orchestrator.
     *
     * @return the id of the overarching action
     */
    @Override
    public long getActionId() {
        return actionId;
    }

    /**
     * The id of the target containing the entities for the action, which will be used to execute
     * the action.
     *
     * @return the id of the target containing the entities for the action.
     */
    @Override
    public long getTargetId() {
        return targetId;
    }

    @Nonnull
    @Override
    public Optional<Workflow> getWorkflow() {
        return Optional.ofNullable(workflow);
    }

    /**
     * Get all of the action item DTOs associated with executing this action
     *
     * @return all of the action item DTOs associated with executing this action
     */
    @Override
    public List<ActionItemDTO> getActionItems() {
        if (actionItems == null) {
            buildActionItems();
        }
        return actionItems;
    }

    /**
     * Return a Set of entities to that are directly involved in the action.
     *
     * This default implementation assumes that only the primary entity (the entity being acted upon)
     * is affected by the action.
     *
     * @return a Set of entities involved in the action
     */
    @Override
    public Set<Long> getControlAffectedEntities() {
        return Collections.singleton(getPrimaryEntityId());
    }

    /**
     * Get the secondary target involved in this action, or null if no secondary target is involved
     *
     * @return the secondary target involved in this action, or null if no secondary target is
     * involved
     */
    @Nullable
    @Override
    public Long getSecondaryTargetId() throws TargetNotFoundException {
        return null;
    }

    /**
     * By convention, the first ActionItem in the list will declare the overarching type of the
     * action being executed, as well as include any additional ContextData needed to execute
     * the action.
     *
     * TODO: Improve this logic. The reason this is here is because it's a convention that was
     * established in OpsManager, and now several (many?) probes rely on the order of the action
     * items to determine their meaning. Hopefully someday we can remove the importance of ordering
     * action items, and remove this logic that treats the first item in the list as special.
     *
     * @param actionItemBuilders the full list of action items, from which the primary one will be extracted
     * @return the primary {@link ActionItemDTO.Builder} for this action
     */
    protected ActionItemDTO.Builder getPrimaryActionItemBuilder(
            @Nonnull List<ActionItemDTO.Builder> actionItemBuilders) {
        return actionItemBuilders.stream()
                .findFirst()
                .orElseThrow(() -> new ContextCreationException("No action item builders found. "
                        + "An action should have at least one action item."));
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    protected abstract long getPrimaryEntityId();

    protected ActionInfo getActionInfo() {
        return actionInfo;
    }

    protected EntityStore getEntityStore() {
        return entityStore;
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     *   data. A builder is returned so that further modifications can be made by subclasses, before
     *   the final build is done.
     *
     * The default implementation creates a single {@link ActionItemDTO.Builder}
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     */
    protected List<ActionItemDTO.Builder> initActionItemBuilders() {
        // Get the full entity, including a combination of both stitched and raw data
        final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        return buildPrimaryActionItem(fullEntityDTO);
    }

    /**
     * Build the primary actionItem for this action.
     * This is separated out from the call to retrieve the fullEntityDTO so that subclasses
     * don't need to repeat the remote call.
     *
     * @param fullEntityDTO a {@link EntityDTO} representing the primary entity for this action
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders} containing a single
     * item representing the primary actionItem for this action
     */
    protected List<ActionItemDTO.Builder> buildPrimaryActionItem(final EntityDTO fullEntityDTO) {
        final ActionItemDTO.Builder actionItemBuilder = ActionItemDTO.newBuilder();
        actionItemBuilder.setActionType(getSDKActionType());
        actionItemBuilder.setUuid(Long.toString(getActionId()));
        actionItemBuilder.setTargetSE(fullEntityDTO);

        // Add additional data for action execution
        actionItemBuilder.addAllContextData(getContextData());

        // TODO: Determine if this special case should be converted to context data
        // Right now, this is treated as a fourth entity (hostedBySE) on the ActionItemDTO
        getHost(fullEntityDTO).ifPresent(actionItemBuilder::setHostedBySE);

        // Using an ArrayList in order to ensure the returned list is mutable so that subclasses
        // can add additional items
        List<ActionItemDTO.Builder> builders = new ArrayList<>();
        builders.add(actionItemBuilder);
        return builders;
    }

    protected EntityDTO getFullEntityDTO(long entityId) {
        try {
            return entityRetriever.fetchAndConvertToEntityDTO(entityId);
        } catch (EntityRetrievalException e) {
            throw new ContextCreationException("Unable to execute action because the full entity"
                    + "data for entity " + entityId + " could not be retrieved.", e);
        }
    }

    protected PerTargetInfo getPerTargetInfo(final long targetId,
                                             final long entityId,
                                             final String entityType) {
        return entityStore.getEntity(entityId)
                .orElseThrow(() -> ContextCreationException.noEntity(entityType, entityId))
                .getEntityInfo(targetId)
                .orElseThrow(() -> ContextCreationException.noEntityTargetInfo(
                        entityType, entityId, targetId));
    }

    protected Optional<EntityDTO> getHost(final EntityDTO entity) {
        // Right now hosted by only gets set for VM -> PM and Container -> Pod relationships.
        // Hosted by for VM -> PM relationships is most notably required by the HyperV probe.
        // TODO (roman, May 16 2017): Generalize to all entities where this is necessary.
        final EntityType entityType = entity.getEntityType();
        if (entityType.equals(EntityType.VIRTUAL_MACHINE) ||
                entityType.equals(EntityType.CONTAINER)) {
            // Look up the raw entity info to find the host
            // TODO: This is bad. Why store host separately from the rest of the entity data? This
            //     should be stored somewhere in the EntityDTO, not in the PerTargetInfo itself!
            //     If that were the case, this getPerTargetInfo lookup would not be needed.
            // Get the raw discovery data discovered for this entity by this particular target
            final PerTargetInfo entityInfo = getPerTargetInfo(getTargetId(),
                    getPrimaryEntityId(),
                    TARGET_LOOKUP_TOKEN);
            // Look up the raw entity info for the host
            final long host = entityInfo.getHost();
            if (host != 0) {
                return Optional.of(getFullEntityDTO(host));
            }
        }
        return Optional.empty();
    }

    /**
     * Add any additional ContextData needed to execute the action.
     *
     * @return a set of context data containing data related to action execution
     */
    protected List<ContextData> getContextData() {
        return dataManager.getContextData(actionInfo);
    }

    @Override
    @Nonnull
    public ActionExecutionDTO buildActionExecutionDto() {
        final ActionExecutionDTO.Builder actionExecutionBuilder = ActionExecutionDTO.newBuilder()
                .setActionOid(getActionId())
                .setActionType(getSDKActionType())
                .addAllActionItem(getActionItems());

        if (explanation != null) {
            actionExecutionBuilder.setExplanation(explanation);
        }

        // if a WorkflowInfo action execution override is present, translate it to a NonMarketEntity
        // and include it in the ActionExecution to be sent to the target
        getWorkflow().ifPresent(actionExecutionBuilder::setWorkflow);
        return actionExecutionBuilder.build();
    }

    /**
     * Create a Workflow DTO representing the given {@link WorkflowDTO.WorkflowInfo}.
     *
     * @param workflowInfo the information describing this Workflow, including ID, displayName,
     *                     and defining data - parameters and properties.
     * @return a newly created {@link Workflow} DTO
     */
    private static Workflow buildWorkflow(WorkflowDTO.WorkflowInfo workflowInfo) {
        final Workflow.Builder wfBuilder = Workflow.newBuilder();
        if (workflowInfo.hasDisplayName()) {
            wfBuilder.setDisplayName(workflowInfo.getDisplayName());
        }
        if (workflowInfo.hasName()) {
            wfBuilder.setId(workflowInfo.getName());
        }
        if (workflowInfo.hasDescription()) {
            wfBuilder.setDescription(workflowInfo.getDescription());
        }
        wfBuilder.addAllParam(workflowInfo.getWorkflowParamList().stream()
                .map(AbstractActionExecutionContext::buildWorkflowParameter)
                .collect(Collectors.toList()));
        // include the 'property' entries from the Workflow
        wfBuilder.addAllProperty(workflowInfo.getWorkflowPropertyList().stream()
                .map(AbstractActionExecutionContext::buildWorkflowProperty)
                .collect(Collectors.toList()));
        if (workflowInfo.hasScriptPath()) {
            wfBuilder.setScriptPath(workflowInfo.getScriptPath());
        }
        if (workflowInfo.hasEntityType()) {
            wfBuilder.setEntityType(EntityDTO.EntityType.forNumber(workflowInfo.getEntityType()));
        }
        if (workflowInfo.hasActionType()) {
            ActionType converted = ActionConversions.convertActionType(workflowInfo.getActionType());
            if (converted != null) {
                wfBuilder.setActionType(converted);
            }
        }
        if (workflowInfo.hasActionPhase()) {
            final ActionScriptPhase converted = ActionConversions.convertActionPhase(workflowInfo.getActionPhase());
            if (converted != null) {
                wfBuilder.setPhase(converted);
            }
        }
        if (workflowInfo.hasTimeLimitSeconds()) {
            wfBuilder.setTimeLimitSeconds(workflowInfo.getTimeLimitSeconds());
        }
        return wfBuilder.build();
    }

    @Nonnull
    private static Workflow.Parameter buildWorkflowParameter(
            @Nonnull WorkflowParameter workflowParam) {
        final Workflow.Parameter.Builder parmBuilder = Workflow.Parameter.newBuilder();
        if (workflowParam.hasDescription()) {
            parmBuilder.setDescription(workflowParam.getDescription());
        }
        if (workflowParam.hasName()) {
            parmBuilder.setName(workflowParam.getName());
        }
        if (workflowParam.hasType()) {
            parmBuilder.setType(workflowParam.getType());
        }
        if (workflowParam.hasMandatory()) {
            parmBuilder.setMandatory(workflowParam.getMandatory());
        }
        return parmBuilder.build();
    }

    @Nonnull
    private static Workflow.Property buildWorkflowProperty(
            @Nonnull WorkflowProperty workflowProperty) {
        final Workflow.Property.Builder propBUilder = Workflow.Property.newBuilder();
        if (workflowProperty.hasName()) {
            propBUilder.setName(workflowProperty.getName());
        }
        if (workflowProperty.hasValue()) {
            propBUilder.setValue(workflowProperty.getValue());
        }
        return propBUilder.build();
    }
}
