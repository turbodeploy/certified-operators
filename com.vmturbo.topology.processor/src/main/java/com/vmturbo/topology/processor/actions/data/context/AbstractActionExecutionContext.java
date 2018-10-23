package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.ActionExecutionException;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * A super-class for action execution context implementations.
 * Contains logic common to all action execution contexts.
 */
public abstract class AbstractActionExecutionContext implements ActionExecutionContext {

    /**
     * Token to use generating error logging during entity lookup. This corresponds to the
     * main entity in any action, the entity being acted upon.
     */
    protected static final String TARGET_LOOKUP_TOKEN = "primary";

    /**
     * The id of this action, as sent from Action Orchestrator
     */
    private final long actionId;

    /**
     * The id of the target on which to execute this action, as sent from Action Orchestrator
     */
    private final long targetId;

    /**
     * Indicates whether this action has a workflow associated with it
     * Workflows allow actions to be executed through a third party action orchestrator
     */
    private final boolean hasWorkflow;

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
     * A list of {@link ActionItemDTO} to send to the probe for action execution.
     * This is the main carrier of data to the probes when executing an action.
     * By convention, the first ActionItem in the list will declare the overarching type of the
     *   action being executed, as well as include any additional ContextData needed to execute
     *   the action.
     */
    protected final List<ActionItemDTO> actionItems = new ArrayList<>();

    protected AbstractActionExecutionContext(@Nonnull final ExecuteActionRequest request,
                                             @Nonnull final ActionDataManager dataManager,
                                             @Nonnull final EntityStore entityStore)
            throws ActionExecutionException {
        Objects.requireNonNull(request);
        this.actionId = request.getActionId();
        this.targetId = request.getTargetId();
        this.hasWorkflow = request.hasWorkflowInfo();
        this.actionInfo = Objects.requireNonNull(request.getActionInfo());
        this.dataManager = Objects.requireNonNull(dataManager);
        this.entityStore = Objects.requireNonNull(entityStore);

        // Build the action items, the primary data carrier to the probe for action execution
        List<ActionItemDTO.Builder> actionItemBuilders = initActionItems();
        List<ActionItemDTO> actions = actionItemBuilders.stream()
                .map(Builder::build)
                .collect(Collectors.toList());
        actionItems.addAll(actions);
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

    /**
     * Indicates whether this action has a workflow associated with it.
     * Workflows allow actions to be executed through a third party action orchestrator.
     *
     * @return true, if this action has a workflow associated with it; otherwise, false.
     */
    @Override
    public boolean hasWorkflow() {
        return hasWorkflow;
    }

    /**
     * Get all of the action item DTOs associated with executing this action
     *
     * @return all of the action item DTOs associated with executing this action
     */
    @Override
    public List<ActionItemDTO> getActionItems() {
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
    public Set<Long> getAffectedEntities() {
        return Collections.singleton(getPrimaryEntityId());
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
     * @throws ActionExecutionException when no action items are found
     */
    protected ActionItemDTO.Builder getPrimaryActionItemBuilder(
            @Nonnull List<ActionItemDTO.Builder> actionItemBuilders)
            throws ActionExecutionException {
        return actionItemBuilders.stream()
                .findFirst()
                .orElseThrow(() -> new ActionExecutionException("No action item builders found. "
                        + "An action should have at least one action item."));
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link com.vmturbo.common.protobuf.ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
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
     * @throws ActionExecutionException if the data required for action execution cannot be retrieved
     */
    protected List<ActionItemDTO.Builder> initActionItems() throws ActionExecutionException {
        // Get the raw discovery data discovered for this entity by this particular target
        // TODO: Update this to use the stitched entity DTO instead of the raw discovery data.
        //       This change will take place in an upcoming, related task.
        final PerTargetInfo targetInfo = getPerTargetInfo(getTargetId(),
                getPrimaryEntityId(),
                TARGET_LOOKUP_TOKEN);

        final ActionItemDTO.Builder actionItemBuilder = ActionItemDTO.newBuilder();
        actionItemBuilder.setActionType(getSDKActionType());
        actionItemBuilder.setUuid(Long.toString(getActionId()));
        actionItemBuilder.setTargetSE(targetInfo.getEntityInfo());

        // Add additional data for action execution
        actionItemBuilder.addAllContextData(getContextData());

        // TODO: Determine if this special case should be converted to context data
        // Right now, this is treated as a fourth entity (hostedBySE) on the ActionItemDTO
        getHost(getTargetId(), targetInfo).ifPresent(actionItemBuilder::setHostedBySE);

        List<ActionItemDTO.Builder> builders = new ArrayList<>();
        builders.add(actionItemBuilder);
        return builders;
    }

    protected PerTargetInfo getPerTargetInfo(final long targetId,
                                             final long entityId,
                                             final String entityType)
            throws ActionExecutionException {
        return entityStore.getEntity(entityId)
                .orElseThrow(() -> ActionExecutionException.noEntity(entityType, entityId))
                .getEntityInfo(targetId)
                .orElseThrow(() -> ActionExecutionException.noEntityTargetInfo(
                        entityType, entityId, targetId));
    }

    protected Optional<EntityDTO> getHost(final long targetId, final PerTargetInfo entityInfo)
            throws ActionExecutionException {
        // Right now hosted by only gets set for VM -> PM and Container -> Pod relationships.
        // Hosted by for VM -> PM relationships is most notably required by the HyperV probe.
        // TODO (roman, May 16 2017): Generalize to all entities where this is necessary.
        if (entityInfo.getEntityInfo().getEntityType().equals(EntityType.VIRTUAL_MACHINE) ||
                entityInfo.getEntityInfo().getEntityType().equals(EntityType.CONTAINER)) {
            final PerTargetInfo hostOfTarget = getPerTargetInfo(targetId,
                    entityInfo.getHost(), "host of target");
            return Optional.of(hostOfTarget.getEntityInfo());
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

}
