package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jersey.repackaged.com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.ActionExecutionException;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 *  A class for collecting data needed for Move action execution
 */
public class MoveContext extends AbstractActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    public MoveContext(@Nonnull final ExecuteActionRequest request,
                       @Nonnull final ActionDataManager dataManager,
                       @Nonnull final EntityStore entityStore,
                       @Nonnull final EntityRetriever entityRetriever)
            throws ActionExecutionException {
        super(request, dataManager, entityStore, entityRetriever);
    }

    /**
     * Get the SDK (probe-facing) type of the over-arching action being executed
     *
     * @return the SDK (probe-facing) type of the over-arching action being executed
     */
    @Override
    public ActionType getSDKActionType() {
        // Check for workflows first, because that would eliminate the possibility of any other
        // special cases applying.
        if (hasWorkflow()) {
            // Workflow moves are treated as regular moves that have an associated workflow
            return ActionType.MOVE;
        }
        // If it's not a workflow, but the target is changing, then it is a cross target move.
        if (isCrossTargetMove()) {
            return ActionType.CROSS_TARGET_MOVE;
        }
        // Move together (aka shared nothing) is a same-target move where another provider
        // (e.g. Storage for a VM) is also changing.
        if (isMoveTogether()) {
            return ActionType.MOVE_TOGETHER;
        }
        return ActionType.MOVE;
    }

    /**
     * Get the type of the over-arching action being executed
     *
     * @return the type of the over-arching action being executed
     */
    @Override
    public ActionDTO.ActionType getActionType() {
        return ActionDTO.ActionType.MOVE;
    }

    /**
     * Return a Set of entities to that are directly involved in the action.
     *
     * The implementation for move includes the primary entity (the entity being acted upon), as
     * well as all providers includes in the list of changes.
     *
     * @return a Set of entities involved in the action
     */
    @Override
    public Set<Long> getAffectedEntities() {
        return getProviderEntityIdsFromMoveAction(getMoveInfo());
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     * data. A builder is returned so that further modifications can be made by subclasses, before
     * the final build is done.
     *
     * This implementation creates one {@link ActionItemDTO.Builder} for each change in the move
     * action.
     * TODO: Move together is currently supported, but cross target move is not since the required
     * additional action items for cross target move are not (yet) being added.
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     * @throws ActionExecutionException if the data required for action execution cannot be retrieved
     */
    @Override
    protected List<ActionItemDTO.Builder> initActionItems() throws ActionExecutionException {
        Move move = getMoveInfo();
        List<ActionItemDTO.Builder> builders = Lists.newArrayList();
        // TODO: Assess whether the performance benefits warrant aggregating all the entities
        // involved in the move and making a bulk call to lookup the TopologyEntityDTOs.
        EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        for (ChangeProvider change : move.getChangesList()) {
            builders.add(actionItemDtoBuilder(getTargetId(), change, getActionId(), fullEntityDTO));
        }
        return builders;
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
    @Override
    protected long getPrimaryEntityId() {
        return getMoveInfo().getTarget().getId();
    }

    /**
     * A convenience method for getting the Move info.
     *
     * @return the Move info associated with this action
     */
    private Move getMoveInfo() {
        return getActionInfo().getMove();
    }

    private ActionItemDTO.Builder actionItemDtoBuilder(long targetId,
                                                       ChangeProvider change,
                                                       long actionId,
                                                       EntityDTO primaryEntity)
            throws ActionExecutionException {
        EntityDTO sourceEntity = getFullEntityDTO(change.getSource().getId());
        EntityDTO destinationEntity = getFullEntityDTO(change.getDestination().getId());

        // Check that the source and destination are the same type
        final EntityType srcEntityType = sourceEntity.getEntityType();
        final EntityType destinationEntityType = destinationEntity.getEntityType();
        if (srcEntityType != destinationEntityType) {
            throw new ActionExecutionException("Mismatched source and destination entity types! " +
                    " Source: " + srcEntityType +
                    " Destination: " + destinationEntityType);
        }

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder()
                .setActionType(srcEntityType == EntityType.STORAGE
                        ? ActionType.CHANGE
                        : ActionType.MOVE)
                .setUuid(Long.toString(actionId))
                .setTargetSE(primaryEntity)
                .setCurrentSE(sourceEntity)
                .setNewSE(destinationEntity)
                .addAllContextData(getContextData());

        getHost(primaryEntity).ifPresent(actionBuilder::setHostedBySE);
        return actionBuilder;
    }

    private long getDestinationEntityId() throws ActionExecutionException {
        return getPrimaryChange().getDestination().getId();
    }

    /**
     * By convention, the first ChangeProvider in the list will be used to determine if the move
     * is a cross-target move.
     *
     * @return the first ChangeProvider in the list for this move action
     */
    private ChangeProvider getPrimaryChange() throws ActionExecutionException {
        return getMoveInfo().getChangesList().stream()
                .findFirst()
                .orElseThrow(() -> new ActionExecutionException("No changes found. "
                        + "A move action should have at least one ChangeProvider."));
    }

    private boolean isMoveTogether() {
        // TODO: This logic is insufficient, because we will have multiple changes staged for
        //   cross-target move as well. We will need a better way to differentiate between move
        //   together and cross-target move. Currently, this move together check will work as long
        //   as we have already checked for, and ruled out, cross-target move.
        // We assume that with multiple actions, they are all MOVE/CHANGE actions
        return getMoveInfo().getChangesCount() > 1;
    }

    private boolean isCrossTargetMove() {
        try {
            // Determine if the destination entity for this move action was discovered by the same
            // target that is being used to execute this action. If not, this is considered a cross-
            // target move. Note that workflows would also cause this condition, and therefor it is
            // necessary to check for the presence of workflows before checking for cross-target move.
            return ! getEntityStore().getEntity(getDestinationEntityId())
                    .orElseThrow(() -> new ActionExecutionException("Entity could not be resolved"))
                    .getTargetInfo(getTargetId())
                    .isPresent();
        } catch (ActionExecutionException e) {
            logger.error("Could not determine disposition of move action. Assuming a regular move.",
                    e);
            return false;
        }

    }

    /**
     * Get a set of provider entity ids for the input Move action.
     *
     * @param moveAction a {@link Move} action.
     * @return a set of provider entity ids.
     */
    private static Set<Long> getProviderEntityIdsFromMoveAction(@Nonnull final Move moveAction) {
        // right now, only support controllable flag for VM move actions.
        if (moveAction.getTarget().hasType()
                && moveAction.getTarget().getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.warn("Ignore controllable logic for Move action with type: " +
                    moveAction.getTarget().getType());
            return Collections.emptySet();
        }
        final Set<Long> entityIds = new HashSet<>();
        // Include the primary entity id
        entityIds.add(moveAction.getTarget().getId());
        // Include all providers affected by the move
        for (ChangeProvider changeProvider : moveAction.getChangesList()) {
            if (changeProvider.hasSource()) {
                entityIds.add(changeProvider.getSource().getId());
            }
            if (changeProvider.hasDestination()) {
                entityIds.add(changeProvider.getDestination().getId());
            }
        }
        return entityIds;
    }
}
