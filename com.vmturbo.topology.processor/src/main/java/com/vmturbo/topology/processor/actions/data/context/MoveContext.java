package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *  A class for collecting data needed for Move action execution
 */
public class MoveContext extends ChangeProviderContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Used for determining the target type of a given target.
     */
    private final TargetStore targetStore;

    /**
     * Used for determining the action policy of a given target.
     */
    private final ProbeStore probeStore;

    /**
     * If true, this move action represents a cross-target move, which means the entity is being
     * moved from a provider discovered on one target to a new provider discovered on a different
     * target.
     *
     * For example, a VM may be moved from a host on one vCenter to a host on a different vCenter.
     */
    private Boolean crossTargetMove;

    public MoveContext(@Nonnull final ExecuteActionRequest request,
                       @Nonnull final ActionDataManager dataManager,
                       @Nonnull final EntityStore entityStore,
                       @Nonnull final EntityRetriever entityRetriever,
                       @Nonnull final TargetStore targetStore,
                       @Nonnull final ProbeStore probeStore) {
        super(request, dataManager, entityStore, entityRetriever);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    @Override
    protected ActionItemDTO.ActionType calculateSDKActionType(@Nonnull final ActionDTO.ActionType actionType) {
        // This switch takes care of the moves that were converted to other types. The conversion
        // could have happened in ActionExecutor.execute().
        switch (actionType) {
            case RESIZE:
                return ActionType.RIGHT_SIZE;
            case ACTIVATE:
                return ActionType.START;
        }

        // The probes expect CHANGE when the action is moving a VM across Storage.
        if (isMoveVmToStorage()) {
            return ActionType.CHANGE;
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
     * Get the secondary target involved in this action, or null if no secondary target is involved.
     *
     * Secondary targets are targets that have discovered the destination entity in the case of a
     * cross-target move (where the destination was not discovered by the same target that
     * discovered the source entity).
     *
     * @return the secondary target involved in this action, or null if no secondary target is
     * involved
     */
    @Nullable
    @Override
    public Long getSecondaryTargetId() throws ContextCreationException {
        // Search for a secondary target only if the move is cross-target
        if (isCrossTargetMove()) {
            // Determine the target ID for the destination entity
            // Collect all targets related to any destination entity in this move
            final Set<Long> destinationTargetIds = getMoveInfo().getChangesList().stream()
                    .map(ChangeProvider::getDestination)
                    .map(ActionEntity::getId)
                    .map(destinationEntityId -> getEntityStore().getEntity(destinationEntityId)
                            .map(Entity::getTargets))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    // This step combines all the Set<Long> in the stream into a single Set<Long>
                    .collect(HashSet::new, Set::addAll, Set::addAll);

            // If there are no destination targets found, throw an exception since cross target
            // moves require a secondary target in order to complete successfully
            if (destinationTargetIds.isEmpty()) {
                throw new ContextCreationException("No secondary target could be found for cross-"
                        + "target move " + getActionId());
            }

            // First try to find a destination target with the same target type as the primary
            // target
            final SDKProbeType primaryTargetType;
            try {
                primaryTargetType = getTargetType(getTargetId());
            } catch (TargetNotFoundException e) {
                throw new ContextCreationException(
                        "Could not determine main target type by id " + getTargetId(), e);
            }
            for (Long targetId: destinationTargetIds) {
                if (targetMatchesType(targetId, primaryTargetType)) {
                    return targetId;
                }
            }
            // If no target of the same type is found, just return the first secondary target found
            return destinationTargetIds.stream()
                    .findFirst()
                    .get();
        }
        return null;
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
    @Override
    protected long getPrimaryEntityId() {
        return ActionDTOUtil.getMoveActionTarget(getActionInfo()).getId();
    }

    /**
     * A convenience method for getting the Move info.
     *
     * @return the Move info associated with this action
     */
    private Move getMoveInfo() {
        return getActionInfo().getMove();
    }

    private long getDestinationEntityId() throws ContextCreationException {
        return getPrimaryChange().getDestination().getId();
    }

    /**
     * By convention, the first ChangeProvider in the list will be used to determine if the move
     * is a cross-target move.
     *
     * @return the first ChangeProvider in the list for this move action
     * @throws ContextCreationException if changes not found
     */
    private ChangeProvider getPrimaryChange() throws ContextCreationException {
        return getMoveInfo().getChangesList().stream()
                .findFirst()
                .orElseThrow(() -> new ContextCreationException("No changes found. "
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

    @Override
    protected ActionType getActionItemType(final EntityType srcEntityType) {
        return srcEntityType == EntityType.STORAGE
                ? ActionType.CHANGE
                : ActionType.MOVE;
    }

    @Override
    protected boolean isCrossTargetMove() {
        if (crossTargetMove == null) {
            final long probeId = targetStore.getTarget(getTargetId()).get().getProbeId();
            final Optional<Entity> entity = getEntityStore().getEntity(getPrimaryEntityId());
            if (!entity.isPresent()) {
                logger.warn("Cannot find the primary entity with ID " + getPrimaryEntityId());
                return false;
            }
            final EntityType entityType = entity.get().getEntityType();
            boolean crossTargetSupported = probeStore.getProbe(probeId).get().getActionPolicyList()
                .stream()
                .filter(ActionPolicyDTO::hasEntityType)
                .filter(actionPolicyDTO -> actionPolicyDTO.getEntityType().equals(entityType))
                .map(ActionPolicyDTO::getPolicyElementList)
                .flatMap(Collection::stream)
                .filter(ActionPolicyElement::hasActionType)
                .map(ActionPolicyElement::getActionType)
                .anyMatch(actionType -> ActionType.CROSS_TARGET_MOVE == actionType);

            if (!crossTargetSupported) {
                return false;
            }
            try {
                // Determine if the destination entity for this move action was discovered by the same
                // target that is being used to execute this action. If not, this is considered a cross-
                // target move. Note that workflows would also cause this condition, and therefor it is
                // necessary to check for the presence of workflows before checking for cross-target move.
                crossTargetMove = !getEntityStore().getEntity(getDestinationEntityId())
                        .orElseThrow(() -> new ContextCreationException("Entity could not be resolved"))
                        .getTargetInfo(getTargetId())
                        .isPresent();
            } catch (ContextCreationException e) {
                logger.error("Could not determine disposition of move action. Assuming a regular move.",
                        e);
                return false;
            }
        }
        return  crossTargetMove;
    }

    /**
     * Checks if the action is MOVE a VM across Storage.
     *
     * @return true if the action is MOVE VM across Storage, false otherwise.
     */
    private boolean isMoveVmToStorage() {
        if (getMoveInfo().hasTarget() &&
            getMoveInfo().getTarget().hasType() &&
            getMoveInfo().getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            return getMoveInfo().getChangesList().stream()
                .map(ChangeProvider::getDestination)
                .map(ActionEntity::getType)
                .allMatch(type -> type == EntityType.STORAGE_VALUE);
        }
        return false;
    }

    private SDKProbeType getTargetType(long targetId) throws TargetNotFoundException {
        return targetStore.getProbeTypeForTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
    }

    private boolean targetMatchesType(long targetId, SDKProbeType targetTypeToMatch)
            throws ContextCreationException {
        try {
            return targetTypeToMatch.equals(getTargetType(targetId));
        } catch (TargetNotFoundException e) {
            // This wrapping is necessary, especially since lambdas cannot throw a checked exception
            throw new ContextCreationException(e.getMessage(), e);
        }
    }

}
