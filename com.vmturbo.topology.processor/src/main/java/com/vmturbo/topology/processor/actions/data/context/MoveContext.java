package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jersey.repackaged.com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *  A class for collecting data needed for Move action execution
 */
public class MoveContext extends AbstractActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Used for determining the target type of a given target
     */
    private final TargetStore targetStore;

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
                       @Nonnull final TargetStore targetStore) {
        super(request, dataManager, entityStore, entityRetriever);
        this.targetStore = Objects.requireNonNull(targetStore);
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
        // This is not a move together, so only a single provider is being placed
        // This could be either a MOVE or a CHANGE depending on whether the provider is storage
        // The appropriate type will have already been set in the ActionItemDTO when it was created
        // Lookup the actual type from the (only) ActionItemDTO
        return getActionItems().stream()
                .map(ActionItemDTO::getActionType)
                .findFirst()
                .orElse(ActionType.MOVE);
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
    public Long getSecondaryTargetId() throws TargetNotFoundException {
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
            SDKProbeType primaryTargetType = getTargetType(getTargetId());
            final Optional<Long> matchingSecondaryTargetId = destinationTargetIds.stream()
                    .filter(targetId -> targetMatchesType(targetId, primaryTargetType))
                    .findFirst();
            if (matchingSecondaryTargetId.isPresent()) {
                return matchingSecondaryTargetId.get();
            }

            // If no target of the same type is found, just return the first secondary target found
            return destinationTargetIds.stream()
                    .findFirst()
                    .get();
        }
        return null;
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
     */
    @Override
    protected List<ActionItemDTO.Builder> initActionItemBuilders() {
        Move move = getMoveInfo();
        List<ActionItemDTO.Builder> builders = Lists.newArrayList();
        // TODO: Assess whether the performance benefits warrant aggregating all the entities
        // involved in the move and making a bulk call to lookup the TopologyEntityDTOs.
        EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        for (ChangeProvider change : move.getChangesList()) {
            builders.add(actionItemDtoBuilder(change, getActionId(), fullEntityDTO));
        }
        // Cross-target moves require adding storage changes, even when storage is staying the same
        if (isCrossTargetMove()) {
            builders.addAll(getActionItemsForUnchangedStorageProviders(fullEntityDTO,
                move.getChangesList()));
        }
        return builders;
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

    /**
     * Get an list of action items describing the unchanged storage providers which are associated
     * with the entity being moved. If the storage is in the ChangeProvider list, then it is not
     * included in the result, since there is already an ActionItemDTO created for it.
     *
     * Cross-target moves require adding storage changes, even when storage is staying the same.
     * In order to provide this, we first retrieve the storage(s) for the entity being moved, using
     * the provided EntityDTO. Then, we construct an action item representing a "change" where the
     * same storage entity is set as both the source and destination of the move.
     *
     * @param fullEntityDTO the entity being moved
     * @return list of action items for unchanged storage providers associated with the entity being moved
     */
    private List<ActionItemDTO.Builder> getActionItemsForUnchangedStorageProviders(
            final EntityDTO fullEntityDTO, final List<ChangeProvider> changeList) {
        TopologyEntityDTO topologyEntityDTO;
        final long primaryEntityId = getPrimaryEntityId();
        topologyEntityDTO = entityRetriever.retrieveTopologyEntity(primaryEntityId)
                .orElseThrow(() ->
                        new ContextCreationException("No entity found for id " + primaryEntityId));
        // Get a set containing all of the storage associated with this entity
        final Set<Long> storageEntityIds = getAllStorageProviderIds(topologyEntityDTO);
        if (storageEntityIds.isEmpty()) {
            throw new ContextCreationException("Could not retrieve "
                    + "storage provider ID, which is required for a cross-target move. "
                    + "Offending action: " + getActionId());
        }
        // find storage ids which are already included in the changeProvider list, and remove
        // from the storageEntityIds which we will create action item for
        final Set<Long> alreadyProcessedStorageIds = changeList.stream()
            .map(ChangeProvider::getSource)
            .filter(source -> source.getType() == EntityType.STORAGE_VALUE)
            .map(ActionEntity::getId)
            .collect(Collectors.toSet());
        storageEntityIds.removeAll(alreadyProcessedStorageIds);

        // For each related storage entity, create a change where the source and destination are
        // both the same. This is a convention used to pass the storage information to the probe.
        return storageEntityIds.stream()
                .map(MoveContext::createStorageChange)
                // Convert the change into an ActionItemDTO, which the probe expects to receive
                .map(storageChange ->
                        actionItemDtoBuilder(storageChange, getActionId(), fullEntityDTO))
                .collect(Collectors.toList());
    }

    private static ChangeProvider createStorageChange(long storageEntityId) {
        // We know the source and destination entity types are going to be "STORAGE", because
        // we filter for the "storage" providers when getting the storage entity IDs.
        return ChangeProvider.newBuilder()
                .setSource(ActionEntity.newBuilder()
                        .setId(storageEntityId)
                        .setType(EntityType.STORAGE_VALUE))
                .setDestination(ActionEntity.newBuilder()
                        .setId(storageEntityId)
                        .setType(EntityType.STORAGE_VALUE))
                .build();
    }

    private ActionItemDTO.Builder actionItemDtoBuilder(final ChangeProvider change,
                                                       final long actionId,
                                                       final EntityDTO primaryEntity) {
        EntityDTO sourceEntity = getFullEntityDTO(change.getSource().getId());
        EntityDTO destinationEntity = getFullEntityDTO(change.getDestination().getId());

        // Check that the source and destination are the same type
        final EntityType srcEntityType = sourceEntity.getEntityType();
        final EntityType destinationEntityType = destinationEntity.getEntityType();
        if (srcEntityType != destinationEntityType) {
            throw new ContextCreationException("Mismatched source and destination entity types! " +
                    " Source: " + srcEntityType +
                    " Destination: " + destinationEntityType);
        }

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder()
                // Storage moves are reproesented as CHANGE in the SDK, but are MOVES in the market
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

    private long getDestinationEntityId() {
        return getPrimaryChange().getDestination().getId();
    }

    /**
     * By convention, the first ChangeProvider in the list will be used to determine if the move
     * is a cross-target move.
     *
     * @return the first ChangeProvider in the list for this move action
     */
    private ChangeProvider getPrimaryChange() {
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

    private boolean isCrossTargetMove() {
        if (crossTargetMove == null) {
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

    private SDKProbeType getTargetType(long targetId) throws TargetNotFoundException {
        return targetStore.getProbeTypeForTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
    }

    private boolean targetMatchesType(long targetId, SDKProbeType targetTypeToMatch) {
        try {
            return targetTypeToMatch.equals(getTargetType(targetId));
        } catch (TargetNotFoundException e) {
            // This wrapping is necessary, especially since lambdas cannot throw a checked exception
            throw new ContextCreationException(e.getMessage(), e);
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

    /**
     * Get the {@link EntityType}.STORAGE storage provider OIDs for the given entity
     *
     * @param topologyEntityDTO an entity for which to retrieve the storage providers
     * @return a set containing the storage provider OIDs for the given entity
     */
    private static Set<Long> getAllStorageProviderIds(TopologyEntityDTO topologyEntityDTO) {
        return topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .filter(commoditiesBoughtFromProvider -> EntityType.STORAGE.equals(
                        EntityType.forNumber(commoditiesBoughtFromProvider.getProviderEntityType())))
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
    }
}
