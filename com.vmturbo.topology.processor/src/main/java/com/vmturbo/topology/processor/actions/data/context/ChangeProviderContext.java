package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ProviderInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;


/**
 * An abstract class for collecting data needed for Move or Scale action execution.
 */
public abstract class ChangeProviderContext extends AbstractActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Comparator used to sort the changes list so host change comes first, and then others
     * (storage move).
     */
    private static final Comparator<ChangeProvider> CHANGE_LIST_COMPARATOR =
        Comparator.comparingInt(change -> change.getSource() != null &&
            change.getSource().getType() == EntityType.PHYSICAL_MACHINE_VALUE ? 0 : 1);

    protected ChangeProviderContext(@Nonnull final ExecuteActionRequest request,
                        @Nonnull final ActionDataManager dataManager,
                        @Nonnull final EntityStore entityStore,
                        @Nonnull final EntityRetriever entityRetriever,
                        @Nonnull final TargetStore targetStore,
                        @Nonnull final ProbeStore probeStore,
                        @Nonnull final GroupAndPolicyRetriever groupAndPolicyRetriever) {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore,
            groupAndPolicyRetriever);
    }

    /**
     * Defines if action is cross-target move.
     *
     * @return True if action is cross-target move.
     */
    protected abstract boolean isCrossTargetMove();

    /**
     * Calculates {@code ActionType} for action execution item.
     *
     * @param srcEntityType Source entity type.
     * @return {@code ActionType} for action execution item.
     */
    protected abstract ActionType getActionItemType(EntityType srcEntityType);

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Set<Long> getControlAffectedEntities() {
        final ActionEntity targetEntity;
        try {
            targetEntity = ActionDTOUtil.getPrimaryEntity(getActionId(), getActionInfo(), true);
        } catch (UnsupportedActionException e) {
            logger.error("Cannot get action primary entity", e);
            return Collections.emptySet();
        }
        // right now, only support controllable flag for VM Move/Scale actions.
        if (targetEntity.hasType()
                && targetEntity.getType() != EntityType.VIRTUAL_MACHINE_VALUE
                && targetEntity.getType() != EntityType.VIRTUAL_VOLUME_VALUE) {
            logger.warn("Ignore controllable logic for action with type: " +
                    targetEntity.getType());
            return Collections.emptySet();
        }
        final Set<Long> entityIds = new HashSet<>();
        // Include the primary entity id
        entityIds.add(targetEntity.getId());
        // Include all providers affected by the move
        for (ChangeProvider changeProvider : ActionDTOUtil.getChangeProviderList(getActionInfo())) {
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
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     * data. A builder is returned so that further modifications can be made by subclasses, before
     * the final build is done.
     *
     * <p>This implementation creates one {@link ActionItemDTO.Builder} for each change in the move
     * action.
     * </p>
     * TODO: Move together is currently supported, but cross target move is not since the required
     *  additional action items for cross target move are not (yet) being added.
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     * @throws ContextCreationException if error faced while creating context
     */
    @Override
    protected List<Builder> initActionItemBuilders() throws ContextCreationException {
        List<ActionItemDTO.Builder> builders = new ArrayList<>();
        // TODO: Assess whether the performance benefits warrant aggregating all the entities
        // involved in the move and making a bulk call to lookup the TopologyEntityDTOs.
        logger.info("Get target entity from repository for action {}", getActionId());
        final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        // sort the changes list so host change comes first, and then others (storage move), since
        // the list coming from AO may be any order, but probe is assuming host move comes first
        // TODO dmitry which probe and why? they should not
        final List<ChangeProvider> changeProviderList = new ArrayList<>(
                ActionDTOUtil.getChangeProviderList(getActionInfo()));
        changeProviderList.sort(CHANGE_LIST_COMPARATOR);
        boolean isPrimary = true;
        for (ChangeProvider change: changeProviderList) {
            builders.add(actionItemDtoBuilder(change, getActionId(), fullEntityDTO, isPrimary));
            isPrimary = false;
        }
        // Cross-target moves require adding storage changes, even when storage is staying the same
        if (isCrossTargetMove()) {
            builders.addAll(getActionItemsForUnchangedStorageProviders(fullEntityDTO,
                    changeProviderList));
        }

        // populate description, risk, execution characteristics
        populatedPrimaryActionAdditionalFields(builders);

        logger.info("ActionDTO builders created for action {}", getActionId());
        return builders;
    }

    /**
     * Get an list of action items describing the unchanged storage providers which are associated
     * with the entity being moved. If the storage is in the ChangeProvider list, then it is not
     * included in the result, since there is already an ActionItemDTO created for it.
     *
     * <p>Cross-target moves require adding storage changes, even when storage is staying the same.
     * In order to provide this, we first retrieve the storage(s) for the entity being moved, using
     * the provided EntityDTO. Then, we construct an action item representing a "change" where the
     * same storage entity is set as both the source and destination of the move.
     * </p>
     *
     * @param fullEntityDTO the entity being moved
     * @param changeList List of {@link ChangeProvider}.
     * @return list of action items for unchanged storage providers associated with the entity being moved
     * @throws ContextCreationException if error faced while creating context
     */
    private List<ActionItemDTO.Builder> getActionItemsForUnchangedStorageProviders(
            final EntityDTO fullEntityDTO, final List<ChangeProvider> changeList)
            throws ContextCreationException {
        final long primaryEntityId = getPrimaryEntityId();
        final TopologyEntityDTO topologyEntityDTO = entityRetriever.retrieveTopologyEntity(primaryEntityId)
                .orElseThrow(() ->
                        new ContextCreationException("No entity found for id " + primaryEntityId));
        // Get a set containing all of the storage associated with this entity
        final Set<Long> storageEntityIds = getAllStorageProviderIds(topologyEntityDTO);
        if (storageEntityIds.isEmpty()) {
            return Collections.emptyList();
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
        final List<ActionItemDTO.Builder> result = new ArrayList<>(storageEntityIds.size());
        for (Long entityId : storageEntityIds) {
            final ChangeProvider storageChange = ChangeProviderContext.createStorageChange(
                    entityId);
            final ActionItemDTO.Builder builder = actionItemDtoBuilder(storageChange, getActionId(),
                    fullEntityDTO, false);
            result.add(builder);
        }
        return result;
    }

    /**
     * Get the {@link EntityType}.STORAGE storage providers and outbound connected storage OIDs
     * for the given entity.
     *
     * @param topologyEntityDTO an entity for which to retrieve the storage providers
     * @return a set containing the storage provider OIDs for the given entity
     */
    private static Set<Long> getAllStorageProviderIds(TopologyEntityDTO topologyEntityDTO) {
        Set<Long> providers = topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(CommoditiesBoughtFromProvider::hasProviderId)
                        .filter(commoditiesBoughtFromProvider -> EntityType.STORAGE == EntityType
                                        .forNumber(commoditiesBoughtFromProvider
                                                        .getProviderEntityType()))
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .collect(Collectors.toSet());
        Set<Long> connected = topologyEntityDTO.getConnectedEntityListList().stream()
                        .filter(ce -> EntityType.forNumber(
                                        ce.getConnectedEntityType()) == EntityType.STORAGE)
                        .map(ConnectedEntity::getConnectedEntityId).collect(Collectors.toSet());
        return new HashSet<>(Sets.union(providers, connected));
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

    protected ActionItemDTO.Builder actionItemDtoBuilder(final ChangeProvider change,
            final long actionId,
            final EntityDTO primaryEntity, final boolean isPrimary) throws ContextCreationException {
        long sourceId = change.getSource().getId();
        long destId = change.getDestination().getId();
        logger.info("Retrieving Source id {} and destination id {} from repository", sourceId, destId);
        EntityDTO sourceEntity = getFullEntityDTO(sourceId);
        EntityDTO destinationEntity = getFullEntityDTO(destId);

        // Check that the source and destination are the same type
        final EntityType srcEntityType = sourceEntity.getEntityType();
        final EntityType destinationEntityType = destinationEntity.getEntityType();
        final Collection<ActionEntity> resources = change.getResourceList();
        if (srcEntityType != destinationEntityType) {
            throw new ContextCreationException("Mismatched source and destination entity types! " +
                    " Source: " + srcEntityType +
                    " Destination: " + destinationEntityType);
        }

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder()
                // Storage moves are represented as CHANGE in the SDK, but are MOVES in the market
                .setActionType(getActionItemType(srcEntityType))
                .setUuid(Long.toString(actionId))
                .setTargetSE(primaryEntity)
                .setCurrentSE(sourceEntity)
                .setNewSE(destinationEntity)
                .addAllContextData(getContextData(isPrimary));
        addResourcesInfo(resources, actionBuilder, EntityType.VIRTUAL_VOLUME);
        getHost(primaryEntity).ifPresent(actionBuilder::setHostedBySE);
        logger.trace("created action item for {}:{}, and provider {} change from {} to {}",
                primaryEntity.getEntityType(), primaryEntity.getDisplayName(),
                srcEntityType, sourceEntity.getDisplayName(), destinationEntity.getDisplayName());
        return actionBuilder;
    }

    private void addResourcesInfo(Collection<ActionEntity> entities,
                    ActionItemDTO.Builder actionBuilder, EntityType entityType) {
        final Collection<String> vendorIds =
                        getVendorIds(entities, r -> r.getType() == entityType.getNumber());
        if (!vendorIds.isEmpty()) {
            final ProviderInfo.Builder providerInfoBuilder =
                            ProviderInfo.newBuilder().setEntityType(entityType)
                                            .addAllIds(vendorIds);
            actionBuilder.addProviders(providerInfoBuilder);
        }
    }

    private Collection<String> getVendorIds(Collection<ActionEntity> entities,
                    Predicate<ActionEntity> filter) {
        return entities.stream().filter(filter).map(ActionEntity::getId)
                        .map(entityRetriever::retrieveTopologyEntity).filter(Optional::isPresent)
                        .map(Optional::get).filter(TopologyEntityDTO::hasOrigin)
                        .map(TopologyEntityDTO::getOrigin).filter(Origin::hasDiscoveryOrigin)
                        .map(o -> o.getDiscoveryOrigin().getDiscoveredTargetDataMap()
                                        .get(this.getTargetId()))
                        .map(PerTargetEntityInformation::getVendorId)
                        .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
    }


}
