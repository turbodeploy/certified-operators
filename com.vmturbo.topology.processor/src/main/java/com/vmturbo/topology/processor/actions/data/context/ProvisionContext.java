package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * A class for collecting data needed for Provision action execution
 */
public class ProvisionContext extends AbstractActionExecutionContext {

    public ProvisionContext(@Nonnull final ExecuteActionRequest request,
                            @Nonnull final ActionDataManager dataManager,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final EntityRetriever entityRetriever) {
        super(request, dataManager, entityStore, entityRetriever);
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     * {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getProvision().getEntityToClone().getId();
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     * data. A builder is returned so that further modifications can be made by subclasses, before
     * the final build is done.
     * <p>
     * The default implementation creates a single {@link Builder}
     *
     * @return a list of {@link Builder ActionItemDTO builders}
     * @throws ContextCreationException if errors faced constructing action
     */
    @Override
    protected List<ActionItemDTO.Builder> initActionItemBuilders() throws ContextCreationException {
        // Retrieve the full, stitched representation of the entity to clone from the repository
        final long primaryEntityId = getPrimaryEntityId();
        final TopologyEntityDTO entityToClone = entityRetriever.retrieveTopologyEntity(primaryEntityId)
            .orElseThrow(() ->
                new ContextCreationException("No entity found for id " + primaryEntityId));
        // Build the primary action item for this provision, where the targetSE is the entity to clone
        final EntityDTO entityToCloneSdkDTO = entityRetriever.convertToEntityDTO(entityToClone);
        final List<ActionItemDTO.Builder> builders = buildPrimaryActionItem(entityToCloneSdkDTO);
        // Create an action item for each provider of the entity to clone. For each action item
        // the provider should be the new service entity and current service entity should be empty.
        final List<Long> allProviderIds = entityToClone.getCommoditiesBoughtFromProvidersList().stream()
            .filter(CommoditiesBoughtFromProvider::hasProviderId)
            .map(CommoditiesBoughtFromProvider::getProviderId)
            .collect(Collectors.toList());
        // If providers are found, create an ADD_PROVIDER action item for each provider
        if (!allProviderIds.isEmpty()) {
            final Collection<TopologyEntityDTO> allProviders =
                entityRetriever.retrieveTopologyEntities(allProviderIds);
            allProviders.stream()
                    .map(topologyEntityDTO -> entityRetriever.convertToEntityDTO(topologyEntityDTO))
                .filter(provider -> filterProvider(entityToCloneSdkDTO, provider))
                .forEach(entityDTO -> builders.add(
                    createAddProviderActionItem(entityToCloneSdkDTO, entityDTO)));
        }
        return builders;
    }

    private boolean filterProvider(@Nonnull final EntityDTO entityToCloneDTO,
                                   @Nonnull final EntityDTO providerEntityDTO) {
        // Providers must be filtered because probes are expecting a specific number of action items.
        // Including extraneous providers can cause probes to fail to extract the needed information.
        switch (entityToCloneDTO.getEntityType()) {
            // Provisioning a PM requires the datacenter as an ADD_PROVIDER action item
            case PHYSICAL_MACHINE:
                return EntityType.DATACENTER == providerEntityDTO.getEntityType();
            // Provisioning a Storage requires sending any disk arrays and logical pools that are
            // present as providers.
            case STORAGE:
                return EntityType.DISK_ARRAY == providerEntityDTO.getEntityType() ||
                    EntityType.LOGICAL_POOL == providerEntityDTO.getEntityType();
            // By default, include all providers
            default:
                return true;
        }
    }

    /**
     * Build an ADD_PROVIDER actionItem for this action.
     *
     * @param providerEntityDTO a {@link EntityDTO} representing a provider entity for this action
     * @return a single {@link ActionItemDTO.Builder ActionItemDTO builder} representing the
     * ADD_PROVIDER for the supplied provider for this action
     */
    protected ActionItemDTO.Builder createAddProviderActionItem(
        @Nonnull final EntityDTO entityToCloneDTO,
        @Nonnull final EntityDTO providerEntityDTO) {
        final ActionItemDTO.Builder actionItemBuilder = ActionItemDTO.newBuilder();
        actionItemBuilder.setActionType(ActionType.ADD_PROVIDER);
        actionItemBuilder.setUuid(Long.toString(getActionId()));
        actionItemBuilder.setTargetSE(entityToCloneDTO);
        actionItemBuilder.setNewSE(providerEntityDTO);
        return actionItemBuilder;
    }
}
