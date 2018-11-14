package com.vmturbo.topology.processor.actions.data;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.conversions.EntityConversionException;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * This class retrieves and converts an entity in order to provide the full entity data for action
 * execution.
 *
 * Entities are the key component of the
 * {@link com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO} that is sent to the
 * probe during action execution. Ideally, the entities that are sent will reflect the full stitched
 * entity data. In order to achieve that, the (stitched) TopologyEntityDTO will be retrieved from
 * the repository and then converted into an SDK EntityDTO.
 */
public class EntityRetriever {

    /**
     * Converts topology processor's entity DTOs to entity DTOs used by SDK probes.
     */
    private final TopologyToSdkEntityConverter entityConverter;

    /**
     * Retrieves the full ToplogyEntityDTO from the Repository service
     */
    private final RepositoryClient repositoryClient;

    /**
     * The context ID for the realtime market. Used when making remote calls to the repository service.
     */
    private final long realtimeTopologyContextId;


    public EntityRetriever(@Nonnull final TopologyToSdkEntityConverter entityConverter,
                           @Nonnull final RepositoryClient repositoryClient,
                           final long realtimeTopologyContextId) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.entityConverter = Objects.requireNonNull(entityConverter);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Nonnull
    public EntityDTO fetchAndConvertToEntityDTO(final long entityId) throws EntityRetrievalException {
        // Get the full (stitched) entity from the Repository Service
        final TopologyEntityDTO topologyEntityDTO = retrieveTopologyEntity(entityId)
                .orElseThrow(() -> new EntityRetrievalException("No entity found for id "
                        + entityId));
        // Convert the entity to an SDK EntityDTO and return it
        try {
            return entityConverter.convertToEntityDTO(topologyEntityDTO);
        } catch (EntityConversionException e) {
            throw new EntityRetrievalException("Could not retrieve full entity data for entity id "
                    + entityId + "due to an entity conversion exception.", e);
        }
    }

    /**
     * Retrieve entity data from Repository service
     *
     * @param entityId the ID of the entity to fetch data about
     * @return stitched entity data corresponding to the provided entity
     */
    public Optional<TopologyEntityDTO> retrieveTopologyEntity(final long entityId) {
        return repositoryClient.retrieveTopologyEntities(
                Collections.singletonList(Long.valueOf(entityId)), realtimeTopologyContextId)
                .getEntitiesList()
                .stream()
                .findFirst();
    }
}
