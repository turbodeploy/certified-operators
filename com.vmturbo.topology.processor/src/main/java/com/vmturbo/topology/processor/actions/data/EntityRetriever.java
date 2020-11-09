package com.vmturbo.topology.processor.actions.data;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.conversions.EntityConversionException;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * This class retrieves and converts an entity in order to provide the full entity data for action
 * execution.
 *
 * <p>Entities are the key component of the
 * {@link com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO} that is sent to the
 * probe during action execution. Ideally, the entities that are sent will reflect the full stitched
 * entity data. In order to achieve that, the (stitched) TopologyEntityDTO will be retrieved from
 * the repository and then converted into an SDK EntityDTO.
 */
public class EntityRetriever {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Converts topology processor's entity DTOs to entity DTOs used by SDK probes.
     */
    private final TopologyToSdkEntityConverter entityConverter;

    /**
     * Repository client to use.
     */
    private final RepositoryClient repositoryClient;

    /**
     * The context ID for the realtime market. Used when making remote calls to the repository service.
     */
    private final long realtimeTopologyContextId;

    /**
     * Constructs entity retriever.
     *
     * @param entityConverter entity converter to convert XL DTOs into SDK ones.
     * @param repositoryClient repository client to use if entities are not found in topology cache
     * @param realtimeTopologyContextId topology context ID to retrieve data from repository
     */
    public EntityRetriever(@Nonnull final TopologyToSdkEntityConverter entityConverter,
                           @Nonnull final RepositoryClient repositoryClient,
                           final long realtimeTopologyContextId) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.entityConverter = Objects.requireNonNull(entityConverter);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Retrieves topology entity by the specified OID and converts it to SDK entity data structure.
     * @param entityId entity to retrieve
     * @return SDK entity data
     * @throws EntityRetrievalException if failed to either retrieve or convert the entity
     */
    @Nonnull
    public EntityDTO fetchAndConvertToEntityDTO(final long entityId)
            throws EntityRetrievalException {
        // Get the full (stitched) entity from the Repository Service
        logger.info("Fetch entity oid {} from repository", entityId);
        final TopologyEntityDTO topologyEntityDTO = retrieveTopologyEntity(entityId)
                .orElseThrow(() -> new EntityRetrievalException("No entity found for id "
                        + entityId));
        // Convert the entity to an SDK EntityDTO and return it
        try {
            logger.info("Entity {} retrieved from repository ", topologyEntityDTO.getDisplayName());
            return entityConverter.convertToEntityDTO(topologyEntityDTO);
        } catch (EntityConversionException e) {
            throw new EntityRetrievalException("Could not retrieve full entity data for entity id "
                    + entityId + "due to an entity conversion exception.", e);
        }
    }

    /**
     * Converts XL entity into SDK entity.
     *
     * @param topologyEntityDTO entity to convert.
     * @return SDK entity
     */
    @Nonnull
    public EntityDTO convertToEntityDTO(TopologyEntityDTO topologyEntityDTO) {
        return entityConverter.convertToEntityDTO(topologyEntityDTO);
    }

    /**
     * Retrieve entity data. This is a shortcut for calling {@link #retrieveTopologyEntities(List)}.
     *
     * @param entityId the ID of the entity to fetch data about
     * @return stitched entity data corresponding to the provided entity
     */
    public Optional<TopologyEntityDTO> retrieveTopologyEntity(final long entityId) {
        final Collection<TopologyEntityDTO> topologyEntities = retrieveTopologyEntities(
                Collections.singletonList(entityId));
        if (topologyEntities.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(topologyEntities.iterator().next());
        }
    }

    /**
     * Retrieve entity data from cached topology data. If nothing found, will try loading from
     * repository component.
     *
     * @param entities the entities to fetch data about
     * @return entity data corresponding to the provided entities
     */
    public Collection<TopologyEntityDTO> retrieveTopologyEntities(List<Long> entities) {
        return repositoryClient.retrieveTopologyEntities(entities, realtimeTopologyContextId)
                        .collect(Collectors.toSet());
    }
}
