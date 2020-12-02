package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * This class converts {@link TopologyEntityDTO}s to {@link EntityDetailsApiDTO}.
 */
public class EntityDetailsMapper {

    /**
     * Return entities with metadata.
     *
     * @param entities list of topology entities.
     * @return a list of {@link EntityDetailsApiDTO}.
     */
    @Nonnull
    public List<EntityDetailsApiDTO> toEntitiesDetails(
            @Nonnull final Collection<TopologyEntityDTO> entities) {
        return entities.stream()
                .map(this::toEntityDetails)
                .collect(Collectors.toList());
    }

    /**
     * Return entity with details.
     *
     * @param entity a topology entity.
     * @return an entity with details.
     */
    @VisibleForTesting
    @Nonnull
    public EntityDetailsApiDTO toEntityDetails(
            @Nonnull final TopologyEntityDTO entity) {
        EntityDetailsApiDTO entityApiDTO = new EntityDetailsApiDTO();
        entityApiDTO.setUuid(entity.getOid());

        // TODO: entityApiDTO.setDetails(a list of DetailDataApiDTO)

        return entityApiDTO;
    }

}
