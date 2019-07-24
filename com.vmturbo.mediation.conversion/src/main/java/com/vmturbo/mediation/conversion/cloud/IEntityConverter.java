package com.vmturbo.mediation.conversion.cloud;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Common interface for handling convert of cloud entity objects, as part of the conversion from
 * classic "fake" cloud entities model to the new cloud model in XL. It changes the entities
 * coming from the cloud probe and the relationship between them, for example: change provider to
 * a different one, add new commodities or remove old ones, add new relationship like
 * "connectedTo", "owns", remove entity from topology, etc.
 */
public interface IEntityConverter {
    /**
     * Method which needs to be implemented by different entity types.
     *
     * @param entity the entity to convert
     * @param converter the {@link CloudDiscoveryConverter} instance which contains all info needed for entity specific converters
     * @return true, if the entity should be kept; false, if the entity needs to be removed
     */
    default boolean convert(@Nonnull final EntityDTO.Builder entity, @Nonnull final CloudDiscoveryConverter converter) {
        return true;
    }
}
