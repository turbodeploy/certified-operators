package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A class implementing this interface maps an entity or list of entities
 * into a specific {@link EntityAspect}.
 */
public interface IAspectMapper {
    /**
     * Map a single {@link TopologyEntityDTO} into one entity aspect object.
     *
     * @param entity the {@link TopologyEntityDTO} to get aspect for
     * @return the entity aspect for the given entity, or null if no aspect for this entity
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity)
            throws InterruptedException, ConversionException;

    /**
     * Map a single {@link ApiPartialEntity} into one entity aspect object.
     *
     * @param entity the {@link ApiPartialEntity} to get aspect for
     * @return the entity aspect for the given entity, or null if no aspect for this entity
     */
    @Nullable
    EntityAspect mapEntityToAspect(@Nonnull ApiPartialEntity entity);

    /**
     * Map a list of entities into a single entity aspect object. This needs to be implemented if
     * {@link IAspectMapper#supportsGroup()} returns true.
     *
     * @param entities list of entities to get aspect for, which are members of a group
     * @return the entity aspect for given list of entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    default EntityAspect mapEntitiesToAspect(@Nonnull final List<TopologyEntityDTO> entities)
            throws InterruptedException, ConversionException {
        return null;
    }

    /**
     * Maps a single {@link EntityAspect} representing a group of entities to multiple {@link EntityAspect} objects,
     * where each new object represents a single entity. For example, since VirtualDisksAspectApiDTO::virtualDisks
     * represents a collection of entities, this method can be implemented to map each member of the collection to a new
     * VirtualDisksAspectApiDTO.
     *
     * <p>An implementation of this interface should return true for {@link IAspectMapper#supportsGroupAspectExpansion()}
     * IFF this method is implemented.
     *
     * @param entities the entities for which aspects are being mapped. Based on the type of each, a the resulting
     *        map can hold OIDs of different objects. For instance, if {@link com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO}
     *        are generated for a list of VirtualMachines, the attachedVirtualMachine uuid will be used.
     * @param entityAspect a single {@link EntityAspect} representing multiple {@link EntityAspect} instances
     * @return a map of UUID -> {@link EntityAspect} representing the expanded group {@link EntityAspect}
     */
    default java.util.Map<String, EntityAspect> mapOneToManyAspects(@Nullable List<TopologyEntityDTO> entities,
                                                                    @Nullable final EntityAspect entityAspect) {
        return null;
    }

    /**
     * Returns the aspect name that can be used for filtering.
     *
     * @return the name of the aspect
     */
    @Nonnull
    AspectName getAspectName();

    /**
     * Defines whether or not this aspect mapper supports group aspect. If this is true, then
     * {@link IAspectMapper#mapEntitiesToAspect(List)} need to be implemented.
     *
     * @return true if group aspect is supported, otherwise false
     */
    default boolean supportsGroup() {
        return false;
    }

    /**
     * Whether the given mapper implements {@link IAspectMapper#mapOneToManyAspects}.
     *
     * @return true if a single aspect representing a group of entities can be generated, and mapped
     * to many distinct
     * {@link EntityAspect} instances
     */
    default boolean supportsGroupAspectExpansion() {
        return false;
    }
}
