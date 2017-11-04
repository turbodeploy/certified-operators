package com.vmturbo.stitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A graph that permits lookup of an entity's providers and consumers in order
 * to permit traversal across a topology during stitching.
 */
public interface StitchingGraph {

    /**
     * Retrieve all providers for a given entity in the {@link StitchingGraph}.
     * If no vertex with the given OID exists, returns an empty stream.
     *
     * OID here is represented as a string because the id field
     * on an EntityDTO is saved as a string.
     *
     * An entity is a provider of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param id The oid of the vertex in the {@link StitchingGraph}.
     * @return All providers for a vertex in the {@link StitchingGraph} by its OID.
     */
    @Nonnull
    Stream<EntityDTO.Builder> getProviderEntities(String id);

    /**
     * Retrieve all providers for a given entity in the {@link StitchingGraph}.
     * If no vertex in the graph matches the associated builder by the
     * builder's id field, returns an empty stream.
     *
     * An entity is a provider of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param entity The builder for the entity whose providers should be retrieved.
     * @return All providers for a vertex in the {@link StitchingGraph} by its associated entity builder.
     */
    @Nonnull
    default Stream<EntityDTO.Builder> getProviderEntities(@Nonnull final EntityDTO.Builder entity) {
        return getProviderEntities(entity.getId());
    }

    /**
     * Retrieve all consumers for a given entity in the {@link StitchingGraph}.
     * If no vertex with the given OID exists, returns an empty stream.
     *
     * OID here is represented as a string because the id field
     * on an EntityDTO is saved as a string.
     *
     * An entity is a consumer of another entity if this entity buys a commodity
     * from that other entity.
     *
     * @param id The oid of the vertex in the {@link StitchingGraph}.
     * @return All providers for a vertex in the {@link StitchingGraph} by its OID.
     */
    @Nonnull
    Stream<EntityDTO.Builder> getConsumerEntities(String id);

    /**
     * Retrieve all consumers for a given entity in the {@link StitchingGraph}.
     * If no vertex in the graph matches the associated builder by the
     * builder's id field, returns an empty stream.
     *
     * An entity is a consumer of another entity if this entity buys a commodity
     * from that other entity.
     *
     * @param entity The builder for the entity whose consumers should be retrieved.
     * @return All providers for a vertex in the {@link StitchingGraph} by its OID.
     */
    @Nonnull
    default Stream<EntityDTO.Builder> getConsumerEntities(@Nonnull final EntityDTO.Builder entity) {
        return getConsumerEntities(entity.getId());
    }
}
