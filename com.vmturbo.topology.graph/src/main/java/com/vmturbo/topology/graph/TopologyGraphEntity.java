package com.vmturbo.topology.graph;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * An entity in a {@link TopologyGraph}. This interface is meant to expose all properties
 * of a {@link TopologyEntityDTO} that are requires for search and supply chain resolution.
 *
 * IMPORTANT - It is important to keep this interface as small as possible. Please do not add
 *             unnecessary properties unless they are really required for searches.
 *
 * @param <E> The type of entity implementation, passed as a generic parameter to force the
 *           connection-related methods (e.g. {@link TopologyGraphEntity#getConsumers()} to return
 *           the right types.
 */
public interface TopologyGraphEntity<E extends TopologyGraphEntity> {

    /**
     * These are the commodity types that need to be persisted and returned by
     * {@link TopologyGraphEntity#soldCommoditiesByType()} because we support searching on their
     * values.
     * <p>
     * Implementations may or may not persist additional commodities.
     */
    Set<Integer> COMM_SOLD_TYPES_TO_PERSIST =
        ImmutableSet.of(CommodityType.VMEM_VALUE, CommodityType.MEM_VALUE);

    /**
     * Get the OID for this entity.
     *
     * @return the OID for this entity.
     */
    long getOid();

    /**
     * Get the entityType. This field corresponds to {@link TopologyEntityDTO#getEntityType()}
     *
     * @return The entityType for the entityBuilder corresponding to this node.
     */
    int getEntityType();

    /**
     * Get the entity's state. This field corresponds to {@link TopologyEntityDTO#getEntityState}
     *
     * @return The {@link EntityState} for the entity builder corresponding to this node.
     */
    @Nonnull
    EntityState getEntityState();

    /**
     * Get the display name for this entity.
     *
     * @return The display name for this entity.
     */
    @Nonnull
    String getDisplayName();

    @Nonnull
    TypeSpecificInfo getTypeSpecificInfo();

    /**
     * Get the entity's environment type. This field corresponds to
     * {@link TopologyEntityDTO#getEnvironmentType()}
     *
     * @return The {@link EnvironmentType} for the entity builder corresponding to this node.
     */
    @Nonnull
    EnvironmentType getEnvironmentType();

    @Nonnull
    Stream<Long> getDiscoveringTargetIds();

    /**
     * Get the commodities sold by this entity, organized by type.
     * This may not return the full list of sold commodities. Implementations are only
     * required to return commodities that are supported in the filters
     * in {@link com.vmturbo.topology.graph.search.filter.TopologyFilterFactory}.
     * See: {@link TopologyGraphEntity#COMM_SOLD_TYPES_TO_PERSIST}.
     */
    @Nonnull
    Map<Integer, CommoditySoldDTO> soldCommoditiesByType();

    /**
     * Get the tags associated with an entity. This field corresponds to
     * {@link TopologyEntityDTO#getTags()}, reformatted into a format that's easier to work with.
     *
     * @return A map of (tag name) -> (values)
     */
    @Nonnull
    Map<String, List<String>> getTags();

    /**
     * Get the set of all entities in the topology that provide commodities to this entity.
     *
     * Note that although the providers are held in a list (for memory consumption reasons), entries
     * in the list are unique.
     *
     * If a {@link TopologyGraphEntity} is in this list, it indicates this entity sells one or more commodities to
     * that {@link TopologyGraphEntity}. A provides relationship is one-to-one with a consumes relation, so if an
     * {@link TopologyGraphEntity} appears in this entity's providers list, this {@link TopologyGraphEntity} will appear
     * in that entity's consumers list.
     *
     * The providers list cannot be modified.
     *
     * @return  All {@link TopologyGraphEntity}s that provide commodities to this {@link TopologyGraphEntity}.
     */
    @Nonnull
    List<E> getProviders();

    /**
     * Get the set of all entities in the topology that consume commodities from this entity.
     *
     * Note that although the consumers are held in a list (for memory consumption reasons), entries
     * in the list are unique.
     *
     * If a {@link TopologyGraphEntity} is in this list, it indicates that entity buys one or more commodities from
     * this {@link TopologyGraphEntity}. A consumes relationship is one-to-one with a provides relation, so if an
     * {@link TopologyGraphEntity} appears in this entity's consumers list, this {@link TopologyGraphEntity} will appear
     * in that entity's providers list.
     *
     * The consumers list cannot be modified.
     *
     * @return  All {@link TopologyGraphEntity}s that consume commodities from this {@link TopologyGraphEntity}.
     */
    @Nonnull
    List<E> getConsumers();

    /**
     * Get the {@link TopologyGraphEntity}s this entity connects to.
     * This is derived from {@link TopologyEntityDTO#getConnectedEntityListList()}.
     */
    @Nonnull
    List<E> getConnectedToEntities();

    /**
     * Get the {@link TopologyGraphEntity}s that are connected to this entity. This is the inverse
     * of {@link TopologyGraphEntity#getConnectedToEntities()}.
     */
    @Nonnull
    List<E> getConnectedFromEntities();

    /**
     * A convenience method that applies a type filter on
     * {@link TopologyGraphEntity#getConnectedFromEntities()}.
     */
    @Nonnull
    default Stream<E> getConnectedFromEntities(final int type) {
        return getConnectedFromEntities().stream()
            .filter(entity -> entity.getEntityType() == type);
    }

    /**
     * Builder for a {@link TopologyGraphEntity}.
     * Use this to allow {@link TopologyGraphCreator} to work with the {@link TopologyGraphEntity}
     * implementation.
     *
     * @param <B> The {@link Builder} implementation.
     * @param <E> The {@link TopologyGraphEntity} implementation.
     */
    interface Builder<B extends Builder, E extends TopologyGraphEntity<E>> {
        /**
         * Clear the consumer and provider lists. Call only if rebuilding a new graph
         * because this will invalidate any prior graphs in which this entity was a participant.
         */
        void clearConsumersAndProviders();

        /**
         * Get the IDs of provider entities (derived from
         * {@link TopologyEntityDTO#getCommoditiesBoughtFromProvidersList()} ).
         * <p>
         * We don't expose the full {@link TopologyEntityDTO} so that implementations aren't forced
         * to keep them in memory.
         */
        @Nonnull
        Set<Long> getProviderIds();

        @Nonnull
        static Set<Long> extractProviderIds(@Nonnull final TopologyEntityDTOOrBuilder entity) {
            return entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        }

        /**
         * Get the IDs of connected to entities (derived from
         * {@link TopologyEntityDTO#getConnectedEntityListList()}).
         * <p>
         * We don't expose the full {@link TopologyEntityDTO} so that implementations aren't forced
         * to keep them in memory.
         */
        @Nonnull
        Set<Long> getConnectionIds();

        @Nonnull
        static Set<Long> extractConnectionIds(@Nonnull final TopologyEntityDTOOrBuilder entity) {
            return entity.getConnectedEntityListList().stream()
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet());
        }

        /**
         * Get the OID of the entity.
         */
        long getOid();

        /**
         * Add a consumer {@link Builder}. This should only be used by {@link TopologyGraphCreator}
         * when constructing the graph.
         *
         * @return The builder, for chaining.
         */
        B addConsumer(B consumer);

        /**
         * Add a provider {@link Builder}. This should only be used by {@link TopologyGraphCreator}
         * when constructing the graph.
         *
         * @return The builder, for chaining.
         */
        B addProvider(B provider);

        /**
         * Add an outgoing connection {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @return The builder, for chaining.
         */
        B addConnectedTo(B connectedTo);

        /**
         * Add an incoming connection {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @return The builder, for chaining.
         */
        B addConnectedFrom(B connectedFrom);

        /**
         * Build the entity.
         */
        E build();
    }
}
