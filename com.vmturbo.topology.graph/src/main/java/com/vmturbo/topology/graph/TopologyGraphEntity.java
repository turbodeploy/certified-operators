package com.vmturbo.topology.graph;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * Get the {@link TopologyGraphEntity}s this entity connects to, with a "normal" connection
     * (no ownership or aggregation).
     *
     * @return all entities connected with outbound "normal" connections to this entity
     */
    @Nonnull
    List<E> getOutboundAssociatedEntities();

    /**
     * Get the {@link TopologyGraphEntity}s that are connected to this entity, with a "normal" connection
     * (no ownership or aggregation).
     *
     * @return all entities connected with inbound "normal" connections to this entity
     */
    @Nonnull
    List<E> getInboundAssociatedEntities();

    /**
     * Get the owner of this entity.
     *
     * @return The owner of this entity
     */
    @Nonnull
    Optional<E> getOwner();

    /**
     * Get the {@link TopologyGraphEntity}s this entity owns.
     *
     * @return the entities this entity owns
     */
    @Nonnull
    List<E> getOwnedEntities();

    /**
     * Get the aggregators of the entity.
     *
     * @return the entities that aggregate this entity
     */
    @Nonnull
    List<E> getAggregators();

    /**
     * Get the {@link TopologyGraphEntity}s this entity aggregates.
     *
     * @return the entities this entity aggregates
     */
    @Nonnull
    List<E> getAggregatedEntities();

    /**
     * Get all {@link TopologyGraphEntity}s connected to this, including owned and aggregated entities.
     * The set of entities returned should be equal to the set of entities returned by
     * {@link TopologyEntityDTO#getConnectedEntityListList()} in the equivalent
     * {@link TopologyEntityDTO} object.
     *
     * @return all entities connected to this entity
     *         (outbound associations, owned entities, and aggregated entities)
     */
    @Nonnull
    default List<E> getConnectedToEntities() {
        return Stream.of(getOutboundAssociatedEntities(), getAggregatedEntities(), getOwnedEntities())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get all {@link TopologyGraphEntity}s that connect to this, including owner and aggregators.
     *
     * @return all entities that connect to this entity (inbound associations, owner, and aggregators)
     */
    @Nonnull
    default List<E> getConnectedFromEntities() {
        return Stream.of(getInboundAssociatedEntities(), getAggregators(), ownerAsList())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get the owner and all aggregators of this entity.
     *
     * @return the owner and all aggregators of this entity
     */
    @Nonnull
    default List<E> getOwnersOrAggregators() {
        return Stream.of(getAggregators(), ownerAsList())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get the owner in a singleton list, or an empty list if no owner exists.
     *
     * @return the owner in a singleton list, or an empty list if no owner exists
     */
    @Nonnull
    default List<E> ownerAsList() {
        return getOwner().map(Collections::singletonList).orElseGet(Collections::emptyList);
    }

    /**
     * Get the owned and aggregated entities.
     *
     * @return the owned and aggregated entities.
     */
    @Nonnull
    default List<E> getOwnedOrAggregatedEntities() {
        return Stream.of(getAggregatedEntities(), getOwnedEntities())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * A convenience method that applies a type filter on
     * {@link TopologyGraphEntity#getConnectedFromEntities()}.
     *
     * @param type entity type of the returned entities
     * @return return all inbound connections of a certain entity type
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
         *
         * @return the set of the ids of all the providers of this entity
         */
        @Nonnull
        Set<Long> getProviderIds();

        /**
         * Return the provider ids for a specific entity.
         *
         * @param entity entity whose provider ids are returned
         * @return provider ids
         */
        @Nonnull
        static Set<Long> extractProviderIds(@Nonnull final TopologyEntityDTOOrBuilder entity) {
            return entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        }

        /**
         * Get the IDs of connected to entities (derived from
         * {@link TopologyEntityDTO#getConnectedEntityListList()}) and their connection types.
         * <p>
         * We don't expose the full {@link TopologyEntityDTO} so that implementations aren't forced
         * to keep them in memory.
         *
         * @return the set of the ids of all the outbound-connected entities for this entity
         */
        @Nonnull
        Set<ConnectedEntity> getConnectionIds();

        /**
         * Return the outbound connected entity ids for a specific entity.
         *
         * @param entity entity whose outbound connected entity ids are returned
         * @return entity ids of all outbound connected entities
         */
        @Nonnull
        static Set<ConnectedEntity> extractConnectionIds(@Nonnull final TopologyEntityDTOOrBuilder entity) {
            return entity.getConnectedEntityListList().stream()
                .collect(Collectors.toSet());
        }

        /**
         * Get the OID of the entity.
         *
         * @return oid of the entity
         */
        long getOid();

        /**
         * Add a consumer {@link Builder}. This should only be used by {@link TopologyGraphCreator}
         * when constructing the graph.
         *
         * @param consumer the new consumer
         * @return The builder, for chaining
         */
        B addConsumer(B consumer);

        /**
         * Add a provider {@link Builder}. This should only be used by {@link TopologyGraphCreator}
         * when constructing the graph.
         *
         * @param provider the new provider
         * @return The builder, for chaining.
         */
        B addProvider(B provider);

        /**
         * Add an outgoing normal connection {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param connectedTo the new outbound associated entity
         * @return The builder, for chaining
         */
        B addOutboundAssociation(B connectedTo);

        /**
         * Add an incoming normal connection {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param connectedFrom the new inbound associated entity
         * @return The builder, for chaining
         */
        B addInboundAssociation(B connectedFrom);

        /**
         * Add an owner {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param owner the new owner
         * @return The builder, for chaining
         */
        B addOwner(B owner);

        /**
         * Add an owned {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param ownedEntity the new owned entity
         * @return The builder, for chaining
         */
        B addOwnedEntity(B ownedEntity);

        /**
         * Add an aggregator {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param aggregator the new aggregator
         * @return The builder, for chaining
         */
        B addAggregator(B aggregator);

        /**
         * Add an aggregated {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param aggregatedEntity the new aggregated entity
         * @return The builder, for chaining
         */
        B addAggregatedEntity(B aggregatedEntity);


        /**
         * Build the entity.
         *
         * @return the built entity
         */
        E build();
    }
}
