package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;

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
     * Get the external vendor identity for the given target identifier, if present.
     *
     * @param targetId target oid
     * @return null if not present (not mandatory in the probes, absent for not discovered origin)
     */
    @Nullable
    String getVendorId(long targetId);

    /**
     * Get this entity's external vendor identifiers from all targets.
     *
     * @return stream of vendor IDs from all discovering targets
     */
    @Nonnull
    Stream<String> getAllVendorIds();

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
     * Auxiliary method. Get the aggregators of the entity,
     * without including its owner.
     *
     * @return the entities that aggregate this entity
     */
    @Nonnull
    List<E> getAggregators();

    /**
     * Auxiliary method. Get the {@link TopologyGraphEntity}s
     * this entity aggregates, except those that it owns.
     *
     * @return the entities this entity aggregates
     */
    @Nonnull
    List<E> getAggregatedEntities();

    /**
     * Auxiliary method. Get the controllers of the entity
     *
     * @return the entities that control this entity
     */
    @Nonnull
    List<E> getControllers();

    /**
     * Auxiliary method. Get the {@link TopologyGraphEntity}s
     * this entity controls
     *
     * @return the entities this entity controls
     */
    @Nonnull
    List<E> getControlledEntities();

    /**
     * Applies a function transitively to all entities in a collection
     * and collects the results. This utility function is meant to
     * facilitate traversals in the topology graph.
     *
     * @param seed the collection of entities to start with
     * @param function the function to apply transitively
     * @param <E1> the precise implementation of {@link TopologyGraphEntity}
     * @return the collection of all entities
     *         obtained after applying the function transitively
     */
    @Nonnull
    static <E1 extends TopologyGraphEntity<E1>> List<E1> applyTransitively(
            @Nonnull List<E1> seed, @Nonnull Function<E1, List<E1>> function) {
        final Set<E1> result = new HashSet<>(seed);
        int oldResultSize = 0;
        while (oldResultSize < result.size()) {
            oldResultSize = result.size();
            result.addAll(result.stream()
                                .flatMap(e -> function.apply(e).stream())
                                .collect(Collectors.toSet()));
        }
        return result.stream().collect(Collectors.toList());
    }

    /**
     * Get all aggregators and controllers of this entity.
     * This is needed to ensure backwards compatibility of reported relationship between
     * containers and container specs.
     * After https://vmturbo.atlassian.net/browse/OM-71015 is released it can happen that there
     * are k8s probes which report older relationships ie. AggregatedBy and newer probes which
     * report ControlledBy between containers and containerspecs. We need to account for both.
     * As of now the probe would report one or the other, so its ok to get a union of both sets.
     *
     * TODO: Remove this and its respective usage when all users move to newer versions.
     *
     * @return all aggregators and controllers of this entity
     */
    @Nonnull
    default List<E> getAggregatorsAndControllers() {
        return Stream.of(getAggregators(), getControllers())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get all aggregated and controlled entities.
     * This is needed to ensure backwards compatibility of reported relationship between
     * containers and container specs.
     * After https://vmturbo.atlassian.net/browse/OM-71015 is released it can happen that there
     * are k8s probes which report older relationships ie. AggregatedBy and newer probes which
     * report ControlledBy between containers and containerspecs. We need to account for both.
     * As of now the probe would report one or the other, so its ok to get a union of both sets.
     *
     * TODO: Remove this and its respective usage when all users move to newer versions.
     *
     * @return all aggregated and controlled entities
     */
    @Nonnull
    default List<E> getAggregatedAndControlledEntities() {
        return Stream.of(getAggregatedEntities(), getControlledEntities())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get all aggregators of this entity, including the owner.
     *
     * @return all aggregators of this entity
     */
    @Nonnull
    default List<E> getAggregatorsAndOwner() {
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
     * Get the entities that this entity aggregates, including those
     * that it owns
     *
     * @return the aggregated entities.
     */
    @Nonnull
    default List<E> getAggregatedAndOwnedEntities() {
        return Stream.of(getAggregatedEntities(), getOwnedEntities())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get all connected entities
     *
     * @return the owned, aggregated and controlled entities.
     */
    @Nonnull
    default List<E> getAllConnectedEntities() {
        return Stream.of(getAggregatedAndOwnedEntities(), getAggregatorsAndOwner(),
                         getInboundAssociatedEntities(), getOutboundAssociatedEntities(), getControllers(), getControlledEntities())
                .flatMap(List::stream)
                .collect(Collectors.toList());

    }

    /**
     * Returns the list of connections that are broadcast by the topology processor
     * for this entity. This is a utility method to reconstruct the {@link ConnectedEntity}s in
     * the input to the graph builder from the topology graph itself. The list includes the following:
     * <ul>
     *     <li>Normal outbound connections of the current entity</li>
     *     <li>Entities owned by the current entity</li>
     *     <li>Entities that aggregate the current entity</li>
     *     <li>Entities that are controlled by the current entity</li>
     * </ul>
     *
     * <p>Note that the list does not include the reverse of the above connections.
     * For example, the list does not contain the owner of the current entity.
     * </p>
     *
     * @return the list of connections as they are broadcast by the topology processor
     */
    default List<ConnectedEntity> getBroadcastConnections() {
        final ArrayList<ConnectedEntity> result = new ArrayList<>();
        getOutboundAssociatedEntities().forEach(e -> result.add(
                RepositoryDTOUtil.connectedEntity(e.getOid(), e.getEntityType(), ConnectionType.NORMAL_CONNECTION)));
        getOwnedEntities().forEach(e -> result.add(
                RepositoryDTOUtil.connectedEntity(e.getOid(), e.getEntityType(), ConnectionType.OWNS_CONNECTION)));
        getAggregators().forEach(e -> result.add(
                RepositoryDTOUtil.connectedEntity(e.getOid(), e.getEntityType(), ConnectionType.AGGREGATED_BY_CONNECTION)));
        getControllers().forEach(e -> result.add(
                RepositoryDTOUtil.connectedEntity(e.getOid(), e.getEntityType(), ConnectionType.CONTROLLED_BY_CONNECTION)));
        return result;
    }

    default EntityWithConnections asEntityWithConnections() {
        final EntityWithConnections.Builder withConnectionsBuilder =
                EntityWithConnections.newBuilder()
                        .setOid(getOid())
                        .setEntityType(getEntityType())
                        .setDisplayName(getDisplayName());
        withConnectionsBuilder.addAllConnectedEntities(getBroadcastConnections());
        return withConnectionsBuilder.build();
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
         * Return the provider ids for a specific entity.
         *
         * @param entity entity whose provider ids are returned
         * @return provider ids
         */
        @Nonnull
        static Set<Long> extractProviderIds(@Nonnull final TopologyEntityImpl entity) {
            return entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(TopologyEntityImpl.CommoditiesBoughtFromProviderView::hasProviderId)
                .map(TopologyEntityImpl.CommoditiesBoughtFromProviderView::getProviderId)
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
         * Return the outbound connected entity ids for a specific entity.
         *
         * @param entity entity whose outbound connected entity ids are returned
         * @return entity ids of all outbound connected entities
         */
        @Nonnull
        static Set<ConnectedEntity> extractConnectionIds(@Nonnull final TopologyEntityImpl entity) {
            return entity.getConnectedEntityListList().stream()
                .map(TopologyEntityImpl.ConnectedEntityView::toProto)
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
         * Add an controller {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param controller the new controller
         * @return The builder, for chaining
         */
        B addController(B controller);

        /**
         * Add an controller {@link Builder}. This should only be used by
         * {@link TopologyGraphCreator} when constructing the graph.
         *
         * @param controlledEntity the new controlled entity
         * @return The builder, for chaining
         */
        B addControlledEntity(B controlledEntity);


        /**
         * Build the entity.
         *
         * @return the built entity
         */
        E build();
    }
}
