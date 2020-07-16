package com.vmturbo.topology.graph.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * A reusable base implementation of a {@link TopologyGraphEntity}, which contains all the
 * essential fields (initialized from a {@link TopologyEntityDTO}) and all the methods related
 * to construction.
 *
 * @param <T> The type of entity.
 */
public class BaseGraphEntity<T extends BaseGraphEntity> implements TopologyGraphEntity<T> {

    private final long oid;
    private final int entityType;
    private EntityState entityState;
    private final String displayName;
    private final EnvironmentType environmentType;
    private final List<DiscoveredTargetId> discoveredTargetIds;


    /**
     * These lists represent all connections of this entity:
     * outbound and inbound associations (which correspond
     * to connections of type
     * {@link ConnectedEntity.ConnectionType#NORMAL_CONNECTION}),
     * aggregations, and ownerships. Note that there can only be one owner.
     * The lists are initialized to {@link Collections#emptyList} and are only
     * assigned to {@link ArrayList}s when elements are inserted, to keep
     * the memory footprint small.
     */
    private List<T> outboundAssociatedEntities = Collections.emptyList();
    private List<T> inboundAssociatedEntities = Collections.emptyList();
    private T owner = null;
    private List<T> ownedEntities = Collections.emptyList();
    private List<T> aggregators = Collections.emptyList();
    private List<T> aggregatedEntities = Collections.emptyList();
    private List<T> controllers = Collections.emptyList();
    private List<T> controlledEntities = Collections.emptyList();
    private List<T> providers = Collections.emptyList();
    private List<T> consumers = Collections.emptyList();

    protected BaseGraphEntity(@Nonnull final TopologyEntityDTO src) {
        this.oid = src.getOid();
        this.displayName = src.getDisplayName();
        this.entityType = src.getEntityType();
        this.environmentType = src.getEnvironmentType();
        this.entityState = src.getEntityState();

        // Will be an empty list if the entity was not discovered.
        if (src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().isEmpty()) {
            this.discoveredTargetIds = Collections.emptyList();
        } else {
            this.discoveredTargetIds = new ArrayList<>(src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataCount());
            src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().forEach((tId, info) -> {
                discoveredTargetIds.add(new DiscoveredTargetId(tId, info.getVendorId()));
            });
        }
    }

    @Override
    public String toString() {
        return displayName + "@" + oid;
    }

    /**
     * Tuple for the ID of this entity local to a particular discovering target.
     */
    protected static class DiscoveredTargetId {
        private final long targetId;
        private final String vendorId;

        DiscoveredTargetId(long targetId, String vendorId) {
            this.targetId = targetId;
            this.vendorId = vendorId;
        }

        public long getTargetId() {
            return targetId;
        }

        public String getVendorId() {
            return vendorId;
        }
    }

    /**
     * Trim all the arrays containing references to related entities to size.
     * This can save several MB in a large topology!
     */
    void trimToSize() {
        Stream.of(consumers, providers, outboundAssociatedEntities, inboundAssociatedEntities,
                ownedEntities, aggregatedEntities, aggregators, controllers, controlledEntities)
            .filter(l -> !l.isEmpty())
            .filter(l -> l instanceof ArrayList)
            .forEach(l -> ((ArrayList)l).trimToSize());
    }

    void clearConsumersAndProviders() {
        owner = null;
        Stream.of(consumers, providers, outboundAssociatedEntities, inboundAssociatedEntities,
                ownedEntities, aggregatedEntities, aggregators, controllers, controlledEntities)
                .filter(l -> !l.isEmpty())
                .forEach(List::clear);
    }

    @Override
    public long getOid() {
        return oid;
    }

    @Override
    public int getEntityType() {
        return entityType;
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return entityState;
    }

    @Nonnull
    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return environmentType;
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return discoveredTargetIds.stream()
            .map(t -> t.targetId);
    }

    @Nullable
    @Override
    public String getVendorId(long targetId) {
        return discoveredTargetIds.stream()
            .filter(t -> t.targetId == targetId)
            .map(t -> t.vendorId)
            .findFirst().orElse(null);
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return discoveredTargetIds.stream()
                .map(t -> t.vendorId);
    }

    @Nonnull
    @Override
    public List<T> getProviders() {
        return providers;
    }

    @Nonnull
    @Override
    public List<T> getConsumers() {
        return consumers;
    }

    @Nonnull
    @Override
    public List<T> getOutboundAssociatedEntities() {
        return outboundAssociatedEntities;
    }

    @Nonnull
    @Override
    public List<T> getInboundAssociatedEntities() {
        return inboundAssociatedEntities;
    }

    @Nonnull
    @Override
    public Optional<T> getOwner() {
        return Optional.ofNullable(owner);
    }

    @Nonnull
    @Override
    public List<T> getOwnedEntities() {
        return ownedEntities;
    }

    @Nonnull
    @Override
    public List<T> getAggregators() {
        return aggregators;
    }

    @Nonnull
    @Override
    public List<T> getAggregatedEntities() {
        return aggregatedEntities;
    }

    @Nonnull
    @Override
    public List<T> getControllers() {
        return controllers;
    }

    @Nonnull
    @Override
    public List<T> getControlledEntities() {
        return controlledEntities;
    }

    void addOutboundAssociation(@Nonnull final T entity) {
        if (this.outboundAssociatedEntities.isEmpty()) {
            this.outboundAssociatedEntities = new ArrayList<>(1);
        }
        this.outboundAssociatedEntities.add(entity);
    }

    /**
     * Update entity's state. This field corresponds to {@link TopologyEntityDTO#getEntityState}
     *
     * @param state the {@link EntityState} for the entity.
     */
    public synchronized void setEntityState(EntityState state) {
        this.entityState = state;
    }

    protected List<DiscoveredTargetId> getDiscoveredTargetIds() {
        return discoveredTargetIds;
    }

    void addInboundAssociation(@Nonnull final T entity) {
        if (this.inboundAssociatedEntities.isEmpty()) {
            this.inboundAssociatedEntities = new ArrayList<>(1);
        }
        this.inboundAssociatedEntities.add(entity);
    }

    void addOwner(@Nonnull final T entity) {
        if (this.owner != null) {
            throw new IllegalStateException("Tried to add multiple owners to entity " + this);
        }
        this.owner = entity;
    }

    void addOwnedEntity(@Nonnull final T entity) {
        if (this.ownedEntities.isEmpty()) {
            this.ownedEntities = new ArrayList<>(1);
        }
        this.ownedEntities.add(entity);
    }

    void addAggregator(@Nonnull final T entity) {
        if (this.aggregators.isEmpty()) {
            this.aggregators = new ArrayList<>(1);
        }
        this.aggregators.add(entity);
    }

    void addAggregatedEntity(@Nonnull final T entity) {
        if (this.aggregatedEntities.isEmpty()) {
            this.aggregatedEntities = new ArrayList<>(1);
        }
        this.aggregatedEntities.add(entity);
    }

    void addController(@Nonnull final T entity) {
        if (this.controllers.isEmpty()) {
            this.controllers = new ArrayList<>(1);
        }
        this.controllers.add(entity);
    }

    void addControlledEntity(@Nonnull final T entity) {
        if (this.controlledEntities.isEmpty()) {
            this.controlledEntities = new ArrayList<>(1);
        }
        this.controlledEntities.add(entity);
    }

    void addProvider(@Nonnull final T entity) {
        if (this.providers.isEmpty()) {
            this.providers = new ArrayList<>(1);
        }
        this.providers.add(entity);
    }

    void addConsumer(@Nonnull T entity) {
        if (this.consumers.isEmpty()) {
            this.consumers = new ArrayList<>(1);
        }
        this.consumers.add(entity);
    }

    /**
     * Builder for {@link BaseGraphEntity}. Takes care of the common parts of building an entity,
     * most importantly adding/removing connections, and trimming the entity when finished.
     *
     * @param <B> The builder subtype, to return correct types for method chaining.
     * @param <E> The {@link BaseGraphEntity} subtype being built.
     */
    public abstract static class Builder<B extends Builder<B, E>, E extends BaseGraphEntity<E>>
            implements TopologyGraphEntity.Builder<B, E> {
        protected final E graphEntity;
        protected final Set<Long> providerIds;
        protected final Set<ConnectedEntity> connectedEntities;

        protected Builder(@Nonnull final TopologyEntityDTO dto, E baseEntity) {
            this.providerIds = dto.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(CommoditiesBoughtFromProvider::hasProviderId)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .collect(Collectors.toSet());
            this.connectedEntities = new HashSet<>(dto.getConnectedEntityListList());
            this.graphEntity = baseEntity;
        }

        protected E getGraphEntity() {
            return graphEntity;
        }

        @Override
        public void clearConsumersAndProviders() {
            graphEntity.clearConsumersAndProviders();
        }

        @Nonnull
        @Override
        public Set<Long> getProviderIds() {
            return providerIds;
        }

        @Nonnull
        @Override
        public Set<ConnectedEntity> getConnectionIds() {
            return connectedEntities;
        }

        @Override
        public long getOid() {
            return graphEntity.getOid();
        }

        @Override
        public E build() {
            graphEntity.trimToSize();
            return graphEntity;
        }

        @Override
        public B addInboundAssociation(final B connectedFrom) {
            graphEntity.addInboundAssociation(connectedFrom.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addOutboundAssociation(final B connectedTo) {
            graphEntity.addOutboundAssociation(connectedTo.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addProvider(final B provider) {
            graphEntity.addProvider(provider.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addConsumer(final B consumer) {
            graphEntity.addConsumer(consumer.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addOwner(final B owner) {
            graphEntity.addOwner(owner.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addOwnedEntity(final B ownedEntity) {
            graphEntity.addOwnedEntity(ownedEntity.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addAggregator(final B aggregator) {
            graphEntity.addAggregator(aggregator.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addAggregatedEntity(final B aggregatedEntity) {
            graphEntity.addAggregatedEntity(aggregatedEntity.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addController(final B controller) {
            graphEntity.addController(controller.getGraphEntity());
            return (B)this;
        }

        @Override
        public B addControlledEntity(final B controlledEntity) {
            graphEntity.addControlledEntity(controlledEntity.getGraphEntity());
            return (B)this;
        }
    }
}
