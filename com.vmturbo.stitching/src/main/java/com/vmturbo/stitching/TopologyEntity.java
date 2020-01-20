package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * A wrapper around {@link TopologyEntityDTO.Builder}.Properties of the entity such as commodity values
 * may be updated, but relationships to other entities in the topology cannot be changed.
 *
 * {@link TopologyEntity}s are equivalent when their OIDs are equal. It is an error to create two different
 * {@link TopologyEntity}s with the same OIDs and behavior is undefined when this is done.
 *
 * The TopologyEntityDTO.Builder within a TopologyEntity may be edited but the TopologyEntity is immutable otherwise.
 */
public class TopologyEntity implements TopologyGraphEntity<TopologyEntity>, JournalableEntity<TopologyEntity> {

    /**
     * A builder for the entity in the topology corresponding to this TopologyEntity.
     */
    private TopologyEntityDTO.Builder entityBuilder;

    /**
     * The set of all entities in the topology that consume commodities from this TopologyEntity.
     * {@see TopologyEntity#getConsumers}
     */
    private final List<TopologyEntity> consumers;

    /**
     * The set of all entities in the topology that provide commodities to this TopologyEntity.
     * {@see TopologyEntity#getProviders}
     */
    private final List<TopologyEntity> providers;

    /**
     * These lists represent all connections of this entity:
     * outbound and inbound associations (which correspond
     * to connections of type
     * {@link TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType#NORMAL_CONNECTION}),
     * aggregations, and ownerships. Note that there can only be one owner.
     *
     * <p>The lists are mutable, because references to them are passed directly
     * to the builder of this object. This is the reason why {@link AtomicReference}
     * is used to store owner.</p>
     */
    private final List<TopologyEntity> inboundAssociatedEntities;
    private final List<TopologyEntity> outboundAssociatedEntities;
    private final AtomicReference<TopologyEntity> owner;
    private final List<TopologyEntity> ownedEntities;
    private final List<TopologyEntity> aggregators;
    private final List<TopologyEntity> aggregatedEntities;
    private long clonedFromEntityOid;

    private TopologyEntity(@Nonnull final TopologyEntityDTO.Builder entity,
                           @Nonnull final List<TopologyEntity> consumers,
                           @Nonnull final List<TopologyEntity> providers,
                           @Nonnull final List<TopologyEntity> outboundAssociatedEntities,
                           @Nonnull final List<TopologyEntity> inboundAssociatedEntities,
                           @Nonnull final AtomicReference<TopologyEntity> owner,
                           @Nonnull final List<TopologyEntity> ownedEntities,
                           @Nonnull final List<TopologyEntity> aggregators,
                           @Nonnull final List<TopologyEntity> aggregatedEntities) {
        this.entityBuilder = Objects.requireNonNull(entity);
        this.consumers = consumers;
        this.providers = providers;
        this.inboundAssociatedEntities = inboundAssociatedEntities;
        this.outboundAssociatedEntities = outboundAssociatedEntities;
        this.owner = owner;
        this.ownedEntities = ownedEntities;
        this.aggregators = aggregators;
        this.aggregatedEntities = aggregatedEntities;
    }

    @Override
    public long getOid() {
        return entityBuilder.getOid();
    }

    @Override
    public int getEntityType() {
        return entityBuilder.getEntityType();
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return entityBuilder.getEntityState();
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return entityBuilder.getEnvironmentType();
    }

    @Nonnull
    @Override
    public EntityType getJournalableEntityType() {
        return EntityType.forNumber(getEntityType());
    }

    @Nonnull
    @Override
    public String removalDescription() {
        throw new IllegalStateException(); // TopologyEntities should never be removed.
    }

    @Nonnull
    @Override
    public String additionDescription() {
        throw new IllegalStateException(); // TopologyEntities should never be added.
    }

    @Nonnull
    @Override
    public TopologyEntity snapshot() {
        // Copy consumers
        final List<TopologyEntity> newConsumers = new ArrayList<>(consumers.size());
        newConsumers.addAll(consumers);

        // Copy providers
        final List<TopologyEntity> newProviders = new ArrayList<>(providers.size());
        newProviders.addAll(providers);

        // Copy inboundAssociatedEntities
        final List<TopologyEntity> newConnectedFromAssociatedEntities = new ArrayList<>(inboundAssociatedEntities);
        newConnectedFromAssociatedEntities.addAll(inboundAssociatedEntities);

        // Copy outboundAssociatedEntities
        final List<TopologyEntity> newConnectedToAssociatedEntities =
                new ArrayList<>(outboundAssociatedEntities.size());
        newConnectedToAssociatedEntities.addAll(outboundAssociatedEntities);

        // Copy owned entities, aggregators, aggregated entities
        final List<TopologyEntity> newOwnedEntities = new ArrayList<>(ownedEntities.size());
        newOwnedEntities.addAll(ownedEntities);
        final List<TopologyEntity> newAggregators = new ArrayList<>(aggregators.size());
        newAggregators.addAll(aggregators);
        final List<TopologyEntity> newAggregatedEntities = new ArrayList<>(aggregatedEntities.size());
        newAggregatedEntities.addAll(aggregatedEntities);

        // Create and return the copy
        return new TopologyEntity(
                entityBuilder.clone(), newConsumers, newProviders,
                newConnectedToAssociatedEntities, newConnectedFromAssociatedEntities,
                owner, newOwnedEntities, newAggregators, newAggregatedEntities);
    }

    @Nonnull
    @Override
    public String getDisplayName() {
        return entityBuilder.getDisplayName();
    }

    @Nonnull
    @Override
    public TypeSpecificInfo getTypeSpecificInfo() {
        return entityBuilder.getTypeSpecificInfo();
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return hasDiscoveryOrigin()
            ? entityBuilder.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet().stream()
            : Stream.empty();
    }

    @Override
    public String getVendorId(long targetId) {
        if (hasDiscoveryOrigin()) {
            PerTargetEntityInformation info = entityBuilder.getOrigin().getDiscoveryOrigin()
                            .getDiscoveredTargetDataMap().get(targetId);
            if (info != null && info.hasVendorId()) {
                return info.getVendorId();
            }
        }
        return null;
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return hasDiscoveryOrigin() ?
            entityBuilder.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().values()
                .stream()
                .filter(PerTargetEntityInformation::hasVendorId)
                .map(PerTargetEntityInformation::getVendorId) :
            Stream.empty();
    }

    @Nonnull
    @Override
    public Map<Integer, List<CommoditySoldDTO>> soldCommoditiesByType() {
        return entityBuilder.getCommoditySoldListList().stream()
            .collect(Collectors.groupingBy(commSold -> commSold.getCommodityType().getType()));
    }

    @Nonnull
    @Override
    public Map<String, List<String>> getTags() {
        return entityBuilder.getTags().getTagsMap().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList()));
    }

    /**
     * Get a builder for the entity associated with this TopologyEntity.
     * The builder may be mutated to modify the properties of the entity.
     *
     * DO NOT modify the providers or consumers of the entity or the graph will be invalidated. This
     * means that commodities bought and sold by entities should not be modified in a way that they change
     * which entities are buying or selling commodities to each other. Properties of commodities such as
     * used or capacity values that don't change which entities are involved in the buying or selling may
     * be modified without issue.
     *
     * @return The property information for the entity associated with this TopologyEntity.
     */
    @Nonnull
    public TopologyEntityDTO.Builder getTopologyEntityDtoBuilder() {
        return entityBuilder;
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getInboundAssociatedEntities() {
        return Collections.unmodifiableList(inboundAssociatedEntities);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getOutboundAssociatedEntities() {
        return Collections.unmodifiableList(outboundAssociatedEntities);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getConsumers() {
        return Collections.unmodifiableList(consumers);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    @Nonnull
    @Override
    public Optional<TopologyEntity> getOwner() {
        return Optional.ofNullable(owner.get());
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getOwnedEntities() {
        return Collections.unmodifiableList(ownedEntities);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getAggregators() {
        return Collections.unmodifiableList(aggregators);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getAggregatedEntities() {
        return Collections.unmodifiableList(aggregatedEntities);
    }

    public long getClonedFromEntityOid() {
        return clonedFromEntityOid;
    }

    private void setClonedFromEntityOid(final long clonedFromEntityOid) {
        this.clonedFromEntityOid = clonedFromEntityOid;
    }

    /**
     * Check if this {@link TopologyEntity} was discovered by a probe.
     *
     * @return True if discovered by a probe, false otherwise.
     */
    public boolean hasDiscoveryOrigin() {
        return entityBuilder.hasOrigin() && entityBuilder.getOrigin().hasDiscoveryOrigin();
    }

    /**
     * Check if this {@link TopologyEntity} was created by a reservation.
     *
     * @return True if created by a reservation, false otherwise.
     */
    public boolean hasReservationOrigin() {
        return entityBuilder.hasOrigin() && entityBuilder.getOrigin().hasReservationOrigin();
    }

    /**
     * Check if this {@link TopologyEntity} was added as part of a plan scenario,
     * such as an entity added by an "add workload" config, or headroom vm's added
     * for cluster headroom calculation.
     *
     * @return True if added as part of a plan scenario, false otherwise.
     */
    public boolean hasPlanOrigin() {
        return entityBuilder.hasOrigin() && entityBuilder.getOrigin().hasPlanScenarioOrigin();
    }

    /**
     * Get the {@link Origin} for this entity. The Origin tracks where and how an entity came to be
     * included in the topology.
     *
     * @return The origin for this entity, or if not present, returns {@link Optional#empty()}.
     */
    public Optional<Origin> getOrigin() {
        return entityBuilder.hasOrigin() ? Optional.of(entityBuilder.getOrigin()) : Optional.empty();
    }

    /**
     * Get the {@link DiscoveryOrigin} for this entity. The DiscoveryOrigin tracks information about which
     * targets discovered the entity and when it was last updated.
     *
     * @return The {@link DiscoveryOrigin} for this entity, or if the entity does not have a discovery origin,
     * returns {@link Optional#empty()}.
     */
    public Optional<DiscoveryOrigin> getDiscoveryOrigin() {
        return hasDiscoveryOrigin() ?
            Optional.of(entityBuilder.getOrigin().getDiscoveryOrigin()) :
            Optional.empty();
    }

    @Override
    public String toString() {
        return "(oid-" + getOid() + ", " + getJournalableEntityType() + ", " + getDisplayName() + ")";
    }

    /**
     * Create a new builder for constructing a {@link TopologyEntity}.
     *
     * @param entityBuilder The builder for the {@link TopologyEntityDTO.Builder} that the
     *                      {@link TopologyEntity} will wrap.
     * @return a new builder for constructing a {@link TopologyEntity}.
     */
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return new Builder(entityBuilder);
    }

    /**
     * A builder for constructing a {@link TopologyEntity}.
     * See {@link TopologyEntity} for further details.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, TopologyEntity> {
        /**
         * Consumers and providers are ArrayLists so that {@link ArrayList#trimToSize()} can be called
         * upon building the {@link TopologyEntity}.
         */
        private final ArrayList<TopologyEntity> consumers;
        private final ArrayList<TopologyEntity> providers;
        private final ArrayList<TopologyEntity> outboundAssociatedEntities;
        private final ArrayList<TopologyEntity> inboundAssociatedEntities;
        private AtomicReference<TopologyEntity> owner = new AtomicReference<>();
        private final ArrayList<TopologyEntity> ownedEntities;
        private final ArrayList<TopologyEntity> aggregators;
        private final ArrayList<TopologyEntity> aggregatedEntities;

        private final TopologyEntity associatedTopologyEntity;

        private Builder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
            this.consumers = new ArrayList<>();
            this.providers = new ArrayList<>();
            this.outboundAssociatedEntities = new ArrayList<>();
            this.inboundAssociatedEntities = new ArrayList<>();
            this.ownedEntities = new ArrayList<>();
            this.aggregators = new ArrayList<>();
            this.aggregatedEntities = new ArrayList<>();
            this.associatedTopologyEntity =
                new TopologyEntity(entityBuilder, this.consumers, this.providers,
                                   this.outboundAssociatedEntities, this.inboundAssociatedEntities,
                                   this.owner, this.ownedEntities,
                                   this.aggregators, this.aggregatedEntities);
        }

        private Builder(@Nonnull final TopologyEntity.Builder topoEntityBuilder) {
            this(topoEntityBuilder.getEntityBuilder().clone());
            this.consumers.addAll(topoEntityBuilder.consumers);
            this.providers.addAll(topoEntityBuilder.providers);
            this.outboundAssociatedEntities.addAll(topoEntityBuilder.outboundAssociatedEntities);
            this.inboundAssociatedEntities.addAll(topoEntityBuilder.inboundAssociatedEntities);
            this.owner.set(owner.get());
            this.ownedEntities.addAll(topoEntityBuilder.ownedEntities);
            this.aggregators.addAll(topoEntityBuilder.aggregators);
            this.aggregatedEntities.addAll(topoEntityBuilder.aggregatedEntities);
        }

        @Override
        public Builder addConsumer(@Nonnull final TopologyEntity.Builder consumer) {
            consumers.add(consumer.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addProvider(@Nonnull final TopologyEntity.Builder provider) {
            providers.add(provider.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addInboundAssociation(@Nonnull final TopologyEntity.Builder connectedFromEntity) {
            this.inboundAssociatedEntities.add(connectedFromEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addOutboundAssociation(@Nonnull final TopologyEntity.Builder connectedToEntity) {
            this.outboundAssociatedEntities.add(connectedToEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addOwner(@Nonnull final TopologyEntity.Builder owner) {
            if (this.owner.get() != null) {
                throw new IllegalStateException("Adding multiple owners to entity " + this);
            }
            this.owner.set(owner.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addOwnedEntity(@Nonnull final TopologyEntity.Builder ownedEntity) {
            this.ownedEntities.add(ownedEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addAggregator(@Nonnull final TopologyEntity.Builder aggregator) {
            this.aggregators.add(aggregator.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addAggregatedEntity(@Nonnull final TopologyEntity.Builder aggregatedEntity) {
            this.aggregatedEntities.add(aggregatedEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public void clearConsumersAndProviders() {
            consumers.clear();
            providers.clear();
            outboundAssociatedEntities.clear();
            inboundAssociatedEntities.clear();
            owner.set(null);
            ownedEntities.clear();
            aggregators.clear();
            aggregatedEntities.clear();
        }

        public Builder setClonedFromEntityOid(final long clonedFromEntityOid) {
            associatedTopologyEntity.setClonedFromEntityOid(clonedFromEntityOid);
            return this;
        }

        @Nonnull
        @Override
        public Set<Long> getProviderIds() {
            return TopologyGraphEntity.Builder.extractProviderIds(
                associatedTopologyEntity.getTopologyEntityDtoBuilder());
        }

        @Nonnull
        @Override
        public Set<ConnectedEntity> getConnectionIds() {
            return TopologyGraphEntity.Builder.extractConnectionIds(
                associatedTopologyEntity.getTopologyEntityDtoBuilder());
        }

        @Override
        public long getOid() {
            return associatedTopologyEntity.getOid();
        }

        public int getEntityType() {
            return associatedTopologyEntity.getEntityType();
        }

        public String getDisplayName() {
            return associatedTopologyEntity.getDisplayName();
        }

        public TopologyEntityDTO.Builder getEntityBuilder() {
            return associatedTopologyEntity.getTopologyEntityDtoBuilder();
        }

        public ArrayList<TopologyEntity> getConsumers() {
            return consumers;
        }

        /**
         * Create a deep copy of this {@link TopologyEntity.Builder}.
         *
         * @return a copy of this Builder
         */
        public TopologyEntity.Builder snapshot() {
            return new TopologyEntity.Builder(this);
        }

        @Override
        public TopologyEntity build() {
            // Trim the arrays to their capacity to reduce memory consumption since
            // the consumers and providers will not be modified after the entity has been built.
            consumers.trimToSize();
            providers.trimToSize();
            outboundAssociatedEntities.trimToSize();
            inboundAssociatedEntities.trimToSize();
            ownedEntities.trimToSize();
            aggregators.trimToSize();
            aggregatedEntities.trimToSize();

            return associatedTopologyEntity;
        }
    }
}