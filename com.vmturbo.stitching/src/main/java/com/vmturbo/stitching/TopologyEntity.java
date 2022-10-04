package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyPOJO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.ThickSearchableProps;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * A wrapper around {@link TopologyEntityImpl}. Properties of the entity such as commodity values
 * may be updated, but relationships to other entities in the topology cannot be changed.
 * <p/>
 * {@link TopologyEntity}s are equivalent when their OIDs are equal. It is an error to create two different
 * {@link TopologyEntity}s with the same OIDs and behavior is undefined when this is done.
 * <p/>
 * The TopologyEntityImpl within a TopologyEntity may be edited but the TopologyEntity is immutable otherwise.
 */
public class TopologyEntity implements TopologyGraphSearchableEntity<TopologyEntity>, JournalableEntity<TopologyEntity> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A {@link TopologyEntityImpl} for the entity in the topology corresponding to this TopologyEntity.
     */
    private final TopologyEntityImpl entityImpl;

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
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType#NORMAL_CONNECTION}),
     * aggregations, ownerships, controls. Note that there can only be one owner.
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
    private final List<TopologyEntity> controllers;
    private final List<TopologyEntity> controlledEntities;
    private TopologyEntityImpl clonedFromEntity;

    private TopologyEntity(@Nonnull final TopologyEntityImpl entity,
                       @Nonnull final List<TopologyEntity> consumers,
                       @Nonnull final List<TopologyEntity> providers,
                       @Nonnull final List<TopologyEntity> outboundAssociatedEntities,
                       @Nonnull final List<TopologyEntity> inboundAssociatedEntities,
                       @Nonnull final AtomicReference<TopologyEntity> owner,
                       @Nonnull final List<TopologyEntity> ownedEntities,
                       @Nonnull final List<TopologyEntity> aggregators,
                       @Nonnull final List<TopologyEntity> aggregatedEntities,
                       @Nonnull final List<TopologyEntity> controllers,
                       @Nonnull final List<TopologyEntity> controlledEntities) {
        this.entityImpl = Objects.requireNonNull(entity);
        this.consumers = consumers;
        this.providers = providers;
        this.inboundAssociatedEntities = inboundAssociatedEntities;
        this.outboundAssociatedEntities = outboundAssociatedEntities;
        this.owner = owner;
        this.ownedEntities = ownedEntities;
        this.aggregators = aggregators;
        this.aggregatedEntities = aggregatedEntities;
        this.controllers = controllers;
        this.controlledEntities = controlledEntities;
    }

    @Override
    public long getOid() {
        return entityImpl.getOid();
    }

    @Override
    public int getEntityType() {
        return entityImpl.getEntityType();
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return entityImpl.getEntityState();
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return entityImpl.getEnvironmentType();
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

        // Copy owned entities, aggregators, aggregated entities, controllers, controlledEntities
        final List<TopologyEntity> newOwnedEntities = new ArrayList<>(ownedEntities.size());
        newOwnedEntities.addAll(ownedEntities);
        final List<TopologyEntity> newAggregators = new ArrayList<>(aggregators.size());
        newAggregators.addAll(aggregators);
        final List<TopologyEntity> newAggregatedEntities = new ArrayList<>(aggregatedEntities.size());
        newAggregatedEntities.addAll(aggregatedEntities);
        final List<TopologyEntity> newControllers = new ArrayList<>(controllers.size());
        newControllers.addAll(controllers);
        final List<TopologyEntity> newControlledEntities = new ArrayList<>(controlledEntities.size());
        newControlledEntities.addAll(controlledEntities);

        // Create and return the copy
        return new TopologyEntity(
                entityImpl.copy(), newConsumers, newProviders,
                newConnectedToAssociatedEntities, newConnectedFromAssociatedEntities,
                owner, newOwnedEntities, newAggregators, newAggregatedEntities, newControllers, newControlledEntities);
    }

    @Nonnull
    @Override
    public String getDisplayName() {
        return entityImpl.getDisplayName();
    }

    @Nonnull
    public TypeSpecificInfoView getTypeSpecificInfo() {
        return entityImpl.getTypeSpecificInfo();
    }

    @Nonnull
    public SearchableProps getSearchableProps() {
        return ThickSearchableProps.newProps(entityImpl);
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return hasDiscoveryOrigin()
            ? entityImpl.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet().stream()
            : Stream.empty();
    }

    /**
     * Get the discovering targets of the entity. If the entity is cloned in a plan topology,
     * get the discovering targets of the original entity.
     *
     * @return the discovering targets of the entity
     */
    @Nonnull
    public List<Long> getTargetIds() {
        if (hasDiscoveryOrigin()) {
            return getDiscoveringTargetIds().collect(Collectors.toList());
        }
        if (hasPlanOrigin()) {
            return entityImpl.getOrigin().getPlanScenarioOrigin()
                    .getOriginalEntityDiscoveringTargetIdsList();
        }
        return Collections.emptyList();
    }

    @Override
    public String getVendorId(long targetId) {
        if (hasDiscoveryOrigin()) {
            PerTargetEntityInformationView info = entityImpl.getOrigin().getDiscoveryOrigin()
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
        return hasDiscoveryOrigin()
            ? entityImpl.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().values()
                .stream()
                .filter(PerTargetEntityInformationView::hasVendorId)
                .map(PerTargetEntityInformationView::getVendorId)
            : Stream.empty();
    }

    /**
     * The sold commodities by type.
     *
     * @return sold commodities by type.
     */
    public Map<Integer, List<TopologyPOJO.CommoditySoldView>> soldCommoditiesByType() {
        return entityImpl.getCommoditySoldListList().stream()
                .collect(Collectors.groupingBy(commSold -> commSold.getCommodityType().getType()));
    }

    /**
     * Get a modifiable {@link TopologyEntityImpl} for the entity associated with this TopologyEntity.
     * The builder may be mutated to modify the properties of the entity.
     * <p/>
     * DO NOT modify the providers or consumers of the entity or the graph will be invalidated. This
     * means that commodities bought and sold by entities should not be modified in a way that they change
     * which entities are buying or selling commodities to each other. Properties of commodities such as
     * used or capacity values that don't change which entities are involved in the buying or selling may
     * be modified without issue.
     *
     * @return The property information for the entity associated with this TopologyEntity.
     */
    @Nonnull
    public TopologyEntityImpl getTopologyEntityImpl() {
        return entityImpl;
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

    @Nonnull
    @Override
    public List<TopologyEntity> getControllers() {
        return Collections.unmodifiableList(controllers);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getControlledEntities() {
        return Collections.unmodifiableList(controlledEntities);
    }

    /**
     * Original entity from which current entity was cloned from.
     * @return original entity
     */
    public Optional<TopologyEntityImpl> getClonedFromEntity() {
        return Optional.ofNullable(clonedFromEntity);
    }

    private void setClonedFromEntity(@Nonnull final TopologyEntityImpl clonedFromEntity) {
        this.clonedFromEntity = clonedFromEntity;
    }

    /**
     * Check if this {@link TopologyEntity} was discovered by a probe.
     *
     * @return True if discovered by a probe, false otherwise.
     */
    public boolean hasDiscoveryOrigin() {
        return entityImpl.hasOrigin() && entityImpl.getOrigin().hasDiscoveryOrigin();
    }

    /**
     * Check if this {@link TopologyEntity} was created by a reservation.
     *
     * @return True if created by a reservation, false otherwise.
     */
    public boolean hasReservationOrigin() {
        return entityImpl.hasOrigin() && entityImpl.getOrigin().hasReservationOrigin();
    }

    /**
     * Check if this {@link TopologyEntity} was added as part of a plan scenario,
     * such as an entity added by an "add workload" config, or headroom vm's added
     * for cluster headroom calculation.
     *
     * @return True if added as part of a plan scenario, false otherwise.
     */
    public boolean hasPlanOrigin() {
        return entityImpl.hasOrigin() && entityImpl.getOrigin().hasPlanScenarioOrigin();
    }

    /**
     * Get the {@link OriginView} for this entity. The Origin tracks where and how an entity came to be
     * included in the topology.
     *
     * @return The origin for this entity, or if not present, returns {@link Optional#empty()}.
     */
    public Optional<OriginView> getOrigin() {
        return entityImpl.hasOrigin() ? Optional.of(entityImpl.getOrigin()) : Optional.empty();
    }

    /**
     * Get the {@link DiscoveryOriginImpl} for this entity. The DiscoveryOrigin tracks information about which
     * targets discovered the entity and when it was last updated.
     *
     * @return The {@link DiscoveryOriginImpl} for this entity, or if the entity does not have a discovery origin,
     * returns {@link Optional#empty()}.
     */
    public Optional<DiscoveryOriginImpl> getDiscoveryOrigin() {
        return hasDiscoveryOrigin()
            ? Optional.of(entityImpl.getOrCreateOrigin().getOrCreateDiscoveryOrigin())
            : Optional.empty();
    }

    /**
     * Get the Stream of 'used' bought commodities with the specific type from consumers.
     *
     * @param type commodity type.
     * @return list of used values.
     */
    public Stream<Double> getCommoditiesUsedByConsumers(CommodityTypeView type) {
        return getConsumers().stream()
                .map(TopologyEntity::getTopologyEntityImpl)
                // The "shopping lists"
                .map(TopologyEntityImpl::getCommoditiesBoughtFromProvidersList)
                .flatMap(List::stream)
                // Those buying from the seller
                .filter(commsBought -> commsBought.getProviderId() == getOid())
                .map(CommoditiesBoughtFromProviderView::getCommodityBoughtList)
                .flatMap(List::stream) // All the commodities bought
                .filter(commBought -> type.equals(commBought.getCommodityType()))
                .filter(CommodityBoughtView::hasUsed)
                .map(CommodityBoughtView::getUsed);
    }

    /**
     * Get a stream of {@link CommodityBoughtImpl} with the specific type from consumers.
     *
     * @param commType commodity type
     * @return a stream of {@link CommodityBoughtImpl}
     */
    @Nonnull
    public Stream<CommodityBoughtImpl> getCommoditiesBoughtImplByConsumers(int commType) {
        return getConsumers().stream()
                .map(TopologyEntity::getTopologyEntityImpl)
                .map(TopologyEntityImpl::getCommoditiesBoughtFromProvidersImplList)
                .flatMap(List::stream)
                .filter(commBoughtFromProvider -> commBoughtFromProvider.getProviderId() == getOid())
                .map(CommoditiesBoughtFromProviderImpl::getCommodityBoughtImplList)
                .flatMap(List::stream)
                .filter(commBought -> commType == commBought.getCommodityType().getType());
    }

    @Override
    public String toString() {
        return "(oid-" + getOid() + ", " + getJournalableEntityType() + ", " + getDisplayName() + ")";
    }

    /**
     * Create a new builder for constructing a {@link TopologyEntity}.
     *
     * @param entityImpl The builder for the {@link TopologyEntityImpl} that the
     *                      {@link TopologyEntity} will wrap.
     * @return a new builder for constructing a {@link TopologyEntity}.
     */
    public static Builder newBuilder(@Nonnull final TopologyEntityImpl entityImpl) {
        return new Builder(entityImpl);
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
        private final AtomicReference<TopologyEntity> owner = new AtomicReference<>();
        private final ArrayList<TopologyEntity> ownedEntities;
        private final ArrayList<TopologyEntity> aggregators;
        private final ArrayList<TopologyEntity> aggregatedEntities;
        private final ArrayList<TopologyEntity> controllers;
        private final ArrayList<TopologyEntity> controlledEntities;
        private final TopologyEntity associatedTopologyEntity;

        private Builder(@Nonnull final TopologyEntityImpl entityImpl) {
            this.consumers = new ArrayList<>();
            this.providers = new ArrayList<>();
            this.outboundAssociatedEntities = new ArrayList<>();
            this.inboundAssociatedEntities = new ArrayList<>();
            this.ownedEntities = new ArrayList<>();
            this.aggregators = new ArrayList<>();
            this.aggregatedEntities = new ArrayList<>();
            this.controllers = new ArrayList<>();
            this.controlledEntities = new ArrayList<>();
            this.associatedTopologyEntity =
                new TopologyEntity(entityImpl, this.consumers, this.providers,
                                   this.outboundAssociatedEntities, this.inboundAssociatedEntities,
                                   this.owner, this.ownedEntities,
                                   this.aggregators, this.aggregatedEntities, this.controllers, this.controlledEntities);
        }

        private Builder(@Nonnull final TopologyEntity.Builder topoEntityBuilder) {
            this(topoEntityBuilder.getTopologyEntityImpl().copy());
            this.consumers.addAll(topoEntityBuilder.consumers);
            this.providers.addAll(topoEntityBuilder.providers);
            this.outboundAssociatedEntities.addAll(topoEntityBuilder.outboundAssociatedEntities);
            this.inboundAssociatedEntities.addAll(topoEntityBuilder.inboundAssociatedEntities);
            this.owner.set(owner.get());
            this.ownedEntities.addAll(topoEntityBuilder.ownedEntities);
            this.aggregators.addAll(topoEntityBuilder.aggregators);
            this.aggregatedEntities.addAll(topoEntityBuilder.aggregatedEntities);
            this.controllers.addAll(topoEntityBuilder.controllers);
            this.controlledEntities.addAll(topoEntityBuilder.controlledEntities);
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
                logger.warn("Attempt to assign multiple owners to entity {}. Keeping owner {};"
                        + " ignoring owner {}. This may happen intermittently when an entity moves"
                        + " between owners across 2 targets.", getOid(), this.owner.get().getOid(),
                        owner.getOid());
            } else {
                this.owner.set(owner.associatedTopologyEntity);
            }
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
        public Builder addController(@Nonnull final TopologyEntity.Builder controller) {
            this.controllers.add(controller.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addControlledEntity(@Nonnull final TopologyEntity.Builder controlledEntity) {
            this.controlledEntities.add(controlledEntity.associatedTopologyEntity);
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
            controllers.clear();
            controlledEntities.clear();
        }

        /**
         * Sets entity from which current entity was cloned.
         * @param clonedFromEntity original entity from which current entity was cloned
         * @return builder of current entity
         */
        public Builder setClonedFromEntity(@Nonnull final TopologyEntityImpl clonedFromEntity) {
            associatedTopologyEntity.setClonedFromEntity(clonedFromEntity);
            return this;
        }

        /**
         * Get the source entity of the current entity.
         *
         * @return the source entity of the current entity
         */
        @Nonnull
        public Optional<TopologyEntityImpl> getClonedFromEntity() {
            return associatedTopologyEntity.getClonedFromEntity();
        }

        @Nonnull
        @Override
        public Set<Long> getProviderIds() {
            return TopologyGraphEntity.Builder.extractProviderIds(
                associatedTopologyEntity.getTopologyEntityImpl());
        }

        @Nonnull
        @Override
        public Set<ConnectedEntity> getConnectionIds() {
            return TopologyGraphEntity.Builder.extractConnectionIds(
                associatedTopologyEntity.getTopologyEntityImpl());
        }

        @Override
        public long getOid() {
            return associatedTopologyEntity.getOid();
        }

        /**
         * Get the entity type.
         *
         * @return the entity type.
         */
        public int getEntityType() {
            return associatedTopologyEntity.getEntityType();
        }

        /**
         * Get the display name.
         *
         * @return the display name.
         */
        public String getDisplayName() {
            return associatedTopologyEntity.getDisplayName();
        }

        /**
         * Get the {@link TopologyEntityImpl}.
         *
         * @return the {@link TopologyEntityImpl}.
         */
        public TopologyEntityImpl getTopologyEntityImpl() {
            return associatedTopologyEntity.getTopologyEntityImpl();
        }

        /**
         * Get the list of consumers.
         *
         * @return the list of consumers.
         */
        public List<TopologyEntity> getConsumers() {
            return consumers;
        }

        /**
         * Get the list of providers.
         *
         * @return the list of providers
         */
        public List<TopologyEntity> getProviders() {
            return providers;
        }

        @Nonnull
        public List<TopologyEntity> getAggregatedEntities() {
            return aggregatedEntities;
        }

        @Nonnull
        public List<TopologyEntity> getOwnedEntities() {
            return ownedEntities;
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
            controllers.trimToSize();
            controlledEntities.trimToSize();
            return associatedTopologyEntity;
        }
    }
}
