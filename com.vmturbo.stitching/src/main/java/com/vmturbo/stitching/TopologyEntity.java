package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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

    private final List<TopologyEntity> connectedFromEntities;

    private final List<TopologyEntity> connectedToEntities;

    private TopologyEntity(@Nonnull final TopologyEntityDTO.Builder entity,
                           @Nonnull final List<TopologyEntity> consumers,
                           @Nonnull final List<TopologyEntity> providers,
                           @Nonnull final List<TopologyEntity> connectedFromEntities,
                           @Nonnull final List<TopologyEntity> connectedToEntities) {
        this.entityBuilder = Objects.requireNonNull(entity);
        this.consumers = consumers;
        this.providers = providers;
        this.connectedFromEntities = connectedFromEntities;
        this.connectedToEntities = connectedToEntities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getOid() {
        return entityBuilder.getOid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEntityType() {
        return entityBuilder.getEntityType();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public EntityState getEntityState() {
        return entityBuilder.getEntityState();
    }

    /**
     * {@inheritDoc}
     */
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

        // Copy connectedFromEntities
        final List<TopologyEntity> newConnectedFromEntities = new ArrayList<>(connectedFromEntities.size());
        newProviders.addAll(connectedFromEntities);

        // Copy connectedToEntities
        final List<TopologyEntity> newConnectedToEntities = new ArrayList<>(connectedToEntities.size());
        newProviders.addAll(connectedToEntities);

        // Create and return the copy
        return new TopologyEntity(entityBuilder.clone(), newConsumers, newProviders,
                newConnectedFromEntities, newConnectedToEntities);
    }

    /**
     * {@inheritDoc}
     */
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
            ? entityBuilder.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList().stream()
            : Stream.empty();
    }

    @Nonnull
    @Override
    public Map<Integer, CommoditySoldDTO> soldCommoditiesByType() {
        return entityBuilder.getCommoditySoldListList().stream()
            .collect(Collectors.toMap(commSold -> commSold.getCommodityType().getType(), Function.identity()));
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

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntity> getConsumers() {
        return Collections.unmodifiableList(consumers);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntity> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntity> getConnectedFromEntities() {
        return Collections.unmodifiableList(connectedFromEntities);
    }

    @Nonnull
    @Override
    public List<TopologyEntity> getConnectedToEntities() {
        return Collections.unmodifiableList(connectedToEntities);
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
     * Get the {@Link Origin} for this entity. The Origin tracks where and how an entity came to be
     * included in the topology.
     *
     * @return The origin for this entity, or if not present, returns {@link Optional#empty()}.
     */
    public Optional<Origin> getOrigin() {
        return entityBuilder.hasOrigin() ? Optional.of(entityBuilder.getOrigin()) : Optional.empty();
    }

    /**
     * Get the {@Link DiscoveryOrigin} for this entity. The DiscoveryOrigin tracks information about which
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
        private final ArrayList<TopologyEntity> connectedFromEntities;
        private final ArrayList<TopologyEntity> connectedToEntities;

        private final TopologyEntity associatedTopologyEntity;

        private Builder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
            this.consumers = new ArrayList<>();
            this.providers = new ArrayList<>();
            this.connectedFromEntities = new ArrayList<>();
            this.connectedToEntities = new ArrayList<>();
            this.associatedTopologyEntity = new TopologyEntity(entityBuilder, this.consumers,
                    this.providers, this.connectedFromEntities, this.connectedToEntities);
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
        public Builder addConnectedFrom(@Nonnull final TopologyEntity.Builder connectedFromEntity) {
            this.connectedFromEntities.add(connectedFromEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public Builder addConnectedTo(@Nonnull final TopologyEntity.Builder connectedToEntity) {
            this.connectedToEntities.add(connectedToEntity.associatedTopologyEntity);
            return this;
        }

        @Override
        public void clearConsumersAndProviders() {
            consumers.clear();
            providers.clear();
            connectedFromEntities.clear();
            connectedToEntities.clear();
        }

        @Nonnull
        @Override
        public Set<Long> getProviderIds() {
            return TopologyGraphEntity.Builder.extractProviderIds(
                associatedTopologyEntity.getTopologyEntityDtoBuilder());
        }

        @Nonnull
        @Override
        public Set<Long> getConnectionIds() {
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

        @Override
        public TopologyEntity build() {
            // Trim the arrays to their capacity to reduce memory consumption since
            // the consumers and providers will not be modified after the entity has been built.
            consumers.trimToSize();
            providers.trimToSize();

            return associatedTopologyEntity;
        }
    }
}