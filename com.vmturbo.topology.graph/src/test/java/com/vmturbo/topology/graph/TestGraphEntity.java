package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class TestGraphEntity implements TopologyGraphEntity<TestGraphEntity> {
    private final long oid;

    private final ApiEntityType entityType;

    private EntityState state = EntityState.POWERED_ON;

    private String displayName;

    private TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.getDefaultInstance();

    private EnvironmentType envType = EnvironmentType.ON_PREM;

    private final Map<Long, String> discoveringTargetIds = new HashMap<>();

    private final Map<Integer, List<CommoditySoldDTO>> commsSoldByType = new HashMap<>();

    private final Map<String, List<String>> tags = new HashMap<>();

    /**
     * Relationships to other entities.
     */
    private final List<TestGraphEntity> providers = new ArrayList<>();
    private final List<TestGraphEntity> consumers = new ArrayList<>();
    private final List<TestGraphEntity> outboundAssociatedEntities = new ArrayList<>();
    private final List<TestGraphEntity> inboundAssociatedEntities = new ArrayList<>();
    private final List<TestGraphEntity> aggregatedEntities = new ArrayList<>();
    private final List<TestGraphEntity> aggregators = new ArrayList<>();
    private final List<TestGraphEntity> ownedEntities = new ArrayList<>();
    private final AtomicReference<TestGraphEntity> owner = new AtomicReference<>();
    private final List<TestGraphEntity> controlledEntities = new ArrayList<>();
    private final List<TestGraphEntity> controllers = new ArrayList<>();

    private TestGraphEntity(final long oid, final ApiEntityType type) {
        this.oid = oid;
        this.entityType = type;
        this.displayName = Long.toString(oid);
    }

    @Override
    public long getOid() {
        return oid;
    }

    @Override
    public int getEntityType() {
        return entityType.typeNumber();
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return state;
    }

    @Nonnull
    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Nonnull
    @Override
    public TypeSpecificInfo getTypeSpecificInfo() {
        return typeSpecificInfo;
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return envType;
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return discoveringTargetIds.keySet().stream();
    }

    @Override
    public String getVendorId(long targetId) {
        return discoveringTargetIds.get(targetId);
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return discoveringTargetIds.values().stream();
    }

    @Nonnull
    @Override
    public Map<Integer, List<CommoditySoldDTO>> soldCommoditiesByType() {
        return commsSoldByType;
    }

    @Nonnull
    @Override
    public Map<String, List<String>> getTags() {
        return tags;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getConsumers() {
        return consumers;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getProviders() {
        return providers;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getOutboundAssociatedEntities() {
        return outboundAssociatedEntities;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getInboundAssociatedEntities() {
        return inboundAssociatedEntities;
    }

    @Nonnull
    @Override
    public Optional<TestGraphEntity> getOwner() {
        return Optional.ofNullable(owner.get());
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getOwnedEntities() {
        return ownedEntities;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getAggregators() {
        return aggregators;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getAggregatedEntities() {
        return aggregatedEntities;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getControllers() {
        return controllers;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getControlledEntities() {
        return controlledEntities;
    }

    /**
     * Create a new builder with a specific oid and a specific entity type.
     *
     * @param oid the oid of the new entity builder
     * @param entityType the entity type of the new entity builder
     * @return the new entity builder
     */
    @Nonnull
    public static Builder newBuilder(final long oid, final ApiEntityType entityType) {
        return new Builder(oid, entityType);
    }

    /**
     * Create a new topology graph.
     *
     * @param topology a map from oids to entity builders
     * @return the topology that is comprised of the given entities
     */
    @Nonnull
    public static TopologyGraph<TestGraphEntity> newGraph(Map<Long, Builder> topology) {
        return new TopologyGraphCreator<>(topology).build();
    }

    /**
     * Create a new topology graph.
     *
     * @param builders a collection of builders whose entities will comprise
     *                 the new topology
     * @return the topology that is comprised of the given entities
     */
    @Nonnull
    public static TopologyGraph<TestGraphEntity> newGraph(Builder... builders) {
        Map<Long, Builder> buildersById = new HashMap<>();
        for (Builder bldr : builders) {
            buildersById.put(bldr.getOid(), bldr);
        }

        return newGraph(buildersById);
    }

    /**
     * Builder class for topology graphs of {@link TestGraphEntity}s.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, TestGraphEntity> {
        private final TestGraphEntity entity;

        private final Set<ConnectedEntity> connectedEntities = new HashSet<>();
        private final Set<Long> providerIds = new HashSet<>();

        private Builder(final long oid, final ApiEntityType entityType) {
            this.entity = new TestGraphEntity(oid, entityType);
        }

        public Builder addConnectedEntity(long connectedEntityId, ConnectionType connectionType) {
            this.connectedEntities.add(ConnectedEntity.newBuilder()
                                            .setConnectedEntityId(connectedEntityId)
                                            .setConnectionType(connectionType)
                                            .build());
            return this;
        }

        public Builder addProviderId(final long providerId) {
            this.providerIds.add(providerId);
            return this;
        }

        public Builder addTag(final String key, final List<String> values) {
            entity.tags.put(key, values);
            return this;
        }

        public Builder addCommSold(final CommoditySoldDTO commSold) {
            entity.commsSoldByType.computeIfAbsent(commSold.getCommodityType().getType(),
                k -> new ArrayList<>()).add(commSold);
            return this;
        }

        public Builder setTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo) {
            entity.typeSpecificInfo  = typeSpecificInfo;
            return this;
        }

        public Builder setName(@Nonnull final String name) {
            entity.displayName = name;
            return this;
        }

        public Builder setState(@Nonnull final EntityState newState) {
            entity.state = newState;
            return this;
        }

        public Builder setEnvironmentType(@Nonnull final EnvironmentType envType) {
            entity.envType = envType;
            return this;
        }

        /**
         * Add an identity on a discovering target.
         *
         * @param target the id of the discovering target
         * @param localName vendor identity
         * @return {@code this} for chaining
         */
        public Builder addTargetIdentity(final long target, String localName) {
            entity.discoveringTargetIds.put(target, localName);
            return this;
        }

        @Override
        public void clearConsumersAndProviders() {
            entity.outboundAssociatedEntities.clear();
            entity.inboundAssociatedEntities.clear();
            entity.providers.clear();
            entity.consumers.clear();
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
            return entity.getOid();
        }

        @Override
        public Builder addConsumer(final Builder consumer) {
            entity.consumers.add(consumer.entity);
            return this;
        }

        @Override
        public Builder addProvider(final Builder provider) {
            entity.providers.add(provider.entity);
            return this;
        }

        @Override
        public Builder addOutboundAssociation(final Builder connectedTo) {
            entity.outboundAssociatedEntities.add(connectedTo.entity);
            return this;
        }

        @Override
        public Builder addInboundAssociation(final Builder connectedFrom) {
            entity.inboundAssociatedEntities.add(connectedFrom.entity);
            return this;
        }

        @Override
        public Builder addOwner(final Builder owner) {
            entity.owner.set(owner.entity);
            return this;
        }

        @Override
        public Builder addOwnedEntity(final Builder ownedEntity) {
            entity.ownedEntities.add(ownedEntity.entity);
            return this;
        }

        @Override
        public Builder addAggregator(final Builder aggregator) {
            entity.aggregators.add(aggregator.entity);
            return this;
        }

        @Override
        public Builder addAggregatedEntity(final Builder aggregatedEntity) {
            entity.aggregatedEntities.add(aggregatedEntity.entity);
            return this;
        }

        @Override
        public Builder addController(final Builder controller) {
            entity.controllers.add(controller.entity);
            return this;
        }

        @Override
        public Builder addControlledEntity(final Builder controlledEntity) {
            entity.controlledEntities.add(controlledEntity.entity);
            return this;
        }

        @Override
        public TestGraphEntity build() {
            return entity;
        }
    }

}
