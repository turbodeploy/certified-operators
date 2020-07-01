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
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.ThinSearchableProps.CommodityValueFetcher;

/**
 * An implementation of a {@link TopologyGraphSearchableEntity} used for tests in lieu of a complicated
 * mock object.
 */
public class TestGraphEntity implements TopologyGraphSearchableEntity<TestGraphEntity>, CommodityValueFetcher {
    private final long oid;

    private final ApiEntityType entityType;

    private EntityState state = EntityState.POWERED_ON;

    private String displayName;

    private SearchableProps searchableProps;

    private EnvironmentType envType = EnvironmentType.ON_PREM;

    private final Map<Long, String> discoveringTargetIds = new HashMap<>();

    private TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.getDefaultInstance();

    private final List<CommoditySoldDTO> commsSoldByType = new ArrayList<>();

    private final Map<String, List<String>> tags = new HashMap<>();

    private AnalysisSettings analysisSettings = AnalysisSettings.getDefaultInstance();

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
        refreshSearchableProps();
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

    @Nullable
    @Override
    public <T extends SearchableProps> T getSearchableProps(@Nonnull Class<T> clazz) {
        if (clazz.isInstance(searchableProps)) {
            return (T)searchableProps;
        } else {
            return null;
        }
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
    public static TopologyGraph<TestGraphEntity> newGraph(Long2ObjectMap<Builder> topology) {
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
        Long2ObjectMap<Builder> buildersById = new Long2ObjectOpenHashMap<>();
        for (Builder bldr : builders) {
            buildersById.put(bldr.getOid(), bldr);
        }

        return newGraph(buildersById);
    }

    @Override
    public float getCommodityCapacity(int type) {
        return (float)commsSoldByType.stream()
                .filter(comm -> comm.getCommodityType().getType() == type)
                .filter(CommoditySoldDTO::hasCapacity)
                .mapToDouble(CommoditySoldDTO::getCapacity)
                .findFirst()
                .orElse(-1);
    }

    @Override
    public float getCommodityUsed(final int type) {
        return (float)commsSoldByType.stream()
            .filter(comm -> comm.getCommodityType().getType() == type)
            .filter(CommoditySoldDTO::hasUsed)
            .mapToDouble(CommoditySoldDTO::getUsed)
            .findFirst()
            .orElse(-1);
    }

    private void refreshSearchableProps() {
        Tags.Builder t = Tags.newBuilder();
        this.tags.forEach((key, values) -> {
            t.putTags(key, TagValuesDTO.newBuilder()
                    .addAllValues(values)
                    .build());
        });
        TagIndex tags = DefaultTagIndex.singleEntity(getOid(), t.build());
        searchableProps = ThinSearchableProps.newProps(tags, this,
                TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName(displayName)
                        .setEntityType(entityType.typeNumber())
                        .setTypeSpecificInfo(typeSpecificInfo)
                        .setAnalysisSettings(analysisSettings)
                        .build());
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
            entity.commsSoldByType.add(commSold);
            return this;
        }

        public Builder setTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo) {
            entity.typeSpecificInfo = typeSpecificInfo;
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

        public Builder setDeletable(boolean deletable) {
            entity.analysisSettings = entity.analysisSettings.toBuilder()
                    .setDeletable(deletable)
                    .build();
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
            entity.refreshSearchableProps();
            return entity;
        }
    }

}
