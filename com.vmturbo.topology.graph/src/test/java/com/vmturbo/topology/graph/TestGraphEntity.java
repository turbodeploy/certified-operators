package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;

public class TestGraphEntity implements TopologyGraphEntity<TestGraphEntity> {
    private final long oid;

    private final UIEntityType entityType;

    private EntityState state = EntityState.POWERED_ON;

    private String displayName;

    private TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.getDefaultInstance();

    private EnvironmentType envType = EnvironmentType.ON_PREM;

    private final Set<Long> discoveringTargetIds = new HashSet<>();

    private final Map<Integer, CommoditySoldDTO> commsSoldByType = new HashMap<>();

    private final Map<String, List<String>> tags = new HashMap<>();

    private final List<TestGraphEntity> providers = new ArrayList<>();
    private final List<TestGraphEntity> consumers = new ArrayList<>();
    private final List<TestGraphEntity> connectedTo = new ArrayList<>();
    private final List<TestGraphEntity> connectedFrom = new ArrayList<>();

    private TestGraphEntity(final long oid, final UIEntityType type) {
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
        return discoveringTargetIds.stream();
    }

    @Nonnull
    @Override
    public Map<Integer, CommoditySoldDTO> soldCommoditiesByType() {
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
    public List<TestGraphEntity> getConnectedToEntities() {
        return connectedTo;
    }

    @Nonnull
    @Override
    public List<TestGraphEntity> getConnectedFromEntities() {
        return connectedFrom;
    }

    @Nonnull
    public static Builder newBuilder(final long oid, final UIEntityType entityType) {
        return new Builder(oid, entityType);
    }

    @Nonnull
    public static TopologyGraph<TestGraphEntity> newGraph(Map<Long, Builder> topology) {
        return new TopologyGraphCreator<>(topology).build();
    }

    @Nonnull
    public static TopologyGraph<TestGraphEntity> newGraph(Builder... builders) {
        Map<Long, Builder> buildersById = new HashMap<>();
        for (Builder bldr : builders) {
            buildersById.put(bldr.getOid(), bldr);
        }

        return newGraph(buildersById);
    }

    public static class Builder implements TopologyGraphEntity.Builder<Builder, TestGraphEntity> {
        private final TestGraphEntity entity;

        private final Set<Long> connectedIds = new HashSet<>();
        private final Set<Long> providerIds = new HashSet<>();

        private Builder(final long oid, final UIEntityType entityType) {
            this.entity = new TestGraphEntity(oid, entityType);
        }

        public Builder addConnectedId(final long consumerId) {
            this.connectedIds.add(consumerId);
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
            entity.commsSoldByType.put(commSold.getCommodityType().getType(), commSold);
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


        @Override
        public void clearConsumersAndProviders() {
            entity.connectedFrom.clear();
            entity.connectedTo.clear();
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
        public Set<Long> getConnectionIds() {
            return connectedIds;
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
        public Builder addConnectedTo(final Builder connectedTo) {
            entity.connectedTo.add(connectedTo.entity);
            return this;
        }

        @Override
        public Builder addConnectedFrom(final Builder connectedFrom) {
            entity.connectedFrom.add(connectedFrom.entity);
            return this;
        }

        @Override
        public TestGraphEntity build() {
            return entity;
        }
    }
}
