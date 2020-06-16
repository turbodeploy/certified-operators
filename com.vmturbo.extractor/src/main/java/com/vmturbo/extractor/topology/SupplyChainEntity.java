package com.vmturbo.extractor.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * {@link TopologyGraphEntity} for use in the repository. The intention is to make it as small
 * as possible while supporting all the searches and {@link com.vmturbo.common.protobuf.search.SearchableProperties}.
 */
public class SupplyChainEntity implements TopologyGraphEntity<SupplyChainEntity> {

    private final long oid;

    private final String displayName;

    private final int type;

    private final List<SupplyChainEntity> outboundAssociatedEntities = new ArrayList<>(0);
    private final List<SupplyChainEntity> inboundAssociatedEntities = new ArrayList<>(0);
    private SupplyChainEntity owner = null;
    private final List<SupplyChainEntity> ownedEntities = new ArrayList<>(0);
    private final List<SupplyChainEntity> aggregators = new ArrayList<>(0);
    private final List<SupplyChainEntity> aggregatedEntities = new ArrayList<>(0);
    private final List<SupplyChainEntity> controllers = new ArrayList<>(0);
    private final List<SupplyChainEntity> controlledEntities = new ArrayList<>(0);
    private final List<SupplyChainEntity> providers = new ArrayList<>(0);
    private final List<SupplyChainEntity> consumers = new ArrayList<>(0);
    private boolean deletable = true;

    public SupplyChainEntity(@Nonnull final TopologyEntityDTO src) {
        this.oid = src.getOid();
        this.displayName = src.getDisplayName();
        this.type = src.getEntityType();

        if (src.hasAnalysisSettings()) {
            this.deletable = src.getAnalysisSettings().getDeletable();
        }
    }

    public int getType() {
        return type;
    }

    /**
     * Create a new instance for a given {@link TopologyEntityDTO} entity.
     * @param entity topology entity
     * @return new {@link SupplyChainEntity}
     */
    @Nonnull
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO entity) {
        return new Builder(entity);
    }

    private void addOutboundAssociation(@Nonnull final SupplyChainEntity entity) {
        this.outboundAssociatedEntities.add(entity);
    }

    private void addInboundAssociation(@Nonnull final SupplyChainEntity entity) {
        this.inboundAssociatedEntities.add(entity);
    }

    private void addOwner(@Nonnull final SupplyChainEntity entity) {
        this.owner = entity;
    }

    private void addOwnedEntity(@Nonnull final SupplyChainEntity entity) {
        this.ownedEntities.add(entity);
    }

    private void addAggregator(@Nonnull final SupplyChainEntity entity) {
        this.aggregators.add(entity);
    }

    private void addAggregatedEntity(@Nonnull final SupplyChainEntity entity) {
        this.aggregatedEntities.add(entity);
    }

    private void addController(@Nonnull final SupplyChainEntity entity) {
        this.controllers.add(entity);
    }

    private void addControlledEntity(@Nonnull final SupplyChainEntity entity) {
        this.controlledEntities.add(entity);
    }

    private void addProvider(@Nonnull final SupplyChainEntity entity) {
        this.providers.add(entity);
    }

    private void addConsumer(@Nonnull SupplyChainEntity entity) {
        this.consumers.add(entity);
    }

    private void clearConsumersAndProviders() {
        providers.clear();
        outboundAssociatedEntities.clear();
        inboundAssociatedEntities.clear();
        owner = null;
        ownedEntities.clear();
        aggregators.clear();
        aggregatedEntities.clear();
        controllers.clear();
        controlledEntities.clear();
    }

    @Override
    public long getOid() {
        return oid;
    }

    @Nonnull
    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Nonnull
    @Override
    public TypeSpecificInfo getTypeSpecificInfo() {
        return TypeSpecificInfo.getDefaultInstance();
    }

    @Override
    public int getEntityType() {
        return type;
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return EnvironmentType.ON_PREM;
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return EntityState.POWERED_ON;
    }

    @Nonnull
    @Override
    public Map<Integer, List<CommoditySoldDTO>> soldCommoditiesByType() {
        return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<String, List<String>> getTags() {
        return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getConsumers() {
        return Collections.unmodifiableList(consumers);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<SupplyChainEntity> getInboundAssociatedEntities() {
        return Collections.unmodifiableList(inboundAssociatedEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<SupplyChainEntity> getOutboundAssociatedEntities() {
        return Collections.unmodifiableList(outboundAssociatedEntities);
    }

    @Nonnull
    @Override
    public Optional<SupplyChainEntity> getOwner() {
        return Optional.ofNullable(owner);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getOwnedEntities() {
        return Collections.unmodifiableList(ownedEntities);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getAggregators() {
        return Collections.unmodifiableList(aggregators);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getAggregatedEntities() {
        return Collections.unmodifiableList(aggregatedEntities);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getControllers() {
        return Collections.unmodifiableList(controllers);
    }

    @Nonnull
    @Override
    public List<SupplyChainEntity> getControlledEntities() {
        return Collections.unmodifiableList(controlledEntities);
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return Stream.empty();
    }

    @Override
    public String getVendorId(long targetId) {
        return "";
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return Stream.empty();
    }

    @Override
    public String toString() {
        return displayName + "@" + oid;
    }

    /**
     * Get deletable state of the topology entity. Default is true.
     *
     * @return true, means the Market can delete this entity.
     *         false, means Market will not generate Delete Actions.
     */
    @Override
    public boolean getDeletable() {
        return deletable;
    }

    /**
     * Builder for {@link SupplyChainEntity}.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, SupplyChainEntity> {
        private final SupplyChainEntity supplyChainEntity;
        private final Set<Long> providerIds;
        private final Set<ConnectedEntity> connectedEntities;

        private Builder(@Nonnull final TopologyEntityDTO dto) {
            this.providerIds = dto.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(CommoditiesBoughtFromProvider::hasProviderId)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .collect(Collectors.toSet());
            this.connectedEntities = new HashSet<>(dto.getConnectedEntityListList());
            this.supplyChainEntity = new SupplyChainEntity(dto);
        }

        @Override
        public void clearConsumersAndProviders() {
            supplyChainEntity.clearConsumersAndProviders();
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
            return supplyChainEntity.oid;
        }

        @Override
        public SupplyChainEntity build() {
            return supplyChainEntity;
        }

        @Override
        public Builder addInboundAssociation(final Builder connectedFrom) {
            supplyChainEntity.addInboundAssociation(connectedFrom.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addOutboundAssociation(final Builder connectedTo) {
            supplyChainEntity.addOutboundAssociation(connectedTo.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addProvider(final Builder provider) {
            supplyChainEntity.addProvider(provider.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addConsumer(final Builder consumer) {
            supplyChainEntity.addConsumer(consumer.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addOwner(final Builder owner) {
            supplyChainEntity.addOwner(owner.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addOwnedEntity(final Builder ownedEntity) {
            supplyChainEntity.addOwnedEntity(ownedEntity.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addAggregator(final Builder aggregator) {
            supplyChainEntity.addAggregator(aggregator.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addAggregatedEntity(final Builder aggregatedEntity) {
            supplyChainEntity.addAggregatedEntity(aggregatedEntity.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addController(final Builder controller) {
            supplyChainEntity.addController(controller.supplyChainEntity);
            return this;
        }

        @Override
        public Builder addControlledEntity(final Builder controlledEntity) {
            supplyChainEntity.addControlledEntity(controlledEntity.supplyChainEntity);
            return this;
        }
    }
}
