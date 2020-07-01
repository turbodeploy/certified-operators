package com.vmturbo.repository.listener.realtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.api.CompressedProtobuf;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.ThinSearchableProps;
import com.vmturbo.topology.graph.ThinSearchableProps.CommodityValueFetcher;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * {@link TopologyGraphEntity} for use in the repository. The intention is to make it as small
 * as possible while supporting all the searches and {@link com.vmturbo.common.protobuf.search.SearchableProperties}.
 */
public class RepoGraphEntity implements TopologyGraphSearchableEntity<RepoGraphEntity>, CommodityValueFetcher {
    private static final Logger logger = LogManager.getLogger();

    private final long oid;

    private final String displayName;

    private final SearchableProps searchableProps;

    private final ActionEntityTypeSpecificInfo actionEntityInfo;

    private final int type;

    private final EnvironmentType environmentType;

    private EntityState state;
    private final List<DiscoveredTargetId> discoveredTargetData;

    private final List<SoldCommodity> soldCommodities;

    /**
     * These lists represent all connections of this entity:
     * outbound and inbound associations (which correspond
     * to connections of type
     * {@link TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType#NORMAL_CONNECTION}),
     * aggregations, and ownerships. Note that there can only be one owner.
     * The lists are initialized to {@link Collections#emptyList} and are only
     * assigned to {@link ArrayList}s when elements are inserted, to keep
     * the memory footprint small.
     */
    private List<RepoGraphEntity> outboundAssociatedEntities = Collections.emptyList();
    private List<RepoGraphEntity> inboundAssociatedEntities = Collections.emptyList();
    private RepoGraphEntity owner = null;
    private List<RepoGraphEntity> ownedEntities = Collections.emptyList();
    private List<RepoGraphEntity> aggregators = Collections.emptyList();
    private List<RepoGraphEntity> aggregatedEntities = Collections.emptyList();
    private List<RepoGraphEntity> controllers = Collections.emptyList();
    private List<RepoGraphEntity> controlledEntities = Collections.emptyList();
    private List<RepoGraphEntity> providers = Collections.emptyList();
    private List<RepoGraphEntity> consumers = Collections.emptyList();

    private final CompressedProtobuf<TopologyEntityDTO, TopologyEntityDTO.Builder> entity;

    private final DefaultTagIndex tags;

    private RepoGraphEntity(@Nonnull final TopologyEntityDTO src,
            @Nonnull final DefaultTagIndex tags,
            @Nonnull final SharedByteBuffer sharedCompressionBuffer) {
        this.oid = src.getOid();
        this.displayName = src.getDisplayName();
        this.type = src.getEntityType();
        this.environmentType = src.getEnvironmentType();
        this.state = src.getEntityState();
        this.tags = tags;
        this.actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(src.getTypeSpecificInfo())
            .map(ActionEntityTypeSpecificInfo.Builder::build)
            .orElse(null);

        // Will be an empty list if the entity was not discovered.
        if (src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().isEmpty()) {
            this.discoveredTargetData = Collections.emptyList();
        } else {
            this.discoveredTargetData = new ArrayList<>(src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataCount());
            src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().forEach((tId, info) -> {
                discoveredTargetData.add(new DiscoveredTargetId(tId, info.getVendorId()));
            });
            ((ArrayList<DiscoveredTargetId>)this.discoveredTargetData).trimToSize();
        }

        final List<CommoditySoldDTO> commsToPersist = src.getCommoditySoldListList().stream()
            .filter(commSold -> {
                final int commType = commSold.getCommodityType().getType();
                return SearchableProps.SEARCHABLE_COMM_TYPES.contains(commType)
                    || ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType);
            })
            .collect(Collectors.toList());
        if (commsToPersist.isEmpty()) {
            soldCommodities = Collections.emptyList();
        } else {
            soldCommodities = new ArrayList<>(commsToPersist.size());
            commsToPersist.forEach(comm -> soldCommodities.add(new SoldCommodity(comm)));
            ((ArrayList<SoldCommodity>)soldCommodities).trimToSize();
        }

        entity = CompressedProtobuf.compress(src.toBuilder()
            .build(), sharedCompressionBuffer);
        this.searchableProps = ThinSearchableProps.newProps(tags, this, src);
    }

    /**
     * Create a new entity builder. This method is only for testing.
     *
     * @param entity The underlying {@link TopologyEntityDTO}.
     * @return The {@link Builder}.
     */
    @Nonnull
    @VisibleForTesting
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO entity) {
        return newBuilder(entity, new DefaultTagIndex(), new SharedByteBuffer());
    }

    /**
     * Create a new entity builder.
     *
     * @param entity The underlying {@link TopologyEntityDTO}.
     * @param tags The {@link DefaultTagIndex} for the topology.
     * @param sharedCompressionBuffer Shared buffer to speed up compression/reduce allocation.
     * @return The {@link Builder}, which can be used for graph construction.
     */
    @Nonnull
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO entity,
            @Nonnull final DefaultTagIndex tags,
            @Nonnull final SharedByteBuffer sharedCompressionBuffer) {
        return new Builder(entity, tags, sharedCompressionBuffer);
    }

    private void addOutboundAssociation(@Nonnull final RepoGraphEntity entity) {
        if (this.outboundAssociatedEntities.isEmpty()) {
            this.outboundAssociatedEntities = new ArrayList<>(1);
        }
        this.outboundAssociatedEntities.add(entity);
    }

    private void addInboundAssociation(@Nonnull final RepoGraphEntity entity) {
        if (this.inboundAssociatedEntities.isEmpty()) {
            this.inboundAssociatedEntities = new ArrayList<>(1);
        }
        this.inboundAssociatedEntities.add(entity);
    }

    private void addOwner(@Nonnull final RepoGraphEntity entity) {
        if (this.owner != null) {
            throw new IllegalStateException("Tried to add multiple owners to entity " + this);
        }
        this.owner = entity;
    }

    private void addOwnedEntity(@Nonnull final RepoGraphEntity entity) {
        if (this.ownedEntities.isEmpty()) {
            this.ownedEntities = new ArrayList<>(1);
        }
        this.ownedEntities.add(entity);
    }

    private void addAggregator(@Nonnull final RepoGraphEntity entity) {
        if (this.aggregators.isEmpty()) {
            this.aggregators = new ArrayList<>(1);
        }
        this.aggregators.add(entity);
    }

    private void addAggregatedEntity(@Nonnull final RepoGraphEntity entity) {
        if (this.aggregatedEntities.isEmpty()) {
            this.aggregatedEntities = new ArrayList<>(1);
        }
        this.aggregatedEntities.add(entity);
    }

    private void addController(@Nonnull final RepoGraphEntity entity) {
        if (this.controllers.isEmpty()) {
            this.controllers = new ArrayList<>(1);
        }
        this.controllers.add(entity);
    }

    private void addControlledEntity(@Nonnull final RepoGraphEntity entity) {
        if (this.controlledEntities.isEmpty()) {
            this.controlledEntities = new ArrayList<>(1);
        }
        this.controlledEntities.add(entity);
    }

    private void addProvider(@Nonnull final RepoGraphEntity entity) {
        if (this.providers.isEmpty()) {
            this.providers = new ArrayList<>(1);
        }
        this.providers.add(entity);
    }

    private void addConsumer(@Nonnull RepoGraphEntity entity) {
        if (this.consumers.isEmpty()) {
            this.consumers = new ArrayList<>(1);
        }
        this.consumers.add(entity);
    }

    @Override
    public float getCommodityCapacity(int type) {
        return (float)soldCommodities.stream()
                .filter(comm -> comm.getType() == type)
                .mapToDouble(SoldCommodity::getCapacity)
                .findFirst().orElse(-1);
    }

    @Override
    public float getCommodityUsed(final int type) {
        return (float)soldCommodities.stream()
            .filter(comm -> comm.getType() == type)
            .mapToDouble(SoldCommodity::getUsed)
            .findFirst().orElse(-1);
    }

    /**
     * Look up sold commodities by type.
     * @param types The types we are looking for.
     * @return Stream of {@link SoldCommodity} objects of the type.
     */
    @Nonnull
    public Stream<SoldCommodity> soldCommoditiesByType(IntOpenHashSet types) {
        return soldCommodities.stream()
                .filter(comm -> types.contains(comm.getType()));
    }

    /**
     * Trim all the arrays containing references to related entities to size.
     * This can save several MB in a large topology!
     */
    private void trimToSize() {
        if (!consumers.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)consumers).trimToSize();
        }
        if (!providers.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)providers).trimToSize();
        }
        if (!outboundAssociatedEntities.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)outboundAssociatedEntities).trimToSize();
        }
        if (!inboundAssociatedEntities.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)inboundAssociatedEntities).trimToSize();
        }
        if (!ownedEntities.isEmpty()) {
            ((ArrayList)ownedEntities).trimToSize();
        }
        if (!aggregators.isEmpty()) {
            ((ArrayList)aggregators).trimToSize();
        }
        if (!aggregatedEntities.isEmpty()) {
            ((ArrayList)aggregatedEntities).trimToSize();
        }
        if (!controllers.isEmpty()) {
            ((ArrayList)controllers).trimToSize();
        }
        if (!controlledEntities.isEmpty()) {
            ((ArrayList)controlledEntities).trimToSize();
        }
    }

    private void clearConsumersAndProviders() {
        if (!consumers.isEmpty()) {
            consumers.clear();
        }
        if (!providers.isEmpty()) {
            providers.clear();
        }
        if (!outboundAssociatedEntities.isEmpty()) {
            outboundAssociatedEntities.clear();
        }
        if (!inboundAssociatedEntities.isEmpty()) {
            inboundAssociatedEntities.clear();
        }
        owner = null;
        if (!ownedEntities.isEmpty()) {
            ownedEntities.clear();
        }
        if (!aggregators.isEmpty()) {
            aggregators.clear();
        }
        if (!aggregatedEntities.isEmpty()) {
            aggregatedEntities.clear();
        }
        if (!controllers.isEmpty()) {
            controllers.clear();
        }
        if (!controlledEntities.isEmpty()) {
            controlledEntities.clear();
        }
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

    @Nullable
    @Override
    public <T extends SearchableProps> T getSearchableProps(@Nonnull Class<T> clazz) {
        if (clazz.isInstance(searchableProps)) {
            return (T)searchableProps;
        } else {
            return null;
        }
    }

    @Override
    public int getEntityType() {
        return type;
    }

    @Nonnull
    @Override
    public EnvironmentType getEnvironmentType() {
        return environmentType;
    }

    @Nonnull
    @Override
    public EntityState getEntityState() {
        return state;
    }

    @Nullable
    public ActionEntityTypeSpecificInfo getActionTypeSpecificInfo() {
        return actionEntityInfo;
    }


    /**
     * Get the FULL topology entity, including all the commodities. This is the entity
     * that was received from the Topology Processor.
     *
     * @return The full, decompressed {@link TopologyEntityDTO}.
     */
    @Nonnull
    public TopologyEntityDTO getTopologyEntity() {
        // Use the fastest available java instance to avoid using off-heap memory.
        try {
            TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            entity.decompressInto(bldr);
            bldr.setEntityState(state);
            return bldr.build();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to decompress entity {}. Error: {}", this.oid, e.getMessage());
            return getPartialTopologyEntity();
        }
    }

    /**
     * Update entity's state. This field corresponds to {@link TopologyEntityDTO#getEntityState}
     *
     * @param state the {@link EntityState} for the entity.
     */
    public synchronized void setEntityState(EntityState state) {
        this.state = state;
    }

    /**
     * Get the tags on this entity.
     *
     * @return The {@link Tags}.
     */
    @Nonnull
    public Tags getTags() {
        Tags.Builder tagsBuilder = Tags.newBuilder();
        tags.getTagsForEntity(oid).forEach((key, vals) ->
                tagsBuilder.putTags(key, TagValuesDTO.newBuilder()
                        .addAllValues(vals)
                        .build()));
        return tagsBuilder.build();
    }

    /**
     * Get a topology entity containing the important (i.e. searchable) fields. This is smaller
     * than the entity returned by {@link RepoGraphEntity#getTopologyEntity()}.
     *
     * @return The partial {@link TopologyEntityDTO}.
     */
    @Nonnull
    private TopologyEntityDTO getPartialTopologyEntity() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityState(state)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setEntityType(type);
        builder.setTags(getTags());

        if (!discoveredTargetData.isEmpty()) {
            DiscoveryOrigin.Builder originBldr = builder.getOriginBuilder().getDiscoveryOriginBuilder();
            discoveredTargetData.forEach(localId -> {
                originBldr.putDiscoveredTargetData(localId.targetId, PerTargetEntityInformation.newBuilder()
                        .setVendorId(localId.vendorId)
                        .build());
            });
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getConsumers() {
        return Collections.unmodifiableList(consumers);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<RepoGraphEntity> getInboundAssociatedEntities() {
        return Collections.unmodifiableList(inboundAssociatedEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<RepoGraphEntity> getOutboundAssociatedEntities() {
        return Collections.unmodifiableList(outboundAssociatedEntities);
    }

    @Nonnull
    @Override
    public Optional<RepoGraphEntity> getOwner() {
        return Optional.ofNullable(owner);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getOwnedEntities() {
        return Collections.unmodifiableList(ownedEntities);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getAggregators() {
        return Collections.unmodifiableList(aggregators);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getAggregatedEntities() {
        return Collections.unmodifiableList(aggregatedEntities);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getControllers() {
        return Collections.unmodifiableList(controllers);
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getControlledEntities() {
        return Collections.unmodifiableList(controlledEntities);
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return discoveredTargetData.stream()
                .map(discoveredTargetId -> discoveredTargetId.targetId);
    }

    @Override
    public String getVendorId(long targetId) {
        return discoveredTargetData.stream()
                .filter(discoveredTargetId -> discoveredTargetId.targetId == targetId)
                .map(discoveredTargetId -> discoveredTargetId.vendorId)
                .findFirst()
                .orElse(null);
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return discoveredTargetData.stream()
                .map(discoveredTargetId -> discoveredTargetId.vendorId);
    }

    @Override
    public String toString() {
        return displayName + "@" + oid;
    }

    /**
     * Returns the list of connections that are broadcast by the topology processor
     * for this entity.  The list includes the following:
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
    public List<ConnectedEntity> getBroadcastConnections() {
        final ArrayList<ConnectedEntity> result = new ArrayList<>();
        getOutboundAssociatedEntities()
            .forEach(e -> result.add(ConnectedEntity.newBuilder()
                                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                                        .setConnectedEntityType(e.getEntityType())
                                        .setConnectedEntityId(e.getOid())
                                        .build()));
        getOwnedEntities()
            .forEach(e -> result.add(ConnectedEntity.newBuilder()
                                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                                        .setConnectedEntityType(e.getEntityType())
                                        .setConnectedEntityId(e.getOid())
                                        .build()));
        getAggregators()
                .forEach(e -> result.add(ConnectedEntity.newBuilder()
                                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                        .setConnectedEntityType(e.getEntityType())
                                        .setConnectedEntityId(e.getOid())
                                        .build()));
        getControllers()
                .forEach(e -> result.add(ConnectedEntity.newBuilder()
                                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                                        .setConnectedEntityType(e.getEntityType())
                                        .setConnectedEntityId(e.getOid())
                                        .build()));
        return result;
    }

    /**
     * Returns a list of {@link RelatedEntity} objects, one for each
     * connection broadcast by the topology processor
     * for this entity.  The broadcast connections include the following:
     * <ul>
     *     <li>Normal outbound connections of the current entity</li>
     *     <li>Entities owned by the current entity</li>
     *     <li>Entities that aggregate the current entity</li>
     *     <li>Entities that controlled by the current entity</li>
     * </ul>
     *
     * @return the list of related entities
     */
    public List<RelatedEntity> getBroadcastRelatedEntities() {
        final ArrayList<RelatedEntity> result = new ArrayList<>();
        Stream.concat(getOutboundAssociatedEntities().stream(),
                      Stream.concat(getOwnedEntities().stream(), Stream.concat(getAggregators().stream(), getControllers().stream())))
                .forEach(e -> result.add(RelatedEntity.newBuilder()
                                            .setEntityType(e.getEntityType())
                                            .setOid(e.getOid())
                                            .setDisplayName(e.getDisplayName())
                                            .build()));
        return result;
    }

    /**
     * Tuple for the ID of this entity local to a particular discovering target.
     */
    private static class DiscoveredTargetId {
        private final long targetId;
        private final String vendorId;

        DiscoveredTargetId(long targetId, String vendorId) {
            this.targetId = targetId;
            this.vendorId = vendorId;
        }
    }

    /**
     * Thin class for commodities sold by this entity, containing only the fields we need.
     */
    public static class SoldCommodity {
        private final int type;
        private final String key;
        private final float capacity;
        private final float used;

        // Need this for AO.
        private final boolean supportsHotReplace;

        private SoldCommodity(@Nonnull final CommoditySoldDTO fromCmmSold) {
            this.type = fromCmmSold.getCommodityType().getType();
            this.key = fromCmmSold.getCommodityType().getKey().intern();
            capacity = (float)fromCmmSold.getCapacity();
            used = (float)fromCmmSold.getUsed();
            supportsHotReplace = fromCmmSold.getHotResizeInfo().getHotReplaceSupported();
        }

        public int getType() {
            return type;
        }

        public float getUsed() {
            return used;

        }

        public float getCapacity() {
            return capacity;
        }

        public boolean isSupportsHotReplace() {
            return supportsHotReplace;
        }

        public String getKey() {
            return key;
        }
    }

    /**
     * Builder for {@link RepoGraphEntity}.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, RepoGraphEntity> {
        private final RepoGraphEntity repoGraphEntity;
        private final Set<Long> providerIds;
        private final Set<ConnectedEntity> connectedEntities;

        private Builder(@Nonnull final TopologyEntityDTO dto,
                @Nonnull final DefaultTagIndex tags,
                @Nonnull final SharedByteBuffer sharedByteBuffer) {
            this.providerIds = dto.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
            this.connectedEntities = dto.getConnectedEntityListList().stream()
                .collect(Collectors.toSet());
            this.repoGraphEntity = new RepoGraphEntity(dto, tags, sharedByteBuffer);
        }

        @Override
        public void clearConsumersAndProviders() {
            repoGraphEntity.clearConsumersAndProviders();
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
            return repoGraphEntity.oid;
        }

        @Override
        public RepoGraphEntity build() {
            repoGraphEntity.trimToSize();
            return repoGraphEntity;
        }

        @Override
        public Builder addInboundAssociation(final Builder connectedFrom) {
            repoGraphEntity.addInboundAssociation(connectedFrom.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addOutboundAssociation(final Builder connectedTo) {
            repoGraphEntity.addOutboundAssociation(connectedTo.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addProvider(final Builder provider) {
            repoGraphEntity.addProvider(provider.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addConsumer(final Builder consumer) {
            repoGraphEntity.addConsumer(consumer.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addOwner(final Builder owner) {
            repoGraphEntity.addOwner(owner.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addOwnedEntity(final Builder ownedEntity) {
            repoGraphEntity.addOwnedEntity(ownedEntity.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addAggregator(final Builder aggregator) {
            repoGraphEntity.addAggregator(aggregator.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addAggregatedEntity(final Builder aggregatedEntity) {
            repoGraphEntity.addAggregatedEntity(aggregatedEntity.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addController(final Builder controller) {
            repoGraphEntity.addController(controller.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addControlledEntity(final Builder controlledEntity) {
            repoGraphEntity.addControlledEntity(controlledEntity.repoGraphEntity);
            return this;
        }
    }

}
