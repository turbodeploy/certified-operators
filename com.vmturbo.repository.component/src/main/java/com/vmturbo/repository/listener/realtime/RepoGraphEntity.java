package com.vmturbo.repository.listener.realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * {@link TopologyGraphEntity} for use in the repository. The intention is to make it as small
 * as possible while supporting all the searches and {@link com.vmturbo.common.protobuf.search.SearchableProperties}.
 */
public class RepoGraphEntity implements TopologyGraphEntity<RepoGraphEntity> {
    private static final Logger logger = LogManager.getLogger();

    private final long oid;

    private final String displayName;

    private final TypeSpecificInfo typeSpecificInfo;

    private final int type;

    private final EnvironmentType environmentType;

    private EntityState state;
    private final Map<String, List<String>> tags;
    private final Map<Integer, List<CommoditySoldDTO>> soldCommodities;
    private final Map<Long, PerTargetEntityInformation> discoveredTargetData;

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
    private List<RepoGraphEntity> providers = Collections.emptyList();
    private List<RepoGraphEntity> consumers = Collections.emptyList();

    /**
     * The full {@link TopologyEntityDTO}, compressed in case we want to retrieve it.
     */
    private final byte[] compressedEntityBytes;

    /**
     * The length of the uncompressed {@link TopologyEntityDTO} - needed for faster lz4 decompression.
     */
    private final int uncompressedLength;

    private RepoGraphEntity(@Nonnull final TopologyEntityDTO src,
                            @Nonnull final SharedByteBuffer sharedCompressionBuffer) {
        this.oid = src.getOid();
        this.displayName = src.getDisplayName();
        this.typeSpecificInfo = src.getTypeSpecificInfo();
        this.type = src.getEntityType();
        this.environmentType = src.getEnvironmentType();
        this.state = src.getEntityState();

        // Will be an empty list if the entity was not discovered.
        this.discoveredTargetData = ImmutableMap
                        .copyOf(src.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap());

        if (src.getTags().getTagsCount() > 0) {
            tags = new HashMap<>(src.getTags().getTagsCount());
            src.getTags().getTagsMap().forEach((tagName, tagValues) -> {
                ArrayList<String> vals = new ArrayList<>(tagValues.getValuesList());
                vals.trimToSize();
                tags.put(tagName, vals);
            });
        } else {
            tags = Collections.emptyMap();
        }

        final List<CommoditySoldDTO> commsToPersist = src.getCommoditySoldListList().stream()
            .filter(commSold -> {
                final int commType = commSold.getCommodityType().getType();
                return COMM_SOLD_TYPES_TO_PERSIST.contains(commType) ||
                    ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType);
            })
            .collect(Collectors.toList());
        if (commsToPersist.isEmpty()) {
            soldCommodities = Collections.emptyMap();
        } else {
            soldCommodities = new HashMap<>(commsToPersist.size());
            commsToPersist.forEach(comm -> soldCommodities.computeIfAbsent(comm.getCommodityType().getType(),
                    // Initialize to 1 because we typically expect just 1 sold commodity.
                    k -> new ArrayList<>(1))
                .add(comm));
            soldCommodities.values().forEach(commList ->
                ((ArrayList<CommoditySoldDTO>)commList).trimToSize());
        }

        // Use the fastest java instance to avoid using JNI & off-heap memory.
        // Note - for a 200k topology we saw the size
        final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();

        final byte[] uncompressedBytes = src.toByteArray();
        uncompressedLength = uncompressedBytes.length;
        final int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
        final byte[] compressionBuffer = sharedCompressionBuffer.getBuffer(maxCompressedLength);
        final int compressedLength = compressor.compress(uncompressedBytes, compressionBuffer);
        this.compressedEntityBytes = Arrays.copyOf(compressionBuffer, compressedLength);
    }

    @Nonnull
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO entity) {
        return new Builder(entity, new SharedByteBuffer());
    }

    @Nonnull
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO entity,
                                     @Nonnull final SharedByteBuffer sharedCompressionBuffer) {
        return new Builder(entity, sharedCompressionBuffer);
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
        return typeSpecificInfo;
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

    @Nonnull
    @Override
    public Map<Integer, List<CommoditySoldDTO>> soldCommoditiesByType() {
        return soldCommodities;
    }

    /**
     * Get the FULL topology entity, including all the commodities. This is the entity
     * that was received from the Topology Processor.
     */
    @Nonnull
    public TopologyEntityDTO getTopologyEntity() {
        // Use the fastest available java instance to avoid using off-heap memory.
        final LZ4FastDecompressor decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
        try {
            TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            bldr.mergeFrom(decompressor.decompress(compressedEntityBytes, uncompressedLength));
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
     * Get a topology entity containing the important (i.e. searchable) fields. This is smaller
     * than the entity returned by {@link RepoGraphEntity#getTopologyEntity()}.
     */
    @Nonnull
    private TopologyEntityDTO getPartialTopologyEntity() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityState(state)
            .setTypeSpecificInfo(typeSpecificInfo)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setEntityType(type);
        Tags.Builder tagsBuilder = Tags.newBuilder();
        tags.forEach((key, vals) ->
            tagsBuilder.putTags(key, TagValuesDTO.newBuilder()
                .addAllValues(vals)
                .build()));
        builder.setTags(tagsBuilder);

        if (!discoveredTargetData.isEmpty()) {
            builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putAllDiscoveredTargetData(discoveredTargetData)));
        }

        soldCommodities.values().forEach(builder::addAllCommoditySoldList);
        return builder.build();
    }

    @Nonnull
    @Override
    public Map<String, List<String>> getTags() {
        return tags;
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
    public Stream<Long> getDiscoveringTargetIds() {
        return discoveredTargetData.keySet().stream();
    }

    @Override
    public String getVendorId(long targetId) {
        return Optional.ofNullable(discoveredTargetData.get(targetId))
                        .filter(PerTargetEntityInformation::hasVendorId)
                        .map(PerTargetEntityInformation::getVendorId).orElse(null);
    }

    @Nonnull
    @Override
    public Stream<String> getAllVendorIds() {
        return discoveredTargetData.values().stream()
            .filter(PerTargetEntityInformation::hasVendorId)
            .map(PerTargetEntityInformation::getVendorId);
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
     * </ul>
     *
     * @return the list of related entities
     */
    public List<RelatedEntity> getBroadcastRelatedEntities() {
        final ArrayList<RelatedEntity> result = new ArrayList<>();
        Stream.concat(getOutboundAssociatedEntities().stream(),
                      Stream.concat(getOwnedEntities().stream(), getAggregators().stream()))
                .forEach(e -> result.add(RelatedEntity.newBuilder()
                                            .setEntityType(e.getEntityType())
                                            .setOid(e.getOid())
                                            .setDisplayName(e.getDisplayName())
                                            .build()));
        return result;
    }

    /**
     * Builder for {@link RepoGraphEntity}.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, RepoGraphEntity> {
        private final RepoGraphEntity repoGraphEntity;
        private final Set<Long> providerIds;
        private final Set<ConnectedEntity> connectedEntities;

        private Builder(@Nonnull final TopologyEntityDTO dto,
                        @Nonnull final SharedByteBuffer sharedByteBuffer) {
            this.providerIds = dto.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
            this.connectedEntities = dto.getConnectedEntityListList().stream()
                .collect(Collectors.toSet());
            this.repoGraphEntity = new RepoGraphEntity(dto, sharedByteBuffer);
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
    }

}
