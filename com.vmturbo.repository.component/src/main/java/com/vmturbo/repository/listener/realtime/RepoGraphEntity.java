package com.vmturbo.repository.listener.realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
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

    private final EntityState state;
    private final Map<String, List<String>> tags;
    private final Map<Integer, CommoditySoldDTO> soldCommodities;
    private final List<Long> discoveringTargetIds;

    private List<RepoGraphEntity> connectedTo = Collections.emptyList();
    private List<RepoGraphEntity> connectedFrom = Collections.emptyList();
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
        if (src.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList().isEmpty()) {
            this.discoveringTargetIds = Collections.emptyList();
        } else {
            final ArrayList<Long> discoveringTargets = new ArrayList<>(src.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList());
            discoveringTargets.trimToSize();
            this.discoveringTargetIds = discoveringTargets;
        }

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
            commsToPersist.forEach(comm -> soldCommodities.put(comm.getCommodityType().getType(), comm));
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

    private void addConnectedTo(@Nonnull final RepoGraphEntity entity) {
        if (this.connectedTo.isEmpty()) {
            this.connectedTo = new ArrayList<>(1);
        }
        this.connectedTo.add(entity);
    }

    private void addConnectedFrom(@Nonnull final RepoGraphEntity entity) {
        if (this.connectedFrom.isEmpty()) {
            this.connectedFrom = new ArrayList<>(1);
        }
        this.connectedFrom.add(entity);
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
        if (!connectedTo.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)connectedTo).trimToSize();
        }
        if (!connectedFrom.isEmpty()) {
            ((ArrayList<RepoGraphEntity>)connectedFrom).trimToSize();
        }
    }

    private void clearConsumersAndProviders() {
        if (!consumers.isEmpty()) {
            consumers.clear();
        }
        if (!providers.isEmpty()) {
            providers.clear();
        }
        if (!connectedTo.isEmpty()) {
            connectedTo.clear();
        }
        if (!connectedFrom.isEmpty()) {
            connectedFrom.clear();
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
    public Map<Integer, CommoditySoldDTO> soldCommoditiesByType() {
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
            return TopologyEntityDTO.parseFrom(decompressor.decompress(compressedEntityBytes, uncompressedLength));
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to decompress entity {}. Error: {}", this.oid, e.getMessage());
            return getPartialTopologyEntity();
        }
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
        tags.forEach((key, vals) -> {
            tagsBuilder.putTags(key, TagValuesDTO.newBuilder()
                .addAllValues(vals)
                .build());
        });
        builder.setTags(tagsBuilder);

        if (!discoveringTargetIds.isEmpty()) {
            builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .addAllDiscoveringTargetIds(discoveringTargetIds)));
        }

        builder.addAllCommoditySoldList(soldCommodities.values());
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
        return consumers;
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getProviders() {
        return providers;
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getConnectedToEntities() {
        return connectedTo;
    }

    @Nonnull
    @Override
    public List<RepoGraphEntity> getConnectedFromEntities() {
        return connectedFrom;
    }

    @Nonnull
    @Override
    public Stream<Long> getDiscoveringTargetIds() {
        return discoveringTargetIds.stream();
    }


    /**
     * Builder for {@link RepoGraphEntity}.
     */
    public static class Builder implements TopologyGraphEntity.Builder<Builder, RepoGraphEntity> {
        private final RepoGraphEntity repoGraphEntity;
        private final Set<Long> providerIds;
        private final Set<Long> connectedIds;

        private Builder(@Nonnull final TopologyEntityDTO dto,
                        @Nonnull final SharedByteBuffer sharedByteBuffer) {
            this.providerIds = dto.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
            this.connectedIds = dto.getConnectedEntityListList().stream()
                .map(ConnectedEntity::getConnectedEntityId)
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
        public Set<Long> getConnectionIds() {
            return connectedIds;
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
        public Builder addConnectedFrom(final Builder connectedFrom) {
            repoGraphEntity.addConnectedFrom(connectedFrom.repoGraphEntity);
            return this;
        }

        @Override
        public Builder addConnectedTo(final Builder connectedTo) {
            repoGraphEntity.addConnectedTo(connectedTo.repoGraphEntity);
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
    }
}
