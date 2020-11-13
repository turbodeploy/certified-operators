package com.vmturbo.repository.listener.realtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * {@link TopologyGraphEntity} for use in the repository. The intention is to make it as small
 * as possible while supporting all the searches and {@link com.vmturbo.common.protobuf.search.SearchableProperties}.
 */
public class RepoGraphEntity extends BaseGraphEntity<RepoGraphEntity> implements CommodityValueFetcher,
        TopologyGraphSearchableEntity<RepoGraphEntity> {
    private static final Logger logger = LogManager.getLogger();

    private final SearchableProps searchableProps;

    private final List<SoldCommodity> soldCommodities;

    private final CompressedProtobuf<TopologyEntityDTO, TopologyEntityDTO.Builder> entity;

    private final ActionEntityTypeSpecificInfo actionEntityInfo;

    private RepoGraphEntity(@Nonnull final TopologyEntityDTO src,
            @Nonnull final DefaultTagIndex tags,
            @Nonnull final SharedByteBuffer sharedCompressionBuffer) {
        super(src);

        this.actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(src)
                .map(ActionEntityTypeSpecificInfo.Builder::build)
                .orElse(null);

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

    @Nullable
    public ActionEntityTypeSpecificInfo getActionTypeSpecificInfo() {
        return actionEntityInfo;
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

    @Override
    public boolean isHotAddSupported(int commodityType) {
        return isHotChangeSupported(commodityType, c -> c.supportsHotAdd);
    }

    @Override
    public boolean isHotRemoveSupported(int commodityType) {
        return isHotChangeSupported(commodityType, c -> c.supportsHotRemove);
    }

    private boolean isHotChangeSupported(int commodityType,
            Function<SoldCommodity, Boolean> function) {
        return soldCommoditiesByType(new IntOpenHashSet(Collections.singleton(commodityType))).map(
                function).findAny().orElse(false);
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

    @Override
    public SearchableProps getSearchableProps() {
        return searchableProps;
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
            bldr.setEntityState(getEntityState());
            return bldr.build();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to decompress entity {}. Error: {}", this.getOid(), e.getMessage());
            return getPartialTopologyEntity();
        }
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
            .setOid(getOid())
            .setDisplayName(getDisplayName())
            .setEntityState(getEntityState())
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setEntityType(getEntityType());

        if (!getDiscoveredTargetIds().isEmpty()) {
            DiscoveryOrigin.Builder originBldr = builder.getOriginBuilder().getDiscoveryOriginBuilder();
            getDiscoveredTargetIds().forEach(localId -> {
                originBldr.putDiscoveredTargetData(localId.getTargetId(), PerTargetEntityInformation.newBuilder()
                        .setVendorId(localId.getVendorId())
                        .build());
            });
        }

        return builder.build();
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
     * Thin class for commodities sold by this entity, containing only the fields we need.
     */
    public static class SoldCommodity {
        private final int type;
        private final String key;
        private final float capacity;
        private final float used;
        private final boolean supportsHotAdd;
        private final boolean supportsHotRemove;

        // Need this for AO.
        private final boolean supportsHotReplace;

        private SoldCommodity(@Nonnull final CommoditySoldDTO fromCmmSold) {
            this.type = fromCmmSold.getCommodityType().getType();
            this.key = fromCmmSold.getCommodityType().getKey().intern();
            capacity = (float)fromCmmSold.getCapacity();
            used = (float)fromCmmSold.getUsed();
            if (fromCmmSold.hasHotResizeInfo()) {
                final HotResizeInfo hotResizeInfo = fromCmmSold.getHotResizeInfo();
                supportsHotAdd = hotResizeInfo.getHotAddSupported();
                supportsHotRemove = hotResizeInfo.getHotRemoveSupported();
                supportsHotReplace = hotResizeInfo.getHotReplaceSupported();
            } else {
                supportsHotAdd = false;
                supportsHotRemove = false;
                supportsHotReplace = false;
            }
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
    public static class Builder extends BaseGraphEntity.Builder<Builder, RepoGraphEntity> {

        protected Builder(@Nonnull TopologyEntityDTO dto, @Nonnull DefaultTagIndex tags,
                @Nonnull SharedByteBuffer sharedCompressionBuffer) {
            super(dto, new RepoGraphEntity(dto, tags, sharedCompressionBuffer));
        }
    }
}
