package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.ThinSearchableProps.CommodityValueFetcher;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * An implementation of a {@link TopologyGraphSearchableEntity} used for tests in lieu of a complicated
 * mock object.
 */
public class TestGraphEntity extends BaseGraphEntity<TestGraphEntity> implements TopologyGraphSearchableEntity<TestGraphEntity>, CommodityValueFetcher {

    private String displayNameOverride = null;
    private EntityState stateOverride = null;
    private SearchableProps searchableProps;

    private EnvironmentType envType = EnvironmentType.ON_PREM;

    private final Map<Long, String> discoveringTargetIds = new HashMap<>();

    private TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.getDefaultInstance();

    private final List<CommoditySoldDTO> commsSoldByType = new ArrayList<>();

    private final Map<String, List<String>> tags = new HashMap<>();

    private AnalysisSettings analysisSettings = AnalysisSettings.getDefaultInstance();

    private TestGraphEntity(final long oid, final ApiEntityType type) {
        super(TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(type.typeNumber())
                .setDisplayName(Long.toString(oid))
                .build());
        refreshSearchableProps();
    }

    @Override
    public String getDisplayName() {
        return displayNameOverride == null ? super.getDisplayName() : displayNameOverride;
    }

    @Override
    public EntityState getEntityState() {
        return stateOverride == null ? super.getEntityState() : stateOverride;
    }

    @Nonnull
    @Override
    public SearchableProps getSearchableProps() {
        return searchableProps;
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

    @Override
    public boolean isHotAddSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfo::getHotAddSupported);
    }

    @Override
    public boolean isHotRemoveSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfo::getHotRemoveSupported);
    }

    private boolean isHotChangeSupported(int commodityType,
            @Nonnull Function<HotResizeInfo, Boolean> function) {
        return commsSoldByType.stream()
                .filter(c -> c.getCommodityType().getType() == commodityType)
                .filter(CommoditySoldDTO::hasHotResizeInfo)
                .map(c -> function.apply(c.getHotResizeInfo()))
                .findAny()
                .orElse(false);
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
                        .setOid(getOid())
                        .setDisplayName(getDisplayName())
                        .setEntityType(getEntityType())
                        .setTypeSpecificInfo(typeSpecificInfo)
                        .setAnalysisSettings(analysisSettings)
                        .build());
    }

    /**
     * Builder class for topology graphs of {@link TestGraphEntity}s.
     */
    public static class Builder extends BaseGraphEntity.Builder<Builder, TestGraphEntity> {

        /**
         * New builder.
         *
         * @param oid OID.
         * @param entityType Entity type.
         */
        public Builder(final long oid, final ApiEntityType entityType) {
            super(TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName(Long.toString(oid))
                .setEntityType(entityType.typeNumber())
                .build(), new TestGraphEntity(oid, entityType));
        }

        /**
         * New builder.
         *
         * @param dto The entity DTO.
         */
        public Builder(TopologyEntityDTO dto) {
            super(dto, new TestGraphEntity(dto.getOid(), ApiEntityType.fromType(dto.getEntityType())));
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
            graphEntity.tags.put(key, values);
            return this;
        }

        public Builder addCommSold(final CommoditySoldDTO commSold) {
            graphEntity.commsSoldByType.add(commSold);
            return this;
        }

        public Builder setTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo) {
            graphEntity.typeSpecificInfo = typeSpecificInfo;
            return this;
        }

        public Builder setName(@Nonnull final String name) {
            graphEntity.displayNameOverride = name;
            return this;
        }

        public Builder setState(@Nonnull final EntityState newState) {
            graphEntity.stateOverride = newState;
            return this;
        }

        public Builder setEnvironmentType(@Nonnull final EnvironmentType envType) {
            graphEntity.envType = envType;
            return this;
        }

        public Builder setDeletable(boolean deletable) {
            graphEntity.analysisSettings = graphEntity.analysisSettings.toBuilder()
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
            graphEntity.discoveringTargetIds.put(target, localName);
            return this;
        }

        @Override
        public TestGraphEntity build() {
            graphEntity.refreshSearchableProps();
            return super.build();
        }
    }

}
