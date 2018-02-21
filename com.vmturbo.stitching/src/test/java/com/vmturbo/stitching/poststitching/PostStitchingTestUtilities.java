package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;

class PostStitchingTestUtilities {

    /**
     * Result builder subclass for use in post-stitching unit tests
     */
    protected static class UnitTestResultBuilder extends EntityChangesBuilder<TopologyEntity> {

        @Override
        public TopologicalChangelog build() {
            return buildInternal();
        }

        @Override
        public EntityChangesBuilder<TopologyEntity>
            queueUpdateEntityAlone(@Nonnull final TopologyEntity entityToUpdate,
                                   @Nonnull final Consumer<TopologyEntity> updateMethod) {
            changes.add(new PostStitchingUnitTestChange(entityToUpdate, updateMethod));
            return this;
        }
    }

    /**
     * Topological change subclass for use in post-stitching unit tests
     */
    private static class PostStitchingUnitTestChange implements TopologicalChange {
        private final TopologyEntity entityToUpdate;
        private final Consumer<TopologyEntity> updateMethod;

        PostStitchingUnitTestChange(@Nonnull final TopologyEntity entityToUpdate,
                                    @Nonnull final Consumer<TopologyEntity> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        public void applyChange() {
            updateMethod.accept(entityToUpdate);
        }
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .addAllCommoditySoldList(commoditiesSold))
            .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
                                             @Nonnull final List<TopologyEntity.Builder> providers) {
        final TopologyEntity.Builder builder = makeTopologyEntityBuilder(entityType, commoditiesSold, commoditiesBought);
        providers.forEach(builder::addProvider);
        return builder.build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder().setEntityType(entityType)
            .addAllCommoditySoldList(commoditiesSold)).build();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setEntityType(entityType)
                .addAllCommoditySoldList(commoditiesSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(commoditiesBought)
                ));
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commoditiesSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(commoditiesBought)
                ));
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final Map<String, String> propertyMap) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .addAllCommoditySoldList(commoditiesSold)
                .putAllEntityPropertyMap(propertyMap))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity) {
        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity, @Nonnull final String key) {
        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setKey(key)
                .setType(type.getNumber()))
            .build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type,
                                              @Nonnull final String key) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setKey(key)
                .setType(type.getNumber()))
            .build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommodityBoughtDTO.Builder makeCommodityBoughtBuilder(@Nonnull final CommodityType type) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()));
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              @Nonnull final String key) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setKey(key)
                .setType(type.getNumber()))
            .build();
    }

    static Setting makeNumericSetting(final float value) {
        return Setting.newBuilder()
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
            .build();
    }
}
