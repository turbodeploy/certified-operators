package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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
