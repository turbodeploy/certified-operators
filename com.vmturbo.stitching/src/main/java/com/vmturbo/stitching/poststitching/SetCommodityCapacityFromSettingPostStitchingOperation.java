package com.vmturbo.stitching.poststitching;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

    /**
     * A post-stitching operation that sets a sold commodity's capacity based on a setting value.
     */
    public class SetCommodityCapacityFromSettingPostStitchingOperation
            implements PostStitchingOperation {

        private final String settingName;
        protected final EntityType entityType;
        protected final ProbeCategory probeCategory;
        protected final CommodityType commodityType;

        private static final Logger logger = LogManager.getLogger();

        public SetCommodityCapacityFromSettingPostStitchingOperation(
                @Nonnull final EntityType entityType,
                @Nonnull final ProbeCategory probeCategory,
                @Nonnull final CommodityType commodityType,
                @Nonnull final String settingName) {
            this.entityType = Objects.requireNonNull(entityType);
            this.settingName = Objects.requireNonNull(settingName);
            this.probeCategory = Objects.requireNonNull(probeCategory);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
                @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.probeCategoryEntityTypeScope(probeCategory, entityType);
        }

        @Nonnull
        @Override
        public TopologicalChangelog<TopologyEntity> performOperation(
                @Nonnull final Stream<TopologyEntity> entities,
                @Nonnull final EntitySettingsCollection settingsCollection,
                @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

            // iterate over entities and if the named setting exists for that entity, find all
            // sold commodities of the correct type and set their capacities according to the
            // value in the setting.
            entities.forEach(entity -> {
                settingsCollection.getEntitySetting(entity.getOid(), settingName).ifPresent(
                        setting -> {
                            resultBuilder.queueUpdateEntityAlone(entity,
                                    entityToUpdate -> entityToUpdate.getTopologyEntityImpl()
                                            .getCommoditySoldListImplList().stream()
                                            .filter(this::commodityTypeMatches)
                                            .forEach(commSold ->
                                                    commSold.setCapacity(setting
                                                            .getNumericSettingValue().getValue())));
                        });
            });
            return resultBuilder.build();
        }

        @Nonnull
        @Override
        public String getOperationName() {
            return String.join("-", getClass().getSimpleName(),
                    probeCategory.getCategory(), entityType.toString(), commodityType.name(),
                    settingName);
        }

        private boolean commodityTypeMatches(CommoditySoldView commodity) {
            return commodity.getCommodityType().getType() == commodityType.getNumber();
        }


}
