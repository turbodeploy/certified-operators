package com.vmturbo.stitching.poststitching;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
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
 * A post-stitching operation that sets a sold commodity's capacity based on a default value.
 */
public class SetDefaultCommodityCapacityPostStitchingOperation implements PostStitchingOperation {
    private final float defaultCapacity;
    private final EntityType entityType;
    private final ProbeCategory probeCategory;
    private final CommodityType commodityType;

    /**
     * Default constructor.
     * @param entityType entity type
     * @param probeCategory probe category
     * @param commodityType commodity type
     * @param defaultCapacity default capacity to set.
     */
    public SetDefaultCommodityCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final CommodityType commodityType,
            final float defaultCapacity) {
        this.entityType = Objects.requireNonNull(entityType);
        this.probeCategory = Objects.requireNonNull(probeCategory);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.defaultCapacity = defaultCapacity;
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
        entities.forEach(entity -> {
                        resultBuilder.queueUpdateEntityAlone(entity,
                                entityToUpdate -> entityToUpdate.getTopologyEntityImpl()
                                        .getCommoditySoldListImplList().stream()
                                        .filter(this::commodityTypeMatches)
                                        .forEach(commSold ->
                                                commSold.setCapacity(defaultCapacity)));
                    });
        return resultBuilder.build();
    }

    private boolean commodityTypeMatches(final CommoditySoldImpl commoditySold) {
        return commoditySold.hasCommodityType() && commoditySold.getCommodityType().getType()
                == commodityType.getNumber();
    }
}
