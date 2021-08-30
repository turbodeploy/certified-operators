package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
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
 * A post-stitching operation that computes the used value of NumVCore commodity.
 */
public class ComputedNumVCoreUsedValuePostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.HYPERVISOR,
            EntityType.PHYSICAL_MACHINE);
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return getClass().getSimpleName();
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(entity -> {
            resultBuilder.queueUpdateEntityAlone(entity,
                entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                    .getCommoditySoldListBuilderList().stream()
                        .filter(this::isNumVCoreCommodity)
                        .forEach(commSold -> {
                            double usedValue = usedValue(commSold, entityToUpdate);
                            commSold.setUsed(usedValue);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Setting used and peak values of commodity "
                                    + "sold {} of {} to {}",
                                        commSold.getCommodityType(),
                                        entityToUpdate.getDisplayName(),
                                        commSold.getUsed());
                            }
                            commSold.setPeak(usedValue);
                        })
            );
        });
        return resultBuilder.build();
    }

    private boolean isNumVCoreCommodity(@Nonnull final TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == CommodityType.NUM_VCORE_VALUE;
    }

    /**
     * Compute the used value of a NumVCore commodity as the sum of its consumer's used values.
     *
     * @param commSold The NumVCore commodity.
     * @param seller an entity that sells the commodity.
     * @return the computed sum used value.
     */
    private double usedValue(@Nonnull final CommoditySoldDTO.Builder commSold,
            @Nonnull final TopologyEntity seller) {
        return seller.getCommoditiesUsedByConsumers(commSold.getCommodityType())
            .mapToDouble(Double::doubleValue)
            .sum();
    }
}