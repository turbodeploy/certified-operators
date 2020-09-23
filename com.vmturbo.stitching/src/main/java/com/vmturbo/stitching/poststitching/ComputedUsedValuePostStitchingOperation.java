package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A post-stitching operation that computes the used value of a sold commodity as
 * the sum of its consumers' used values.
 */
public class ComputedUsedValuePostStitchingOperation implements PostStitchingOperation {

    private final CommodityType commodityType;
    private final EntityType sellerType;

    private static final Logger logger = LogManager.getLogger();

    public ComputedUsedValuePostStitchingOperation(@Nonnull final EntityType sellerType,
                                            @Nonnull final CommodityType commodityType) {
        this.sellerType = sellerType;
        this.commodityType = commodityType;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(sellerType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(@Nonnull final Stream<TopologyEntity> entities,
                    @Nonnull final EntitySettingsCollection settingsCollection,
                    @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(entity -> {
            resultBuilder.queueUpdateEntityAlone(entity,
                entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                    .getCommoditySoldListBuilderList().stream()
                        .filter(this::commodityTypeMatches).findFirst() // assume only one sold
                        .ifPresent(commSold -> commSold.setUsed(usedValue(commSold, entityToUpdate))));
        });

        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return getClass().getSimpleName() + "_" + sellerType + "_" + commodityType;
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == commodityType.getNumber();
    }

    /**
     * Compute the used value of a sold commodity as the sum of its consumers' used values.
     * It is assumed that the seller sells only one commodity of type {@link #commodityType}.
     *
     * @param commSold is used to filter commodities bought based on type and key
     * @param seller an entity that sells the commodity
     * @return the computed used value
     */
    private double usedValue(CommoditySoldDTO.Builder commSold, TopologyEntity seller) {
        Stream<Double> usedCommoditiesByConsumers
                = seller.getCommoditiesUsedByConsumers(commSold.getCommodityType());
        double used = usedCommoditiesByConsumers.mapToDouble(Double::doubleValue).sum();
        logger.debug("Setting used value of commodity sold {} of {}, oid = {} to {}",
                commodityType, seller.getDisplayName(), seller.getOid(), used);
        return used;
    }

}
