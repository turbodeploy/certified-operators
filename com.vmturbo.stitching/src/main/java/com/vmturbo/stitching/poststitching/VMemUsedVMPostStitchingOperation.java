package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
 * A post-stitching operation that computes the used value of a sold commodity as
 * the max value from sum of its consumers' used values and its own used value for
 * commodity type VMEM, entity type VIRTUAL_MACHINE and probe category HYPERVISOR.
 */
public class VMemUsedVMPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.HYPERVISOR,
                EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull Stream<TopologyEntity> entities,
            @Nonnull EntitySettingsCollection settingsCollection,
            @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(entity -> resultBuilder.queueUpdateEntityAlone(entity,
                entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                        .getCommoditySoldListBuilderList()
                        .stream()
                        .filter(this::commodityTypeMatches)
                        .forEach(commSold -> {
                            usedValue(commSold, entityToUpdate).ifPresent(commSold::setUsed);
                        })));

        return resultBuilder.build();
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == CommodityType.VMEM.getNumber();
    }

    /**
     * Get the used value as maximum from the current used value and the sum of its consumers'
     * used values.
     * If neither original commodity have 'used' value nor any of the consumers have them then
     * return an empty optional.
     *
     * @param commSold is used to filter commodities bought based on type and key.
     * @param seller an entity that sells the commodity.
     * @return the computed used value or an empty optional.
     */
    private Optional<Double> usedValue(CommoditySoldDTO.Builder commSold, TopologyEntity seller) {
        List<Double> usedCommoditiesByConsumers
                = seller.getCommoditiesUsedByConsumers(commSold.getCommodityType())
                .collect(Collectors.toList());

        if (!commSold.hasUsed() && usedCommoditiesByConsumers.isEmpty()) {
            return Optional.empty();
        }

        double totalUsedCommodities = 0;
        for (Double usedCommodity : usedCommoditiesByConsumers) {
            totalUsedCommodities += usedCommodity;
        }
        double used = Math.max(totalUsedCommodities, commSold.getUsed());
        logger.debug("Setting used value of commodity sold {} of {}, oid = {} to {}",
                CommodityType.VMEM, seller.getDisplayName(), seller.getOid(), used);
        return Optional.of(used);
    }
}
