package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for setting bought commodity used values from the sold commodity historical max value.
 * At present, this is only needed for AWS RDS Database servers' Connection commodities.
 */
public class SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<Integer> COMMODITY_TYPES_OF_INTEREST = ImmutableSet.of(CommonDTO.CommodityDTO.CommodityType.CONNECTION_VALUE);

    @NotNull
    @Override
    public StitchingScope<TopologyEntity> getScope(@NotNull StitchingScope.StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeEntityTypeScope(SDKProbeType.AWS.getProbeType(), CommonDTO.EntityDTO.EntityType.DATABASE_SERVER);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull final Stream<TopologyEntity> entities,
                     @Nonnull final EntitySettingsCollection settingsCollection,
                     @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        Iterable<TopologyEntity> entitiesIterable = entities::iterator;
        long count = 0;
        for (TopologyEntity entity : entitiesIterable) {
            TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
            for (CommoditySoldDTO.Builder commSold : entityBuilder.getCommoditySoldListBuilderList()) {
                if (!COMMODITY_TYPES_OF_INTEREST.contains(commSold.getCommodityType().getType())
                        || !commSold.getHistoricalUsed().hasMaxQuantity()
                        || !commSold.hasCapacity()) {
                    continue;
                }
                Optional<CommodityBoughtDTO.Builder> commBoughtToUpdate = findCommodityBoughtToUpdate(entityBuilder, commSold.getCommodityType());
                if (commBoughtToUpdate.isPresent()) {
                    // We don't want the bought used to be greater than the current capacity. If that happens,
                    // AWS RDS entities will scale up (out of the current tier because the current tier cannot support
                    // the historical_max connections). But that should not happen because connections is not a
                    // driving commodity for scaling RDS instances. So we cap the bought used to the
                    // current capacity.
                    double boughtUsed = Math.min(commSold.getHistoricalUsed().getMaxQuantity(), commSold.getCapacity());
                    resultBuilder.queueUpdateEntityAlone(entity, toUpdate ->
                            commBoughtToUpdate.get().setUsed(boughtUsed));
                    count++;
                }
            }
        }
        logger.info("Updated {} commodities bought with used from sold commodity historical max / capacity.", count);
        return resultBuilder.build();
    }

    private Optional<CommodityBoughtDTO.Builder> findCommodityBoughtToUpdate(TopologyEntityDTO.Builder entityBuilder,
                                                                             CommodityType commodityTypeToFind) {
        for (CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider : entityBuilder.getCommoditiesBoughtFromProvidersBuilderList()) {
            for (CommodityBoughtDTO.Builder commBought : commoditiesBoughtFromProvider.getCommodityBoughtBuilderList()) {
                if (commodityTypeToFind.equals(commBought.getCommodityType())) {
                    return Optional.of(commBought);
                }
            }
        }
        return Optional.empty();
    }
}