package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
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
 * A post-stitching operation that computes the used value of QxVcpu commodity (e.g. Q1_VCPU, Q2_VCPU...).
 * And the computed used value should be the average value from its consumers' used values.
 * For example, host 1 has two consumer VM1 and VM2: VM1 has 100 used value of Q1_VCPU and VM2 has
 * 50 used vlaue of Q1_VCPU, after this calculation, host 1 will have 50 used value of Q1_VCPU.
 */
public class ComputedQxVcpuUsedValuePostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    // A list of QxVcpu commodity which needs to compute used value.
    private final Set<Integer> commodityQxVcpuSet =
            ImmutableSet.of(CommodityType.Q1_VCPU_VALUE, CommodityType.Q2_VCPU_VALUE,
                    CommodityType.Q3_VCPU_VALUE, CommodityType.Q4_VCPU_VALUE, CommodityType.Q5_VCPU_VALUE,
                    CommodityType.Q6_VCPU_VALUE, CommodityType.Q7_VCPU_VALUE, CommodityType.Q8_VCPU_VALUE,
                    CommodityType.Q16_VCPU_VALUE, CommodityType.Q32_VCPU_VALUE, CommodityType.Q64_VCPU_VALUE);

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
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
                                    .filter(this::isQxVcpuCommodity)
                                    .forEach(commSold -> {
                                            OptionalDouble usedValue = avgUsedValue(commSold,
                                            entityToUpdate);
                                            if (usedValue.isPresent()) {
                                                commSold.setUsed(usedValue.getAsDouble());
                                                if (logger.isDebugEnabled()) {
                                                    logger.debug("Setting used value of commodity " +
                                                            "sold {} of {} to {}",
                                                        commSold.getCommodityType(),
                                                        entityToUpdate.getDisplayName(),
                                                        commSold.getUsed());
                                                }
                                            }
                                            Optional<Double> peakValue = peakValue(commSold,
                                            entityToUpdate);

                                            if (peakValue.isPresent()) {
                                                commSold.setPeak(peakValue.get());
                                                if (logger.isDebugEnabled()) {
                                                    logger.debug("Setting max value of commodity sold {} of {} to {}",
                                                        commSold.getCommodityType(),
                                                        entityToUpdate.getDisplayName(), commSold.getPeak());
                                                }
                                            }
                                        }
                                    ));
        });
        return resultBuilder.build();
    }

    private boolean isQxVcpuCommodity(@Nonnull final TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodityQxVcpuSet.contains(commodity.getCommodityType().getType());

    }

    /**
     * Compute the used value of a QxVcpu commodity as the average of its consumer's used values.
     *
     * @param commSold The commodity (e.g. Q1_VCPU commodity) needs to calculate average used value.
     * @param seller an entity that sells the commodity.
     * @return the computed average used value.
     */
    private OptionalDouble avgUsedValue(@Nonnull final CommoditySoldDTO.Builder commSold,
                                @Nonnull final TopologyEntity seller) {
        return seller.getCommoditiesUsedByConsumers(commSold.getCommodityType())
                .mapToDouble(Double::doubleValue)
                .average();
    }

    private Optional<Double> peakValue(@Nonnull final CommoditySoldDTO.Builder commSold,
                                @Nonnull final TopologyEntity seller) {
        final Optional<Double> commodityBoughtQxVcpuMaxValue = seller.getConsumers().stream()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .flatMap(entityDtoBuilder ->
                entityDtoBuilder.getCommoditiesBoughtFromProvidersList().stream())
            .filter(commodityBoughtFromProvider ->
                commodityBoughtFromProvider.getProviderId() == seller.getOid())
            .flatMap(commodityBoughtFromProvider ->
                commodityBoughtFromProvider.getCommodityBoughtList().stream())
            .filter(commodityBought ->
                commSold.getCommodityType().equals(commodityBought.getCommodityType()))
            .map(CommodityBoughtDTO::getPeak)
            .max(Double::compareTo);
        return commodityBoughtQxVcpuMaxValue;
    }
}
