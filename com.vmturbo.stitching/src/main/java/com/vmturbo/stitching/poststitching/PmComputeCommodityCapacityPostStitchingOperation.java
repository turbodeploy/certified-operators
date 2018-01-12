package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
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
 * A post-stitching operation that involves a source commodity and a compute commodity. If both
 * commodities are present and there is an overprovision percentage setting, and the compute
 * commodity does not have a set capacity, the compute commodity capacity is set to the overprovision
 * percentage multiplied by the source commodity capacity.
 */
public abstract class PmComputeCommodityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final EntitySettingSpecs overprovisionSettingType;
    private final CommodityType sourceCommodityType;
    private final CommodityType computeCommodityType;

    private static final Logger logger = LogManager.getLogger();

    PmComputeCommodityCapacityPostStitchingOperation(@Nonnull final EntitySettingSpecs setting,
                                                     @Nonnull final CommodityType sourceType,
                                                     @Nonnull final CommodityType computeType) {
        overprovisionSettingType = setting;
        sourceCommodityType = sourceType;
        computeCommodityType = computeType;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
        @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                                                 @Nonnull final EntitySettingsCollection settingsCollection,
                                                 @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(topologyEntity -> resultBuilder.queueUpdateEntityAlone(topologyEntity, entity -> {
            final TopologyEntityDTO.Builder entityBuilder = topologyEntity.getTopologyEntityDtoBuilder();

            settingsCollection.getEntitySetting(entity, overprovisionSettingType)
                .ifPresent(overprovisionSetting -> {
                    getCommoditiesOfType(entityBuilder.getCommoditySoldListBuilderList(), computeCommodityType)
                        .filter(computeCommodity -> !computeCommodity.hasCapacity() || shouldOverwriteCapacity())
                        .forEach(computeCommodity -> {
                            final String key = computeCommodity.getCommodityType().getKey();
                            final Optional<CommoditySoldDTO> sourceCommodity =
                                findMatchingSource(key, entity.getOid(), entityBuilder.getCommoditySoldListList());

                            if (sourceCommodity.isPresent()) {
                                final CommoditySoldDTO source = sourceCommodity.get();
                                final double computeCapacity =
                                    calculateComputeCapacity(source.getCapacity(), overprovisionSetting);
                                computeCommodity.setCapacity(computeCapacity);

                                logger.debug("Setting {} capacity for entity {} to {} from {} capacity {}",
                                    computeCommodityType.name(), entity.getOid(), computeCapacity,
                                    sourceCommodityType.name(), source.getCapacity());
                            } else {
                                logger.warn("Cannot set {} capacity due to no {} commodity with " +
                                        "key {} in entity {}", computeCommodityType, sourceCommodityType,
                                    key, entity.getOid());
                            }
                        });
                });
        }));

        return resultBuilder.build();
    }

    /**
     * Finds a source commodity to match the key of a compute commodity, if it exists.
     * @throws IllegalStateException if a duplicate source commodity is encountered.
     * @param key the compute commodity key
     * @param oid the OID of the entity that contains all the commodities
     * @param allCommodities the list of commodities potentially containing a matching source commodity
     * @return an empty optional if there is no matching source commodity, or an optional of the
     * matching source commodity.
     */
    private Optional<CommoditySoldDTO> findMatchingSource(@Nonnull final String key, final long oid,
                                                          @Nonnull final List<CommoditySoldDTO> allCommodities) {
        return allCommodities.stream()
            .filter(commodity ->
                (commodity.getCommodityType().getType() == sourceCommodityType.getNumber()) &&
                    Objects.equals(commodity.getCommodityType().getKey(), key))
            /* the IllegalStateException is thrown if there are multiple commodities with the same
            type and key, which should not happen. */
            .reduce((expectedCommodity, unexpectedCommodity) -> {
                throw new IllegalStateException("Found multiple commodities of type " +
                    sourceCommodityType + " with key " + key + " in entity " + oid);
            });
    }

    /**
     * Get all commodities of a certain type from a list
     * @param commodityBuilders the commodities to search through
     * @param type the commodity type to search for
     * @return list of all the commodities of the requested type from the original list
     */
    private Stream<CommoditySoldDTO.Builder> getCommoditiesOfType(
        @Nonnull final List<CommoditySoldDTO.Builder> commodityBuilders, @Nonnull final CommodityType type) {
        return commodityBuilders.stream()
            .filter(commodity -> commodity.getCommodityType().getType() == type.getNumber());
    }

    /**
     * Calculate the capacity of a compute commodity based on the overprovision setting and the
     * source capacity
     * @param sourceCapacity the source capacity
     * @param overprovisionSetting the overprovision setting containing the percentage to multiply
     * @return the compute commodity's capacity
     */
    private double calculateComputeCapacity(final double sourceCapacity,
                                            @Nonnull final Setting overprovisionSetting) {
        final float overprovisionPercent =
            overprovisionSetting.getNumericSettingValue().getValue();
        final double overprovisionFactor = overprovisionPercent / 100.0;
        return sourceCapacity * overprovisionFactor;
    }

    /**
     * Each subclass should specify whether calculated compute capacity should overwrite
     * a capacity that is already present in the compute commodity.
     * @return true if existing capacity should be overwritten, false otherwise.
     */
    abstract boolean shouldOverwriteCapacity();
}
