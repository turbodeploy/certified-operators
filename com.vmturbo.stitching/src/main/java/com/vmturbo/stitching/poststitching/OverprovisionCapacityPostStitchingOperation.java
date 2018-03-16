package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
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
 * A post-stitching operation that involves a source commodity and an overprovisioned commodity.
 * When:
 *  - Both the source and the overprovisioned commodities are present
 *  - The entity has an overprovision percentage setting
 *  - The overprovisioned commodity does not have a set capacity, or permits overwriting existing
 *    capacity
 * The overprovisioned commodity capacity is set to the overprovision percentage multiplied by the
 * source commodity capacity.
 */
public abstract class OverprovisionCapacityPostStitchingOperation implements
                                                                        PostStitchingOperation {

    private final EntitySettingSpecs overprovisionSettingType;
    private final CommodityType sourceCommodityType;
    private final CommodityType overprovCommodityType;

    private static final Logger logger = LogManager.getLogger();

    OverprovisionCapacityPostStitchingOperation(@Nonnull final EntitySettingSpecs setting,
                                                @Nonnull final CommodityType sourceType,
                                                @Nonnull final CommodityType overprovType) {
        overprovisionSettingType = setting;
        sourceCommodityType = sourceType;
        overprovCommodityType = overprovType;
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                               @Nonnull final EntitySettingsCollection settingsCollection,
                               @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

       entities.forEach(entity -> {
           final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();

           final long entityOid = entity.getOid();
           final Map<CommoditySoldDTO.Builder, Double> overprovisionedToSourceCapacity =
               getEligibleCommodities(entityBuilder.getCommoditySoldListBuilderList())
                   .map(overprovisionedCommodity -> new CommodityPair(overprovisionedCommodity,
                       findMatchingSource(overprovisionedCommodity.getCommodityType().getKey(),
                           entityOid, entityBuilder.getCommoditySoldListList())))
                   .filter(commodityPair -> commodityPair.source.isPresent())
                   .collect(Collectors.toMap(commodityPair -> commodityPair.overprovisioned,
                       commodityPair -> commodityPair.source.get().getCapacity()));

           if (!overprovisionedToSourceCapacity.isEmpty()) {
               Optional<Setting> overprovisionSetting =
                   settingsCollection.getEntitySetting(entity, overprovisionSettingType);

               if (!overprovisionSetting.isPresent()) {
                   logger.warn("Could not update {} capacities for entity {} ; no {} setting found",
                       overprovCommodityType, entityOid, overprovisionSettingType.getSettingName());
               } else {
                   resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate ->
                        overprovisionedToSourceCapacity.forEach((overprov, sourceCapacity) -> {
                           final double overprovisionedCapacity = calculateOverprovisionedCapacity(
                               sourceCapacity, overprovisionSetting.get());

                           overprov.setCapacity(overprovisionedCapacity);

                           logger.debug("Setting {} capacity for entity {} to {} " +
                                   "from {} capacity {}", overprovCommodityType.name(),
                               entityForUpdate.getOid(), overprovisionedCapacity,
                               sourceCommodityType.name(), sourceCapacity);
                       })
                   );
               }
           }
       });

        return resultBuilder.build();
    }

    /**
     * Determine whether a commodity builder has a capacity greater than zero and, if it does,
     * whether it should overwrite the capacity.
     *
     * @param commodity The commodity builder to check
     * @return true if the commodity builder should set a new capacity, and false otherwise
     */
    private boolean canUpdateCapacity(@Nonnull final CommoditySoldDTO.Builder commodity) {
        return !commodity.hasCapacity() || commodity.getCapacity() == 0 || shouldOverwriteCapacity();
    }

    /**
     * Determine whether a commodity is of the overprovisioned type
     * @param commodity the commodity to check
     * @return true if it is the overprovisioned commodity type, false otherwise
     */
    private boolean commodityIsOverprovisionedType(
        @Nonnull final CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == overprovCommodityType.getNumber();
    }

    /**
     * Determines if a commodity is of the source type and matches a specific key.
     * @param commodity the commodity to check
     * @param key the key that should be matched
     * @return true if the commodity is of the right type and matches the key, false otherwise
     */
    private boolean isMatchingSourceCommodity(@Nonnull final CommoditySoldDTO commodity,
                                              @Nonnull final String key) {
        return (commodity.getCommodityType().getType() == sourceCommodityType.getNumber()) &&
            Objects.equals(commodity.getCommodityType().getKey(), key);
    }

    /**
     * Finds a source commodity (read-only), if it is present, to match the key of an
     * overprovisioned commodity builder.
     *
     * @throws IllegalStateException if a duplicate source commodity is encountered.
     * @param key the overprovisioned commodity key
     * @param oid the OID of the entity that contains all the commodities
     * @param allCommodities the list of commodities potentially containing a matching source
     *                       commodity
     * @return an empty optional if there is no matching source commodity, or an optional of the
     * matching source commodity.
     */
    private Optional<CommoditySoldDTO> findMatchingSource(@Nonnull final String key, final long oid,
                                             @Nonnull final List<CommoditySoldDTO> allCommodities) {
        Optional<CommoditySoldDTO> found = allCommodities.stream()
            .filter(commodity -> isMatchingSourceCommodity(commodity, key))
            /* the IllegalStateException is thrown if there are multiple commodities with the same
            type and key, which should not happen. */
            .reduce((expectedCommodity, unexpectedCommodity) -> {
                throw new IllegalStateException("Found multiple commodities of type " +
                    sourceCommodityType + " with key " + key + " in entity " + oid);
            });
        if (!found.isPresent()) {
            logger.warn("Cannot set {} capacity due to no {} commodity with " +
                    "key {} in entity {}", overprovCommodityType, sourceCommodityType,
                key, oid);
        }
        return found;
    }

    /**
     * Get all commodities of the overprovisioned commodity type eligible for capacity updates from
     * a list
     *
     * @param commodityBuilders the commodities to search through
     * @return list of all the commodities from the original list that are of the overprovisioned
     *         type and are eligible for capacity updates
     */
    private Stream<CommoditySoldDTO.Builder> getEligibleCommodities(
                            @Nonnull final List<CommoditySoldDTO.Builder> commodityBuilders) {
        return commodityBuilders.stream().filter(commodity ->
            commodityIsOverprovisionedType(commodity) && canUpdateCapacity(commodity));
    }

    /**
     * Calculate the capacity of an overprovisioned commodity based on the overprovision setting and the
     * source capacity
     * @param sourceCapacity the source capacity
     * @param overprovisionSetting the overprovision setting containing the percentage to multiply
     * @return the overprovisioned commodity's capacity
     */
    private double calculateOverprovisionedCapacity(final double sourceCapacity,
                                                    @Nonnull final Setting overprovisionSetting) {
        final float overprovisionPercent =
            overprovisionSetting.getNumericSettingValue().getValue();
        final double overprovisionFactor = overprovisionPercent / 100.0;
        return sourceCapacity * overprovisionFactor;
    }

    /**
     *
     */
    private static class CommodityPair {
        public final CommoditySoldDTO.Builder overprovisioned;
        public final Optional<CommoditySoldDTO> source;

        public CommodityPair(@Nonnull final CommoditySoldDTO.Builder overprovisioned,
                             @Nonnull final Optional<CommoditySoldDTO> source) {
            this.overprovisioned = Objects.requireNonNull(overprovisioned);
            this.source = Objects.requireNonNull(source);
        }
    }

    /**
     * Each subclass should specify whether calculated overprovisioned capacity should overwrite
     * a capacity that is already present in the overprovisioned commodity.
     * @return true if existing capacity should be overwritten, false otherwise.
     */
    abstract boolean shouldOverwriteCapacity();

    /**
     * Post-stitching operation for the purpose of setting CPU Provisioned commodity capacities for
     * physical machines.
     *
     * If the PM in question has a CPU commodity, a CPU Provisioned commodity,
     * and a setting for CPU overprovisioned percentage, then the CPU Provisioned commodity's
     * capacity is set to the CPU commodity capacity multiplied by the overprovisioned percentage.
     */
    public static class CpuProvisionedPostStitchingOperation extends
        OverprovisionCapacityPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
        }

        public CpuProvisionedPostStitchingOperation() {
            super(EntitySettingSpecs.CpuOverprovisionedPercentage,
                CommodityType.CPU, CommodityType.CPU_PROVISIONED);
        }

        @Override
        boolean shouldOverwriteCapacity() {
            return true;
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Memory Provisioned commodity capacities
     * for physical machines.
     *
     * If the PM in question has a Memory commodity, a Memory Provisioned commodity, and a setting
     * for memory overprovisioned percentage, then the Memory Provisioned commodity's capacity is
     * set to the Memory commodity capacity multiplied by the overprovisioned percentage.
     */
    public static class MemoryProvisionedPostStitchingOperation extends
        OverprovisionCapacityPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
        }

        public MemoryProvisionedPostStitchingOperation() {
            super(EntitySettingSpecs.MemoryOverprovisionedPercentage,
                CommodityType.MEM, CommodityType.MEM_PROVISIONED);
        }

        @Override
        boolean shouldOverwriteCapacity() {
            return true;
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Memory Allocation commodity capacities
     * for physical machines if not already set.
     *
     * If the entity in question has a Memory commodity, a Memory Allocation commodity with unset
     * capacity, and a setting for memory overprovisioned percentage, then the Memory Allocation
     * commodity's capacity is set to the Memory commodity capacity multiplied by the
     * overprovisioned percentage.
     */
    public static class MemoryAllocationPostStitchingOperation extends
        OverprovisionCapacityPostStitchingOperation {

        public MemoryAllocationPostStitchingOperation() {
            super(EntitySettingSpecs.MemoryOverprovisionedPercentage, CommodityType.MEM,
                CommodityType.MEM_ALLOCATION);
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
        }

        @Override
        boolean shouldOverwriteCapacity() {
            return false;
        }
    }

    /**
     * Post-stitching operation for the purpose of setting CPU Allocation commodity capacities for
     * physical machines if not already set.
     *
     * If the PM in question has a CPU commodity, a CPU Allocation commodity with unset capacity,
     * and a setting for CPU overprovisioned percentage, then the CPU Allocation commodity's
     * capacity is set to the CPU commodity capacity multiplied by the overprovisioned percentage.
     */
    public static class PmCpuAllocationPostStitchingOperation extends
        OverprovisionCapacityPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
        }

        public PmCpuAllocationPostStitchingOperation() {
            super(EntitySettingSpecs.CpuOverprovisionedPercentage, CommodityType.CPU,
                CommodityType.CPU_ALLOCATION);
        }

        @Override
        boolean shouldOverwriteCapacity() {
            return false;
        }
    }
}
