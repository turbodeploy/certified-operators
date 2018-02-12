package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
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
 * Post-stitching operation for the purpose of setting Storage Access commodity capacities for
 * Storage entities, if not set through connections with Logical Pools or Disk Arrays.
 *
 * If the entity in question has no Logical Pool or Disk Array providers, a setting for
 * IOPS Capacity, and any Storage Access commodities with capacity unset, then the commodities'
 * capacities are set to the capacity specified by the setting.
 */
public class IndependentStorageAccessPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    private static final Predicate<CommoditySoldDTOOrBuilder> COMMODITY_CAN_UPDATE = commodity ->
        commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE &&
            !commodity.hasCapacity();
    private static final Set<Integer> INVALID_TYPES =
        Sets.newHashSet(EntityType.LOGICAL_POOL_VALUE, EntityType.DISK_ARRAY_VALUE);

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                              @Nonnull final EntitySettingsCollection settingsCollection,
                              @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.filter(entity -> entityIsIndependent(entity) && hasCommoditiesToUpdate(entity))
            .forEach(entity -> {
                Optional<Setting> iopsCapacitySetting =
                    settingsCollection.getEntitySetting(entity, EntitySettingSpecs.IOPSCapacity);
                if (iopsCapacitySetting.isPresent()) {
                    final float iopsCapacity =
                        iopsCapacitySetting.get().getNumericSettingValue().getValue();
                    resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
                        logger.debug("Setting Storage Access capacity for entity {} using IOPS " +
                            "Capacity setting {}", entityForUpdate.getOid(), iopsCapacity);
                        getCommoditiesToUpdate(entityForUpdate).forEach(commodity ->
                            commodity.setCapacity(iopsCapacity));
                    });
                } else {
                    logger.warn("Could not set Storage Access capacity for entity {} because no " +
                        "IOPS Capacity setting was present", entity.getOid());
                }

            });

        return resultBuilder.build();
    }

    /**
     * Retrieve from an entity all commodities of type Storage Access with unset capacities.
     *
     * @param entity The entity to get commodities from
     * @return a stream of commodity builders for update
     */
    private Stream<CommoditySoldDTO.Builder> getCommoditiesToUpdate(
                                                            @Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(COMMODITY_CAN_UPDATE);
    }

    /**
     * Determine if an entity has any commodities available for update.
     *
     * @param entity the entity to check commodities from
     * @return true if the entity has any updateable commodities, or false if it doesn't and
     *         therefore should not be processed.
     */
    private boolean hasCommoditiesToUpdate(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .anyMatch(COMMODITY_CAN_UPDATE);
    }

    /**
     * Determine whether an entity is eligible for independent update or if its Storage Access
     * capacity should be set by its providers.
     *
     * @param entity the entity to check for eligibility
     * @return true if the entity is independent or false if the entity has providers that should
     *         set its Storage Access capacity.
     */
    private boolean entityIsIndependent(@Nonnull final TopologyEntity entity) {
        return entity.getProviders().stream()
            .noneMatch(provider -> INVALID_TYPES.contains(provider.getEntityType()));
    }
}
