package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * Storage entities. The entity's Storage Access capacity is set to that of its Disk Array or
 * Logical Pool provider. If its Disk Array or Logical Pool provider does not sell Storage Access
 * capacity, the Storage Access capacity is set from the IOPS setting directly.
 *
 * This operation must occur after any StorageAccessCapacityPostStitchingOperations
 * so that all possible providers have their capacity set properly.
 * TODO: It is assumed that an entity has only one of these, but this may not be the case.
 */
public class StorageEntityAccessCapacityPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();
    private static final double IOPS_CAPACITY_GLOBAL_DEFAULT =
            EntitySettingSpecs.IOPSCapacity.getSettingSpec().getNumericSettingValueType().getDefault();

    private static final Predicate<CommoditySoldDTOOrBuilder> COMMODITY_IS_STORAGE_ACCESS =
        commodity -> commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE;

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(@Nonnull final Stream<TopologyEntity> entities,
                              @Nonnull final EntitySettingsCollection settingsCollection,
                              @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(storage -> {
            final Optional<Double> providerCapacity = findProviderCapacity(storage.getProviders());
            // TODO: should iopsUserSetting be used first: OM-60141
//          final Optionahttps://vmturbo.atlassian.net/browse/OM-60141l<Setting> iopsUserSetting =
//                  settingsCollection.getEntityUserSetting(storage, EntitySettingSpecs.IOPSCapacity);
            final Optional<Setting> iopsSetting =
                    settingsCollection.getEntitySetting(storage, EntitySettingSpecs.IOPSCapacity);
            // The provider capacity, if set, overrides the setting-derived capacity AND any
            // capacity already set in the entity.
            if (providerCapacity.isPresent()) {
                resultBuilder.queueUpdateEntityAlone(storage, entityForUpdate -> {
                    logger.debug("Setting unset Storage Access capacities for Storage {} based on provider capacity: {}",
                            entityForUpdate.getOid(), providerCapacity.get());
                    getCommoditiesToUpdate(entityForUpdate).forEach(commodity ->
                            commodity.setCapacity(providerCapacity.get()));
                });
            } else {
                final double settingVal;
                if (iopsSetting.isPresent()) {
                    settingVal = iopsSetting.get().getNumericSettingValue().getValue();
                } else {
                    logger.debug("Could not find Storage Access capacity for Storage {} ({}) because "
                                    + "it had no Disk Array or Logical Pool provider with Storage Access capacity, and"
                                    + " no setting for IOPS capacity. Using default IOPS capacity {}",
                            storage.getOid(), storage.getDisplayName(), IOPS_CAPACITY_GLOBAL_DEFAULT);
                    settingVal = IOPS_CAPACITY_GLOBAL_DEFAULT;
                }
                // If there is no storage access provider (which happens with hypervisor probes),
                // and the probe did not set an explicit storage access capacity, derive
                // the storage access capacity from the IOPS capacity setting.
                resultBuilder.queueUpdateEntityAlone(storage, entityForUpdate -> {
                    logger.debug("Setting unset Storage Access capacities for Storage {} using IOPS " +
                            "Capacity setting {}", entityForUpdate.getOid(), settingVal);
                    getCommoditiesToUpdate(entityForUpdate)
                            // Only update the unset commodities.
                            .filter(comm -> !comm.hasCapacity() || comm.getCapacity() == 0)
                            .forEach(commodity -> commodity.setCapacity(settingVal));
                });
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
    private Stream<CommoditySoldDTO.Builder> getCommoditiesToUpdate(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(COMMODITY_IS_STORAGE_ACCESS);
    }

    /**
     * Retrieve Storage Access capacity from a Storage's providers to be propagated to that Storage.
     * The Storage should have exactly one Disk Array or Logical Pool provider with a Storage Access
     * commodity that has capacity greater than zero. If it has more than one, the first to be
     * found is used.
     *
     * @param providers providers to search for Storage Access capacity
     * @return an eligible provider's Storage Access capacity or Optional.empty() if none is found
     */
    private Optional<Double> findProviderCapacity(@Nonnull final List<TopologyEntity> providers) {
        return providers.stream()
            .filter(provider -> provider.getEntityType() == EntityType.LOGICAL_POOL_VALUE ||
                provider.getEntityType() == EntityType.DISK_ARRAY_VALUE)
            .flatMap(provider ->
                provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream())
            .filter(COMMODITY_IS_STORAGE_ACCESS)
            .filter(commodity -> commodity.hasCapacity() && commodity.getCapacity() > 0)
            .map(CommoditySoldDTO::getCapacity)
            .findFirst();
    }
}
