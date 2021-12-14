package com.vmturbo.stitching.poststitching;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
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
 * Class supporting post-stitching operation for the purpose of setting StorageAccess or
 * StorageLatency capacity for Storage entities.
 */
public class StorageEntityIopsOrLatencyCapacityPostStitchingOp
        extends BaseEntityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final CommodityType commodity;

    private final EntitySettingSpecs setting;

    /**
     * Create a new StorageEntityIopsOrLatencyCapacityPostStitchingOp.
     *
     * @param commodityType the type of commodity to set capacities for (StorageAccess or
     *         StorageLatency)
     * @param setting the setting spec related to the commodity
     */
    public StorageEntityIopsOrLatencyCapacityPostStitchingOp(CommodityType commodityType,
            EntitySettingSpecs setting) {
        this.commodity = commodityType;
        this.setting = setting;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(storage -> {
            // If there is a user setting for the commodity's capacity then use it
            Optional<Setting> entitySetting =
                    settingsCollection.getEntityUserSetting(storage, setting);
            if (entitySetting.isPresent()) {
                setCapacity(resultBuilder, storage, commodity.getNumber(), true,
                        entitySetting.get().getNumericSettingValue().getValue());
                return;
            }

            // Continue only if there is no preexisting capacity
            Optional<Double> preexistingCapacity = getCapacity(storage, commodity);
            if (preexistingCapacity.isPresent()) {
                return;
            }

            // We always start with the closest provider
            Optional<Double> parentPoolCapacity =
                    getProviderCapacity(storage.getProviders(), commodity.getNumber(),
                            Collections.singleton(EntityType.LOGICAL_POOL_VALUE));

            // If the capacity is not specified on the LP then check the DA
            Optional<Double> providerCapacity;
            if (!parentPoolCapacity.isPresent()) {
                providerCapacity =
                        getProviderCapacity(storage.getProviders(), commodity.getNumber(),
                                Collections.singleton(EntityType.DISK_ARRAY_VALUE));
            } else {
                providerCapacity = parentPoolCapacity;
            }

            // Finally, if we cannot find a capacity then use the default setting
            double capacity = providerCapacity.orElseGet(() -> (double)setting
                    .getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault());
            setCapacity(resultBuilder, storage, commodity.getNumber(), true, capacity);
        });
        return resultBuilder.build();
    }
}