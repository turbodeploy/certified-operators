package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

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
 * Set capacity for the storage access commodity sold by virtual volumes.
 * Priority (different from storage):
 * - first discovered by the probes - some probes may explicitly discover capacity per-vhd
 * - then propagated from the provider
 * - then per-entity policy value
 * - then policy default
 */
public class VolumeEntityAccessCapacityPostStitchingOperation
                extends BaseEntityCapacityPostStitchingOperation implements PostStitchingOperation {
    @Override
    public StitchingScope<TopologyEntity>
                    getScope(StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_VOLUME);
    }

    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(Stream<TopologyEntity> entities,
                    EntitySettingsCollection settingsCollection,
                    EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities
            .forEach(volume -> {
                Optional<Double> providerCapacity = getProviderCapacity(volume.getProviders(),
                                CommodityType.STORAGE_ACCESS_VALUE,
                                ImmutableSet.of(EntityType.STORAGE_VALUE));
                setCapacity(resultBuilder, volume, CommodityType.STORAGE_ACCESS_VALUE, false,
                                providerCapacity.orElse(getNumericPolicyValue(volume, settingsCollection,
                                                EntitySettingSpecs.IOPSCapacity)));
        });
        return resultBuilder.build();
    }

}
