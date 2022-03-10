package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation used as a short term workaround for OM-33440.
 *
 * We need this for now because in HyperV and VMM there can be VMs that are not part of a cluster.
 * Right now the probe is not creating any cluster commodity for them, so those VMs are free to
 * shop around without constraint (which means that they can also go from HyperV to VC).
 * In order to prevent that, classic is doing some processing on them, in order to flag them as
 * not movable. Here we are doing the same.
 *
 * This hack should be removed when the probes are dealing with it correctly.
 */
public class SetMovableFalseForHyperVAndVMMNotClusteredVmsOperation implements PostStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {

        // this operations applies to HyperV and VMM probes only
        final Set<String> probeTypes = ImmutableSet.of(
            SDKProbeType.HYPERV.getProbeType(),
            SDKProbeType.VMM.getProbeType());

        return stitchingScopeFactory.multiProbeEntityTypeScope(probeTypes, EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull final Stream<TopologyEntity> entities,
                     @Nonnull final EntitySettingsCollection settingsCollection,
                     @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        // iterate over every entity
        entities.forEach(entity -> {

            final TopologyEntityImpl entityBuilder = entity.getTopologyEntityImpl();

            // filter commodities bought from a PM
            final Optional<CommoditiesBoughtFromProviderImpl> commoditiesBoughtFromProviderOptional =
                    entityBuilder.getCommoditiesBoughtFromProvidersImplList().stream()
                            .filter(commBoughtFromProv -> EntityType.PHYSICAL_MACHINE_VALUE == commBoughtFromProv.getProviderEntityType())
                            .findFirst();

            commoditiesBoughtFromProviderOptional.ifPresent(commoditiesBoughtFromProvider -> {

                // check if the cluster commodity is present
                final boolean isClusteredVm = commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                        .anyMatch(commodityBought -> CommodityType.CLUSTER_VALUE ==
                                commodityBought.getCommodityType().getType());

                // if not a clustered vm, we need to change movable to false
                if (!isClusteredVm) {
                    resultBuilder.queueUpdateEntityAlone(entity, entityToUpdate -> {
                        commoditiesBoughtFromProvider.setMovable(false);
                    });
                }

            });
        });
        return resultBuilder.build();
    }
}