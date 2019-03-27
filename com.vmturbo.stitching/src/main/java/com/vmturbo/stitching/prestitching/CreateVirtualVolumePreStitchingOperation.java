package com.vmturbo.stitching.prestitching;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.SetUtils;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntityToAdd;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * Prestitching operation that creates a VirtualVolume entity for each combination of VirtualMachine
 * and Storage, and sets up the relationship so that VM is connected to VirtualVolume, and
 * VirtualVolume is connected to Storage. This is added to support the "Lock VMs to Datastores"
 * feature in classic.
 *
 * Note: Currently this applies to all Hypervisor probes.
 */
public class CreateVirtualVolumePreStitchingOperation implements PreStitchingOperation {

    /**
     * A prefix we attach to the beginning of the VirtualVolume ID or displayName
     */
    public final static String VIRTUAL_VOLUME_ID_PREFIX = "VirtualVolume-";

    /**
     * The prefix that the VimStorageBrowsing probe attaches to VM and Storage IDs in the ID field
     * of the EntityDTO. This is needed to ensure the id generated here is same as that from vc
     * storage browsing probe.
     */
    public final static String STORAGE_BROWSING_ID_PREFIX = "dsb-";

    /**
     * The separator to use when generating id or displayName for volume
     */
    public final static String ID_SEPARATOR = "-";

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // Apply this calculation to all vms discovered by Hypervisor probes
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.HYPERVISOR,
                EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(@Nonnull Stream<StitchingEntity> entities,
                                                                  @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        List<EntityToAdd> volumesToAdd = new ArrayList<>();
        entities.forEach(vm ->
            vm.getProviders().stream()
                .filter(provider -> provider.getEntityType() == EntityType.STORAGE)
                .filter(storage -> !hasConnectedVolume(vm, storage))
                .forEach(storage -> {
                    // create volume for each combination of vm and storage
                    final EntityDTO volume = createVirtualVolume(vm, storage);
                    volumesToAdd.add(new EntityToAdd(volume, vm, storage, ConnectionType.NORMAL_CONNECTION));
                })
        );
        resultBuilder.queueEntitiesAddition(volumesToAdd);
        return resultBuilder.build();
    }

    /**
     * Create a new VirtualVolume EntityDTO for the given vm entity and storage entity.
     *
     * @param vm the vm entity for which to create volume
     * @param storage the storage for which to create volume
     * @return new VirtualVolume EntityDTO
     */
    public EntityDTO createVirtualVolume(@Nonnull StitchingEntity vm,
                                         @Nonnull StitchingEntity storage) {
        final EntityDTO.Builder vmBuilder = vm.getEntityBuilder();
        final EntityDTO.Builder stBuilder = storage.getEntityBuilder();

        return EntityDTO.newBuilder()
            .setId(generateVolumeId(vmBuilder.getId(), stBuilder.getId()))
            .setDisplayName(generateVolumeDisplayName(vmBuilder.getDisplayName(), stBuilder.getDisplayName()))
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .build();
    }

    /**
     * Check if there is already a connected volume for the given combination of vm and storage.
     *
     * @param vm the virtual machine entity for which to get attached volume
     * @param storage the storage to for which to get connected volume
     * @return true if the vm is already connected to a volume and the volume is connected to
     * the given storage, false if not
     */
    private boolean hasConnectedVolume(@Nonnull StitchingEntity vm,
                                       @Nonnull StitchingEntity storage) {
        return SetUtils.emptyIfNull(vm.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION)).stream()
            .filter(c -> c.getEntityType() == EntityType.VIRTUAL_VOLUME)
            .flatMap(volume -> SetUtils.emptyIfNull(volume.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION)).stream())
            .filter(c -> c.getEntityType() == EntityType.STORAGE)
            .anyMatch(st -> st.getOid() == storage.getOid());
    }

    /**
     * Generate id for the new volume entity. It is a combination of prefix, vm id and storage id.
     * This is following the volume id convention in vc storage browsing probe. It should be
     * consistent with that from vc storage browsing probe, to ensure stitching happens correctly.
     *
     * @param vmId id of the vm, which is connected the volume
     * @param stId id of the storage, which the volume is connected to
     * @return id of the new volume entity
     */
    private static String generateVolumeId(@Nonnull String vmId, @Nonnull String stId) {
        return VIRTUAL_VOLUME_ID_PREFIX + STORAGE_BROWSING_ID_PREFIX + vmId + ID_SEPARATOR +
            STORAGE_BROWSING_ID_PREFIX + stId;
    }

    /**
     * Generate displayName for the new volume entity. It is a combination of prefix, vm displayName
     * and storage displayName. This is following the volume naming convention in vc storage
     * browsing probe.
     *
     * @param vmDisplayName displayName of the vm, which is connected the volume
     * @param stDisplayName displayName of the storage, which the volume is connected to
     * @return displayName of the new volume entity
     */
    private static String generateVolumeDisplayName(@Nonnull String vmDisplayName, @Nonnull String stDisplayName) {
        return VIRTUAL_VOLUME_ID_PREFIX + vmDisplayName + ID_SEPARATOR + stDisplayName;
    }
}
