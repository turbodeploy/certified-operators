package com.vmturbo.mediation.vcenter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.mediation.vmware.sdk.VimAccount;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VirtualMachineFileDescriptor;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class VimStorageBrowsingConversionProbeTest {
    private VimAccount vimAccount = Mockito.mock(VimAccount.class);
    private DiscoveryContextDTO discoveryContext = null;

    private static final String STORAGE_BROWSING_PATH = VimStorageBrowsingConversionProbeTest.class
        .getClassLoader().getResource("data/vCenter_Storage_Browsing_vsphere_dc7.eng.vmturbo.com.txt")
        .getPath();

    /**
     * Used captured data from a discovery of vsphere-dc7.eng.vmturbo.com to ensure that the
     * discovery conversion is working properly.  Check that we end up with the right number of
     * VMs, Storages, and VirtualVolumes and that the volumes have the correct sets of files
     * associated with them.
     */
    @Test
    public void testDc7() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(STORAGE_BROWSING_PATH);
        VimStorageBrowsingConversionProbe probe = Mockito.spy(new VimStorageBrowsingConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(vimAccount, discoveryContext);

        DiscoveryResponse newResponse = probe.discoverTarget(vimAccount, discoveryContext);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 3 different entity types in new topology
        assertEquals(3, entitiesByType.size());

        // check each entity type count
        assertEquals(62, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(89, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(24, entitiesByType.get(EntityType.STORAGE).size());

        // ensure other fields are consistent with original discovery response
        verifyVolumes(entitiesByType.get(EntityType.VIRTUAL_MACHINE),
                entitiesByType.get(EntityType.STORAGE),
                entitiesByType.get(EntityType.VIRTUAL_VOLUME));
    }

    /**
     * Check that we have a volume for each vm-storage pair where the vm has files from the storage
     * and that there is a volume of wasted files for each storage that has files on it and that
     * the file counts on the volumes are correct.
     *
     * @param vms list of VirtualMachine DTOs
     * @param storages list of Storage DTOs
     * @param volumes list of Volume DTOs
     */
    private void verifyVolumes(@Nonnull List<EntityDTO> vms,
                               @Nonnull List<EntityDTO> storages,
                               @Nonnull List<EntityDTO> volumes) {
        Map<String, EntityDTO> vmMap = vms.stream()
                .collect(Collectors.toMap(EntityDTO::getId, Function.identity()));
        Map<String, EntityDTO> storageMap = storages.stream()
                .collect(Collectors.toMap(EntityDTO::getId, Function.identity()));
        Map<String, EntityDTO> volumeMap = volumes.stream()
                .collect(Collectors.toMap(EntityDTO::getId, Function.identity()));
        // check that we create a volume for each vm-storage pair where the vm has files from the
        // storage
        vms.forEach(vm -> {
            vm.getVirtualMachineData().getFileList().stream()
                .map(VirtualMachineFileDescriptor::getStorageId)
                .collect(Collectors.toSet())
                .stream()
                .map(storageId -> storageMap.get(
                    AddVirtualVolumeDiscoveryConverter.STORAGE_BROWSING_ID_PREFIX + storageId))
                .forEach(storage -> checkVolume(vm, storage, volumeMap.get(
                    AddVirtualVolumeDiscoveryConverter.combineStringsForVolumeNaming(
                        vm.getId(), storage.getId()))));
        });

        // check that each storage has an associated volume for wasted files
        storages.forEach(storage -> checkWastedFilesVolume(storage, volumeMap.get(
                    AddVirtualVolumeDiscoveryConverter.combineStringsForVolumeNaming(
                        storage.getId(), AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX))));
    }

    private void checkVolume(final EntityDTO vm, final EntityDTO storage, final EntityDTO volume) {
        assertNotNull(vm);
        assertNotNull(storage);
        assertNotNull(volume);
        assertTrue(vm.getLayeredOverList().contains(volume.getId()));
        assertTrue(volume.getLayeredOverList().contains(storage.getId()));
        // check that all files between vm and storage made it to the volume
        assertEquals(vm.getVirtualMachineData().getFileList().stream()
                .filter(file -> storage.getId().equals(
                    AddVirtualVolumeDiscoveryConverter.STORAGE_BROWSING_ID_PREFIX
                        + file.getStorageId()))
                .count(),
                volume.getVirtualVolumeData().getFileCount());
    }

    private void checkWastedFilesVolume(final EntityDTO storage, final EntityDTO volume) {
        assertNotNull(storage);
        assertNotNull(volume);
        assertTrue(volume.getLayeredOverList().contains(storage.getId()));
    }
}
