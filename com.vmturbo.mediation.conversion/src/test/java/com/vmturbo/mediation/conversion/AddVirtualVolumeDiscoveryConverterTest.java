package com.vmturbo.mediation.conversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class AddVirtualVolumeDiscoveryConverterTest {

    private static final Path SIMPLE_TEST_CASE = ResourcePath.getTestResource(
            AddVirtualVolumeDiscoveryConverterTest.class, "data/OneStorageOneVM");

    private static final Path STORAGE_ONLY_TEST_CASE = ResourcePath.getTestResource(
            AddVirtualVolumeDiscoveryConverterTest.class, "data/OneStorage");

    private static final Path VM_ONLY_TEST_CASE = ResourcePath.getTestResource(
            AddVirtualVolumeDiscoveryConverterTest.class, "data/OneVM");

    private static final Path STORAGE_NO_FILES_TEST_CASE = ResourcePath.getTestResource(
            AddVirtualVolumeDiscoveryConverterTest.class, "data/OneEmptyStorage");

    @Test
    public void testNormalCase() {
        final String wastedFilePath = "/File1.log";
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(SIMPLE_TEST_CASE.toString());
        AddVirtualVolumeDiscoveryConverter vimConverter =
                new AddVirtualVolumeDiscoveryConverter(discoveryResponse, true);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
                .stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(2, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(1, entitiesByType.get(EntityType.STORAGE).size());
        // check that there is only 1 wasted file in the wasted file volume
        List<EntityDTO> wastedVolumes = entitiesByType.get(EntityType.VIRTUAL_VOLUME).stream()
                .filter(vVol -> vVol.getId()
                        .endsWith(AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX))
                .collect(Collectors.toList());
        assertEquals(1, wastedVolumes.size());
        assertEquals(1, wastedVolumes.get(0).getVirtualVolumeData().getFileCount());
        assertEquals(wastedFilePath,
            wastedVolumes.get(0).getVirtualVolumeData().getFile(0).getPath());
        assertEquals(1, wastedVolumes.get(0).getLayeredOverList().size());
        assertEquals(wastedVolumes.get(0).getLayeredOver(0),
            entitiesByType.get(EntityType.STORAGE).get(0).getId());
    }

    @Test
    public void testStorageOnlyCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(STORAGE_ONLY_TEST_CASE.toString());
        AddVirtualVolumeDiscoveryConverter vimConverter =
                new AddVirtualVolumeDiscoveryConverter(discoveryResponse, true);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
                .stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertNull(entitiesByType.get(EntityType.VIRTUAL_MACHINE));
        assertEquals(1, entitiesByType.get(EntityType.STORAGE).size());
        // check that there are 3 wasted files in the volume
        EntityDTO wastedVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).get(0);
        assertTrue(wastedVolume.getId()
                .endsWith(AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX));
        assertEquals(3, wastedVolume.getVirtualVolumeData().getFileCount());
        assertEquals(1, wastedVolume.getLayeredOverList().size());
        assertEquals(wastedVolume.getLayeredOver(0), entitiesByType.get(EntityType.STORAGE).get(0).getId());
        assertEquals(1, wastedVolume.getVirtualVolumeData().getFileList().get(0).getLinkedPathsCount());
    }

    @Test
    public void testVMOnlyCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(VM_ONLY_TEST_CASE.toString());
        AddVirtualVolumeDiscoveryConverter vimConverter =
            new AddVirtualVolumeDiscoveryConverter(discoveryResponse, true);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
                .stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertNull(entitiesByType.get(EntityType.STORAGE));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        // check that there are 2 files in the volume
        EntityDTO virtualVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).get(0);
        assertFalse(virtualVolume.getId()
                .endsWith(AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX));
        assertEquals(2, virtualVolume.getVirtualVolumeData().getFileCount());
        EntityDTO virtualMachine = entitiesByType.get(EntityType.VIRTUAL_MACHINE).get(0);
        assertEquals(1, virtualMachine.getLayeredOverList().size());
        assertEquals(virtualMachine.getLayeredOver(0), virtualVolume.getId());
    }

    /**
     * Test that an empty storage (one with no files) still generates a wasted storage volume.
     */
    @Test
    public void testEmptyStorage() {
        DiscoveryResponse discoveryResponse =
            TestUtils.readResponseFromFile(STORAGE_NO_FILES_TEST_CASE.toString());
        AddVirtualVolumeDiscoveryConverter vimConverter =
            new AddVirtualVolumeDiscoveryConverter(discoveryResponse, true);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
            .stream()
            .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertNull(entitiesByType.get(EntityType.VIRTUAL_MACHINE));
        assertEquals(1, entitiesByType.get(EntityType.STORAGE).size());
        // check that there are 0 wasted files in the volume
        EntityDTO wastedVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).get(0);
        assertTrue(wastedVolume.getId()
            .endsWith(AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX));
        assertEquals(0, wastedVolume.getVirtualVolumeData().getFileCount());
        assertEquals(1, wastedVolume.getLayeredOverList().size());
        assertEquals(wastedVolume.getLayeredOver(0), entitiesByType.get(EntityType.STORAGE).get(0).getId());
    }

    /**
     * Test that networks that VMs are layered over are carried over to the {@code connected_networks}
     * field.
     */
    @Test
    public void testConnectedNetworks() {
        final String netId = "netId";
        final String netName = "netName";
        final String vmId = "vmId";
        final String vmName = "foo";
        final String nonNetId = "nonNetId";
        final EntityDTO vm = EntityDTO.newBuilder()
                                .setId(vmId)
                                .setDisplayName(vmName)
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .addLayeredOver(netId)
                                .addLayeredOver(nonNetId)
                                .build();
        final EntityDTO network = EntityDTO.newBuilder()
                                    .setId(netId)
                                    .setDisplayName(netName)
                                    .setEntityType(EntityType.NETWORK)
                                    .build();
        final DiscoveryResponse discoveryResponse = DiscoveryResponse.newBuilder()
                                                        .addEntityDTO(vm)
                                                        .addEntityDTO(network)
                                                        .build();
        final DiscoveryResponse convertedDiscoveryResponse =
                new AddVirtualVolumeDiscoveryConverter(discoveryResponse, false).convert();

        assertEquals(2, convertedDiscoveryResponse.getEntityDTOCount());

        final EntityDTO convertedVM;
        final EntityDTO convertedNetwork;
        if (convertedDiscoveryResponse.getEntityDTO(0).getId().equals(vmId)) {
            convertedVM = convertedDiscoveryResponse.getEntityDTO(0);
            convertedNetwork = convertedDiscoveryResponse.getEntityDTO(1);
        } else {
            convertedVM = convertedDiscoveryResponse.getEntityDTO(1);
            convertedNetwork = convertedDiscoveryResponse.getEntityDTO(0);
        }

        assertEquals(vmId, convertedVM.getId());
        assertEquals(vmName, convertedVM.getDisplayName());
        assertEquals(EntityType.VIRTUAL_MACHINE, convertedVM.getEntityType());
        assertEquals(0, convertedVM.getLayeredOverCount());
        assertEquals(1, convertedVM.getVirtualMachineData().getConnectedNetworkCount());
        assertEquals(netName, convertedVM.getVirtualMachineData().getConnectedNetwork(0));

        assertEquals(netId, convertedNetwork.getId());
        assertEquals(netName, convertedNetwork.getDisplayName());
        assertEquals(EntityType.NETWORK, convertedNetwork.getEntityType());
    }
}
