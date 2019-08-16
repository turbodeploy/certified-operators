package com.vmturbo.mediation.conversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class AddVirtualVolumeDiscoveryConverterTest {

    private static final String SIMPLE_TEST_CASE = AddVirtualVolumeDiscoveryConverterTest.class
        .getClassLoader().getResource("data/OneStorageOneVM").getPath();

    private static final String STORAGE_ONLY_TEST_CASE = AddVirtualVolumeDiscoveryConverterTest.class
        .getClassLoader().getResource("data/OneStorage").getPath();

    private static final String VM_ONLY_TEST_CASE = AddVirtualVolumeDiscoveryConverterTest.class
        .getClassLoader().getResource("data/OneVM").getPath();

    @Test
    public void testNormalCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(SIMPLE_TEST_CASE);
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
        Optional<EntityDTO> wastedVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).stream()
                .filter(vVol -> vVol.getId()
                        .endsWith(AddVirtualVolumeDiscoveryConverter.WASTED_VOLUMES_SUFFIX))
                .findFirst();
        assertTrue(wastedVolume.isPresent());
        assertEquals(3, wastedVolume.get().getVirtualVolumeData().getFileCount());
        assertEquals(1, wastedVolume.get().getLayeredOverList().size());
        assertEquals(wastedVolume.get().getLayeredOver(0),
            entitiesByType.get(EntityType.STORAGE).get(0).getId());
    }

    @Test
    public void testStorageOnlyCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(STORAGE_ONLY_TEST_CASE);
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
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(VM_ONLY_TEST_CASE);
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
}
