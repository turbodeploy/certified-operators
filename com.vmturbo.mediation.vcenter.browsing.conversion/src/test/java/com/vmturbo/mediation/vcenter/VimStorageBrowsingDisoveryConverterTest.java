package com.vmturbo.mediation.vcenter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

public class VimStorageBrowsingDisoveryConverterTest {
    private static final String SIMPLE_TEST_CASE =
            "src/test/resources/data/OneStorageOneVM";

    private static final String STORAGE_ONLY_TEST_CASE =
            "src/test/resources/data/OneStorage";

    private static final String VM_ONLY_TEST_CASE =
            "src/test/resources/data/OneVM";

    @Test
    public void testNormalCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(SIMPLE_TEST_CASE);
        VimStorageBrowsingDiscoveryConverter vimConverter =
                new VimStorageBrowsingDiscoveryConverter(discoveryResponse);
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
                        .endsWith(VimStorageBrowsingDiscoveryConverter.WASTED_VOLUMES_SUFFIX))
                .findFirst();
        assertTrue(wastedVolume.isPresent());
        assertEquals(3, wastedVolume.get().getVirtualVolumeData().getFileCount());
        assertEquals(1, wastedVolume.get().getLayeredOverList().size());
        assertTrue(wastedVolume.get().getLayeredOver(0)
                .equals(entitiesByType.get(EntityType.STORAGE).get(0).getId()));
    }

    @Test
    public void testStorageOnlyCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(STORAGE_ONLY_TEST_CASE);
        VimStorageBrowsingDiscoveryConverter vimConverter =
                new VimStorageBrowsingDiscoveryConverter(discoveryResponse);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
                .stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertTrue(entitiesByType.get(EntityType.VIRTUAL_MACHINE) == null);
        assertEquals(1, entitiesByType.get(EntityType.STORAGE).size());
        // check that there are 3 wasted files in the volume
        EntityDTO wastedVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).get(0);
        assertTrue(wastedVolume.getId()
                .endsWith(VimStorageBrowsingDiscoveryConverter.WASTED_VOLUMES_SUFFIX));
        assertEquals(3, wastedVolume.getVirtualVolumeData().getFileCount());
        assertEquals(1, wastedVolume.getLayeredOverList().size());
        assertTrue(wastedVolume.getLayeredOver(0)
                .equals(entitiesByType.get(EntityType.STORAGE).get(0).getId()));
    }

    @Test
    public void testVMOnlyCase() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(VM_ONLY_TEST_CASE);
        VimStorageBrowsingDiscoveryConverter vimConverter =
                new VimStorageBrowsingDiscoveryConverter(discoveryResponse);
        DiscoveryResponse convertedResponse = vimConverter.convert();
        Map<EntityType, List<EntityDTO>> entitiesByType = convertedResponse.getEntityDTOList()
                .stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertTrue(entitiesByType.get(EntityType.STORAGE) == null);
        assertEquals(1, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        // check that there are 2 files in the volume
        EntityDTO virtualVolume = entitiesByType.get(EntityType.VIRTUAL_VOLUME).get(0);
        assertFalse(virtualVolume.getId()
                .endsWith(VimStorageBrowsingDiscoveryConverter.WASTED_VOLUMES_SUFFIX));
        assertEquals(2, virtualVolume.getVirtualVolumeData().getFileCount());
        EntityDTO virtualMachine = entitiesByType.get(EntityType.VIRTUAL_MACHINE).get(0);
        assertEquals(1, virtualMachine.getLayeredOverList().size());
        assertTrue(virtualMachine.getLayeredOver(0)
                .equals(virtualVolume.getId()));
    }
}
