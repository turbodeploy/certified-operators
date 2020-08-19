package com.vmturbo.mediation.vmware.sdk;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class VimSdkConversionProbeTest {

    private VimAccount vimAccount =
        Mockito.mock(VimAccount.class);

    private static final Path VCENTER_FILE_PATH = ResourcePath.getTestResource(
            VimSdkConversionProbeTest.class, "data/vCenter_vsphere_dc20.eng.vmturbo.com-FULL.txt");

    /**
     * Tests than conversion probe correctly returns original DTO plus creates virtual volumes.
     * @throws Exception
     */
    @Test
    public void testVirtualVolumeCreation() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(VCENTER_FILE_PATH.toString());
        VimSdkConversionProbe probe = Mockito.spy(new VimSdkConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(vimAccount);
        DiscoveryResponse newResponse = probe.discoverTarget(vimAccount);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 8 different entity types in new topology
        assertEquals(8, entitiesByType.size());

        // check each changed entity
        assertEquals(3, entitiesByType.get(EntityType.STORAGE).size());
        assertEquals(3, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(2, entitiesByType.get(EntityType.VIRTUAL_DATACENTER).size());
        assertEquals(1, entitiesByType.get(EntityType.DATACENTER).size());
        assertEquals(1, entitiesByType.get(EntityType.PHYSICAL_MACHINE).size());
        assertEquals(3, entitiesByType.get(EntityType.APPLICATION_COMPONENT).size());
        assertEquals(6, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());

        // ensure other fields are consistent with original discovery response
        verifyOtherFieldsNotModified(oldResponse, newResponse);
    }

    private void verifyOtherFieldsNotModified(@Nonnull DiscoveryResponse oldResponse,
            @Nonnull DiscoveryResponse newResponse) {
        assertEquals(oldResponse.getDiscoveredGroupList(), newResponse.getDiscoveredGroupList());
        assertEquals(oldResponse.getEntityProfileList(), newResponse.getEntityProfileList());
        assertEquals(oldResponse.getDeploymentProfileList(), newResponse.getDeploymentProfileList());
        assertEquals(oldResponse.getNotificationList(), newResponse.getNotificationList());
        assertEquals(oldResponse.getMetadataDTOList(), newResponse.getMetadataDTOList());
        assertEquals(oldResponse.getNonMarketEntityDTOList(), newResponse.getNonMarketEntityDTOList());
        assertEquals(oldResponse.getCostDTOList(), newResponse.getCostDTOList());
        assertEquals(oldResponse.getDiscoveryContext(), newResponse.getDiscoveryContext());
    }
}
