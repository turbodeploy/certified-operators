package com.vmturbo.mediation.vmware.sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class VimSdkConversionProbeTest {

    private VimAccountWithStorageBrowsingFlag vimAccount =
        Mockito.mock(VimAccountWithStorageBrowsingFlag.class);

    private static final String VCENTER_FILE_PATH = VimAccountWithStorageBrowsingFlag.class
        .getClassLoader().getResource("data/vCenter_vsphere_dc20.eng.vmturbo.com-FULL.txt")
        .getPath();

    @Test
    public void testStorageBrowsingDisabled() throws Exception {
        testVCenter(false);
    }

    @Test
    public void testStorageBrowsingEnabled() throws Exception {
        testVCenter(true);
    }

    /**
     * Test that we discover what we expect in the discovery test file and that derived target is
     * present if storage browsing flag is true and absent if it is false.
     *
     * @param isStorageBrowsingEnabled indicates whether or not storage browsing is enabled.
     * @throws Exception
     */
    private void testVCenter(boolean isStorageBrowsingEnabled) throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(VCENTER_FILE_PATH);
        VimSdkConversionProbe probe = Mockito.spy(new VimSdkConversionProbe());
        Mockito.doReturn(isStorageBrowsingEnabled).when(vimAccount).isStorageBrowsingEnabled();
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
        assertEquals(3, entitiesByType.get(EntityType.APPLICATION).size());
        assertEquals(6, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());

        // ensure other fields are consistent with original discovery response
        verifyOtherFieldsNotModified(oldResponse, newResponse, isStorageBrowsingEnabled);
    }

    private void verifyOtherFieldsNotModified(@Nonnull DiscoveryResponse oldResponse,
            @Nonnull DiscoveryResponse newResponse, boolean isStorageBrowsingEnabled) {
        assertEquals(oldResponse.getDiscoveredGroupList(), newResponse.getDiscoveredGroupList());
        assertEquals(oldResponse.getEntityProfileList(), newResponse.getEntityProfileList());
        assertEquals(oldResponse.getDeploymentProfileList(), newResponse.getDeploymentProfileList());
        assertEquals(oldResponse.getNotificationList(), newResponse.getNotificationList());
        assertEquals(oldResponse.getMetadataDTOList(), newResponse.getMetadataDTOList());
        if (isStorageBrowsingEnabled) {
            assertEquals(oldResponse.getDerivedTargetList(), newResponse.getDerivedTargetList());
        } else {
            assertEquals(0, newResponse.getDerivedTargetCount());
        }
        assertEquals(oldResponse.getNonMarketEntityDTOList(), newResponse.getNonMarketEntityDTOList());
        assertEquals(oldResponse.getCostDTOList(), newResponse.getCostDTOList());
        assertEquals(oldResponse.getDiscoveryContext(), newResponse.getDiscoveryContext());
    }
}
