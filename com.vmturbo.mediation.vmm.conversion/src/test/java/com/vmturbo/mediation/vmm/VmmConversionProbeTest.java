package com.vmturbo.mediation.vmm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.mediation.vmm.sdk.VmmAccount;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class VmmConversionProbeTest {

    private static final String VMM_FILE_PATH = VmmConversionProbeTest.class.getClassLoader()
        .getResource("data/VMM_hp_dl390.corp.vmturbo.com-2019.07.17.19.33.00.703-FULL.txt")
        .getPath();

    private VmmAccount vmmAccount = Mockito.mock(VmmAccount.class);

    /**
     * Test that virtual volumes are added correctly in new discovery response.
     */
    @Test
    public void testVmmConversionProbe() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(VMM_FILE_PATH);
        VmmConversionProbe probe = Mockito.spy(new VmmConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(vmmAccount);
        DiscoveryResponse newResponse = probe.discoverTarget(vmmAccount);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 8 different entity types in new topology
        assertEquals(10, entitiesByType.size());

        // check each changed entity
        assertEquals(66, entitiesByType.get(EntityType.APPLICATION).size());
        assertEquals(66, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(140, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(17, entitiesByType.get(EntityType.STORAGE).size());
        assertEquals(17, entitiesByType.get(EntityType.DISK_ARRAY).size());
        assertEquals(2, entitiesByType.get(EntityType.DATACENTER).size());
        assertEquals(5, entitiesByType.get(EntityType.PHYSICAL_MACHINE).size());
        assertEquals(5, entitiesByType.get(EntityType.NETWORK).size());
        assertEquals(5, entitiesByType.get(EntityType.VIRTUAL_DATACENTER).size());
        assertEquals(2, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());

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
        assertEquals(newResponse.getDerivedTargetList(), newResponse.getDerivedTargetList());
        assertEquals(oldResponse.getNonMarketEntityDTOList(), newResponse.getNonMarketEntityDTOList());
        assertEquals(oldResponse.getCostDTOList(), newResponse.getCostDTOList());
        assertEquals(oldResponse.getDiscoveryContext(), newResponse.getDiscoveryContext());
    }
}
