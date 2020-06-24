package com.vmturbo.mediation.hyperv;

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

public class HyperVConversionProbeTest {

    private static final Path HYPERV_FILE_PATH =
        ResourcePath.getTestResource(HyperVConversionProbeTest.class,
          "data/Hyper_V_hv08_cluster1.corp.vmturbo.com-2019.07.17.19.34.29.595-FULL.txt");

    private HypervAccount hypervAccount = Mockito.mock(HypervAccount.class);

    /**
     * Test that virtual volumes are added correctly in new discovery response.
     */
    @Test
    public void testHyperVConversionProbe() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(HYPERV_FILE_PATH.toString());
        HyperVConversionProbe probe = Mockito.spy(new HyperVConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(hypervAccount);
        DiscoveryResponse newResponse = probe.discoverTarget(hypervAccount);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 8 different entity types in new topology
        assertEquals(8, entitiesByType.size());

        // check each changed entity
        assertEquals(2, entitiesByType.get(EntityType.APPLICATION_COMPONENT).size());
        assertEquals(2, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(6, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(2, entitiesByType.get(EntityType.STORAGE).size());
        assertEquals(2, entitiesByType.get(EntityType.DISK_ARRAY).size());
        assertEquals(1, entitiesByType.get(EntityType.DATACENTER).size());
        assertEquals(1, entitiesByType.get(EntityType.PHYSICAL_MACHINE).size());
        assertEquals(1, entitiesByType.get(EntityType.NETWORK).size());

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
