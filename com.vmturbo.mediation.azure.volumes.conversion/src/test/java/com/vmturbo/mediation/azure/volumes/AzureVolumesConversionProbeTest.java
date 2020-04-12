package com.vmturbo.mediation.azure.volumes;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.azure.AzureAccount;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * A class to test {@link AzureVolumesConversionProbe}.
 */
public class AzureVolumesConversionProbeTest extends AzureVolumesConversionProbe {

    private AzureAccount azureAccount = Mockito.mock(AzureAccount.class);

    private static final String AZURE_ENGINEERING_WASTED_VOLUMES_FILE_PATH =
        AzureVolumesConversionProbeTest.class.getClassLoader().getResource(
        "data/azure_wasted_volumes_engineering.management.core.windows.net.txt").getPath();

    @Test
    public void testEngineering() throws Exception {
        DiscoveryResponse oldResponse =
            TestUtils.readResponseFromFile(AZURE_ENGINEERING_WASTED_VOLUMES_FILE_PATH);
        AzureVolumesConversionProbe probe = Mockito.spy(new AzureVolumesConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(azureAccount);
        DiscoveryResponse newResponse = probe.discoverTarget(azureAccount);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 10 different entity types in new topology
        assertEquals(3, entitiesByType.size());

        // check each changed entity
        assertEquals(27, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(2, entitiesByType.get(EntityType.STORAGE_TIER).size());
        assertEquals(12, entitiesByType.get(EntityType.REGION).size());


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
        assertEquals(oldResponse.getDerivedTargetList(), newResponse.getDerivedTargetList());
        assertEquals(oldResponse.getNonMarketEntityDTOList(), newResponse.getNonMarketEntityDTOList());
        assertEquals(oldResponse.getCostDTOList(), newResponse.getCostDTOList());
        assertEquals(oldResponse.getDiscoveryContext(), newResponse.getDiscoveryContext());
    }
}
