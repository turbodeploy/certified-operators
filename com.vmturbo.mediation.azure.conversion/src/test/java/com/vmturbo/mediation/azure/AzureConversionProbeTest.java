package com.vmturbo.mediation.azure;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class AzureConversionProbeTest extends AzureConversionProbe {

    private AzureAccount azureAccount = Mockito.mock(AzureAccount.class);
    private DiscoveryContextDTO discoveryContext = null;

    private static final String AZURE_ENGINEERING_FILE_PATH = AzureConversionProbeTest.class
        .getClassLoader().getResource("data/azure_engineering.management.core.windows.net.txt")
        .getPath();

    private static final String AZURE_PRODUCTMGMT_FILE_PATH = AzureConversionProbeTest.class
        .getClassLoader().getResource("data/azure_productmgmt.management.core.windows.net.txt")
        .getPath();

    @Test
    public void testEngineering() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        AzureConversionProbe probe = Mockito.spy(new AzureConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(azureAccount, discoveryContext);
        DiscoveryResponse newResponse = probe.discoverTarget(azureAccount, discoveryContext);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 10 different entity types in new topology
        assertEquals(10, entitiesByType.size());

        // check each changed entity
        assertEquals(2, entitiesByType.get(EntityType.DATABASE).size());
        assertEquals(44, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(91, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(1, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());
        assertEquals(119, entitiesByType.get(EntityType.CLOUD_SERVICE).size());
        assertEquals(216, entitiesByType.get(EntityType.COMPUTE_TIER).size());
        assertEquals(19, entitiesByType.get(EntityType.DATABASE_TIER).size());
        assertEquals(4, entitiesByType.get(EntityType.STORAGE_TIER).size());
        assertEquals(30, entitiesByType.get(EntityType.REGION).size());

        // unmodified
        assertEquals(46, entitiesByType.get(EntityType.APPLICATION).size());

        // ensure other fields are consistent with original discovery response
        verifyOtherFieldsNotModified(oldResponse, newResponse);
    }

    @Test
    public void testProductmgmt() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(AZURE_PRODUCTMGMT_FILE_PATH);
        AzureConversionProbe probe = Mockito.spy(new AzureConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(azureAccount, discoveryContext);
        DiscoveryResponse newResponse = probe.discoverTarget(azureAccount, discoveryContext);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 9 different entity types in new topology
        assertEquals(9, entitiesByType.size());

        // check each changed entity
        assertEquals(6, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(6, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(1, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());
        assertEquals(119, entitiesByType.get(EntityType.CLOUD_SERVICE).size());
        assertEquals(210, entitiesByType.get(EntityType.COMPUTE_TIER).size());
        assertEquals(19, entitiesByType.get(EntityType.DATABASE_TIER).size());
        assertEquals(4, entitiesByType.get(EntityType.STORAGE_TIER).size());
        assertEquals(30, entitiesByType.get(EntityType.REGION).size());

        // unmodified
        assertEquals(6, entitiesByType.get(EntityType.APPLICATION).size());

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
