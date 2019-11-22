package com.vmturbo.mediation.azure.volumes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.azure.AzureAccount;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;

/**
 * A class to test {@link AzureVolumesConversionProbe}.
 */
public class AzureVolumesConversionProbeTest extends AzureVolumesConversionProbe {

    private AzureAccount azureAccount = Mockito.mock(AzureAccount.class);

    private static final String AZURE_ENGINEERING_WASTED_VOLUMES_FILE_PATH =
        AzureVolumesConversionProbeTest.class.getClassLoader().getResource(
        "data/azure_wasted_volumes_engineering.management.core.windows.net.txt").getPath();

    private static final AzureVolumesProbe AZURE_VOLUMES_PROBE = new AzureVolumesProbe();

    private static final Set<TemplateDTO> AZURE_VOLUMES_PROBE_SUPPLY_CHAIN_DEFINITION =
            AZURE_VOLUMES_PROBE.getSupplyChainDefinition();

    // List of cloud entity types, which don't exist in original Azure volumes probe discovery
    // response, including the original Azure volumes probe entity types.
    private static final Set<EntityType> AZURE_VOLUMES_CONVERSION_PROBE_ENTITY_TYPES =
            TestUtils.getCloudEntityTypes(
                    AZURE_VOLUMES_PROBE_SUPPLY_CHAIN_DEFINITION, NEW_NON_SHARED_ENTITY_TYPES
            );

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

    @Test
    public void testGetSupplyChainDefinition() {
        AzureVolumesConversionProbe volumesConversionProbe = new AzureVolumesConversionProbe();
        AzureVolumesProbe volumesProbe = new AzureVolumesProbe();

        Set<TemplateDTO> entitiesInSupplyChain = volumesConversionProbe.getSupplyChainDefinition();
        Set<EntityType> entitiesWithMergeData =
                getEntityTypesWithMergedEntityMetaData(entitiesInSupplyChain);

        // Verify that merged entity meta data are created correctly to entity types who require it.
        assertEquals(Sets.union(NEW_NON_SHARED_ENTITY_TYPES,
                getEntityTypesWithMergedEntityMetaData(volumesProbe.getSupplyChainDefinition())),
                entitiesWithMergeData);

        assertTrue(
                TestUtils.verifyEntityTypes(entitiesInSupplyChain,
                        AZURE_VOLUMES_CONVERSION_PROBE_ENTITY_TYPES)
        );
    }

    private Set<EntityType> getEntityTypesWithMergedEntityMetaData(Set<TemplateDTO> supplyChain) {
        return supplyChain.stream()
                .filter(TemplateDTO::hasMergedEntityMetaData)
                .map(TemplateDTO::getTemplateClass)
                .collect(Collectors.toSet());
    }
}
