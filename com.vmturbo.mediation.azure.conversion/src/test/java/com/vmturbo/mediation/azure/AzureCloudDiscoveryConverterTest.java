package com.vmturbo.mediation.azure;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.BeforeClass;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class AzureCloudDiscoveryConverterTest {

    private static final String AZURE_ENGINEERING_FILE_PATH = AzureCloudDiscoveryConverterTest.class
        .getClassLoader().getResource("data/azure_engineering.management.core.windows.net.txt")
        .getPath();

    private static CloudDiscoveryConverter azureConverter;

    private static CloudProviderConversionContext azureConversionContext;

    private static Map<EntityType, List<EntityDTO>> rawEntitiesByType;

    @BeforeClass
    public static void setup() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        azureConversionContext = new AzureConversionContext();
        azureConverter = new CloudDiscoveryConverter(discoveryResponse, azureConversionContext);
        azureConverter.preProcess();

        rawEntitiesByType = discoveryResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
    }

    /**
     * Verify that some fields in EntityDTO are not modified.
     */
    private void verifyUnmodifiedFields(EntityDTO oldEntity, EntityDTO.Builder newEntity) {
        assertEquals(oldEntity.getDisplayName(), newEntity.getDisplayName());
        assertEquals(oldEntity.getCommoditiesSoldList(), newEntity.getCommoditiesSoldList());
        assertEquals(oldEntity.getProfileId(), newEntity.getProfileId());
        assertEquals(oldEntity.getEntityPropertiesList(), newEntity.getEntityPropertiesList());
        assertEquals(oldEntity.getPowerState(), newEntity.getPowerState());
        assertEquals(oldEntity.getConsumerPolicy(), newEntity.getConsumerPolicy());

        if (oldEntity.getEntityType() == EntityType.VIRTUAL_MACHINE) {
            assertEquals(oldEntity.getVirtualMachineData(), newEntity.getVirtualMachineData());
        } else if (oldEntity.getEntityType() == EntityType.DATABASE) {
            assertEquals(oldEntity.getApplicationData(), newEntity.getApplicationData());
        }
    }
}
