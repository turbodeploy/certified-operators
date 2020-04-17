package com.vmturbo.mediation.aws;

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

public class AwsCloudDiscoveryConverterTest {

    private static final String AWS_ENGINEERING_FILE_PATH = AwsCloudDiscoveryConverterTest.class
        .getClassLoader().getResource("data/aws_engineering.aws.amazon.com.txt").getPath();


    private static CloudDiscoveryConverter awsConverter;

    private static CloudProviderConversionContext awsConversionContext;

    private static Map<EntityType, List<EntityDTO>> rawEntitiesByType;

    private static Map<EntityType, List<EntityDTO.Builder>> newEntitiesByType;

    @BeforeClass
    public static void setup() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        awsConversionContext = new AwsConversionContext();
        awsConverter = new CloudDiscoveryConverter(discoveryResponse, awsConversionContext);
        awsConverter.preProcess();

        rawEntitiesByType = discoveryResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        newEntitiesByType = awsConverter.getNewEntitiesGroupedByType();
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
