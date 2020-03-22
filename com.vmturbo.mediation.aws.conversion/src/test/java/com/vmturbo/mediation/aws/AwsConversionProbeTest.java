package com.vmturbo.mediation.aws;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.aws.client.AwsAccount;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class AwsConversionProbeTest extends AwsConversionProbe {

    private AwsAccount awsAccount = Mockito.mock(AwsAccount.class);
    private DiscoveryContextDTO discoveryContext = null;

    private static final String AWS_ENGINEERING_FILE_PATH = AwsConversionProbeTest.class
        .getClassLoader().getResource("data/aws_engineering.aws.amazon.com.txt").getPath();

    private static final String AWS_ADVENG_FILE_PATH = AwsConversionProbeTest.class
        .getClassLoader().getResource("data/aws_adveng.aws.amazon.com.txt").getPath();

    @Test
    public void testEngineering() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        AwsConversionProbe probe = Mockito.spy(new AwsConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(awsAccount, discoveryContext);

        DiscoveryResponse newResponse = probe.discoverTarget(awsAccount, discoveryContext);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 14 different entity types in new topology
        assertEquals(14, entitiesByType.size());

        // check each changed entity
        assertEquals(9, entitiesByType.get(EntityType.DATABASE_SERVER).size());
        assertEquals(129, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(176, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(3, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());
        assertEquals(144, entitiesByType.get(EntityType.CLOUD_SERVICE).size());
        assertEquals(146, entitiesByType.get(EntityType.COMPUTE_TIER).size());
        assertEquals(43, entitiesByType.get(EntityType.DATABASE_SERVER_TIER).size());
        assertEquals(7, entitiesByType.get(EntityType.STORAGE_TIER).size());

        // unmodified
        assertEquals(24, entitiesByType.get(EntityType.LOAD_BALANCER).size());
        assertEquals(28, entitiesByType.get(EntityType.SERVICE).size());
        assertEquals(187, entitiesByType.get(EntityType.APPLICATION_COMPONENT).size());
        assertEquals(27, entitiesByType.get(EntityType.RESERVED_INSTANCE).size());
        assertEquals(15, entitiesByType.get(EntityType.REGION).size());
        assertEquals(45, entitiesByType.get(EntityType.AVAILABILITY_ZONE).size());

        // ensure other fields are consistent with original discovery response
        verifyOtherFieldsNotModified(oldResponse, newResponse);
    }

    @Test
    public void testAdveng() throws Exception {
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(AWS_ADVENG_FILE_PATH);
        AwsConversionProbe probe = Mockito.spy(new AwsConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(awsAccount, discoveryContext);

        DiscoveryResponse newResponse = probe.discoverTarget(awsAccount, discoveryContext);

        // check entityDTO field (new EntityDTOs created, etc.)
        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify there are 14 different entity types in new topology
        assertEquals(14, entitiesByType.size());

        // check each changed entity
        assertEquals(1, entitiesByType.get(EntityType.DATABASE_SERVER).size());
        assertEquals(15, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());
        assertEquals(16, entitiesByType.get(EntityType.VIRTUAL_VOLUME).size());
        assertEquals(1, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());
        assertEquals(144, entitiesByType.get(EntityType.CLOUD_SERVICE).size());
        assertEquals(146, entitiesByType.get(EntityType.COMPUTE_TIER).size());
        assertEquals(43, entitiesByType.get(EntityType.DATABASE_SERVER_TIER).size());
        assertEquals(7, entitiesByType.get(EntityType.STORAGE_TIER).size());
        assertEquals(15, entitiesByType.get(EntityType.REGION).size());

        // unmodified
        assertEquals(3, entitiesByType.get(EntityType.LOAD_BALANCER).size());
        assertEquals(3, entitiesByType.get(EntityType.SERVICE).size());
        assertEquals(20, entitiesByType.get(EntityType.APPLICATION_COMPONENT).size());
        assertEquals(1, entitiesByType.get(EntityType.RESERVED_INSTANCE).size());
        assertEquals(43, entitiesByType.get(EntityType.AVAILABILITY_ZONE).size());

        // ensure other fields are consistent with original discovery response
        verifyOtherFieldsNotModified(oldResponse, newResponse);

        // check that displayName field is cleared for sub account target
        assertThat(entitiesByType.get(EntityType.BUSINESS_ACCOUNT).get(0).hasDisplayName(), is(false));
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
