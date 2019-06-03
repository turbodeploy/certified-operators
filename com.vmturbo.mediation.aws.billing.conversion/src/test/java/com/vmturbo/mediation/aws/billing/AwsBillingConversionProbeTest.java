package com.vmturbo.mediation.aws.billing;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.mediation.cloud.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Test {@link AwsBillingConversionProbe}
 */
public class AwsBillingConversionProbeTest {

    private AwsAccount awsAccount = Mockito.mock(AwsAccount.class);
    private DiscoveryContextDTO discoveryContext = null;

    private static final String AWS_BILLING_FILE_PATH =
        "data/aws_billing_engineering.aws.amazon.com_billing.txt";

    @Test
    public void testDiscovery() throws Exception {
        URL file = getClass().getClassLoader().getResource(AWS_BILLING_FILE_PATH);
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(file.getPath());
        AwsBillingConversionProbe probe = Mockito.spy(new AwsBillingConversionProbe());
        Mockito.doReturn(oldResponse).when(probe).getRawDiscoveryResponse(awsAccount,
            discoveryContext);

        DiscoveryResponse newResponse = probe.discoverTarget(awsAccount, discoveryContext);
        Assert.assertNotNull(newResponse);

        Map<EntityType, List<EntityDTO>> entitiesByType = newResponse.getEntityDTOList().stream()
            .collect(Collectors.groupingBy(EntityDTO::getEntityType));

        // verify VMs are part of top level EntityDTO
        assertEquals(2, entitiesByType.size());

        assertEquals(4, entitiesByType.get(EntityType.BUSINESS_ACCOUNT).size());
        assertEquals(143, entitiesByType.get(EntityType.VIRTUAL_MACHINE).size());

        // verify classic AwsBillingProbe response is not broken
        List<EntityDTO> vmList = Lists.newLinkedList();
        newResponse.getNonMarketEntityDTOList().forEach(nonMarketEntityDTO -> vmList.addAll(
            nonMarketEntityDTO.getCloudServiceData().getBillingData().getVirtualMachinesList()));
        Assert.assertEquals(143, vmList.size());
    }
}
