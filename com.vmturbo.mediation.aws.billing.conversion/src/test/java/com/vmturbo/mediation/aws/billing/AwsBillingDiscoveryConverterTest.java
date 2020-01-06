package com.vmturbo.mediation.aws.billing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Test {@link AwsBillingDiscoveryConverter}
 */
public class AwsBillingDiscoveryConverterTest {

    private static final String AWS_BILLING_FILE_PATH =
        "data/aws_billing_engineering.aws.amazon.com_billing.txt";

    @Test
    public void testConvert() {
        URL file = getClass().getClassLoader().getResource(AWS_BILLING_FILE_PATH);
        DiscoveryResponse oldResponse = TestUtils.readResponseFromFile(file.getPath());
        AwsBillingDiscoveryConverter converter = new AwsBillingDiscoveryConverter(oldResponse);
        DiscoveryResponse newResponse = converter.convert();
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

        // verify flags set correctly
        entitiesByType.get(EntityType.VIRTUAL_MACHINE).forEach(vm -> {
            assertEquals(EntityOrigin.PROXY, vm.getOrigin());
            assertFalse(vm.getKeepStandalone());
            assertFalse(vm.getCommoditiesBoughtList().stream()
                    .map(CommodityBought::getBoughtList).flatMap(Collection::stream)
            .anyMatch(commodity -> commodity.getCommodityType() == CommodityType.COUPON));
        });
    }
}
