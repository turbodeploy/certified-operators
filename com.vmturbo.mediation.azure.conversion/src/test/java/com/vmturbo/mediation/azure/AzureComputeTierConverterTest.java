package com.vmturbo.mediation.azure;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ComputeTierConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;

/**
 * Test class for the Azure Compute Tier class.
 */
public class AzureComputeTierConverterTest {

    private ComputeTierConverter converter;

    /**
     * Initializes instance of ComputeTierConverter.
     */
    @Before
    public void setUp() {
        converter = new AzureComputeTierConverter();
    }

    /**
     * Test for the instance disk commodities in computeTier.
     */
    @Test
    public void testInstanceDiskTypeSizeData() {
        final InstanceDiskType diskType = InstanceDiskType.SSD;
        final int numInstanceDisks = 2;
        final int instanceDiskSize = 100;
        final String computeTierId = "c1.medium";
        final CloudDiscoveryConverter cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        when(cloudDiscoveryConverter.getProfileDTO(computeTierId))
                .thenReturn(EntityProfileDTO.newBuilder()
                        .setId("1122")
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .setVmProfileDTO(VMProfileDTO.newBuilder()
                                .setInstanceDiskSize(instanceDiskSize)
                                .setNumInstanceDisks(numInstanceDisks)
                                .setInstanceDiskType(diskType)
                                .build()).build());
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(computeTierId);
        converter.convert(builder, cloudDiscoveryConverter);
        assertTrue(builder.getCommoditiesSoldList()
                .stream()
                .filter(c -> c.getCommodityType() == CommodityType.INSTANCE_DISK_TYPE)
                .count() > 0);
        assertTrue(builder.getCommoditiesSoldList()
                .stream()
                .filter(c -> c.getCommodityType() == CommodityType.INSTANCE_DISK_SIZE)
                .count() > 0);
    }

    /**
     * Test for no instance disk commodities in computeTier
     * for a VM profile with no InstanceDisk data.
     */
    @Test
    public void testNoInstanceDiskTypeSizeData() {
        final String computeTierId = "c1.medium";
        final CloudDiscoveryConverter cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        when(cloudDiscoveryConverter.getProfileDTO(computeTierId))
                .thenReturn(EntityProfileDTO.newBuilder()
                        .setId("1122")
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .setVmProfileDTO(VMProfileDTO.newBuilder()
                                .build()).build());
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(computeTierId);
        converter.convert(builder, cloudDiscoveryConverter);
        assertTrue(builder.getCommoditiesSoldList()
                .stream()
                .filter(c -> c.getCommodityType() == CommodityType.INSTANCE_DISK_TYPE)
                .count() == 0);
        assertTrue(builder.getCommoditiesSoldList()
                .stream()
                .filter(c -> c.getCommodityType() == CommodityType.INSTANCE_DISK_SIZE)
                .count() == 0);
    }
}
