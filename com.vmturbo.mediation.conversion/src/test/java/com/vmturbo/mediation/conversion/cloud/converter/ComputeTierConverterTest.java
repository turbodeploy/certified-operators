package com.vmturbo.mediation.conversion.cloud.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.LicenseMapEntry;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for ComputeTierConverter.
 */
public class ComputeTierConverterTest {

    private ComputeTierConverter converter;

    /**
     * Initializes instance of ComputeTierConverter.
     */
    @Before
    public void setUp() {
        converter = new ComputeTierConverter(SDKProbeType.AZURE);
    }

    /**
     * Test that computeTier family is set from the VMProfileDTO instanceSizeFamily field.
     */
    @Test
    public void testInstanceSizeFamilySet() {
        final String family = "m4";
        final String quotaFamily = "standardDFamily";
        final String computeTierId = family + ".small";
        final CloudDiscoveryConverter cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        when(cloudDiscoveryConverter.getProfileDTO(computeTierId))
                .thenReturn(EntityProfileDTO.newBuilder()
                        .setId("1122")
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .setVmProfileDTO(VMProfileDTO.newBuilder()
                            .setInstanceSizeFamily(family)
                            .setQuotaFamily(quotaFamily)
                            .build()).build());
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(computeTierId);
        converter.convert(builder, cloudDiscoveryConverter);
        assertEquals(family, builder.getComputeTierData().getFamily());
        assertEquals(quotaFamily, builder.getComputeTierData().getQuotaFamily());
    }

    /**
     * Test license commodity types.
     */
    @Test
    public void testLicenseCommodity() {
        final CloudDiscoveryConverter cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        final String computeTierId = "testComputeTier1";
        final LicenseMapEntry license1 = LicenseMapEntry.newBuilder().setRegion("region 1")
            .addLicenseName("linux")
            .addLicenseName("linux sql enterprise")
            .build();
        final LicenseMapEntry license2 = LicenseMapEntry.newBuilder().setRegion("region 2")
            .addLicenseName("windows server")
            .build();
        when(cloudDiscoveryConverter.getProfileDTO(computeTierId))
            .thenReturn(EntityProfileDTO.newBuilder()
                .setId("1122")
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setVmProfileDTO(VMProfileDTO.newBuilder()
                    .addAllLicense(Arrays.asList(license1, license2))
                    .build()).build());
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(computeTierId);
        final boolean result = converter.convert(builder, cloudDiscoveryConverter);
        assertTrue(result);
        Collection<String> licenseNames = builder.getCommoditiesSoldList().stream().filter(
            commodityDTO -> CommodityType.LICENSE_ACCESS == commodityDTO.getCommodityType())
            .map(CommodityDTO::getKey)
            .collect(Collectors.toList());
        assertEquals(3, licenseNames.size());
        assertThat(licenseNames, containsInAnyOrder("Linux", "Linux_SQL_Server_Enterprise",
            "Windows"));
    }

    /**
     * Test for the instance store data in computeTier data.
     */
    @Test
    public void testInstanceStoreData() {
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
        assertEquals(diskType, builder.getComputeTierData().getInstanceDiskType());
        assertEquals(numInstanceDisks, builder.getComputeTierData().getNumInstanceDisks());
        assertEquals(instanceDiskSize, builder.getComputeTierData().getInstanceDiskSizeGb());
    }

    /**
     * Checks that AWS profile should be connected to all storage tiers.
     */
    @Test
    public void checkAwsProfileConnectedToAllStorageTiers() {
        converter = new ComputeTierConverter(SDKProbeType.AWS);
        final CloudDiscoveryConverter cloudDiscoveryConverter = Mockito.mock(CloudDiscoveryConverter.class);
        final String storageid1 = "storageid1";
        final Collection<String> allStorageTierIds = ImmutableList.of(storageid1, "storageid2");
        Mockito.when(cloudDiscoveryConverter.getAllStorageTierIds())
                        .thenReturn(ImmutableSet.copyOf(allStorageTierIds));
        final String profileId = "i3.4xlarge";
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(profileId);
        final EntityProfileDTO entityProfileDto = EntityProfileDTO.newBuilder().setId(profileId)
                        .setVmProfileDTO(VMProfileDTO.newBuilder().setDiskType(storageid1).build())
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .build();
        Mockito.when(cloudDiscoveryConverter.getProfileDTO(profileId))
                        .thenReturn(entityProfileDto);
        converter.convert(builder, cloudDiscoveryConverter);
        assertEquals(allStorageTierIds, builder.getLayeredOverList());
    }
}
