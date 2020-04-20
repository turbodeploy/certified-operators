package com.vmturbo.mediation.conversion.cloud.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for {@link VirtualMachineConverter}.
 */
public class VirtualMachineConverterTest {

    private  static final String COMM_PROVIDER_ID = "commProviderId";
    private static final String REGION_ID = "testRegionId";
    private static final String VM_ID = "testVmId";
    private static final String VM_PROFILE_ID = "testVmProfileId";
    private static final String ZONE_ID = "Zone111";
    private static final String VOLUME_ID = "Volume_Ephemeral0";
    private static final String STORAGE_TIER_ID = "aws::st::SSD";
    private  CloudDiscoveryConverter cloudDiscoveryConverter;
    private VirtualMachineConverter converter;
    private VirtualMachineConverter awsConverter;

    /**
     * Initializes instance of {@link VirtualMachineConverter}.
     */
    @Before
    public void setUp() {
        converter = new VirtualMachineConverter(SDKProbeType.AZURE);
        cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        awsConverter = new VirtualMachineConverter(SDKProbeType.AWS);
    }

    /**
     * Test supported license access commodities.
     */
    @Test
    public void testLicenseCommodity() {
        final String guestOsName = "Linux 3.10.0-957.10.1.el7.x86_64";
        final EntityDTO.Builder entityToConvert = setupEntityToConvert(guestOsName);
        boolean result = converter.convert(entityToConvert, cloudDiscoveryConverter);
        assertTrue(result);
        Collection<String> licenseNames = getLicenseAccessCommodityName(entityToConvert);
        assertEquals(1, licenseNames.size());
        assertEquals("Linux", licenseNames.iterator().next());
    }

    /**
     * Test unknown license access commodities.
     */
    @Test
    public void testUnknownLicenseCommodity() {
        final String guestOsName =
            "turbonomic-opsmgr-603-854e84fc-e1d3-4392-847f-868b4c2ed137-ami-7a662f00.4";
        final EntityDTO.Builder entityToConvert = setupEntityToConvert(guestOsName);
        boolean result = converter.convert(entityToConvert, cloudDiscoveryConverter);
        assertTrue(result);
        Collection<String> licenseNames = getLicenseAccessCommodityName(entityToConvert);
        assertEquals("UNKNOWN", licenseNames.iterator().next());
    }

    /**
     * Test missing guest OS name.
     */
    @Test
    public void testMissingGuestOsName() {
        final EntityDTO.Builder entityToConvert = setupEntityToConvert(null);
        boolean result = converter.convert(entityToConvert, cloudDiscoveryConverter);
        assertTrue(result);
        Collection<String> licenseNames = getLicenseAccessCommodityName(entityToConvert);
        assertEquals("UNKNOWN", licenseNames.iterator().next());
    }

    /**
     * tests the instance store to volume conversion.
     */
    @Test
    public void testInstanceStoreConversion() {
        final EntityDTO.Builder entityToConvert = setupEntityToConvert(null);
        entityToConvert.getVirtualMachineDataBuilder().setNumEphemeralStorages(1);
        EntityDTO.Builder volume = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME)
                .setId(VOLUME_ID);
        when(cloudDiscoveryConverter.createEphemeralVolumeId(anyString(),
                anyInt(), anyString()))
                .thenReturn(VOLUME_ID);
        when(cloudDiscoveryConverter.getStorageTierId(anyString()))
                .thenReturn(STORAGE_TIER_ID);
        when(cloudDiscoveryConverter.getNewEntityBuilder(anyString())).thenReturn(volume);
        boolean result = awsConverter.convert(entityToConvert, cloudDiscoveryConverter);
        assertTrue(result);
        assertEquals(1, entityToConvert.getLayeredOverList().stream()
                .filter(id -> id.equals(VOLUME_ID)).count());
    }

    private EntityDTO.Builder setupEntityToConvert(final String guestOsName) {
        final CommodityBought commodityBought = CommodityBought.newBuilder()
            .setProviderId(COMM_PROVIDER_ID)
            .setProviderType(EntityType.PHYSICAL_MACHINE)
            .build();
        VirtualMachineData.Builder vmDataBuilder = VirtualMachineData.newBuilder();
        if (guestOsName != null) {
            vmDataBuilder.setGuestName(guestOsName);
        }
        final VirtualMachineData vmData = vmDataBuilder.build();
        final EntityDTO.Builder entityToConvert = EntityDTO.newBuilder()
            .setId(VM_ID)
            .setProfileId(VM_PROFILE_ID)
            .setVirtualMachineData(vmData)
            .addAllCommoditiesBought(Arrays.asList(commodityBought));
        when(cloudDiscoveryConverter.getProfileDTO(VM_PROFILE_ID))
            .thenReturn(EntityProfileDTO.newBuilder()
                .setId(VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .build());
        when(cloudDiscoveryConverter.getRawEntityDTO(VM_ID))
            .thenReturn(EntityDTO.newBuilder()
                .setId(VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addAllCommoditiesBought(Lists.newArrayList())
                .build());
        when(cloudDiscoveryConverter.getRawEntityDTO(VM_ID))
            .thenReturn(EntityDTO.newBuilder()
                .setId(VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addAllCommoditiesBought(Arrays.asList(commodityBought))
                .build());
        when(cloudDiscoveryConverter.getRawEntityDTO(COMM_PROVIDER_ID))
            .thenReturn(EntityDTO.newBuilder()
                .setId(COMM_PROVIDER_ID)
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .build());
        when(cloudDiscoveryConverter.getRegionIdFromAzId(COMM_PROVIDER_ID))
            .thenReturn(REGION_ID);
        return entityToConvert;
    }

    private Collection<String> getLicenseAccessCommodityName(final EntityDTO.Builder convertedEntity) {
        return convertedEntity.getCommoditiesBoughtList()
            .stream()
            .map(CommodityBought::getBoughtList)
            .flatMap(List::stream)
            .filter(commBought -> CommodityType.LICENSE_ACCESS == commBought.getCommodityType())
            .map(CommodityDTO::getKey)
            .collect(Collectors.toList());
    }
}
