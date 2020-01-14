package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

public class VirtualMachineInfoMapperTest {

    private static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    private static final String CONNECTED_NETWORK_NAME_2 = "network 2";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST = ImmutableList.of(
        CONNECTED_NETWORK_NAME_1, CONNECTED_NETWORK_NAME_2);

    private EntityDTOOrBuilder createEntityDTOBuilder(EntityDTO.LicenseModel licenseModel) {
        return EntityDTO.newBuilder()
                .setVirtualMachineData(VirtualMachineData.newBuilder()
                        .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                        .addAllConnectedNetwork(CONNECTED_ENTITY_NAME_LIST)
                        .setLicenseModel(licenseModel)
                        .build());
    }

    private TypeSpecificInfo createTypeSpecificInfo(EntityDTO.LicenseModel licenseModel) {
        return TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                        .setGuestOsInfo(OS.newBuilder()
                                .setGuestOsType(OSType.UNKNOWN_OS)
                                .setGuestOsName(""))
                        .setNumCpus(4)
                        .setLicenseModel(licenseModel)
                        .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                        .addAllConnectedNetworks(CONNECTED_ENTITY_NAME_LIST)
                        .setDriverInfo(DriverInfo.getDefaultInstance())
                        .build())
                .build();
    }

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = createEntityDTOBuilder(EntityDTO.LicenseModel.LICENSE_INCLUDED);
        TypeSpecificInfo expected = createTypeSpecificInfo(EntityDTO.LicenseModel.LICENSE_INCLUDED);
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
            ImmutableMap.of(SupplyChainConstants.NUM_CPUS, "4"));
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * Test that the license model (license included / Azure BYOL) is updated in the type
     * specific info.
     */
    @Test
    public void testExtractTypeSpecificInfoWithAhub() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = createEntityDTOBuilder(EntityDTO.LicenseModel.AHUB);
        TypeSpecificInfo expected = createTypeSpecificInfo(EntityDTO.LicenseModel.AHUB);
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
                ImmutableMap.of(SupplyChainConstants.NUM_CPUS, "4"));
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * Test that the pre-requisite data is created correctly in the VirtualMachineInfo.
     */
    @Test
    public void testExtractTypeSpecificInfoForPreReqData() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = createEntityDTOBuilder(EntityDTO.LicenseModel.LICENSE_INCLUDED);
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
            ImmutableMap.of(
                "isEnaSupported", "true",
                "NVMe", "true",
                "architecture", "64-bit",
                "virtualizationType", "HVM"));
        // assert
        assertEquals(result.getVirtualMachine().getDriverInfo().getHasEnaDriver(), true);
        assertEquals(result.getVirtualMachine().getDriverInfo().getHasNvmeDriver(), true);
        assertEquals(result.getVirtualMachine().getArchitecture(), Architecture.BIT_64);
        assertEquals(result.getVirtualMachine().getVirtualizationType(), VirtualizationType.HVM);
    }

    /**
     * Test VirtualMachineInfoMapper::parseGuestName.
     */
    @Test
    public void testParseGuestName() {
        final Builder unknown = VirtualMachineInfoMapper.parseGuestName("ABCD");
        assertEquals(OSType.UNKNOWN_OS, unknown.getGuestOsType());
        final Builder linuxSqlEnterprise = VirtualMachineInfoMapper
                .parseGuestName("LINuX_WitH_SQL_ENTErpriSE");
        assertEquals(OSType.LINUX_WITH_SQL_ENTERPRISE, linuxSqlEnterprise.getGuestOsType());
        final Builder windowsByol = VirtualMachineInfoMapper.parseGuestName("WINDOWS_BYOL");
        assertEquals(OSType.WINDOWS_BYOL, windowsByol.getGuestOsType());
        final Builder linuxOS = VirtualMachineInfoMapper.parseGuestName("LINUX");
        assertEquals(OSType.LINUX, linuxOS.getGuestOsType());
    }
}