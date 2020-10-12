package com.vmturbo.topology.processor.conversions.typespecific;

import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.BIT_64;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.HVM;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.PROPERTY_ARCHITECTURE;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.PROPERTY_IS_ENA_SUPPORTED;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.PROPERTY_LOCKS;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.PROPERTY_SUPPORTS_NVME;
import static com.vmturbo.common.protobuf.VirtualMachineProtoUtil.PROPERTY_VM_VIRTUALIZATION_TYPE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

public class VirtualMachineInfoMapperTest {

    private static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    private static final String CONNECTED_NETWORK_NAME_2 = "network 2";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST = ImmutableList.of(
        CONNECTED_NETWORK_NAME_1, CONNECTED_NETWORK_NAME_2);
    private static final String LOCK_MESSAGE = "[Scope: vm1, name: vm-lock-1, notes: VM lock]";

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
                                .setGuestOsName(StringConstants.UNKNOWN))
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
     * Test that the prerequisites are created correctly in the VirtualMachineInfo.
     */
    @Test
    public void testExtractTypeSpecificInfoForPrerequisites() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = createEntityDTOBuilder(EntityDTO.LicenseModel.LICENSE_INCLUDED);
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
            ImmutableMap.of(
                    PROPERTY_IS_ENA_SUPPORTED, Boolean.TRUE.toString(),
                    PROPERTY_SUPPORTS_NVME, Boolean.TRUE.toString(),
                    PROPERTY_ARCHITECTURE, BIT_64,
                    PROPERTY_VM_VIRTUALIZATION_TYPE, HVM,
                    PROPERTY_LOCKS, LOCK_MESSAGE));
        // assert
        VirtualMachineInfo vmInfo = result.getVirtualMachine();
        assertTrue(vmInfo.getDriverInfo().getHasEnaDriver());
        assertTrue(vmInfo.getDriverInfo().getHasNvmeDriver());
        assertEquals(vmInfo.getArchitecture(), Architecture.BIT_64);
        assertEquals(vmInfo.getVirtualizationType(), VirtualizationType.HVM);
        assertEquals(vmInfo.getLocks(), LOCK_MESSAGE);
    }

    /**
     * Test VirtualMachineInfoMapper::parseGuestName.
     */
    @Test
    public void testParseGuestName() {
        final OS unknown = VirtualMachineInfoMapper.parseGuestName("ABCD");
        assertEquals(OSType.UNKNOWN_OS, unknown.getGuestOsType());
        final OS linuxSqlEnterprise = VirtualMachineInfoMapper
                .parseGuestName("LINuX_WitH_SQL_ENTErpriSE");
        assertEquals(OSType.LINUX_WITH_SQL_ENTERPRISE, linuxSqlEnterprise.getGuestOsType());
        final OS windowsByol = VirtualMachineInfoMapper.parseGuestName("WINDOWS_BYOL");
        assertEquals(OSType.WINDOWS_BYOL, windowsByol.getGuestOsType());
        final OS linuxOS = VirtualMachineInfoMapper.parseGuestName("LINUX");
        assertEquals(OSType.LINUX, linuxOS.getGuestOsType());
        final OS emptyOS = VirtualMachineInfoMapper.parseGuestName("");
        assertEquals(OSType.UNKNOWN_OS, emptyOS.getGuestOsType());
        assertEquals(StringConstants.UNKNOWN, emptyOS.getGuestOsName());
    }

    /**
     * Create an entity with VM related data for the purpose of testing dynamic memory.
     *
     * @param withMemory when true, the VM related data will include a memory object
     * @param dynamic the value of the dynamic property of the memory object (when created)
     * @return and entity DTO with VM related data
     */
    private EntityDTO createEntityDTO(boolean withMemory, boolean dynamic) {
        EntityDTO.VirtualMachineRelatedData.Builder builder =
                EntityDTO.VirtualMachineRelatedData.newBuilder();
        if (withMemory) {
            builder.setMemory(EntityDTO.MemoryData.newBuilder()
                    .setId("memory")
                    .setDynamic(dynamic)
                    .build());
        }
        return EntityDTO.newBuilder()
                .setVirtualMachineData(VirtualMachineData.getDefaultInstance())
                .setId("vm")
                .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setVirtualMachineRelatedData(builder.build()).build();
    }

    private TypeSpecificInfo vmInfo(EntityDTO vm) {
        return (new VirtualMachineInfoMapper())
            .mapEntityDtoToTypeSpecificInfo(vm, Collections.emptyMap());
    }

    /**
     * Verify that dynamic memory is converted properly.
     */
    @Test
    public void testDynamicMemory() {
        EntityDTO vmWithoutMemory = createEntityDTO(false, false);
        TypeSpecificInfo result1 = vmInfo(vmWithoutMemory);
        assertFalse(result1.getVirtualMachine().getDynamicMemory());

        EntityDTO vmWithStaticMemory = createEntityDTO(true, false);
        TypeSpecificInfo result2 = vmInfo(vmWithStaticMemory);
        assertFalse(result2.getVirtualMachine().getDynamicMemory());

        EntityDTO vmWithDynamicMemory = createEntityDTO(true, true);
        TypeSpecificInfo result3 = vmInfo(vmWithDynamicMemory);
        assertTrue(result3.getVirtualMachine().getDynamicMemory());
    }
}