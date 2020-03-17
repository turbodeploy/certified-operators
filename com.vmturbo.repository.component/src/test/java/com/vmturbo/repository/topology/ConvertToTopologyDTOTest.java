package com.vmturbo.repository.topology;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.repository.dto.ApplicationInfoRepoDTO;
import com.vmturbo.repository.dto.BusinessUserInfoRepoDTO;
import com.vmturbo.repository.dto.DesktopPoolInfoRepoDTO;
import com.vmturbo.repository.dto.GuestOSRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;
import com.vmturbo.repository.dto.VirtualVolumeInfoRepoDTO;

public class ConvertToTopologyDTOTest {

    private static final String SE_OID_STRING = "1234";
    private static final String TEST_IP_ADDRESS = "1.2.3.4";
    private static final boolean TEST_IP_ELASTIC = false;
    private static final Float TEST_STORAGE_AMOUNT_CAPACITY = 2.717f;
    private static final Float TEST_STORAGE_ACCESS_CAPACITY = 3.141f;
    private static final RedundancyType TEST_REDUNDANCY_TYPE = RedundancyType.LRS;
    private static final long TEST_DESKTOP_VM_REFERENCE_ID = 200L;
    private static final String TEST_DESKTOP_CLONE_SNAPSHOT = "/Clone Snapshot";
    private static final ImmutableMap<Long, Long> TEST_BUSINESS_USER_VM_OID_TO_SESSION_DURATION =
            ImmutableMap.of(100L, 1000000L, 200L, 2000000L);

    @Test
    public void testConvertApplicationInfoRepoDTO() {
        // arrange
        final ServiceEntityRepoDTO serviceEntityRepoDTO =
            buildTestServiceEntityRepoDTO(EntityType.APPLICATION_VALUE);

        final ApplicationInfoRepoDTO applicationInfoRepoDTO = new ApplicationInfoRepoDTO();
        final IpAddressRepoDTO repoIpAddressDTO = new IpAddressRepoDTO();
        repoIpAddressDTO.setIpAddress(TEST_IP_ADDRESS);
        repoIpAddressDTO.setElastic(TEST_IP_ELASTIC);
        applicationInfoRepoDTO.setIpAddress(repoIpAddressDTO);
        serviceEntityRepoDTO.setApplicationInfoRepoDTO(applicationInfoRepoDTO);
        // act
        final TopologyEntityDTO resultTopoEntityDTO = ServiceEntityRepoDTOConverter
            .convertToTopologyEntityDTO(serviceEntityRepoDTO);
        // assert
        assertTrue(resultTopoEntityDTO.hasTypeSpecificInfo());
        assertTrue(resultTopoEntityDTO.getTypeSpecificInfo().hasApplication());
        final ApplicationInfo applicationInfo = resultTopoEntityDTO.getTypeSpecificInfo().getApplication();
        assertTrue(applicationInfo.hasIpAddress());
        final IpAddress ipAddress = applicationInfo.getIpAddress();
        assertThat(ipAddress.getIpAddress(), equalTo(repoIpAddressDTO.getIpAddress()));
        assertThat(ipAddress.getIsElastic(), equalTo(repoIpAddressDTO.getElastic()));
    }

    @Test
    public void testConvertVirtualMachineInfoRepoDTO() {
        // arrange
        final ServiceEntityRepoDTO serviceEntityRepoDTO =
            buildTestServiceEntityRepoDTO(EntityType.VIRTUAL_MACHINE_VALUE);

        final VirtualMachineInfoRepoDTO virtualMachineInfoRepoDTO = new VirtualMachineInfoRepoDTO();
        final IpAddressRepoDTO repoIpAddressDTO = new IpAddressRepoDTO();
        repoIpAddressDTO.setIpAddress(TEST_IP_ADDRESS);
        repoIpAddressDTO.setElastic(TEST_IP_ELASTIC);
        virtualMachineInfoRepoDTO.setIpAddressInfoList(Collections.singletonList(repoIpAddressDTO));
        virtualMachineInfoRepoDTO.setGuestOsInfo(new GuestOSRepoDTO(OSType.UNKNOWN_OS,
            OSType.UNKNOWN_OS.name()));
        virtualMachineInfoRepoDTO.setTenancy(Tenancy.DEDICATED.name());

        serviceEntityRepoDTO.setVirtualMachineInfoRepoDTO(virtualMachineInfoRepoDTO);
        // act
        final TopologyEntityDTO resultTopoEntityDTO = ServiceEntityRepoDTOConverter
            .convertToTopologyEntityDTO(serviceEntityRepoDTO);
        // assert
        assertTrue(resultTopoEntityDTO.hasTypeSpecificInfo());
        assertTrue(resultTopoEntityDTO.getTypeSpecificInfo().hasVirtualMachine());
        final VirtualMachineInfo virtualMachineInfo = resultTopoEntityDTO.getTypeSpecificInfo()
            .getVirtualMachine();
        final List<IpAddress> ipAddressese = virtualMachineInfo.getIpAddressesList();
        assertThat(ipAddressese.size(), equalTo(1));
        final IpAddress ipAddress = ipAddressese.iterator().next();
        assertThat(ipAddress.getIpAddress(), equalTo(repoIpAddressDTO.getIpAddress()));
        assertThat(ipAddress.getIsElastic(), equalTo(repoIpAddressDTO.getElastic()));
        // tenancy
    }

    @Test
    public void testConvertVirtualVolumeInfoRepoDTO() {
        // arrange
        final ServiceEntityRepoDTO serviceEntityRepoDTO =
            buildTestServiceEntityRepoDTO(EntityType.VIRTUAL_VOLUME_VALUE);

        final VirtualVolumeInfoRepoDTO virtualVolumeInfoRepoDTO = new VirtualVolumeInfoRepoDTO();
        final IpAddressRepoDTO repoIpAddressDTO = new IpAddressRepoDTO();
        repoIpAddressDTO.setIpAddress(TEST_IP_ADDRESS);
        repoIpAddressDTO.setElastic(TEST_IP_ELASTIC);
        virtualVolumeInfoRepoDTO.setStorageAmountCapacity(TEST_STORAGE_AMOUNT_CAPACITY);
        virtualVolumeInfoRepoDTO.setStorageAccessCapacity(TEST_STORAGE_ACCESS_CAPACITY);
        virtualVolumeInfoRepoDTO.setRedundancyType(TEST_REDUNDANCY_TYPE.getNumber());

        serviceEntityRepoDTO.setVirtualVolumeInfoRepoDTO(virtualVolumeInfoRepoDTO);
        // act
        final TopologyEntityDTO resultTopoEntityDTO = ServiceEntityRepoDTOConverter
            .convertToTopologyEntityDTO(serviceEntityRepoDTO);
        // assert
        assertTrue(resultTopoEntityDTO.hasTypeSpecificInfo());
        assertTrue(resultTopoEntityDTO.getTypeSpecificInfo().hasVirtualVolume());
        final VirtualVolumeInfo virtualVolumeInfo = resultTopoEntityDTO.getTypeSpecificInfo()
            .getVirtualVolume();
        assertThat(virtualVolumeInfo.getStorageAmountCapacity(), equalTo(TEST_STORAGE_AMOUNT_CAPACITY));
        assertThat(virtualVolumeInfo.getStorageAccessCapacity(), equalTo(TEST_STORAGE_ACCESS_CAPACITY));
        assertThat(virtualVolumeInfo.getRedundancyType(), equalTo(TEST_REDUNDANCY_TYPE));
    }

    /**
     * Test for {@link DesktopPoolInfoRepoDTO#createTypeSpecificInfo()}.
     */
    @Test
    public void testConvertDesktopPoolInfoRepoDTO() {
        // arrange
        final ServiceEntityRepoDTO serviceEntityRepoDTO =
                buildTestServiceEntityRepoDTO(EntityType.DESKTOP_POOL_VALUE);
        final DesktopPoolInfoRepoDTO desktopPoolInfoRepoDTO = new DesktopPoolInfoRepoDTO();
        desktopPoolInfoRepoDTO.setVmReferenceId(TEST_DESKTOP_VM_REFERENCE_ID);
        desktopPoolInfoRepoDTO.setSnapshot(TEST_DESKTOP_CLONE_SNAPSHOT);
        desktopPoolInfoRepoDTO.setProvisionType(DesktopPoolProvisionType.ON_DEMAND);
        desktopPoolInfoRepoDTO.setCloneType(DesktopPoolCloneType.LINKED);
        desktopPoolInfoRepoDTO.setAssignmentType(DesktopPoolAssignmentType.DYNAMIC);
        serviceEntityRepoDTO.setDesktopPoolInfoRepoDTO(desktopPoolInfoRepoDTO);
        // act
        final TopologyEntityDTO resultTopoEntityDTO =
                ServiceEntityRepoDTOConverter.convertToTopologyEntityDTO(serviceEntityRepoDTO);
        // assert
        assertTrue(resultTopoEntityDTO.hasTypeSpecificInfo());
        assertTrue(resultTopoEntityDTO.getTypeSpecificInfo().hasDesktopPool());
        final DesktopPoolInfo desktopPoolInfo =
                resultTopoEntityDTO.getTypeSpecificInfo().getDesktopPool();
        Assert.assertEquals(desktopPoolInfo.getVmWithSnapshot().getVmReferenceId(), TEST_DESKTOP_VM_REFERENCE_ID);
        Assert.assertEquals(desktopPoolInfo.getVmWithSnapshot().getSnapshot(), TEST_DESKTOP_CLONE_SNAPSHOT);
        Assert.assertEquals(desktopPoolInfo.getAssignmentType(), DesktopPoolAssignmentType.DYNAMIC);
        Assert.assertEquals(desktopPoolInfo.getCloneType(), DesktopPoolCloneType.LINKED);
        Assert.assertEquals(desktopPoolInfo.getProvisionType(), DesktopPoolProvisionType.ON_DEMAND);
    }

    /**
     * Test for {@link BusinessUserInfoRepoDTO#createTypeSpecificInfo()}.
     */
    @Test
    public void testConvertBusinessUserInfoRepoDTO() {
        // arrange
        final ServiceEntityRepoDTO serviceEntityRepoDTO =
                buildTestServiceEntityRepoDTO(EntityType.BUSINESS_USER_VALUE);
        final BusinessUserInfoRepoDTO businessUserInfoRepoDTO = new BusinessUserInfoRepoDTO();
        businessUserInfoRepoDTO.setVmOidToSessionDuration(
                TEST_BUSINESS_USER_VM_OID_TO_SESSION_DURATION);
        serviceEntityRepoDTO.setBusinessUserInfoRepoDTO(businessUserInfoRepoDTO);
        // act
        final TopologyEntityDTO resultTopoEntityDTO =
                ServiceEntityRepoDTOConverter.convertToTopologyEntityDTO(serviceEntityRepoDTO);
        // assert
        assertTrue(resultTopoEntityDTO.hasTypeSpecificInfo());
        assertTrue(resultTopoEntityDTO.getTypeSpecificInfo().hasBusinessUser());
        final BusinessUserInfo businessUserInfo =
                resultTopoEntityDTO.getTypeSpecificInfo().getBusinessUser();
        Assert.assertEquals(businessUserInfo.getVmOidToSessionDurationMap(),
                TEST_BUSINESS_USER_VM_OID_TO_SESSION_DURATION);
    }

    private ServiceEntityRepoDTO buildTestServiceEntityRepoDTO(final int entityType) {
        final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setOid(SE_OID_STRING);
        serviceEntityRepoDTO.setDisplayName("SE_DISPLAY_NAME");
        serviceEntityRepoDTO.setEntityType(ApiEntityType.fromType(entityType).apiStr());
        return serviceEntityRepoDTO;
    }
}
