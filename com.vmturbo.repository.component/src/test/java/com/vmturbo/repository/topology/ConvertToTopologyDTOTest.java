package com.vmturbo.repository.topology;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.ApplicationInfoRepoDTO;
import com.vmturbo.repository.dto.GuestOSRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;
import com.vmturbo.repository.dto.VirtualVolumeInfoRepoDTO;

public class ConvertToTopologyDTOTest {

    private static final String SE_OID_STRING = "1234";
    public static final String TEST_IP_ADDRESS = "1.2.3.4";
    public static final boolean TEST_IP_ELASTIC = false;
    private static final Float TEST_STORAGE_AMOUNT_CAPACITY = 2.717f;
    private static final Float TEST_STORAGE_ACCESS_CAPACITY = 3.141f;
    private static final RedundancyType TEST_REDUNDANCY_TYPE = RedundancyType.LRS;

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
        // guestOsType
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

    private ServiceEntityRepoDTO buildTestServiceEntityRepoDTO(final int entityType) {
        final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setOid(SE_OID_STRING);
        serviceEntityRepoDTO.setDisplayName("SE_DISPLAY_NAME");
        serviceEntityRepoDTO.setEntityType(RepoObjectType
            .mapEntityType(entityType));
        return serviceEntityRepoDTO;
    }
}