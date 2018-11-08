package com.vmturbo.repository.topology;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.repository.dto.ApplicationInfoRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualVolumeInfoRepoDTO;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Test conversion from ServiceEntityDTO.proto to RepoDTO.
 **/
public class ConvertToRepoDTOTest {

    public static final String TEST_IP_ADDRESS = "1.2.3.4";
    public static final boolean TEST_IP_ELASTIC = false;

    private static final long TEST_OID = 1234L;
    private static final float TEST_STORAGE_AMOUNT_CAPACITY = 2.717f;
    private static final float TEST_STORAGE_ACCESS_CAPACITY = 3.141f;
    public static final RedundancyType TEST_REDUNDANCY_TYPE = RedundancyType.ZRS;

    @Test
    public void testConvertApplicationInfoToRepoDTO() {
        // arrange
        final TopologyEntityDTO.Builder applicationEntityDTOBuilder = TopologyEntityDTO.newBuilder()
            .setOid(TEST_OID)
            .setEntityType(EntityType.APPLICATION_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setApplication(
                ApplicationInfo.newBuilder()
                    .setIpAddress(IpAddress.newBuilder()
                        .setIpAddress(TEST_IP_ADDRESS)
                        .setIsElastic(TEST_IP_ELASTIC))));

        // act
        final ServiceEntityRepoDTO repoDTO = TopologyEntityDTOConverter
            .convertToServiceEntityRepoDTO(applicationEntityDTOBuilder.build());
        // assert
        assertNotNull(repoDTO);
        final ApplicationInfoRepoDTO applicationInfoRepoDTO = repoDTO.getApplicationInfoRepoDTO();
        assertNotNull(applicationInfoRepoDTO);
        final IpAddressRepoDTO ipAddress = applicationInfoRepoDTO.getIpAddress();
        assertNotNull(ipAddress);
        assertThat(ipAddress.getIpAddress(), equalTo(TEST_IP_ADDRESS));
        assertThat(ipAddress.getElastic(), equalTo(TEST_IP_ELASTIC));
    }

    @Test
    public void testConvertEmptyApplicationInfoToRepoDTO() {
        // arrange
        final TopologyEntityDTO.Builder applicationEntityDTOBuilder = TopologyEntityDTO.newBuilder()
            .setOid(TEST_OID)
            .setEntityType(EntityType.APPLICATION_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setApplication(
                ApplicationInfo.newBuilder()));

        // act
        final ServiceEntityRepoDTO repoDTO = TopologyEntityDTOConverter
            .convertToServiceEntityRepoDTO(applicationEntityDTOBuilder.build());
        // assert
        assertNotNull(repoDTO);
        final ApplicationInfoRepoDTO applicationInfoRepoDTO = repoDTO.getApplicationInfoRepoDTO();
        assertNotNull(applicationInfoRepoDTO);
        final IpAddressRepoDTO ipAddress = applicationInfoRepoDTO.getIpAddress();
        assertNull(ipAddress);
    }

    @Test
    public void testConvertVirtualVolumeInfoToRepoDTO() {
        // arrange
        final TopologyEntityDTO.Builder virtualVolumeEntityDTOBuilder = TopologyEntityDTO.newBuilder()
            .setOid(TEST_OID)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(
                VirtualVolumeInfo.newBuilder()
                    .setStorageAmountCapacity(TEST_STORAGE_AMOUNT_CAPACITY)
                    .setStorageAccessCapacity(TEST_STORAGE_ACCESS_CAPACITY)
                    .setRedundancyType(TEST_REDUNDANCY_TYPE)));

        // act
        final ServiceEntityRepoDTO repoDTO = TopologyEntityDTOConverter
            .convertToServiceEntityRepoDTO(virtualVolumeEntityDTOBuilder.build());
        // assert
        assertNotNull(repoDTO);
        final VirtualVolumeInfoRepoDTO virtualVolumeInfoRepoDTO = repoDTO.getVirtualVolumeInfoRepoDTO();
        assertNotNull(virtualVolumeInfoRepoDTO);
        final Float storageAmountCapacity = virtualVolumeInfoRepoDTO.getStorageAmountCapacity();
        assertThat(storageAmountCapacity, equalTo(TEST_STORAGE_AMOUNT_CAPACITY));
        final Float storageAccessCapacity = virtualVolumeInfoRepoDTO.getStorageAccessCapacity();
        assertThat(storageAccessCapacity, equalTo(TEST_STORAGE_ACCESS_CAPACITY));
        final Integer redundancyType = virtualVolumeInfoRepoDTO.getRedundancyType();
        assertThat(redundancyType, equalTo(TEST_REDUNDANCY_TYPE.getNumber()));
    }

    @Test
    public void testConvertEmptyVirtualVolumeInfoToRepoDTO() {
        // arrange
        final TopologyEntityDTO.Builder virtualVolumeEntityDTOBuilder = TopologyEntityDTO.newBuilder()
            .setOid(TEST_OID)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(
                VirtualVolumeInfo.getDefaultInstance()));

        // act
        final ServiceEntityRepoDTO repoDTO = TopologyEntityDTOConverter
            .convertToServiceEntityRepoDTO(virtualVolumeEntityDTOBuilder.build());
        // assert
        assertNotNull(repoDTO);
        final VirtualVolumeInfoRepoDTO virtualVolumeInfoRepoDTO = repoDTO.getVirtualVolumeInfoRepoDTO();
        assertNotNull(virtualVolumeInfoRepoDTO);
        assertNull(virtualVolumeInfoRepoDTO.getStorageAmountCapacity());
        assertNull(virtualVolumeInfoRepoDTO.getStorageAccessCapacity());
        assertNull(virtualVolumeInfoRepoDTO.getRedundancyType());
    }
}