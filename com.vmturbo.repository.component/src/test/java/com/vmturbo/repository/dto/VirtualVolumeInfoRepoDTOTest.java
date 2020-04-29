package com.vmturbo.repository.dto;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;

/**
 * Class to test {@link VirtualVolumeInfoRepoDTO} conversion to and from {@link TypeSpecificInfo}.
 */
public class VirtualVolumeInfoRepoDTOTest {

    private static final float STORAGE_ACCESS_CAPACITY = 1.1f;
    private static final float STORAGE_AMOUNT_CAPACITY = 2.2f;
    private static final float IO_THROUGHPUT_CAPACITY = 3.3f;
    private static final RedundancyType REDUNDANCY_TYPE = RedundancyType.RAGRS;
    private static final String SNAPSHOT_ID = "snap-1234";
    private static final double DELTA = 0.000001;

    /**
     * Test filling a RepoDTO from a {@link TypeSpecificInfo} with data fields populated.
     */
    @Test
    public void testFillFromTypeSpecificInfo() {
        // arrange
        final TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setStorageAccessCapacity(STORAGE_ACCESS_CAPACITY)
                        .setStorageAmountCapacity(STORAGE_AMOUNT_CAPACITY)
                        .setIoThroughputCapacity(IO_THROUGHPUT_CAPACITY)
                        .setRedundancyType(REDUNDANCY_TYPE)
                        .setSnapshotId(SNAPSHOT_ID)
                        .setAttachmentState(AttachmentState.ATTACHED)
                        .setEncryption(true)
                        .setIsEphemeral(true)
                        .build())
                .build();

        final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final VirtualVolumeInfoRepoDTO testVirtualVolumeRepoDTO = new VirtualVolumeInfoRepoDTO();
        // act
        testVirtualVolumeRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertEquals(STORAGE_ACCESS_CAPACITY,
                testVirtualVolumeRepoDTO.getStorageAccessCapacity(), DELTA);
        assertEquals(STORAGE_AMOUNT_CAPACITY,
                testVirtualVolumeRepoDTO.getStorageAmountCapacity(), DELTA);
        assertEquals(IO_THROUGHPUT_CAPACITY,
            testVirtualVolumeRepoDTO.getIoThroughputCapacity(), DELTA);
        assertEquals(Integer.valueOf(REDUNDANCY_TYPE.getNumber()),
                testVirtualVolumeRepoDTO.getRedundancyType());
        assertEquals(SNAPSHOT_ID, testVirtualVolumeRepoDTO.getSnapshotId());
        assertEquals(Integer.valueOf(AttachmentState.ATTACHED.getNumber()),
                testVirtualVolumeRepoDTO.getAttachmentState());
        assertTrue(testVirtualVolumeRepoDTO.getEncryption());
        assertTrue(testVirtualVolumeRepoDTO.getEphemeral());
    }

    /**
     * Test filling a RepoDTO from an empty {@link TypeSpecificInfo}.
     */
    @Test
    public void testFillFromEmptyTypeSpecificInfo() {
        // arrange
        TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
                .build();
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final VirtualVolumeInfoRepoDTO testVirtualVolumeInfoRepoDTO = new VirtualVolumeInfoRepoDTO();
        // act
        testVirtualVolumeInfoRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertNull(testVirtualVolumeInfoRepoDTO.getStorageAccessCapacity());
        assertNull(testVirtualVolumeInfoRepoDTO.getStorageAmountCapacity());
        assertNull(testVirtualVolumeInfoRepoDTO.getIoThroughputCapacity());
        assertNull(testVirtualVolumeInfoRepoDTO.getRedundancyType());
        assertNull(testVirtualVolumeInfoRepoDTO.getSnapshotId());
        assertNull(testVirtualVolumeInfoRepoDTO.getAttachmentState());
        assertNull(testVirtualVolumeInfoRepoDTO.getEncryption());
        assertNull(testVirtualVolumeInfoRepoDTO.getEphemeral());
    }

    /**
     * Test extracting a {@link TypeSpecificInfo} from a RepoDTO.
     */
    @Test
    public void testCreateFromRepoDTO() {
        // arrange
        VirtualVolumeInfoRepoDTO testDto = new VirtualVolumeInfoRepoDTO();
        testDto.setStorageAccessCapacity(STORAGE_ACCESS_CAPACITY);
        testDto.setStorageAmountCapacity(STORAGE_AMOUNT_CAPACITY);
        testDto.setIoThroughputCapacity(IO_THROUGHPUT_CAPACITY);
        testDto.setRedundancyType(REDUNDANCY_TYPE.getNumber());
        testDto.setSnapshotId(SNAPSHOT_ID);
        testDto.setAttachmentState(AttachmentState.ATTACHED.getNumber());
        testDto.setEncryption(true);
        testDto.setEphemeral(true);
        VirtualVolumeInfo expected = VirtualVolumeInfo.newBuilder()
                .setStorageAccessCapacity(STORAGE_ACCESS_CAPACITY)
                .setStorageAmountCapacity(STORAGE_AMOUNT_CAPACITY)
                .setIoThroughputCapacity(IO_THROUGHPUT_CAPACITY)
                .setRedundancyType(REDUNDANCY_TYPE)
                .setSnapshotId(SNAPSHOT_ID)
                .setAttachmentState(AttachmentState.ATTACHED)
                .setEncryption(true)
                .setIsEphemeral(true)
                .build();
        // act
        TypeSpecificInfo result = testDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasVirtualVolume());
        final VirtualVolumeInfo virtualVolumeInfo = result.getVirtualVolume();
        assertThat(virtualVolumeInfo, equalTo(expected));
    }

    /**
     * Test extracting a {@link TypeSpecificInfo} from an empty RepoDTO.
     */
    @Test
    public void testCreateFromEmptyRepoDTO() {
        // arrange
        VirtualVolumeInfoRepoDTO testDto = new VirtualVolumeInfoRepoDTO();
        VirtualVolumeInfo expected = VirtualVolumeInfo.newBuilder()
                .build();
        // act
        TypeSpecificInfo result = testDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasVirtualVolume());
        final VirtualVolumeInfo virtualVolumeInfo = result.getVirtualVolume();
        assertEquals(expected, virtualVolumeInfo);
    }
}
