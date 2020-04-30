package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class VirtualVolumeInfoMapperTest {


    private static final float STORAGE_ACCESS_CAPACITY = 1.1f;
    private static final float STORAGE_AMOUNT_CAPACITY = 2.2f;
    private static final float STORAGE_THROUGHPUT_CAPACITY = 3.3f;
    public static final RedundancyType REDUNDANCY_TYPE = RedundancyType.RAGRS;
    private static final String SNAPSHOT_ID = "snap-1234";

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder virtualVolumeEntityDTO = EntityDTO.newBuilder()
                .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                        .setStorageAccessCapacity(STORAGE_ACCESS_CAPACITY)
                        .setStorageAmountCapacity(STORAGE_AMOUNT_CAPACITY)
                        .setIoThroughputCapacity(STORAGE_THROUGHPUT_CAPACITY)
                        .setRedundancyType(REDUNDANCY_TYPE)
                        .setSnapshotId(SNAPSHOT_ID)
                        .setAttachmentState(AttachmentState.ATTACHED)
                        .setEncrypted(true)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setStorageAccessCapacity(STORAGE_ACCESS_CAPACITY)
                        .setStorageAmountCapacity(STORAGE_AMOUNT_CAPACITY)
                        .setIoThroughputCapacity(STORAGE_THROUGHPUT_CAPACITY)
                        .setRedundancyType(REDUNDANCY_TYPE)
                        .setSnapshotId(SNAPSHOT_ID)
                        .setAttachmentState(AttachmentState.ATTACHED)
                        .setEncryption(true)
                        .build())
                .build();
        DatabaseInfo.newBuilder()
                .build();
        final VirtualVolumeInfoMapper testBuilder = new VirtualVolumeInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(
                virtualVolumeEntityDTO, Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}