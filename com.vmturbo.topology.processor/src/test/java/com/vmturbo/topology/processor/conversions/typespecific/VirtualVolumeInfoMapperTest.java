package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.StorageCompatibilityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.UsageType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class VirtualVolumeInfoMapperTest {

    private static final RedundancyType REDUNDANCY_TYPE = RedundancyType.RAGRS;
    private static final String SNAPSHOT_ID = "snap-1234";

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final double hourlyBilledOps = 123D;
        final VirtualVolumeFileDescriptor file = VirtualVolumeFileDescriptor.newBuilder()
                .setPath("path")
                .build();
        final EntityDTOOrBuilder virtualVolumeEntityDTO = EntityDTO.newBuilder()
                .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                        .setRedundancyType(REDUNDANCY_TYPE)
                        .setSnapshotId(SNAPSHOT_ID)
                        .setAttachmentState(AttachmentState.ATTACHED)
                        .setEncrypted(true)
                        .setHourlyBilledOps(hourlyBilledOps)
                        .setStorageCompatibilityForConsumer(StorageCompatibilityType.PREMIUM)
                        .setUsageType(UsageType.SITE_RECOVERY)
                        .addFile(file)
                        .build());
        final TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setRedundancyType(REDUNDANCY_TYPE)
                        .setSnapshotId(SNAPSHOT_ID)
                        .setAttachmentState(AttachmentState.ATTACHED)
                        .setEncryption(true)
                        .setHourlyBilledOps(hourlyBilledOps)
                        .setStorageCompatibilityForConsumer(StorageCompatibilityType.PREMIUM)
                        .setUsageType(UsageType.SITE_RECOVERY)
                        .addFiles(file)
                        .build())
                .build();
        final VirtualVolumeInfoMapper testBuilder = new VirtualVolumeInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(
                virtualVolumeEntityDTO, Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}
