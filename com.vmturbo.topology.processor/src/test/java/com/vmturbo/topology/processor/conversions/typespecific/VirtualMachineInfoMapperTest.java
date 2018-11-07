package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class VirtualMachineInfoMapperTest {


    private static final float ACCESS_CAPACITY = 123.4f;
    private static final float AMOUNT_CAPACITY = 567.8f;

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = EntityDTO.newBuilder()
                .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                        .setStorageAccessCapacity(ACCESS_CAPACITY)
                        .setStorageAmountCapacity(AMOUNT_CAPACITY)
                        .setRedundancyType(RedundancyType.ZRS)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setStorageAccessCapacity(ACCESS_CAPACITY)
                        .setStorageAmountCapacity(AMOUNT_CAPACITY)
                        .setRedundancyType(RedundancyType.ZRS)
                        .build())
                .build();
        final VirtualVolumeInfoMapper testBuilder = new VirtualVolumeInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO);
        // assert
        assertThat(result, equalTo(expected));
    }
}