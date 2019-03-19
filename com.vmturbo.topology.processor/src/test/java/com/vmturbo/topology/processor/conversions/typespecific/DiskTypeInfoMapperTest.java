package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeIopsData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskArrayData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.IopsItemData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.IopsItemNames;

public class DiskTypeInfoMapperTest {


    private static final Long NUM_SSD = 10L;
    private static final Long NUM_RPM_7200_DISKS = 666L;
    private static final Long NUM_RPM_10K_DISKS = 2333L;
    private static final Long NUM_RPM_15K_DISKS = 555L;
    private static final Long NUM_V_SERIES_DISKS = 888L;

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder daEntityDTO = EntityDTO.newBuilder()
            .setDiskArrayData(DiskArrayData.newBuilder()
                .setIopsComputeData(ComputeIopsData.newBuilder()
                    .addIopsItems(IopsItemData.newBuilder()
                        .setIopsItemName(IopsItemNames.NUM_SSD.name())
                        .setIopsItemValue(NUM_SSD))
                    .addIopsItems(IopsItemData.newBuilder()
                        .setIopsItemName(IopsItemNames.NUM_7200_DISKS.name())
                        .setIopsItemValue(NUM_RPM_7200_DISKS))
                    .addIopsItems(IopsItemData.newBuilder()
                        .setIopsItemName(IopsItemNames.NUM_10K_DISKS.name())
                        .setIopsItemValue(NUM_RPM_10K_DISKS))
                    .addIopsItems(IopsItemData.newBuilder()
                        .setIopsItemName(IopsItemNames.NUM_15K_DISKS.name())
                        .setIopsItemValue(NUM_RPM_15K_DISKS))
                    .addIopsItems(IopsItemData.newBuilder()
                        .setIopsItemName(IopsItemNames.NUM_VSERIES_DISKS.name())
                        .setIopsItemValue(NUM_V_SERIES_DISKS)))
                    .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setDiskArray(DiskArrayInfo.newBuilder()
                .setDiskTypeInfo(DiskTypeInfo.newBuilder()
                    .setNumSsd(NUM_SSD)
                    .setNum7200Disks(NUM_RPM_7200_DISKS)
                    .setNum10KDisks(NUM_RPM_10K_DISKS)
                    .setNum15KDisks(NUM_RPM_15K_DISKS)
                    .setNumVSeriesDisks(NUM_V_SERIES_DISKS)))
            .build();

        final DiskArrayInfoMapper testBuilder = new DiskArrayInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(daEntityDTO,
            Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}