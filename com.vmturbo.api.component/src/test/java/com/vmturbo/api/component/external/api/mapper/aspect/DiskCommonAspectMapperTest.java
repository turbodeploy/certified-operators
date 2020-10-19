package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class DiskCommonAspectMapperTest extends BaseAspectMapperTest {

    private static final Long NUM_SSD = 10L;
    private static final Long NUM_RPM_7200_DISKS = 666L;
    private static final Long NUM_RPM_10K_DISKS = 2333L;
    private static final Long NUM_RPM_15K_DISKS = 555L;
    private static final Long NUM_V_SERIES_DISKS = 888L;

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.DISK_ARRAY, TypeSpecificInfo.newBuilder()
                .setDiskArray(DiskArrayInfo.newBuilder()
                    .setDiskTypeInfo(DiskTypeInfo.newBuilder()
                        .setNumSsd(NUM_SSD)
                        .setNum7200Disks(NUM_RPM_7200_DISKS)
                        .setNum10KDisks(NUM_RPM_10K_DISKS)
                        .setNum15KDisks(NUM_RPM_15K_DISKS)
                        .setNumVSeriesDisks(NUM_V_SERIES_DISKS)))
                .build());

        DiskArrayAspectMapper testMapper = new DiskArrayAspectMapper();
        // act
        final STEntityAspectApiDTO daAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertEquals(NUM_SSD, daAspect.getSsdDiskCount());
        assertEquals(NUM_RPM_7200_DISKS, daAspect.getRpm7200DiskCount());
        assertEquals(NUM_RPM_10K_DISKS, daAspect.getRpm10KDiskCount());
        assertEquals(NUM_RPM_15K_DISKS, daAspect.getRpm15KDiskCount());
        assertEquals(NUM_V_SERIES_DISKS, daAspect.getvSeriesDiskCount());
    }
}