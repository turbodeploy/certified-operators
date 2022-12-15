package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerSpecInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CPUThrottlingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerSpecData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test ContainerSpecInfo {@link ContainerSpecInfo}.
 */
public class ContainerSpecInfoMapperTest {

    /**
     * Test ContainerSpecInfo {@link ContainerSpecInfo}.
     */
    @Test
    public void testContainerSpecInfo() {
        final ContainerSpecData containerSpecData = ContainerSpecData.newBuilder()
                .setCpuThrottlingType(CPUThrottlingType.timeBased)
                .build();
        final EntityDTO.Builder entityDTOBuilder = EntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_SPEC)
                .setId("Foo")
                .setContainerSpecData(containerSpecData);
        final ContainerSpecInfoMapper containerSpecInfoMapper = new ContainerSpecInfoMapper();
        final TypeSpecificInfo info = containerSpecInfoMapper
                .mapEntityDtoToTypeSpecificInfo(entityDTOBuilder, Collections.emptyMap());
        assertTrue(info.hasContainerSpec());
        assertEquals(CPUThrottlingType.timeBased, info.getContainerSpec().getCpuThrottlingType());
    }
}
