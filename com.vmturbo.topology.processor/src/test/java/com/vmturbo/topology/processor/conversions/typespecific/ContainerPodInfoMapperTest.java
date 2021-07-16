package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPodInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerPodData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link ContainerPodInfoMapper}.
 */
public class ContainerPodInfoMapperTest {

    /**
     * Test ContainerPodInfo {@link ContainerPodInfo}.
     */
    @Test
    public void testContainerPodInfo() {
        final ContainerPodData containerPodData = ContainerPodData.newBuilder()
                .setHostingNodeCpuFrequency(1.0)
                .build();
        final EntityDTO.Builder entityDTOBuilder = EntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD)
                .setId("foo")
                .setContainerPodData(containerPodData);
        ContainerPodInfoMapper mapper = new ContainerPodInfoMapper();
        final TypeSpecificInfo info =
                mapper.mapEntityDtoToTypeSpecificInfo(entityDTOBuilder, Collections.emptyMap());
        assertTrue(info.hasContainerPod());
        assertEquals(1.0, info.getContainerPod().getHostingNodeCpuFrequency(), 0.001);
    }
}
