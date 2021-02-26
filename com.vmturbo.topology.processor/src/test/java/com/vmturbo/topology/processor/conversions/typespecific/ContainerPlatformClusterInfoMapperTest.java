package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerPlatformClusterData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link ContainerPlatformClusterInfoMapper}.
 */
public class ContainerPlatformClusterInfoMapperTest {

    private static final double VCPU_OVERCOMMITMENT = 0.4;
    private static final double VMEM_OVERCOMMITMENT = 1.2;
    private static final double DELTA = 0.0001;

    /**
     * Test ContainerPlatformClusterInfo.
     */
    @Test
    public void testContainerPlatformClusterInfo() {
        final ContainerPlatformClusterData containerPlatformClusterData = ContainerPlatformClusterData.newBuilder()
                .setVcpuOvercommitment(VCPU_OVERCOMMITMENT)
                .setVmemOvercommitment(VMEM_OVERCOMMITMENT)
                .build();
        final EntityDTO.Builder entityDTOBuilder = EntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER)
                .setId("foo")
                .setContainerPlatformClusterData(containerPlatformClusterData);

        ContainerPlatformClusterInfoMapper mapper = new ContainerPlatformClusterInfoMapper();
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entityDTOBuilder, Collections.emptyMap());
        assertTrue(info.hasContainerPlatformCluster());
        assertEquals(VCPU_OVERCOMMITMENT, info.getContainerPlatformCluster().getVcpuOvercommitment(), DELTA);
        assertEquals(VMEM_OVERCOMMITMENT, info.getContainerPlatformCluster().getVmemOvercommitment(), DELTA);
    }
}
