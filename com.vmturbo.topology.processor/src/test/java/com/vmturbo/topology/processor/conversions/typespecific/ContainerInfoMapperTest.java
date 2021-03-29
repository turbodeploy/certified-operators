package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link ContainerInfoMapper}.
 */
public class ContainerInfoMapperTest {

    /**
     * Test ContainerInfo {@link ContainerInfo}.
     */
    @Test
    public void testContainerInfo() {
        final ContainerData containerData = ContainerData.newBuilder()
                .setHasCpuLimit(true)
                .setHasMemLimit(false)
                .build();
        final EntityDTO.Builder entityDTOBuilder = EntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER)
                .setId("foo")
                .setContainerData(containerData);
        ContainerInfoMapper mapper = new ContainerInfoMapper();
        final TypeSpecificInfo info =
                mapper.mapEntityDtoToTypeSpecificInfo(entityDTOBuilder, Collections.emptyMap());
        assertTrue(info.hasContainer());
        assertTrue(info.getContainer().getHasCpuLimit());
        assertFalse(info.getContainer().getHasMemLimit());
    }
}
