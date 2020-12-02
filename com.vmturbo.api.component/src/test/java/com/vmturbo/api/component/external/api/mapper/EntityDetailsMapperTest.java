package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests the methods of {@link EntityDetailsMapper}.
 */
public class EntityDetailsMapperTest {

    private final EntityDetailsMapper entityDetailsMapper = new EntityDetailsMapper();

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link EntityDetailsApiDTO}.
     */
    @Test
    public void testToEntityDetailsApiDTO() {
        final long oid = 1L;

        final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();

        final EntityDetailsApiDTO result =
                entityDetailsMapper.toEntityDetails(topologyEntityDTO);

        assertEquals(oid, result.getUuid());
    }
}
