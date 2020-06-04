package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.ComputeTierAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for {@link ComputeTierAspectMapper}.
 */
public class ComputeTierAspectMapperTest extends BaseAspectMapperTest {
    private static final String TEST_TIER_FAMILY = "m5";
    private static final int TEST_TIER_NUM_INSTANCEDISKS = 2;
    private static final int TEST_TIER_SIZE_INSTANCEDISK = 200;

    /**
     * Test that correct aspects are created for compute tier entity.
     */
    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyDTO.TypeSpecificInfo typeSpecificInfo = TopologyDTO.TypeSpecificInfo.newBuilder()
                .setComputeTier(TopologyDTO.TypeSpecificInfo.ComputeTierInfo.newBuilder()
                        .setFamily(TEST_TIER_FAMILY)
                        .setInstanceDiskSizeGb(TEST_TIER_SIZE_INSTANCEDISK)
                        .setNumInstanceDisks(TEST_TIER_NUM_INSTANCEDISKS)
                        .build()).build();
        final TopologyDTO.TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
                CommonDTO.EntityDTO.EntityType.COMPUTE_TIER, typeSpecificInfo);
        final ComputeTierAspectMapper mapper = new ComputeTierAspectMapper();
        // act
        EntityAspect result = mapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertTrue(result instanceof ComputeTierAspectApiDTO);
        ComputeTierAspectApiDTO computeTierAspectApiDTO = (ComputeTierAspectApiDTO)result;
        assertEquals(TEST_TIER_FAMILY, computeTierAspectApiDTO.getTierFamily());
        assertEquals(new Float(TEST_TIER_NUM_INSTANCEDISKS), computeTierAspectApiDTO.getNumInstanceStorages());
        assertEquals(new Float(TEST_TIER_SIZE_INSTANCEDISK), computeTierAspectApiDTO.getInstanceStorageSize());
    }
}
