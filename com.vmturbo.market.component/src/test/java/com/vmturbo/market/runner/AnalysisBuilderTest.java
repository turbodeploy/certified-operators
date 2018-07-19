package com.vmturbo.market.runner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.Set;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.Analysis.AnalysisBuilder;

/**
 * Test that each field of AnalysisBuilder is passed through correctly to the new Analysis object.
 */
public class AnalysisBuilderTest {

    public static final long TEST_CONTEXT_ID = 123L;
    public static final long TEST_TOPOLOGY_ID = 456L;
    public static final long TEST_ID_GEN_PREFIX = 1L;
    public static final long OID1 = 123L;
    public static final long OID2 = 456L;
    public static final int TEST_ENTITY_TYPE = 1000;
    AnalysisBuilder testBuilder;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(TEST_ID_GEN_PREFIX);
        testBuilder = (new Analysis.AnalysisFactory()).newAnalysisBuilder();
    }

    @Test
    public void testSetTopologyInfo() {
        // Arrange
        TopologyDTO.TopologyInfo testTopologyInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyContextId(TEST_CONTEXT_ID)
                .setTopologyId(TEST_TOPOLOGY_ID)
                .setTopologyType(TopologyDTO.TopologyType.PLAN)
                .build();
        // Act
        Analysis analysis = testBuilder.setTopologyInfo(testTopologyInfo).build();

        // Assert
        assertEquals(testTopologyInfo, analysis.getTopologyInfo());
    }

    @Test
    public void testSetTopologyDTOs() {
        // Arrange
        Set<TopologyDTO.TopologyEntityDTO> testTopologyDTOs = Sets.newHashSet(
                topologyEntityDTO(OID1),
                topologyEntityDTO(OID2)
        );
        // Act
        Analysis analysis = testBuilder.setTopologyDTOs(testTopologyDTOs)
            .build();
        // Assert
        assertThat(analysis.getTopology().values(), Matchers.containsInAnyOrder(testTopologyDTOs.toArray()));
    }

    @Test
    public void testSetIncludeVDCTrue() {
        // Act
        Analysis analysis = testBuilder.setIncludeVDC(true)
            .build();
        // Assert
        assertTrue(analysis.getIncludeVDC());
    }

    @Test
    public void testSetIncludeVDCFalse() {
        // Act
        Analysis analysis = testBuilder.setIncludeVDC(false)
            .build();
        // Assert
        assertFalse(analysis.getIncludeVDC());
    }

    @Test
    public void testMaxPlacementsOverride() {
        // Act
        Analysis analysis = testBuilder.setIncludeVDC(false)
            .setMaxPlacementsOverride(Optional.of(55))
            .build();

        // Assert
        assertEquals(Optional.of(55), analysis.getMaxPlacementsOverride());
    }

    @Test
    public void testRightsizeWatermark() {
        Analysis analysis = testBuilder.setIncludeVDC(false)
                .setRightsizeLowerWatermark(0.1f)
                .setRightsizeUpperWatermark(0.7f)
                .build();

        assertEquals(0.1f, analysis.getRightsizeLowerWatermark(), 0.000001);
        assertEquals(0.7f, analysis.getRightsizeUpperWatermark(), 0.000001);
    }

    private TopologyDTO.TopologyEntityDTO topologyEntityDTO(long oid) {
        return  TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(TEST_ENTITY_TYPE)
                .setOid(oid)
                .build();
    }
}