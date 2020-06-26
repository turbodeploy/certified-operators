package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;

/**
 * Unit tests for {@link CachedTopology}.
 */
public class CachedTopologyTest {

    private CachedTopology cachedTopology = new CachedTopology();

    /**
     * Test getting a normal cached topology.
     */
    @Test
    public void testGetNormalTopology() {
        List<Builder> entities = Lists.newArrayList(TopologyEntityUtils.topologyEntity(1, EntityType.VIRTUAL_MACHINE),
            TopologyEntityUtils.topologyEntity(2, EntityType.PHYSICAL_MACHINE),
            TopologyEntityUtils.topologyEntity(3, EntityType.STORAGE));
        cachedTopology.updateTopology(entities.stream()
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity())));

        CachedTopologyResult result = cachedTopology.getTopology(TopologyInfo.getDefaultInstance());
        // Size 3
        assertThat(result.toString(), containsString("3"));
        assertThat(result.getEntities().keySet(), containsInAnyOrder(1L, 2L, 3L));
    }

    /**
     * Test getting a cached topology for a reservation plan.
     */
    @Test
        public void testGetReservationTopology() {
        List<Builder> entities = Lists.newArrayList(TopologyEntityUtils.topologyEntity(1, EntityType.APPLICATION),
            TopologyEntityUtils.topologyEntity(2, EntityType.PHYSICAL_MACHINE),
            TopologyEntityUtils.topologyEntity(3, EntityType.STORAGE));
        cachedTopology.updateTopology(entities.stream()
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity())));

        CachedTopologyResult result = cachedTopology.getTopology(TopologyInfo.newBuilder()
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(PlanProjectType.RESERVATION_PLAN))
            .build());

        // Size 2
        assertThat(result.toString(), containsString("2"));
        assertThat(result.toString(), containsString("Removed"));
        assertThat(result.toString(), containsString(ApiEntityType.APPLICATION.toString()));
        assertThat(result.getEntities().keySet(), containsInAnyOrder(2L, 3L));
    }

}