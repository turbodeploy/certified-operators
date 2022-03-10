package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;

/**
 * Unit tests for {@link CachedTopology}.
 */
public class CachedTopologyTest {

    private final List<TopologyEntityDTO.Builder> entities = Lists.newArrayList(TopologyEntityUtils.topologyEntity(1, EntityType.VIRTUAL_MACHINE).getTopologyEntityImpl().toProtoBuilder(),
            TopologyEntityUtils.topologyEntity(2, EntityType.PHYSICAL_MACHINE).getTopologyEntityImpl().toProtoBuilder(),
            TopologyEntityUtils.topologyEntity(3, EntityType.STORAGE).getTopologyEntityImpl().toProtoBuilder());

    /**
     * Test getting a normal cached topology.
     */
    @Test
    public void testGetNormalTopology() {
        CachedTopology cachedTopology = new CachedTopology(false);
        doNormalTopologyTest(cachedTopology);
    }

    /**
     * Test getting a normal cached topology, with serialization.
     */
    @Test
    public void testGetNormalTopologySerialized() {
        CachedTopology cachedTopology = new CachedTopology(true);
        doNormalTopologyTest(cachedTopology);
    }

    /**
     * Test getting individual entities.
     */
    @Test
    public void testGetTopologyEntities() {
        CachedTopology cachedTopology = new CachedTopology(false);
        doTopologyEntitiesTest(cachedTopology);
    }

    /**
     * Test getting individual entities from a topology with serialization enabled.
     */
    @Test
    public void testGetTopologyEntitiesSerialized() {
        CachedTopology cachedTopology = new CachedTopology(true);
        doTopologyEntitiesTest(cachedTopology);
    }

    private void doTopologyEntitiesTest(CachedTopology cachedTopology) {
        cachedTopology.updateTopology(entities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO.Builder::getOid, Function.identity())));
        TopologyEntityDTO e = cachedTopology.getCachedEntitiesAsTopologyEntityDTOs(Collections.singletonList(1L)).get(1L);
        assertThat(e, is(entities.get(0).build()));
    }

    private void doNormalTopologyTest(CachedTopology cachedTopology) {
        cachedTopology.updateTopology(entities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO.Builder::getOid, Function.identity())));

        CachedTopologyResult result = cachedTopology.getTopology();
        // Size 3
        assertThat(result.toString(), containsString("3"));
        assertThat(result.getEntities().keySet(), containsInAnyOrder(1L, 2L, 3L));
    }
}
