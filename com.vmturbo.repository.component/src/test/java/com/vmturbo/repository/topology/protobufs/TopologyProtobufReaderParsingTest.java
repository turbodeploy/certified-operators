package com.vmturbo.repository.topology.protobufs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Test converting ArangoDB stored strings to raw topology DTOs.
 *
 */
public class TopologyProtobufReaderParsingTest {

    private static final TopologyEntityDTO ENTITY_DTO = TopologyEntityDTO.newBuilder()
            .setEntityType(55)
            .setDisplayName("foo")
            .setOid(7L)
            .build();

    @Test
    public void testNotParsable() {
        List<ProjectedTopologyEntity> empty = TopologyProtobufReader.parseJson(1L, Collections.singleton("NOT-PARSABLE"))
                .collect(Collectors.toList());

        assertTrue(empty.isEmpty());
    }

    @Test
    public void testJsonParserOldFormat() {
        String str = ComponentGsonFactory.createGsonNoPrettyPrint().toJson(ENTITY_DTO);

        List<ProjectedTopologyEntity> dtos = TopologyProtobufReader.parseJson(1L, Collections.singleton(str)).collect(Collectors.toList());
        assertThat(dtos.size(), is(1));
        assertThat(dtos.get(0).getEntity(), is(ENTITY_DTO));
    }

    @Test
    public void testJsonParserNewFormat() {
        ProjectedTopologyEntity projectedTopologyEntity = ProjectedTopologyEntity.newBuilder()
                .setEntity(ENTITY_DTO)
                .setProjectedPriceIndex(20)
                .setOriginalPriceIndex(19)
                .build();
        String str = ComponentGsonFactory.createGsonNoPrettyPrint().toJson(projectedTopologyEntity);

        List<ProjectedTopologyEntity> dtos = TopologyProtobufReader.parseJson(1L, Collections.singleton(str)).collect(Collectors.toList());
        assertThat(dtos, contains(projectedTopologyEntity));
    }

}
