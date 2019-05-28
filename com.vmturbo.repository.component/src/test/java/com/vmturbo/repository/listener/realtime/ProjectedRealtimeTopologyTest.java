package com.vmturbo.repository.listener.realtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;

public class ProjectedRealtimeTopologyTest {

    final ProjectedTopologyEntity projectedVm = ProjectedTopologyEntity.newBuilder()
        .setOriginalPriceIndex(1)
        .setProjectedPriceIndex(2)
        .setEntity(TopologyEntityDTO.newBuilder()
            .setOid(1)
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName("vm"))
        .build();

    final ProjectedTopologyEntity projectedHost1 = ProjectedTopologyEntity.newBuilder()
        .setOriginalPriceIndex(1)
        .setProjectedPriceIndex(2)
        .setEntity(TopologyEntityDTO.newBuilder()
            .setOid(2)
            .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
            .setDisplayName("pm"))
        .build();

    final ProjectedTopologyEntity projectedHost2 = ProjectedTopologyEntity.newBuilder()
        .setOriginalPriceIndex(1)
        .setProjectedPriceIndex(2)
        .setEntity(TopologyEntityDTO.newBuilder()
            .setOid(3)
            .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
            .setDisplayName("pm"))
        .build();

    @Test
    public void testProjectedRealtimeTopology() {
        final long topologyId = 10;
        final TopologyInfo originalTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(9)
            .setTopologyType(TopologyType.REALTIME)
            .build();
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(mock(GlobalSupplyChainCalculator.class));
        final ProjectedTopologyBuilder projectedTopologyBuilder =
            liveTopologyStore.newProjectedTopology(topologyId, originalTopologyInfo);
        projectedTopologyBuilder.addEntities(Collections.singletonList(projectedVm));
        projectedTopologyBuilder.addEntities(Arrays.asList(projectedHost1, projectedHost2));
        projectedTopologyBuilder.finish();

        final ProjectedRealtimeTopology topology = liveTopologyStore.getProjectedTopology().get();
        assertThat(topology.getOriginalTopologyInfo(), is(originalTopologyInfo));
        assertThat(topology.getTopologyId(), is(topologyId));

        // Test get specific entities
        assertThat(topology.getEntities(Collections.singleton(projectedVm.getEntity().getOid()),
            Collections.emptySet()).collect(Collectors.toList()), containsInAnyOrder(projectedVm.getEntity()));

        // Test get specific entities, restrict by type
        assertThat(topology.getEntities(Sets.newHashSet(projectedVm.getEntity().getOid(), projectedHost1.getEntity().getOid()),
            Collections.singleton(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .collect(Collectors.toList()), containsInAnyOrder(projectedVm.getEntity()));

        // Test get all entities of type.
        assertThat(topology.getEntities(Collections.emptySet(),
            Collections.singleton(UIEntityType.PHYSICAL_MACHINE.typeNumber()))
            .collect(Collectors.toList()),
                containsInAnyOrder(projectedHost1.getEntity(), projectedHost2.getEntity()));
    }

    @Test
    public void testProjectedRealtimeTopologyDiags() throws DiagnosticsException {
        final long topologyId = 10;
        final TopologyInfo originalTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(9)
            .setTopologyType(TopologyType.REALTIME)
            .build();
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(mock(GlobalSupplyChainCalculator.class));
        final ProjectedTopologyBuilder projectedTopologyBuilder =
            liveTopologyStore.newProjectedTopology(topologyId, originalTopologyInfo);
        projectedTopologyBuilder.addEntities(Collections.singletonList(projectedVm));
        projectedTopologyBuilder.addEntities(Arrays.asList(projectedHost1, projectedHost2));
        projectedTopologyBuilder.finish();

        final ProjectedRealtimeTopology topology = liveTopologyStore.getProjectedTopology().get();

        final List<String> diags = topology.collectDiags().collect(Collectors.toList());
        JsonFormat.Parser parser = JsonFormat.parser();
        final List<TopologyEntityDTO> deserializedDiags = topology.collectDiags()
            .map(entityDiag -> {
                TopologyEntityDTO.Builder entityBldr = TopologyEntityDTO.newBuilder();
                try {
                    parser.merge(entityDiag, entityBldr);
                    return entityBldr.build();
                } catch (InvalidProtocolBufferException e) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        assertThat(deserializedDiags, containsInAnyOrder(projectedVm.getEntity(),
            projectedHost2.getEntity(), projectedHost1.getEntity()));
    }

}