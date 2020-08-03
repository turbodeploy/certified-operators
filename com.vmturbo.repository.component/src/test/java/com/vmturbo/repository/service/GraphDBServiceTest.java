package com.vmturbo.repository.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javaslang.control.Either;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * unit test for {@link SupplyChainService}.
 */
public class GraphDBServiceTest {

    private final ArangoDBExecutor graphDBExecutor = Mockito.mock(ArangoDBExecutor.class);

    private final GraphDefinition graphDefinition = Mockito.mock(GraphDefinition.class);

    private final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);

    private GraphDBService graphDBService;

    @Before
    public void setup() throws Exception {
        final TopologyID topologyId = new TopologyID(1, 2, TopologyID.TopologyType.SOURCE);
        when(result.getRealtimeTopologyId()).thenReturn(Optional.of(topologyId));
        when(graphDefinition.getSEVertexCollection(topologyId)).thenCallRealMethod();

        graphDBService = new GraphDBService(
            graphDBExecutor,
            graphDefinition,
            result);
    }

    @Test
    public void testGetSupplyChain() throws Exception {
        final SupplyChainSubgraph subgraph = Mockito.mock(SupplyChainSubgraph.class);
        final SupplyChainNode node = SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .build();
        when(subgraph.toSupplyChainNodes()).thenReturn(
            Collections.singletonList(node));
        when(graphDBExecutor.executeSupplyChainCmd(any(GraphCmd.GetSupplyChain.class))).thenReturn(
            Try.success(subgraph));

        final Map<String, SupplyChainNode> nodes =
            graphDBService.getSupplyChain(Optional.empty(), Optional.empty(), "123",
                    Optional.empty(), Collections.emptySet(), Collections.emptySet()).get()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        assertEquals(node, nodes.get(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
    }
}
