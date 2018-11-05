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

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * unit test for {@link SupplyChainService}.
 */
public class GraphDBServiceTest {

    private final GraphDBExecutor graphDBExecutor = Mockito.mock(GraphDBExecutor.class);

    private final GraphDefinition graphDefinition = Mockito.mock(GraphDefinition.class);

    private final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);

    private GraphDBService graphDBService;

    @Before
    public void setup() throws Exception {

        final TopologyDatabase topologyDatabase = Mockito.mock(TopologyDatabase.class);
        when(result.getRealtimeDatabase())
            .thenReturn(Optional.of(topologyDatabase));
        final TopologyID topologyId = new TopologyID(1, 2, TopologyID.TopologyType.SOURCE);
        when(result.getRealtimeTopologyId()).thenReturn(Optional.of(topologyId));

        graphDBService = new GraphDBService(
            graphDBExecutor,
            graphDefinition,
            result);
    }

    @Test
    public void testGetSupplyChain() throws Exception {
        final SupplyChainSubgraph subgraph = Mockito.mock(SupplyChainSubgraph.class);
        final SupplyChainNode node = SupplyChainNode.newBuilder()
            .setEntityType(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .build();
        when(subgraph.toSupplyChainNodes()).thenReturn(
            Collections.singletonList(node));
        when(graphDBExecutor.executeSupplyChainCmd(any(GraphCmd.GetSupplyChain.class))).thenReturn(
            Try.success(subgraph));

        final Map<String, SupplyChainNode> nodes =
            graphDBService.getSupplyChain(Optional.empty(), Optional.empty(), "123").get()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        assertEquals(node, nodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue()));
    }

    @Test
    public void testRetrieveRealTimeTopologyEntities() throws Exception {
        when(graphDefinition.getServiceEntityVertex()).thenReturn("111");
        final Try<Collection<ServiceEntityRepoDTO>> results =
                Try.of(() -> Collections.EMPTY_SET);
        when(graphDBExecutor.executeServiceEntityMultiGetCmd(any(GraphCmd.ServiceEntityMultiGet.class))).thenReturn(
                results);
        final Either either =
                graphDBService.retrieveRealTimeTopologyEntities(Collections.EMPTY_SET);
        assertTrue(either.isRight());
    }


    @Test
    public void testRetrieveRealTimeTopologyEntitiesWithoutReadTimeTopologyId() throws Exception {
        when(result.getRealtimeTopologyId()).thenReturn(Optional.empty());
        when(graphDefinition.getServiceEntityVertex()).thenReturn("111");
        final Try<Collection<ServiceEntityRepoDTO>> results =
                Try.of(() -> Collections.EMPTY_SET);
        when(graphDBExecutor.executeServiceEntityMultiGetCmd(any(GraphCmd.ServiceEntityMultiGet.class))).thenReturn(
                results);
        final Either either =
                graphDBService.retrieveRealTimeTopologyEntities(Collections.EMPTY_SET);
        assertTrue(either.isLeft());
    }
}
