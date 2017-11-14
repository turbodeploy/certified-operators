package com.vmturbo.repository.service;

import static com.vmturbo.repository.graph.result.ResultsFixture.CONTAINER_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static com.vmturbo.repository.graph.result.ResultsFixture.supplyChainQueryResultFor;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainExecutorResult;
import com.vmturbo.repository.graph.result.SupplyChainQueryResult;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * unit test for {@link SupplyChainService}.
 */
public class GraphDBServiceTest {

    private final GraphDBExecutor graphDBExecutor = Mockito.mock(GraphDBExecutor.class);

    private final GraphDefinition graphDefinition = Mockito.mock(GraphDefinition.class);

    private GraphDBService graphDBService;

    private final int numPMs = 5;
    private final int numVMs = 2;
    private final int numContainers = 2;

    private final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
    private final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);
    private final List<ServiceEntityRepoDTO> containerInstances = fill(numContainers, CONTAINER_TYPE);

    private final SupplyChainQueryResult queryResult = supplyChainQueryResultFor(
        PM_TYPE, pmInstances,
        new ImmutableList.Builder<ServiceEntityRepoDTO>().addAll(vmInstances).addAll(containerInstances).build());

    @Before
    public void setup() throws Exception {
        final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);
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
        when(graphDBExecutor.executeSupplyChainCmd(any(GraphCmd.GetSupplyChain.class))).thenReturn(
            Try.success(new SupplyChainExecutorResult(
                    Collections.emptyList(),
                    Collections.singletonList(queryResult))
            ));

        final Map<String, SupplyChainNode> nodes =
            graphDBService.getSupplyChain(Optional.empty(), "123").get()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        assertEquals(numPMs, nodes.get(PM_TYPE).getMemberOidsList().size());
        assertEquals(numVMs, nodes.get(VM_TYPE).getMemberOidsList().size());
        assertEquals(numContainers, nodes.get(CONTAINER_TYPE).getMemberOidsList().size());

        assertEquals(2, nodes.get(PM_TYPE).getConnectedConsumerTypesList().size());
        assertThat(
            nodes.get(PM_TYPE).getConnectedConsumerTypesList().stream().collect(Collectors.toList()),
            containsInAnyOrder(VM_TYPE, CONTAINER_TYPE)
        );
    }
}
