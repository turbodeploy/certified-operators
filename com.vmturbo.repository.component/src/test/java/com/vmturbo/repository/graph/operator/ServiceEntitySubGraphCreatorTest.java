package com.vmturbo.repository.graph.operator;

import static com.vmturbo.repository.graph.result.ResultsFixture.BA_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DC_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.parameter.CollectionParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter;
import com.vmturbo.repository.graph.parameter.VertexParameter;


@RunWith(MockitoJUnitRunner.class)
public class ServiceEntitySubGraphCreatorTest {

    private ServiceEntitySubGraphCreator serviceEntitySubGraphCreatorTest;

    @Mock
    private GraphDatabaseDriver graphDatabaseBuilder;

    private GraphDefinition graphDefinition;

    private GraphCreatorFixture fixture;

    private static final int BATCH_SIZE = 2;
    private static final int REPLICA_COUNT = 1;

    @Before
    public void setUp() {
        fixture = new GraphCreatorFixture();

        graphDefinition = fixture.getGraphDefinition();

        serviceEntitySubGraphCreatorTest = new ServiceEntitySubGraphCreator(
                graphDatabaseBuilder,
                graphDefinition,
                BATCH_SIZE, REPLICA_COUNT);
    }

    @Test
    public void testInit() throws CollectionOperationException {
        serviceEntitySubGraphCreatorTest.init();

        ArgumentCaptor<CollectionParameter> p = ArgumentCaptor.forClass(CollectionParameter.class);
        verify(graphDatabaseBuilder, times(4)).createCollection(p.capture());
        assertEquals(graphDefinition.getServiceEntityVertex(), p.getAllValues().get(0).getName());
        assertEquals(graphDefinition.getProviderRelationship(), p.getAllValues().get(1).getName());
    }

    @Test
    public void testReset() throws CollectionOperationException {
        serviceEntitySubGraphCreatorTest.reset();

        verify(graphDatabaseBuilder).emptyCollection(graphDefinition.getServiceEntityVertex());
        verify(graphDatabaseBuilder).emptyCollection(graphDefinition.getProviderRelationship());
    }

    @Test
    public void testCreateInBatchModeSize2() throws Exception {
        // Topology with four vertices and three edges created for the graph
        List<ServiceEntityRepoDTO> ses = generateTopology();
        final int batchSize = 2;
        final int numOfVertexCreation = 3;
        final int numOfEdgeCreation = 3;

        testCreateInBatchMode(ses, batchSize, numOfVertexCreation, numOfEdgeCreation);
    }

    @Test
    public void testCreateInBatchModeSize1() throws Exception {
        // Topology with four vertices and three edges created for the graph
        List<ServiceEntityRepoDTO> ses = generateTopology();
        final int batchSize = 1;
        final int numOfVertexCreation = 5;
        final int numOfEdgeCreation = 5;

        testCreateInBatchMode(ses, batchSize, numOfVertexCreation, numOfEdgeCreation);
    }

    @Test
    public void testCreateInBatchModeSize100() throws Exception {
        // Topology with four vertices and three edges created for the graph
        List<ServiceEntityRepoDTO> ses = generateTopology();
        final int batchSize = 100;
        final int numOfVertexCreation = 1;
        final int numOfEdgeCreation = 2;

        testCreateInBatchMode(ses, batchSize, numOfVertexCreation, numOfEdgeCreation);
    }

    /**
     * Create a topology with the following entities:
     *     one BusinessAccount,
     *     one DC,
     *     one PM,
     *     two VMs,
     * where BusinessAccount is connected to two VMs, two VMs are hosted by the PM, which is
     * hosted by the DC
     *
     * So, five vertices and five edges will be created.
     *     BA --> VM1, BA --> VM2, VM1 --> PM, VM2 --> PM, PM --> DC
     */
    private List<ServiceEntityRepoDTO> generateTopology() {
        final ServiceEntityRepoDTO ba = fill(1, BA_TYPE).get(0);
        final ServiceEntityRepoDTO dc = fill(1, DC_TYPE).get(0);
        final ServiceEntityRepoDTO pm = fill(1, PM_TYPE).get(0);
        final List<ServiceEntityRepoDTO> vmInstances = fill(2, VM_TYPE);
        // set connected
        ba.setConnectedEntityList(vmInstances.stream().map(vm -> {
            ConnectedEntityRepoDTO connectedEntity = new ConnectedEntityRepoDTO();
            connectedEntity.setConnectedEntityId(Long.valueOf(vm.getOid()));
            return connectedEntity;
        }).collect(Collectors.toList()));
        // set consumes
        pm.setProviders(Arrays.asList(dc.getOid()));
        vmInstances.get(0).setProviders(Arrays.asList(pm.getOid()));
        vmInstances.get(1).setProviders(Arrays.asList(pm.getOid()));

        return Arrays.asList(ba, dc, pm, vmInstances.get(0), vmInstances.get(1));
    }

    private void testCreateInBatchMode(
            List<ServiceEntityRepoDTO> ses,
            int batchSize,
            int numOfExecutionForVertexCreation,
            int numOfExecutionForEdgeCreation) throws Exception {
        ServiceEntitySubGraphCreator creator = new ServiceEntitySubGraphCreator(
                graphDatabaseBuilder,
                graphDefinition,
                batchSize, REPLICA_COUNT);

        creator.create(ses, null);

        ArgumentCaptor<VertexParameter> vp = ArgumentCaptor.forClass(VertexParameter.class);
        ArgumentCaptor<EdgeParameter> ep = ArgumentCaptor.forClass(EdgeParameter.class);
        verify(graphDatabaseBuilder, times(numOfExecutionForVertexCreation))
                .createVerticesInBatch(vp.capture());
        verify(graphDatabaseBuilder, times(numOfExecutionForEdgeCreation))
                .createEdgesInBatch(ep.capture());
    }
}
