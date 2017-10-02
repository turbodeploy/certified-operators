package com.vmturbo.repository.topology;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.operator.GraphCreatorFixture;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;

/**
 * Unit tests for {@link TopologyEventHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyEventHandlerTest {

    private TopologyEventHandler topologyEventHandlerTest;

    @Mock
    GraphDatabaseDriverBuilder mockGraphDatabaseDriverBuilder;

    @Mock
    GraphDatabaseDriver mockGraphDatabaseDriver;

    @Mock
    TopologyIDManager mockTopologyIDManager;

    GraphCreatorFixture graphCreatorFixture = new GraphCreatorFixture();

    GraphDefinition graphDefinition = graphCreatorFixture.getGraphDefinition();

    @Before
    public void setup() throws GraphDatabaseException {
        when(mockGraphDatabaseDriverBuilder.build(Matchers.anyString())).thenReturn(mockGraphDatabaseDriver);
        when(mockTopologyIDManager.databaseName(Matchers.any())).thenReturn("db-1");
        topologyEventHandlerTest = new TopologyEventHandler(
                    mockGraphDatabaseDriverBuilder, graphDefinition, mockTopologyIDManager);
    }

    @Test
    public void testCreateRealtimeTopologyGraph() throws Exception {
        final TopologyID tid = init(100L, 200L, TopologyType.SOURCE);

        verify(mockTopologyIDManager).register(tid);
        verify(mockTopologyIDManager).setCurrentRealTimeTopologyId(tid);
    }

    @Test
    public void testCreateNonRealtimeTopologyGraph() throws Exception {
        final TopologyID tid = init(100L, 200L, TopologyType.PROJECTED);

        verify(mockTopologyIDManager).register(tid);
        verify(mockTopologyIDManager, never()).setCurrentRealTimeTopologyId(Matchers.any());
    }

    @Test
    public void testDropDatabase() throws Exception {
        final TopologyID tid = init(100L, 200L, TopologyType.SOURCE);

        verify(mockTopologyIDManager).register(tid);
        verify(mockTopologyIDManager).setCurrentRealTimeTopologyId(tid);

        final TopologyID tid2 = init(300L, 400L, TopologyType.SOURCE);

        verify(mockTopologyIDManager).register(tid2);
        verify(mockTopologyIDManager).setCurrentRealTimeTopologyId(tid2);
        // Only one topology will be kept. The old one should be removed.
        verify(mockTopologyIDManager).deRegister(tid);
    }

    private TopologyID init(long contextId, long topologyId, TopologyType type) throws GraphDatabaseException {
        TopologyID tid = new TopologyID(contextId, topologyId, type);
        topologyEventHandlerTest.initializeTopologyGraph(tid);
        topologyEventHandlerTest.register(tid);
        return tid;
    }
}
