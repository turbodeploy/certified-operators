package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager.RegisteredTopologyLoader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

public class TopologyLifecycleManagerTest {

    private GraphDatabaseDriverBuilder graphDatabaseDriverBuilder =
            mock(GraphDatabaseDriverBuilder.class);

    private GraphDefinition graphDefinition = mock(GraphDefinition.class);

    private TopologyProtobufsManager topologyProtobufsManager =
            mock(TopologyProtobufsManager.class);

    private final long realtimeContextId = 7;

    private TopologyLifecycleManager topologyLifecycleManager;

    @Before
    public void setup() {
        topologyLifecycleManager = new TopologyLifecycleManager(graphDatabaseDriverBuilder,
                graphDefinition, topologyProtobufsManager, realtimeContextId);
    }

    @Test
    public void testNoTopology() {
        assertFalse(topologyLifecycleManager.getRealtimeDatabase().isPresent());
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());
        assertFalse(topologyLifecycleManager.getTopologyId(1,
                TopologyType.PROJECTED).isPresent());
        assertFalse(topologyLifecycleManager.databaseOf(1,
                TopologyType.PROJECTED).isPresent());
    }

    @Test
    public void testRegisterTopology() {
        final TopologyID source =
                new TopologyID(1L, 1L, TopologyType.SOURCE);
        final TopologyID projected =
                new TopologyID(1L, 2L, TopologyType.PROJECTED);
        topologyLifecycleManager.registerTopology(source);
        topologyLifecycleManager.registerTopology(projected);

        assertEquals(source,
                topologyLifecycleManager.getTopologyId(1L, TopologyType.SOURCE).get());
        assertEquals(projected,
                topologyLifecycleManager.getTopologyId(1L, TopologyType.PROJECTED).get());

        assertTrue(topologyLifecycleManager.databaseOf(1L, TopologyType.SOURCE).isPresent());
        assertTrue(topologyLifecycleManager.databaseOf(1L, TopologyType.PROJECTED).isPresent());
    }

    @Test
    public void testRegisterTopologyOverwritePrevious() {
        final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder =
                mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);

        final TopologyLifecycleManager topologyLifecycleManager =
            new TopologyLifecycleManager(graphDatabaseDriverBuilder, graphDefinition,
                    topologyProtobufsManager, realtimeContextId);

        final TopologyID source =
                new TopologyID(1L, 1L, TopologyType.SOURCE);
        topologyLifecycleManager.registerTopology(source);

        when(graphDatabaseDriverBuilder.build(eq(source.toDatabaseName()))).thenReturn(mockDriver);

        final TopologyID newSource =
                new TopologyID(1L, 2L, TopologyType.SOURCE);
        topologyLifecycleManager.registerTopology(newSource);

        verify(mockDriver).dropDatabase();
    }

    @Test
    public void testTopologyLoader() throws Exception {
        final TopologyID tid =
                new TopologyID(1L, 1L, TopologyType.SOURCE);
        final long pollingIntervalMs = 10;
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        when(mockDriverBuilder.listDatabases())
                .thenReturn(Sets.newHashSet(tid.toDatabaseName(), "BLAH"));
        doReturn(true).when(mockManager).registerTopology(any(), anyBoolean());

        final RegisteredTopologyLoader loader =
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager);
        loader.run();

        verify(mockDriverBuilder).listDatabases();
        // Exactly one registerTopology call - "BLAH" should be ignored.
        verify(mockManager).registerTopology(any(), anyBoolean());
        verify(mockManager).registerTopology(eq(tid), eq(false));
    }

    @Test
    public void testTopologyLoaderDropsNonRegistered() throws Exception {
        final TopologyID tid =
                new TopologyID(1L, 1L, TopologyType.SOURCE);
        final long pollingIntervalMs = 10;
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(mockDriverBuilder.listDatabases())
                .thenReturn(Sets.newHashSet(tid.toDatabaseName()));
        when(mockDriverBuilder.build(any())).thenReturn(mockDriver);

        // Return false to indicate that the topology was NOT registered.
        doReturn(false).when(mockManager).registerTopology(any(), anyBoolean());

        final RegisteredTopologyLoader loader =
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager);
        loader.run();

        verify(mockDriverBuilder).listDatabases();
        verify(mockManager).registerTopology(eq(tid), eq(false));

        // Verify that the loader drops the non-registered topology.
        verify(mockDriverBuilder).build(eq(tid.toDatabaseName()));
        verify(mockDriver).dropDatabase();
    }

    @Test
    public void testTopologyLoaderException() throws Exception {
        final TopologyID tid =
                new TopologyID(1L, 1L, TopologyType.SOURCE);
        final long pollingIntervalMs = 10;
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        when(mockDriverBuilder.listDatabases())
                // First try - throw exception.
                .thenThrow(mock(GraphDatabaseException.class))
                // Second time - return the real thing.
                .thenReturn(Collections.singleton(tid.toDatabaseName()));
        doReturn(true).when(mockManager).registerTopology(any(), anyBoolean());

        final RegisteredTopologyLoader loader =
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager);
        loader.run();

        // Once when the exception gets thrown, and once to get the actual database name.
        verify(mockDriverBuilder, times(2)).listDatabases();
        verify(mockManager).registerTopology(eq(tid), eq(false));
    }
}
