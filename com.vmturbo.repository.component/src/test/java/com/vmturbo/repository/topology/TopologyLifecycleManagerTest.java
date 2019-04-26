package com.vmturbo.repository.topology;

import static com.vmturbo.repository.topology.TopologyLifecycleManager.deleteObsoletedRealtimeDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
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

    private ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    private TopologyLifecycleManager topologyLifecycleManager;

    @Before
    public void setup() {
        topologyLifecycleManager = new TopologyLifecycleManager(graphDatabaseDriverBuilder,
                graphDefinition, topologyProtobufsManager, realtimeContextId, scheduler, 0, 2, 2, false);
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
                    topologyProtobufsManager, realtimeContextId, mock(ScheduledExecutorService.class),
                0, 2, 2, false);

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
    public void testRealtimeTopologyID() {
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());

        // verify that "isPresent" is false when there is no topology yet.
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());

        // register a topology
        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE));
        Optional<TopologyID> optionalDynamicTopologyID = topologyLifecycleManager.getRealtimeTopologyId();
        assertTrue(optionalDynamicTopologyID.isPresent());
        TopologyID dynamicTopologyID = optionalDynamicTopologyID.get();
        assertEquals("topology-7-SOURCE-1", dynamicTopologyID.toDatabaseName());
        // verify the dynamic database works too
        TopologyDatabase db = dynamicTopologyID.database();
        assertEquals("topology-7-SOURCE-1",TopologyDatabases.getDbName(db));

        // register another source topology, and validate that the dynamic topology id follows along.
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(graphDatabaseDriverBuilder.build(eq("topology-7-SOURCE-1"))).thenReturn(mockDriver);

        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 2L, TopologyType.SOURCE));
        assertEquals(2L, dynamicTopologyID.getTopologyId());
        assertEquals("topology-7-SOURCE-2",TopologyDatabases.getDbName(db));
    }

    @Test
    public void testRealtimeDatabase() {
        // verify that "isPresent" is false when there is no topology yet.
        assertFalse(topologyLifecycleManager.getRealtimeDatabase().isPresent());

        final TopologyID source =
                new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE);
        topologyLifecycleManager.registerTopology(source);
        // now we should have a valid lazy reference
        Optional<TopologyDatabase> optionalTopologyDatabase = topologyLifecycleManager.getRealtimeDatabase();
        assertTrue(optionalTopologyDatabase.isPresent());
        TopologyDatabase lazyRealtimeDatabase = optionalTopologyDatabase.get();
        assertEquals("topology-7-SOURCE-1",TopologyDatabases.getDbName(lazyRealtimeDatabase));
        // verify that hasValue is true when there is a topology registered
        assertTrue(topologyLifecycleManager.getRealtimeDatabase().isPresent());

        // register another source topology, and validate that the database reference follows along.
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(graphDatabaseDriverBuilder.build(eq(source.toDatabaseName()))).thenReturn(mockDriver);

        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 2L, TopologyType.SOURCE));
        assertEquals(2L, topologyLifecycleManager.getRealtimeTopologyId().get().getTopologyId());
        assertEquals("topology-7-SOURCE-2",TopologyDatabases.getDbName(lazyRealtimeDatabase));
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
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager, realtimeContextId);
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
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager, realtimeContextId);
        loader.run();

        verify(mockDriverBuilder).listDatabases();
        verify(mockManager).registerTopology(eq(tid), eq(false));

        // Verify that the loader drops the non-registered non-realtime topology.
        verify(mockDriverBuilder).build(eq(tid.toDatabaseName()));
        verify(mockDriver).dropDatabase();
    }

    @Test
    public void testTopologyLoaderNotDropingRealtimeNonRegistered() throws Exception {
        final TopologyID tid =
            new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE);
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
            new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager, realtimeContextId);
        loader.run();

        verify(mockDriverBuilder).listDatabases();
        verify(mockManager).registerTopology(eq(tid), eq(false));

        // Verify that the loader drops the non-registered non-realtime topology.
        verify(mockDriverBuilder, never()).build(eq(tid.toDatabaseName()));
        verify(mockDriver, never()).dropDatabase();
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
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager, realtimeContextId);
        loader.run();

        // Once when the exception gets thrown, and once to get the actual database name.
        verify(mockDriverBuilder, times(2)).listDatabases();
        verify(mockManager).registerTopology(eq(tid), eq(false));
    }

    @Test
    public void testDelayedDrop() {
        // create a lifecycle manager with a delayed drop setting of 5 seconds.
        TopologyLifecycleManager lifecycleManager = new TopologyLifecycleManager(graphDatabaseDriverBuilder,
                graphDefinition, topologyProtobufsManager, realtimeContextId, scheduler, 5, 2, 2, false);

        // register a topology
        final TopologyID source = new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE);
        lifecycleManager.registerTopology(source);

        // register another source topology, and validate that the first database drop was scheduled with a 5 second delay.
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(graphDatabaseDriverBuilder.build(eq(source.toDatabaseName()))).thenReturn(mockDriver);

        lifecycleManager.registerTopology(new TopologyID(realtimeContextId, 2L, TopologyType.SOURCE));

        // we know the drop task itself works from other tests - we'll just verify that the drop is scheduled
        verify(scheduler).schedule((Runnable) any(), eq(5L), eq(TimeUnit.SECONDS));
    }


    // Verify it will always tyr to drop the DB associated with passed in Toplogy Id.
    // Also when there are less realtime topologies than numberOfExpectedRealtimeSourceDB and
    // numberOfExpectedRealtimeProjectedDB, it will not drop any of them.
    @Test
    public void testCleanUpObsoletedRealtimeTopologies() throws Exception {
        final TopologyID tid =
            new TopologyID(realtimeContextId, 1, TopologyType.SOURCE);
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(mockDriverBuilder.listDatabases())
            .thenReturn(getTopologyIDSet(2, 1000));
        when(mockDriverBuilder.build(any())).thenReturn(mockDriver);

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, realtimeContextId, 2, 2);
       // cleaner.run();

        verify(mockDriverBuilder).listDatabases();
        // want to drop the earliest (with topologyId 1)
        verify(mockDriverBuilder, times(1)).build(eq("topology-7-SOURCE-1"));
        verify(mockDriver, times(1)).dropDatabase();
    }

    // Verify when there are more realtime topologies than numberOfExpectedRealtimeSourceDB and
    // numberOfExpectedRealtimeProjectedDB, it will drop all the obsoleted ones.
    @Test
    public void testCleanUpObsoletedRealtimeTopologiesDropsObsoletedRealtimeDB() throws Exception {
        final TopologyID tid =
            new TopologyID(realtimeContextId, 1, TopologyType.SOURCE);
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        final long scale = 72931736035616L;
        when(mockDriverBuilder.listDatabases())
            .thenReturn(getTopologyIDSet(3, scale));
        when(mockDriverBuilder.build(any())).thenReturn(mockDriver);

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, realtimeContextId, 2, 2);

        verify(mockDriverBuilder).listDatabases();
        // want to drop the earliest (with topologyId 1)
        verify(mockDriverBuilder, times(3))
            .build(matches("^topology-7-(PROJECTED|SOURCE)-(1|72931736035617)$"));
        verify(mockDriver, times(3)).dropDatabase();
    }

    @Test
    public void testCleanUpObsoletedRealtimeTopologiesException() throws Exception {
        final TopologyID tid =
            new TopologyID(realtimeContextId, 1, TopologyType.SOURCE);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(mockDriverBuilder.build(any())).thenReturn(mockDriver);
        when(mockDriverBuilder.listDatabases())
            // First try - throw exception.
            .thenThrow(mock(GraphDatabaseException.class))
            // Second time - return the real thing.
            .thenReturn(getTopologyIDSet(3, 1000));

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, realtimeContextId, 2, 2);
        verify(mockDriver, times(1)).dropDatabase();
    }

    // get topology set with both real time and plan topologies.
    private Set<String> getTopologyIDSet(int n, long scale) {
        List<TopologyID> topologyIDS = Lists.newArrayList();
        // reverse the order to make sure sorting are involved.
        for (int i =n; i >= 1; i--) {
            topologyIDS.add(new TopologyID(realtimeContextId, i, TopologyType.SOURCE));
            topologyIDS.add(new TopologyID(realtimeContextId, i +  scale, TopologyType.PROJECTED));
        }
        // Always added not real time topology (e.g. for plans), and they should be ignored
        topologyIDS.add(new TopologyID(realtimeContextId + 1, n + 1, TopologyType.SOURCE));
        topologyIDS.add(new TopologyID(realtimeContextId + 1, n + 1 + scale, TopologyType.PROJECTED));
        return topologyIDS.stream().map(TopologyID::toDatabaseName).collect(Collectors.toSet());
    }

}
