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
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager.RegisteredTopologyLoader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

public class TopologyLifecycleManagerTest {

    private GraphDatabaseDriverBuilder graphDatabaseDriverBuilder =
            mock(GraphDatabaseDriverBuilder.class);

    private GraphDefinition graphDefinition = mock(GraphDefinition.class);

    private TopologyProtobufsManager topologyProtobufsManager =
            mock(TopologyProtobufsManager.class);

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private final long realtimeContextId = 7;

    private ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    private TopologyLifecycleManager topologyLifecycleManager;

    private GlobalSupplyChainManager globalSupplyChainManager =
            mock(GlobalSupplyChainManager.class);

    private GraphDBExecutor graphDBExecutor = mock(GraphDBExecutor.class);

    private static final String DATABASE_NAME = "Tturbonomic";

    @Before
    public void setup() {
        topologyLifecycleManager = new TopologyLifecycleManager(graphDatabaseDriverBuilder,
            graphDefinition, topologyProtobufsManager, realtimeContextId, scheduler,
            liveTopologyStore, 0, 2, 2,
            globalSupplyChainManager, graphDBExecutor,false);
        when(graphDBExecutor.getArangoDatabaseName()).thenReturn(DATABASE_NAME);
    }

    @Test
    public void testNoTopology() {
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());
        assertFalse(topologyLifecycleManager.getTopologyId(1,
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
    }

    @Test
    public void testRealtimeTopologyID() {
        // verify that "isPresent" is false when there is no topology yet.
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());

        // register a topology
        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE));
        Optional<TopologyID> optionalDynamicTopologyID = topologyLifecycleManager.getRealtimeTopologyId();
        assertTrue(optionalDynamicTopologyID.isPresent());
        TopologyID dynamicTopologyID = optionalDynamicTopologyID.get();

        // register another source topology, and validate that the dynamic topology id follows along.
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(graphDatabaseDriverBuilder.build(eq(DATABASE_NAME), eq("-7-S-1"))).thenReturn(mockDriver);

        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 2L, TopologyType.SOURCE));
        assertEquals(2L, dynamicTopologyID.getTopologyId());
    }

    @Test
    public void testTopologyLoader() throws Exception {
        final TopologyID tid =
            new TopologyID(1L, 1L, TopologyType.SOURCE);
        final long pollingIntervalMs = 10;
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        when(mockDriverBuilder.listCollections(DATABASE_NAME))
                .thenReturn(Sets.newHashSet("globalSCEntitiesInfo" + tid.toCollectionNameSuffix(), "BLAH"));
        doReturn(true).when(mockManager).registerTopology(any(), anyBoolean());
        doReturn(Optional.empty()).when(mockManager).getRealtimeTopologyId();

        final RegisteredTopologyLoader loader =
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager,
                        globalSupplyChainManager, realtimeContextId, graphDBExecutor);
        loader.run();

        verify(mockDriverBuilder).listCollections(DATABASE_NAME);
        // Exactly one registerTopology call - "BLAH" should be ignored.
        verify(mockManager).registerTopology(any(), anyBoolean());
        verify(mockManager).registerTopology(eq(tid), eq(false));
    }

    @Test
    public void testTopologyLoaderException() throws Exception {
        final TopologyID tid =
            new TopologyID(1L, 1L, TopologyType.SOURCE);
        final long pollingIntervalMs = 10;
        final TopologyLifecycleManager mockManager = mock(TopologyLifecycleManager.class);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        when(mockDriverBuilder.listCollections(DATABASE_NAME))
                // First try - throw exception.
                .thenThrow(mock(GraphDatabaseException.class))
                // Second time - return the real thing.
                .thenReturn(Collections.singleton("globalSCEntitiesInfo" + tid.toCollectionNameSuffix()));
        doReturn(true).when(mockManager).registerTopology(any(), anyBoolean());
        doReturn(Optional.empty()).when(mockManager).getRealtimeTopologyId();

        final RegisteredTopologyLoader loader =
                new RegisteredTopologyLoader(pollingIntervalMs, mockDriverBuilder, mockManager,
                        globalSupplyChainManager, realtimeContextId, graphDBExecutor);
        loader.run();

        // Once when the exception gets thrown, and once to get the actual database name.
        verify(mockDriverBuilder, times(2)).listCollections(DATABASE_NAME);
        verify(mockManager).registerTopology(eq(tid), eq(false));
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
        when(mockDriverBuilder.listCollections(DATABASE_NAME))
            .thenReturn(getTopologyIDSet(2, 1000));
        when(mockDriverBuilder.build(any(), any())).thenReturn(mockDriver);

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, globalSupplyChainManager, realtimeContextId,
            2, 2, graphDBExecutor);
       // cleaner.run();

        verify(mockDriverBuilder).listCollections(DATABASE_NAME);
        // want to drop the earliest (with topologyId 1)
        verify(mockDriverBuilder, times(1)).build(eq(DATABASE_NAME), eq("-7-S-1"));
        verify(mockDriver, times(1)).dropCollections();
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
        when(mockDriverBuilder.listCollections(DATABASE_NAME))
            .thenReturn(getTopologyIDSet(3, scale));
        when(mockDriverBuilder.build(any(), any())).thenReturn(mockDriver);

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, globalSupplyChainManager, realtimeContextId,
            2, 2, graphDBExecutor);

        verify(mockDriverBuilder).listCollections(DATABASE_NAME);
        // want to drop the earliest (with topologyId 1)
        verify(mockDriverBuilder, times(3))
            .build(eq(DATABASE_NAME), matches("^-7-(P|S)-(1|72931736035617)$"));
        verify(mockDriver, times(3)).dropCollections();
    }

    @Test
    public void testCleanUpObsoletedRealtimeTopologiesException() throws Exception {
        final TopologyID tid =
            new TopologyID(realtimeContextId, 1, TopologyType.SOURCE);
        final GraphDatabaseDriverBuilder mockDriverBuilder = mock(GraphDatabaseDriverBuilder.class);
        final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);
        when(mockDriverBuilder.build(any(), any())).thenReturn(mockDriver);
        when(mockDriverBuilder.listCollections(DATABASE_NAME))
            // First try - throw exception.
            .thenThrow(mock(GraphDatabaseException.class))
            // Second time - return the real thing.
            .thenReturn(getTopologyIDSet(3, 1000));

        deleteObsoletedRealtimeDB(tid, mockDriverBuilder, globalSupplyChainManager, realtimeContextId,
            2, 2, graphDBExecutor);
        verify(mockDriver, times(1)).dropCollections();
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
        return topologyIDS.stream().map(tid -> "topology" + tid.toCollectionNameSuffix()).collect(Collectors.toSet());
    }

}
