package com.vmturbo.repository.topology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager.ArangoProjectedTopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.ArangoSourceTopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.EntityConverter;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyGraphCreatorFactory;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufWriter;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

public class TopologyCreatorTest {

    private GraphDatabaseDriverBuilder graphDatabaseDriverBuilder =
            mock(GraphDatabaseDriverBuilder.class);

    private TopologyProtobufsManager topologyProtobufsManager =
            mock(TopologyProtobufsManager.class);

    private final Consumer<TopologyID> onComplete = mock(Consumer.class);

    private final TopologyGraphCreatorFactory graphCreatorFactory =
            mock(TopologyGraphCreatorFactory.class);

    private final long realtimeTopologyContextId = 123456L;

    private final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);

    private final TopologyProtobufWriter protobufWriter = mock(TopologyProtobufWriter.class);

    private final EntityConverter entityConverter = mock(EntityConverter.class);

    private final TopologyGraphCreator graphCreator = mock(TopologyGraphCreator.class);

    private final GlobalSupplyChainManager globalSupplyChainManager =
            mock(GlobalSupplyChainManager.class);

    private final GraphDBExecutor graphDBExecutor = mock(GraphDBExecutor.class);

    private final TopologyID realtimeSourceId =
        new TopologyID(realtimeTopologyContextId, 1L, TopologyType.SOURCE);
    private final TopologyID realtimeProjectedId =
        new TopologyID(realtimeTopologyContextId, 1L, TopologyType.PROJECTED);
    private final TopologyID planSourceId =
        new TopologyID(1L, 1L, TopologyType.SOURCE);
    private final TopologyID planProjectedId =
        new TopologyID(1L, 1L, TopologyType.PROJECTED);


    @Before
    public void setup() {
        when(graphDatabaseDriverBuilder.build(any(), any())).thenReturn(mockDriver);
        when(topologyProtobufsManager.createSourceTopologyProtobufWriter(anyLong()))
            .thenReturn(protobufWriter);
        when(topologyProtobufsManager.createProjectedTopologyProtobufWriter(anyLong()))
                .thenReturn(protobufWriter);
        when(graphCreatorFactory.newGraphCreator(eq(mockDriver))).thenReturn(graphCreator);
    }

    @Test
    public void testTopologyCreatorRealtimeSourceTopology() {
        new ArangoSourceTopologyCreator(realtimeSourceId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);

        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createProjectedTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorRealtimeProjectedTopology() {
        new ArangoProjectedTopologyCreator(realtimeProjectedId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);

        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createProjectedTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorPlanSourceTopology() {
        new ArangoSourceTopologyCreator(planSourceId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);

        verify(graphDatabaseDriverBuilder).build(eq(graphDBExecutor.getArangoDatabaseName()),
            eq(planSourceId.toCollectionNameSuffix()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager)
            .createSourceTopologyProtobufWriter(eq(planSourceId.getTopologyId()));
    }

    @Test
    public void testTopologyCreatorPlanProjectedTopology() {
        new ArangoProjectedTopologyCreator(planProjectedId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);

        verify(graphDatabaseDriverBuilder).build(eq(graphDBExecutor.getArangoDatabaseName()),
            eq(planProjectedId.toCollectionNameSuffix()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager)
            .createProjectedTopologyProtobufWriter(eq(planProjectedId.getTopologyId()));
    }

    @Test
    public void testTopologyCreatorInitialize() throws Exception {
        ArangoSourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.initialize();
        verify(graphCreator).init();
    }

    @Test
    public void testTopologyCreatorAddEntities() throws Exception {
        ArangoSourceTopologyCreator creator = newTopologyCreator(planSourceId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorProjectedAddEntities() throws Exception {
        ArangoProjectedTopologyCreator creator = newProjectedTopologyCreator(planProjectedId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorComplete() throws Exception {
        ArangoSourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.complete();
        verify(onComplete).accept(eq(planSourceId));
    }

    @Test
    public void testTopologyCreatorRollback() throws Exception {
        ArangoSourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.rollback();
        verify(mockDriver).dropCollections();
    }

    @Test
    public void testProjectedTopologyCreatorRollback() throws Exception {
        ArangoProjectedTopologyCreator creator = newProjectedTopologyCreator(planProjectedId);
        creator.rollback();
        verify(mockDriver).dropCollections();
        verify(protobufWriter).delete();
    }

    private ArangoSourceTopologyCreator newTopologyCreator(TopologyID tid) {
        return new ArangoSourceTopologyCreator(tid,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);
    }

    private ArangoProjectedTopologyCreator newProjectedTopologyCreator(TopologyID tid) {
        return new ArangoProjectedTopologyCreator(tid,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                globalSupplyChainManager,
                graphDBExecutor,
                realtimeTopologyContextId, 2, 2);
    }
}
