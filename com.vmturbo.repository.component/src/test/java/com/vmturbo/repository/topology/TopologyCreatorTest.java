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
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager.EntityConverter;
import com.vmturbo.repository.topology.TopologyLifecycleManager.ProjectedTopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.SourceTopologyCreator;
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
        when(graphDatabaseDriverBuilder.build(any())).thenReturn(mockDriver);
        when(topologyProtobufsManager.createTopologyProtobufWriter(anyLong()))
                .thenReturn(protobufWriter);
        when(graphCreatorFactory.newGraphCreator(eq(mockDriver))).thenReturn(graphCreator);
    }

    @Test
    public void testTopologyCreatorRealtimeSourceTopology() {
        new SourceTopologyCreator(realtimeSourceId,
                graphDatabaseDriverBuilder,
                onComplete,
                graphCreatorFactory,
                entityConverter);

        verify(graphDatabaseDriverBuilder).build(eq(realtimeSourceId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorRealtimeProjectedTopology() {
        new ProjectedTopologyCreator(realtimeProjectedId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                realtimeTopologyContextId);

        verify(graphDatabaseDriverBuilder).build(eq(realtimeProjectedId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorPlanSourceTopology() {
        new SourceTopologyCreator(planSourceId,
                graphDatabaseDriverBuilder,
                onComplete,
                graphCreatorFactory,
                entityConverter);

        verify(graphDatabaseDriverBuilder).build(eq(planSourceId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorPlanProjectedTopology() {
        new ProjectedTopologyCreator(planProjectedId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                realtimeTopologyContextId);

        verify(graphDatabaseDriverBuilder).build(eq(planProjectedId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager)
            .createTopologyProtobufWriter(eq(planProjectedId.getTopologyId()));
    }

    @Test
    public void testTopologyCreatorInitialize() throws Exception {
        SourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.initialize();
        verify(graphCreator).init();
    }

    @Test
    public void testTopologyCreatorAddEntities() throws Exception {
        SourceTopologyCreator creator = newTopologyCreator(planSourceId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter, never()).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorProjectedAddEntities() throws Exception {
        ProjectedTopologyCreator creator = newProjectedTopologyCreator(planProjectedId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorComplete() throws Exception {
        SourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.complete();
        verify(onComplete).accept(eq(planSourceId));
    }

    @Test
    public void testTopologyCreatorRollback() throws Exception {
        SourceTopologyCreator creator = newTopologyCreator(planSourceId);
        creator.rollback();
        verify(mockDriver).dropDatabase();
    }

    @Test
    public void testProjectedTopologyCreatorRollback() throws Exception {
        ProjectedTopologyCreator creator = newProjectedTopologyCreator(planProjectedId);
        creator.rollback();
        verify(mockDriver).dropDatabase();
        verify(protobufWriter).delete();
    }

    private SourceTopologyCreator newTopologyCreator(TopologyID tid) {
        return new SourceTopologyCreator(tid,
                graphDatabaseDriverBuilder,
                onComplete,
                graphCreatorFactory,
                entityConverter);
    }

    private ProjectedTopologyCreator newProjectedTopologyCreator(TopologyID tid) {
        return new ProjectedTopologyCreator(tid,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter,
                realtimeTopologyContextId);
    }
}
