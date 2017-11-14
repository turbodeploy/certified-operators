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
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
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

    private final GraphDatabaseDriver mockDriver = mock(GraphDatabaseDriver.class);

    private final TopologyProtobufWriter protobufWriter = mock(TopologyProtobufWriter.class);

    private final EntityConverter entityConverter = mock(EntityConverter.class);

    private final TopologyGraphCreator graphCreator = mock(TopologyGraphCreator.class);

    private final TopologyID sourceId =
            new TopologyID(1L, 1L, TopologyType.SOURCE);
    private final TopologyID projectedId =
            new TopologyID(1L, 1L, TopologyType.PROJECTED);

    @Before
    public void setup() {
        when(graphDatabaseDriverBuilder.build(any())).thenReturn(mockDriver);
        when(topologyProtobufsManager.createTopologyProtobufWriter(anyLong()))
                .thenReturn(protobufWriter);
        when(graphCreatorFactory.newGraphCreator(eq(mockDriver))).thenReturn(graphCreator);
    }

    @Test
    public void testTopologyCreatorConstructor() {

        new TopologyCreator(sourceId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter);

        verify(graphDatabaseDriverBuilder).build(eq(sourceId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager, never()).createTopologyProtobufWriter(anyLong());
    }

    @Test
    public void testTopologyCreatorProjectedTopology() {
        new TopologyCreator(projectedId,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter);

        verify(graphDatabaseDriverBuilder).build(eq(projectedId.toDatabaseName()));
        verify(graphCreatorFactory).newGraphCreator(eq(mockDriver));
        verify(topologyProtobufsManager)
            .createTopologyProtobufWriter(eq(projectedId.getTopologyId()));
    }

    @Test
    public void testTopologyCreatorInitialize() throws Exception {
        TopologyCreator creator = newTopologyCreator(sourceId);
        creator.initialize();
        verify(graphCreator).init();
    }

    @Test
    public void testTopologyCreatorAddEntities() throws Exception {
        TopologyCreator creator = newTopologyCreator(sourceId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter, never()).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorProjectedAddEntities() throws Exception {
        TopologyCreator creator = newTopologyCreator(projectedId);

        when(entityConverter.convert(any())).thenReturn(Collections.emptySet());
        creator.addEntities(Collections.emptySet());

        verify(entityConverter).convert(any());
        verify(graphCreator).updateTopologyToDb(any());
        verify(protobufWriter).storeChunk(any());
    }

    @Test
    public void testTopologyCreatorComplete() throws Exception {
        TopologyCreator creator = newTopologyCreator(sourceId);
        creator.complete();
        verify(onComplete).accept(eq(sourceId));
    }

    @Test
    public void testTopologyCreatorRollback() throws Exception {
        TopologyCreator creator = newTopologyCreator(sourceId);
        creator.rollback();
        verify(mockDriver).dropDatabase();
    }

    @Test
    public void testProjectedTopologyCreatorRollback() throws Exception {
        TopologyCreator creator = newTopologyCreator(projectedId);
        creator.rollback();
        verify(mockDriver).dropDatabase();
        verify(protobufWriter).delete();
    }

    private TopologyCreator newTopologyCreator(TopologyID tid) {
        return new TopologyCreator(tid,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                onComplete,
                graphCreatorFactory,
                entityConverter);

    }
}
