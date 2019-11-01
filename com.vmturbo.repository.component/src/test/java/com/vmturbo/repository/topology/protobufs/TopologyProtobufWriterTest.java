package com.vmturbo.repository.topology.protobufs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionEntity;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Verify that some ArangoDB operations are invoked as expected.
 *
 */
public class TopologyProtobufWriterTest {

    ArangoDatabaseFactory factory;
    ArangoDB db;
    ArangoDatabase database;
    private ArangoCollection collection; // writer

    @Before
    public void setup() {
        db = mock(ArangoDB.class);
        factory = mock(ArangoDatabaseFactory.class);
        database = mock(ArangoDatabase.class);
        collection = mock(ArangoCollection.class);
        when(factory.getArangoDriver()).thenReturn(db);
        when(db.getDatabases()).thenReturn(Lists.newArrayList());
        when(db.createDatabase(Mockito.any())).thenReturn(true);
        when(db.db(Mockito.any())).thenReturn(database);
        when(database.collection(Mockito.eq("topology-dtos-2222"))).thenReturn(collection);
        CollectionEntity info = mock(CollectionEntity.class);
        when(collection.getInfo()).thenReturn(info);
    }

    @Test
    public void testWriter() {
        TopologyProtobufsManager tpm = new TopologyProtobufsManager(factory);
        final TopologyProtobufWriter writer = tpm.createProjectedTopologyProtobufWriter(2222);
        verify(db).createDatabase(Mockito.eq("topology-protobufs"));
        verify(database).collection(Mockito.eq("topology-dtos-2222"));
        verify(database).createCollection(Mockito.eq("topology-dtos-2222"));
        assertSame(writer.topologyCollection, collection);

        assertEquals(0, writer.sequenceNumber);
        writer.storeChunk(Lists.emptyList());
        writer.storeChunk(Lists.emptyList());
        verify(collection, times(2)).insertDocument(any());
        assertEquals(2, writer.sequenceNumber);

        writer.delete();
        verify(collection).drop();
    }
}
