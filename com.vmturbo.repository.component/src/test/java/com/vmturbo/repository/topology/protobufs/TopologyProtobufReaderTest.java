package com.vmturbo.repository.topology.protobufs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.arangodb.model.CollectionCreateOptions;
import com.google.common.collect.Maps;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Verify that some ArangoDB operations are invoked as expected.
 *
 */
public class TopologyProtobufReaderTest {

    private static long NUM_CHUNKS = 7;
    ArangoDatabaseFactory factory;
    ArangoDB db;
    ArangoDatabase database;
    CollectionCreateOptions collectionCreateOptions;
    private ArangoCollection collection;

    @Before
    public void setup() {
        db = mock(ArangoDB.class);
        factory = mock(ArangoDatabaseFactory.class);
        database = mock(ArangoDatabase.class);
        collection = mock(ArangoCollection.class);
        collectionCreateOptions = mock(CollectionCreateOptions.class);
        CollectionPropertiesEntity count = mock(CollectionPropertiesEntity.class);
        when(factory.getArangoDriver()).thenReturn(db);
        when(db.getDatabases()).thenReturn(Lists.newArrayList());
        when(db.db(Mockito.any())).thenReturn(database);
        when(database.collection(Mockito.eq("topology-dtos-1111"))).thenReturn(collection);
        when(collection.count()).thenReturn(count);
        CollectionEntity info = mock(CollectionEntity.class);
        when(collection.getInfo()).thenReturn(info);
        when(count.getCount()).thenReturn(NUM_CHUNKS);
    }

    @Test
    public void testReader() {
        BaseDocument doc = mock(BaseDocument.class);
        // Document returns an empty map, so reader.nextChunk doesn't write anything
        when(doc.getProperties()).thenReturn(Maps.newHashMap());
        when(collection.getDocument(anyString(), any())).thenReturn(doc);

        TopologyProtobufsManager tpm = new TopologyProtobufsManager(factory, "Tturbonomic",
                collectionCreateOptions);
        final TopologyProtobufReader reader = tpm.createTopologyProtobufReader(1111,
                Optional.empty());
        verify(database).collection(Mockito.eq("topology-dtos-1111"));
        // Reader does not create the collection
        verify(database, never()).createCollection(Mockito.eq("topology-dtos-1111"));
        assertSame(reader.topologyCollection, collection);

        assertEquals(0, reader.sequenceNumber);
        long seq = 0;
        while (reader.hasNext()) {
            reader.nextChunk();
            seq++;
        }
        assertEquals(NUM_CHUNKS, seq);

        reader.delete();
        verify(collection).drop();
    }
}
