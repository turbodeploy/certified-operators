package com.vmturbo.repository.topology.protobufs;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.arangodb.ArangoDBException;
import com.arangodb.model.CollectionCreateOptions;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Test exceptiuon handling of {@link TopologyProtobufsManager}.
 *
 */
public class TopologyProtobufsManagerTest {

    private CollectionCreateOptions defaultCollectionOptions = new CollectionCreateOptions();

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElement() {
        ArangoDBException exception = Mockito.mock(ArangoDBException.class);
        Mockito.when(exception.getErrorNum()).thenReturn(1203);
        Mockito.when(exception.getResponseCode()).thenReturn(404);
        ArangoDatabaseFactory factory = Mockito.mock(ArangoDatabaseFactory.class);
        Mockito.when(factory.getArangoDriver()).thenThrow(exception);
        TopologyProtobufsManager tpm = new TopologyProtobufsManager(factory, "turbonomic-",
                defaultCollectionOptions);
        tpm.createTopologyProtobufReader(1111, Optional.empty());
    }

    @Test(expected = ArangoDBException.class)
    public void testArangoDBException() {
        ArangoDBException exception = Mockito.mock(ArangoDBException.class);
        Mockito.when(exception.getErrorNum()).thenReturn(100);
        Mockito.when(exception.getResponseCode()).thenReturn(200);
        ArangoDatabaseFactory factory = Mockito.mock(ArangoDatabaseFactory.class);
        Mockito.when(factory.getArangoDriver()).thenThrow(exception);
        TopologyProtobufsManager tpm = new TopologyProtobufsManager(factory, "turbonomic-",
                defaultCollectionOptions);
        tpm.createTopologyProtobufReader(1111, Optional.empty());
    }
}
