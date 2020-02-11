package com.vmturbo.repository.graph.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.CollectionCreateOptions;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.parameter.CollectionParameter;
import com.vmturbo.repository.graph.parameter.EdgeDefParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter.EdgeType;
import com.vmturbo.repository.graph.parameter.GraphParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter.GraphIndexType;
import com.vmturbo.repository.graph.parameter.VertexParameter;


@RunWith(MockitoJUnitRunner.class)
public class ArangoGraphDatabaseDriverTest {

    private ArangoGraphDatabaseDriver arangoGraphDatabaseDriverTest;

    @Mock
    ArangoDB mockArangoDriver;

    @Mock
    ArangoDatabase mockArangoDatabase;

    @Mock
    ArangoCollection mockArangoCollection;

    private final String database = "db-test";

    private final String collectionNameSuffix = "-1-S-2";

    @Captor
    private ArgumentCaptor<ArrayList<EdgeDefinition>> edgeDefinitionEntityCaptor;

    @Before
    public void setUp() {
        when(mockArangoDriver.db(database)).thenReturn(mockArangoDatabase);
        when(mockArangoDatabase.collection(any())).thenReturn(mockArangoCollection);
        arangoGraphDatabaseDriverTest = new ArangoGraphDatabaseDriver(mockArangoDriver, database, collectionNameSuffix);
    }

    @Test
    public void testCreateVertexCollection() throws Exception {
        final String name = "collection-1";
        CollectionParameter p = new CollectionParameter.Builder(name).build();

        arangoGraphDatabaseDriverTest.createCollection(p);

        ArgumentCaptor<CollectionCreateOptions> arg = ArgumentCaptor.forClass(CollectionCreateOptions.class);
        verify(mockArangoDatabase).createCollection(eq(name + collectionNameSuffix), arg.capture());
        assertEquals(CollectionType.DOCUMENT, arg.getValue().getType());
    }

    @Test
    public void testCreateEdgeCollection() throws Exception {
        final String name = "collection-1";
        CollectionParameter p = new CollectionParameter.Builder(name).edge().build();

        arangoGraphDatabaseDriverTest.createCollection(p);

        ArgumentCaptor<CollectionCreateOptions> arg = ArgumentCaptor.forClass(CollectionCreateOptions.class);
        verify(mockArangoDatabase).createCollection(eq(name + collectionNameSuffix), arg.capture());
        assertEquals(CollectionType.EDGES, arg.getValue().getType());
    }

    @Test
    public void testCreateEdgeOfTypeConsumes() throws Exception {
        final String from = "from-1";
        final String to = "to-1";
        final String edgeCollection = "edgeCollection-1";
        EdgeParameter p = new EdgeParameter.Builder(edgeCollection, from, to, EdgeType.CONSUMES).build();
        arangoGraphDatabaseDriverTest.createEdge(p);

        BaseEdgeDocument edgeDocument = new BaseEdgeDocument(from, to);
        edgeDocument.addAttribute("type", p.getEdgeType());

        verify(mockArangoDatabase).collection(edgeCollection + collectionNameSuffix);
        verify(mockArangoCollection).insertDocument(edgeDocument);
    }

    @Test
    public void testCreateEdgeOfTypeConnected() throws Exception {
        final String from = "from-1";
        final String to = "to-1";
        final String edgeCollection = "edgeCollection-1";
        EdgeParameter p = new EdgeParameter.Builder(edgeCollection, from, to, EdgeType.CONNECTED).build();
        arangoGraphDatabaseDriverTest.createEdge(p);

        BaseEdgeDocument edgeDocument = new BaseEdgeDocument(from, to);
        edgeDocument.addAttribute("type", p.getEdgeType());

        verify(mockArangoDatabase).collection(edgeCollection + collectionNameSuffix);
        verify(mockArangoCollection).insertDocument(edgeDocument);
    }

    @Test
    public void testCreateEdgeInBatch() throws Exception {
        final String from1 = "from-1";
        final String to1 = "to-1";
        final String from2 = "from-2";
        final String to2 = "to-2";
        final List<String> froms = Arrays.asList(from1, from2);
        final List<String> tos = Arrays.asList(to1, to2);
        final String edgeCollection = "edgeCollection-1";
        EdgeParameter p = new EdgeParameter.Builder(edgeCollection, "from", "to", EdgeType.CONSUMES).build();

        arangoGraphDatabaseDriverTest.createEdgesInBatch(
                p.withFroms(froms).withTos(tos));

        verify(mockArangoDatabase).collection(edgeCollection + collectionNameSuffix);

        BaseEdgeDocument edgeDocument1 = new BaseEdgeDocument(from1, to1);
        edgeDocument1.addAttribute("type", p.getEdgeType());
        BaseEdgeDocument edgeDocument2 = new BaseEdgeDocument(from2, to2);
        edgeDocument2.addAttribute("type", p.getEdgeType());
        verify(mockArangoCollection).insertDocuments(Sets.newHashSet(Arrays.asList(
                edgeDocument1, edgeDocument2)));
    }

    @Test
    public void testCreateEdgeInBatchWithEmptyInput() throws Exception {
        final List<String> froms = Arrays.asList();
        final List<String> tos = Arrays.asList();
        final String edgeCollection = "edgeCollection-1";
        EdgeParameter p = new EdgeParameter.Builder(edgeCollection, "from", "to", EdgeType.CONSUMES).build();

        arangoGraphDatabaseDriverTest.createEdgesInBatch(
                p.withFroms(froms).withTos(tos));

        verify(mockArangoDatabase, never()).collection(edgeCollection + collectionNameSuffix);
        verify(mockArangoCollection, never()).insertDocuments(any());
    }

    @Test
    public void testCreateGraph() throws Exception {
        final String graphName = "graph-1";
        final String vertexCollection1 = "vc-1";
        final String edgeCollection1 = "ec-1";
        final String vertexCollection2 = "vc-2";
        final String edgeCollection2 = "ec-2";
        EdgeDefParameter edp1 = new EdgeDefParameter.Builder(vertexCollection1, edgeCollection1).build();
        EdgeDefParameter edp2 = new EdgeDefParameter.Builder(vertexCollection2, edgeCollection2).build();
        GraphParameter p = new GraphParameter.Builder(graphName)
                .addEdgeDef(edp1).addEdgeDef(edp2).build();

        arangoGraphDatabaseDriverTest.createGraph(p);

        verify(mockArangoDatabase).createGraph(eq(graphName + collectionNameSuffix), edgeDefinitionEntityCaptor.capture());
        assertEquals(edgeCollection1 + collectionNameSuffix, edgeDefinitionEntityCaptor.getValue().get(0).getCollection());
        assertTrue(edgeDefinitionEntityCaptor.getValue().get(0).getFrom().contains(vertexCollection1 + collectionNameSuffix));
        assertEquals(edgeCollection2 + collectionNameSuffix, edgeDefinitionEntityCaptor.getValue().get(1).getCollection());
        assertTrue(edgeDefinitionEntityCaptor.getValue().get(1).getFrom().contains(vertexCollection2 + collectionNameSuffix));
    }

    @Test
    public void testCreateVertex() throws Exception {
        final String key = "key-1";
        final String vertexCollection = "vertexCollection-1";
        final MockVertexObject value = new MockVertexObject("name-1", "uuid-1");
        VertexParameter p = new VertexParameter.Builder(vertexCollection).value(value).key(key).build();
        arangoGraphDatabaseDriverTest.createVertex(p);

        verify(mockArangoDatabase).collection(vertexCollection + collectionNameSuffix);

        final Map<String, Object> newVertex = new HashMap<String, Object>() {{
            put("_key", key);
            put("name", "name-1");
            put("uuid", "uuid-1");
        }};
        verify(mockArangoCollection).insertDocument(new BaseDocument(newVertex));
    }

    @Test
    public void testCreateVerticesInBatch() throws Exception {
        final String vertexCollection = "vertexCollection-1";
        final List<String> keys = Arrays.asList("key-1", "key-2");
        final List<MockVertexObject> values = Arrays.asList(
                                                  new MockVertexObject("name-1", "uuid-1"),
                                                  new MockVertexObject("name-2", "uuid-2"));
        VertexParameter p = new VertexParameter.Builder(vertexCollection).build();
        arangoGraphDatabaseDriverTest.createVerticesInBatch(p.withValues(values).withKeys(keys));

        verify(mockArangoDatabase).collection(vertexCollection + collectionNameSuffix);

        final Map<String, Object> newVertex1 = new HashMap<String, Object>() {{
            put("_key", "key-1");
            put("name", "name-1");
            put("uuid", "uuid-1");
        }};

        final Map<String, Object> newVertex2 = new HashMap<String, Object>() {{
            put("_key", "key-2");
            put("name", "name-2");
            put("uuid", "uuid-2");
        }};

        verify(mockArangoCollection).insertDocuments(Sets.newHashSet(Arrays.asList(
                               new BaseDocument(newVertex1), new BaseDocument(newVertex2))));
    }

    @Test
    public void testCreateVerticesInBatchWithEmptyInput() throws Exception {
        final List<String> values = Arrays.asList();
        VertexParameter p = new VertexParameter.Builder("any").build();
        arangoGraphDatabaseDriverTest.createVerticesInBatch(p.withValues(values));

        verify(mockArangoDatabase, never()).collection(any());
        verify(mockArangoCollection, never()).insertDocuments(any());
    }

    @Test
    public void testCreateIndex() throws Exception {
        final String vertexCollection = "vertexCollection-1";
        GraphIndexType graphIndexType = GraphIndexType.FULLTEXT;
        final String field = "oid";
        IndexParameter p = new IndexParameter.Builder(vertexCollection, graphIndexType)
                                                .addField(field)
                                                .unique(true)
                                                .build();

        arangoGraphDatabaseDriverTest.createIndex(p);

        verify(mockArangoCollection).createFulltextIndex(Arrays.asList(field), null);
    }

    @Test(expected = GraphDatabaseException.class)
    public void testServerDown() throws Exception {
        doThrow(new ArangoDBException("test-exception"))
                .when(mockArangoDriver).getVersion();

        arangoGraphDatabaseDriverTest.isServerUp();
    }

    @Test
    public void testServerUp() throws Exception {
        when(mockArangoDriver.getVersion()).thenReturn(new ArangoDBVersion());
        assertTrue(arangoGraphDatabaseDriverTest.isServerUp());
    }

    private class MockVertexObject {
        private String name;
        private String uuid;

        public MockVertexObject(String name, String uuid) {
            this.name = name;
            this.uuid = uuid;
        }

        public String getName() {
            return name;
        }

        public void setName() {
            this.name = name;
        }

        public String getUuid() {
            return uuid;
        }

        public void setUuid() {
            this.uuid = uuid;
        }
    }
}
