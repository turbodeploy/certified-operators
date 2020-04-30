package com.vmturbo.repository.graph.driver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.IndexType;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.model.SkiplistIndexOptions;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.IndexOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.parameter.CollectionParameter;
import com.vmturbo.repository.graph.parameter.EdgeDefParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter;
import com.vmturbo.repository.graph.parameter.GraphParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter.GraphIndexType;
import com.vmturbo.repository.graph.parameter.VertexParameter;

/**
 * Provides methods to construct graphs in Arango DB, including creation of vertices, edges,
 * collections, and graphs. By providing a graph parameter {@link GraphParameter}, where the
 * graph configuration (e.g., name, associated edge definitions, wait for sync, ...) are specified,
 * the graph can be created by calling {@link #createGraph}.
 * Similarly, parameters for vertices, edges and collections can be used for their creation.
 */
public class ArangoGraphDatabaseDriver implements GraphDatabaseDriver {
    private final Logger logger = LoggerFactory.getLogger(ArangoGraphDatabaseDriver.class);

    /**
     * The driver.
     */
    private final ArangoDB arangoDB;

    /**
     * The driver with an associated database.
     */
    private final ArangoDatabase arangoDatabase;

    /**
     * The database where this driver takes operations.
     */
    private final String database;

    /**
     * The index type mapping.
     * Left package scope for testing purposes.
     */
    static final Map<IndexParameter.GraphIndexType, IndexType> INDEX_TYPE_MAPPING =
            new ImmutableMap.Builder<IndexParameter.GraphIndexType, IndexType>()
                    .put(IndexParameter.GraphIndexType.FULLTEXT, IndexType.fulltext)
                    .put(IndexParameter.GraphIndexType.HASH, IndexType.hash)
                    .build();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String DOCUMENT_KEY_STRING = "_key";

    private final String arangoCollectionNameSuffix;

    /**
     * Constructs the ArangoGraphDatabaseDriver.
     *
     * @param arangoDB   The underlying arangodb driver
     * @param database The associated database name
     * @param arangoCollectionNameSuffix ArangoDB collection name suffix to to appended to collection
     *                                   names.
     */
    public ArangoGraphDatabaseDriver(final @Nonnull ArangoDB arangoDB,
                                     final @Nonnull String database,
                                     final @Nonnull String arangoCollectionNameSuffix) {
        this.arangoDB = Objects.requireNonNull(arangoDB);
        this.database = Objects.requireNonNull(database);
        this.arangoDatabase = this.arangoDB.db(database);
        this.arangoCollectionNameSuffix = arangoCollectionNameSuffix;
    }

    /**
     * Creates a single edge.
     *
     * @param p The parameter to create the edge.
     * @throws EdgeOperationException In case of any error creating an edge.
     */
    @Override
    public synchronized void createEdge(final @Nonnull EdgeParameter p)
            throws EdgeOperationException {
        final BaseEdgeDocument newEdge = new BaseEdgeDocument(p.getFrom(), p.getTo());
        newEdge.addAttribute("type", p.getEdgeType());
        try {
            arangoDatabase.collection(fullCollectionName(p.getEdgeCollection())).insertDocument(newEdge);
        } catch (ArangoDBException e) {
            throw new EdgeOperationException(e.getMessage(), e);
        }
    }

    /**
     * Creates a collection of edges.
     *
     * @param p The parameter to create the edges
     * @throws EdgeOperationException In case of any error creating one or more edges.
     */
    @Override
    public synchronized void createEdgesInBatch(final @Nonnull EdgeParameter p)
            throws EdgeOperationException {
        if (p.getFroms().isEmpty()) { // ArangoDriver doesn't handle the empty case
            return;
        }

        final Collection<BaseEdgeDocument> newEdges = new HashSet<>();
        try {
            int index = 0;
            for (String from : p.getFroms()) {
                final String to = p.getTos().get(index++);
                final BaseEdgeDocument edgeDocument = new BaseEdgeDocument(from, to);
                // set the type of the edge so we can traverse by edge type
                edgeDocument.addAttribute("type", p.getEdgeType());
                newEdges.add(edgeDocument);
            }
            arangoDatabase.collection(fullCollectionName(p.getEdgeCollection())).insertDocuments(newEdges);
        } catch (ArangoDBException e) {
            throw new EdgeOperationException(e.getMessage(), e);
        }
    }

    /**
     * Creates a single vertex.
     *
     * @param p The parameter to create the vertex
     * @throws VertexOperationException In case of any error creating a vertex.
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void createVertex(final @Nonnull VertexParameter p)
            throws VertexOperationException {
        final Map<String, Object> propMap = objectMapper.convertValue(p.getValue(), Map.class);
        propMap.put(DOCUMENT_KEY_STRING, p.getKey());
        try {
            arangoDatabase.collection(fullCollectionName(p.getCollection())).insertDocument(new BaseDocument(propMap));
        } catch (ArangoDBException e) {
            throw new VertexOperationException(e.getMessage(), e);
        }
    }

    /**
     * Creates a collection of vertices.
     *
     * @param p The parameter to create the vertices.
     * @throws VertexOperationException In case of any error creating one or more vertices.
     */
    @Override
    public synchronized void createVerticesInBatch(final @Nonnull VertexParameter p)
            throws VertexOperationException {
        if (p.getValues().isEmpty()) { // ArangoDriver doesn't handle the empty case
            return;
        }

        final Collection<BaseDocument> newVertexes = new HashSet<>();
        try {
            int index = 0;
            for (Object obj : p.getValues()) {
                final String key = p.getKeys().get(index++);
                // FIXME The oid of long type seems not working with ArangoDB.
                // The value are stored in a wrong value, e.g., 71708548999329 -> -224974687.
                final Map<String, Object> propMap = objectMapper.convertValue(obj, Map.class);
                propMap.put(DOCUMENT_KEY_STRING, key);
                newVertexes.add(new BaseDocument(propMap));
//                newVertexes.add(createVPackSlice(propMap));
            }
            arangoDatabase.collection(fullCollectionName(p.getCollection())).insertDocuments(newVertexes);
        } catch (ArangoDBException e) {
            throw new VertexOperationException(e.getMessage(), e);
        }
    }

    /**
     * Creates a collection of edges or vertices.
     *
     * @param p The parameter to create the collection
     * @throws CollectionOperationException In case of any error creating the collection.
     */
    @Override
    public synchronized void createCollection(final @Nonnull CollectionParameter p)
            throws CollectionOperationException {
        final CollectionCreateOptions collectionOptions = new CollectionCreateOptions();
        collectionOptions.journalSize((long)p.getJournalSize());
        collectionOptions.numberOfShards(p.getNumberOfShards());
        collectionOptions.replicationFactor(p.getReplicaCount());
        logger.debug("Creating collection with {} shards and {} replicas.", p.getNumberOfShards(), p.getReplicaCount());
        if (p.isEdge()) {
            collectionOptions.type(CollectionType.EDGES);
        } else {
            collectionOptions.type(CollectionType.DOCUMENT);
        }
        try {
            arangoDatabase.createCollection(fullCollectionName(p.getName()), collectionOptions);
        } catch (ArangoDBException e) {
            throw new CollectionOperationException(e.getMessage(), e);
        }
    }

    /**
     * Empties a collection identified by name.
     *
     * @param name The name of the collection
     * @throws CollectionOperationException In case of any error performing the operation.
     */
    @Override
    public synchronized void emptyCollection(final @Nonnull String name)
            throws CollectionOperationException {
        try {
            arangoDatabase.collection(fullCollectionName(name)).truncate();
        } catch (ArangoDBException e) {
            throw new CollectionOperationException(e.getMessage(), e);
        }
    }

    /**
     * Creates a graph.
     *
     * @param p The graph description.
     * @throws CollectionOperationException In case of any error performing the operation.
     */
    @Override
    public synchronized void createGraph(final @Nonnull GraphParameter p)
            throws GraphOperationException {
        try {
            arangoDatabase.createGraph(fullCollectionName(p.getName()), p.getEdgeDefs().stream().map(edp -> createEdgeDef(edp))
                               .collect(Collectors.toList()), extractGraphCreateOptions(p));
        } catch (ArangoDBException e) {
            throw new GraphOperationException(e.getMessage(), e);
        }
    }

    /**
     * Given a {@link GraphParameter} object, possibly create and return a {@link GraphCreateOptions}
     * based on any custom graph creation attibutes set in the GraphParameter object instance.
     *
     * If there are no custom parameters found, this may return a null object (which is allowed
     * by the arangdb driver methods)
     *
     * @param p the GraphParameter object to inspect.
     * @return a GraphCreateOptions instance based on the input parameters. May be null if no graph
     * creation option values are found in the GraphParameter instance.
     */
    @Nullable
    protected GraphCreateOptions extractGraphCreateOptions(final GraphParameter p) {
        if (null == p) { return null; }

        // handle non-default values. We're only concerned about replica count for now. But waitForSync
        // may be another important parameter to respect as well if we become concerned about
        // transactionality of the creation process.
        if (p.getReplicaCount() > 1) {
            logger.debug("Setting graph create options to {} replicas", p.getReplicaCount());
            return new GraphCreateOptions().replicationFactor(p.getReplicaCount());
        }
        return null;
    }

    /**
     * Creates an index.
     *
     * @param p The parameter to create the indices
     * @throws IndexOperationException In case of any error performing the operation.
     */
    @Override
    public synchronized void createIndex(final @Nonnull IndexParameter p)
            throws IndexOperationException {

        try {
            final ArangoCollection ac = arangoDatabase.collection(fullCollectionName(p.getCollectionName()));

            if (IndexParameter.GraphIndexType.FULLTEXT.equals(p.getIndexType())) {
                ac.createFulltextIndex(p.getFieldNames(), null);
            } else if (IndexParameter.GraphIndexType.HASH.equals(p.getIndexType())) {
                ac.createHashIndex(p.getFieldNames(), null);
            } else if (GraphIndexType.SKIPLIST.equals(p.getIndexType())) {
                SkiplistIndexOptions skiplistIndexOptions = new SkiplistIndexOptions();
                skiplistIndexOptions.deduplicate(p.isDeduplicate());
                skiplistIndexOptions.sparse(p.isSparse());
                skiplistIndexOptions.unique(p.isUnique());
                ac.ensureSkiplistIndex(p.getFieldNames(), skiplistIndexOptions);
            } else {
                throw new IndexOperationException("Invalid index type: " + p.getIndexType());
            }
        } catch (ArangoDBException e) {
            throw new IndexOperationException(e.getMessage(), e);
        }
    }

    /**
     * Checks whether the database server is up.
     *
     * @return {@code true} iff the server is up.
     * @throws GraphDatabaseException In case of any error performing the operation.
     */
    @Override
    public synchronized boolean isServerUp() throws GraphDatabaseException {
        // Assume the DB server is up if the getVersion method doesn't throw an exception.
        try {
            arangoDB.getVersion();
        } catch (ArangoDBException e) {
            throw new GraphDatabaseException(e.getMessage(), e);
        }
        return true;
    }

    /**
     * Creates an edge definition in the DB.
     *
     * @param p The parameter to create the edge definition
     * @return The edge definition
     */
    private EdgeDefinition createEdgeDef(final @Nonnull EdgeDefParameter p) {
        EdgeDefinition edgeDef = new EdgeDefinition();

        edgeDef.collection(fullCollectionName(p.getEdgeCollection()));
        edgeDef.from(fullCollectionName(p.getFromCollection()));
        edgeDef.to(fullCollectionName(p.getToCollection()));

        return edgeDef;
    }

    /**
     * Updates the document for the key and value.
     *
     * @param p         The vertex parameter
     * @param valuesMap The map for values updated.
     * @throws VertexOperationException In case of an error finding the document.
     */
    @Override
    public synchronized void updateDocuments(final @Nonnull VertexParameter p,
                                            final @Nonnull Map<String, Map<String, Object>> valuesMap)
            throws VertexOperationException {

        Collection<VPackSlice> values = new ArrayList<>();
        valuesMap.forEach((key, value) -> {
            value.put(DOCUMENT_KEY_STRING, key);
            values.add(createVPackSlice(value));
        });

        try {
            arangoDatabase.collection(fullCollectionName(p.getCollection())).updateDocuments(values);
        } catch (ArangoDBException e) {
            throw new VertexOperationException("Error updating the arangodb documents", e);
        }
    }

    private VPackSlice createVPackSlice(final @Nonnull Map<String, Object> values) {
        final VPackBuilder builder = new VPackBuilder();
        builder.add(ValueType.OBJECT);

        // Need check if it always works using obj.toString()
        values.entrySet().stream().filter(en -> en.getValue() != null).forEach(en -> {
            builder.add(en.getKey(), en.getValue().toString());
        });

        return builder.close().slice();
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public void dropCollections() {
        arangoDatabase.getCollections().stream()
            // Do not try to drop system collections - we won't be able to anyways.
            .filter(collection -> !collection.getIsSystem())
            .map(CollectionEntity::getName)
            .filter(collection -> collection.contains(arangoCollectionNameSuffix))
            .map(arangoDatabase::collection)
            .forEach(ArangoCollection::drop);
    }

    private String fullCollectionName(String collectionName) {
        return collectionName + arangoCollectionNameSuffix;
    }
}
