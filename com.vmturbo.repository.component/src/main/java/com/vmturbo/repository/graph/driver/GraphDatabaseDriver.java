package com.vmturbo.repository.graph.driver;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.IndexOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.parameter.CollectionParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter;
import com.vmturbo.repository.graph.parameter.GraphParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter;
import com.vmturbo.repository.graph.parameter.VertexParameter;

/**
 * Provides methods to construct graphs in a graph database, including creation of vertices, edges,
 * collections, and graphs. By providing a graph parameter {@link GraphParameter}, where the
 * graph configuration (e.g., name, associated edge definitions, wait for sync, ...) are specified,
 * the graph can be created by calling {@link #createGraph}.
 * Similarly, parameters for vertices, edges and collections can be used for their creation.
 */
public interface GraphDatabaseDriver {
    /**
     * Creates an edge in the DB.
     *
     * @param p The parameter to create the edge
     * @throws EdgeOperationException In case of any error performing the operation.
     */
    public void createEdge(final @Nonnull EdgeParameter p) throws EdgeOperationException;

    /**
     * Creates a set of edges in the batch mode in the DB.
     *
     * @param p The parameter to create the edges
     * @throws EdgeOperationException In case of any error performing the operation.
     */
    public void createEdgesInBatch(final @Nonnull EdgeParameter p) throws EdgeOperationException;

    /**
     * Creates a vertex in the DB.
     *
     * @param p The parameter to create the vertex
     * @throws VertexOperationException In case of any error performing the operation.
     */
    public void createVertex(final @Nonnull VertexParameter p) throws VertexOperationException;

    /**
     * Creates a set of vertices in the batch mode in the DB.
     *
     * @param p The parameter to create the vertices
     * @throws VertexOperationException In case of any error performing the operation.
     */
    public void createVerticesInBatch(final @Nonnull VertexParameter p) throws VertexOperationException;

    /**
     * Creates a collection of type either EDGE or DOCUMENT in the DB.
     *
     * @param p The parameter to create the collection
     * @throws CollectionOperationException In case of any error performing the operation.
     */
    public void createCollection(final @Nonnull CollectionParameter p) throws CollectionOperationException;

    /**
     * Empties a collection in the DB.
     *
     * @param name The name of the collection
     * @throws CollectionOperationException In case of any error performing the operation.
     */
    public void emptyCollection(final @Nonnull String name) throws CollectionOperationException;


    /**
     * Creates a graph in the DB.
     *
     * @param p The parameter to create the graph
     * @throws GraphOperationException In case of any error performing the operation.
     */
    public void createGraph(final @Nonnull GraphParameter p) throws GraphOperationException;

    /**
     * Creates indices in the DB.
     *
     * @param indexParameter The parameter to create the indices
     * @throws IndexOperationException In case of any error performing the operation.
     */
    public void createIndex(final @Nonnull IndexParameter indexParameter) throws IndexOperationException;

    /**
     * Checks if the DB server is up and accessible. If not, an exception will be thrown.
     *
     * @return True if the server is up
     * @throws GraphDatabaseException In case of any error performing the operation.
     */
    public boolean isServerUp() throws GraphDatabaseException;

    /**
     * Updates the documents for the key and value.
     *
     * @param p The vertex parameter.
     * @param values         The values.
     * @throws VertexOperationException In case of an error finding the document.
     */
    void updateDocuments(final @Nonnull VertexParameter p,
                        final @Nonnull Map<String, Map<String, Object>> values)
            throws VertexOperationException;

    /**
     * Gets the associated database.
     * @return The database name
     */
    public String getDatabase();

    /**
     * Drop collections with given collection suffix.
     */
    void dropCollections();
}
