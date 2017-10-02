package com.vmturbo.repository.graph.driver;

/**
 * The interface to build {@link GraphDatabaseDriver} instances
 */
public interface GraphDatabaseDriverBuilder {

    /**
     * Builds a {@link GraphDatabaseDriver} instance with a database.
     * @param database The database name
     * @return
     */
    public GraphDatabaseDriver build(final String database);
}
