package com.vmturbo.repository.graph.driver;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;

/**
 * The interface to build {@link GraphDatabaseDriver} instances
 */
public interface GraphDatabaseDriverBuilder {

    /**
     * Builds a {@link GraphDatabaseDriver} instance with a database.
     * @param database The database name
     * @return
     */
    GraphDatabaseDriver build(final String database);

    /**
     * List all the databases defined so far.
     * @return The set of database names.
     * @throws GraphDatabaseException If there is an issue connecting to the database.
     */
    @Nonnull
    Set<String> listDatabases() throws GraphDatabaseException;
}
