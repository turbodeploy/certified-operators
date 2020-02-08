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
     *
     * @param database The database name
     * @param collectionNameSuffix Collection name suffix to be appended to collection names.
     * @return {@link GraphDatabaseDriver}.
     */
    GraphDatabaseDriver build(String database, String collectionNameSuffix);

    /**
     * List all collections created so far from the given database. This excludes system collections.
     *
     * @param arangoDatabaseName Given database from which collections are listed.
     * @return The set of collection names.
     * @throws GraphDatabaseException If there is an issue connecting to the database.
     */
    Set<String> listCollections(String arangoDatabaseName) throws GraphDatabaseException;
}
