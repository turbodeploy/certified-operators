package com.vmturbo.repository.graph.driver;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.arangodb.ArangoDB;

/**
 * A builder for {@link GraphDatabaseDriver}s that connect to ArangoDB.
 */
public class ArangoDatabaseDriverBuilder implements GraphDatabaseDriverBuilder {

    /**
     * The system database is the "special" Arango database that has extra properties
     * and a special role when interacting with other databases.
     *
     * See: https://docs.arangodb.com/3.2/Manual/DataModeling/Databases/WorkingWith.html#issystem
     */
    private static final String SYSTEM_DATABASE_NAME = "_system";

    /**
     * The single underlying ArangoDB connection, shared between all {@link GraphDatabaseDriver}s.
     */
    private ArangoDatabaseFactory arangoFactory;

    public ArangoDatabaseDriverBuilder(@Nonnull final ArangoDatabaseFactory arangoFactory) {
        this.arangoFactory = arangoFactory;
    }

    @Override
    public GraphDatabaseDriver build(final String database) {
        return new ArangoGraphDatabaseDriver(arangoFactory.getArangoDriver(), database);
    }

    @Override
    public Set<String> listDatabases() {
        return arangoFactory.getArangoDriver().getAccessibleDatabases().stream()
            .filter(name -> !name.equals(SYSTEM_DATABASE_NAME))
            .collect(Collectors.toSet());
    }
}
