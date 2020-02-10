package com.vmturbo.repository.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Container for all the migrations for the Repository component.
 */
public class RepositoryMigrationsLibrary {

    /**
     * The single underlying ArangoDB connection, shared between all components.
     */
    private final ArangoDatabaseFactory arangoFactory;

    /**
     * Manages all the migrations in the Repository.
     *
     * @param arangoFactory provides a connection to ArangoDB
     */
    public RepositoryMigrationsLibrary(@Nonnull final ArangoDatabaseFactory arangoFactory) {
        this.arangoFactory = Objects.requireNonNull(arangoFactory);
    }

    /**
     * Get the list of migrations, in the order they should be run.
     *
     * @return Sorted map of migrations by name.
     */
    public SortedMap<String, Migration> getMigrations() {
        return ImmutableSortedMap.<String, Migration>naturalOrder()
            .put(V_01_00_00__PURGE_ALL_LEGACY_PLANS.class.getSimpleName(),
                new V_01_00_00__PURGE_ALL_LEGACY_PLANS(arangoFactory))
            .build();
    }
}
