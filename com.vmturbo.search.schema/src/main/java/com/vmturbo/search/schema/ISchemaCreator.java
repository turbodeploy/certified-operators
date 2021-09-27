package com.vmturbo.search.schema;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.search.metadata.DbFieldDescriptor.Location;

/**
 * Interface for search schema DDL queries construction.
 * Search data are temporary and consequently schema needs not to be persisted or migrated.
 * It is created on demand when the data are written.
 * Expected workflow:
 * - createWithoutIndexes with a temporary suffix
 * - (import data - implemented elsewhere)
 * - createIndexes
 * - replace 'main' schema with temporary schema
 * The methods return lists of SQL queries to execute, to be in line with extractor's
 * DslRecordSink logic.
 */
public interface ISchemaCreator {
    /**
     * Create tables without indexes and constraints.
     * If there is a schema in place, it should be dropped.
     *
     * @param suffix to add to table names
     * @param location table location
     * @return list of queries to execute
     */
    @Nonnull
    List<String> createWithoutIndexes(@Nonnull String suffix, @Nullable Location location);

    /**
     * Create indexes on previously created schema.
     * Assuming that the data has been imported already without them - performance optimization.
     *
     * @param suffix to add to table names
     * @return list of queries to execute
     */
    @Nonnull
    List<String> createIndexes(@Nonnull String suffix);

    /**
     * Atomically (transactionally if possible) replace main schema with temporary schema.
     * Intended to be called from 'post hook' of record sink.
     *
     * @param oldSuffix suffix for storing current data as old data, will be deleted after
     *         replace schema.
     * @param newSuffix suffix for storing new data as current data.
     * @param location table location
     * @return list of queries to execute
     */
    @Nonnull
    List<String> replace(@Nonnull String oldSuffix, @Nonnull String newSuffix,
            @Nullable Location location);
}
