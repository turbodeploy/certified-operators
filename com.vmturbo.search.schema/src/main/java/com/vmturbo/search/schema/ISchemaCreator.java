package com.vmturbo.search.schema;

import java.util.List;

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
     * @return list of queries to execute
     */
    List<String> createWithoutIndexes(String suffix);

    /**
     * Create indexes on previously created schema.
     * Assuming that the data has been imported already without them - performance optimization.
     *
     * @param suffix to add to table names
     * @return list of queries to execute
     */
    List<String> createIndexes(String suffix);

    /**
     * Atomically (transactionally if possible) replace schema with one suffix by another.
     * Intended to be called from 'post hook' of record sink.
     *
     * @param srcSuffix source data
     * @param dstSuffix destination name, if there is a schema in place, it should be dropped
     * @return list of queries to execute
     */
    List<String> replace(String srcSuffix, String dstSuffix);
}
