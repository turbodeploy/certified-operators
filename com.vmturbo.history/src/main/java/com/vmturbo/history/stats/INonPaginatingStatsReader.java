/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.db.VmtDbException;

/**
 * {@link INonPaginatingStatsReader} reads records from the database.
 *
 * @param <R> type of the records which will be loaded by reader implementation.
 */
public interface INonPaginatingStatsReader<R extends Record> {
    /**
     * Partition the list of entities to read into chunks of this size in order not to flood the DB.
     */
    int ENTITIES_PER_CHUNK = 50000;
    /**
     * Reads and returns collection of records related to entities specified by entity identifiers
     * and filtered by conditions in {@link StatsFilter} instance.
     *
     * @param entityIds collection of entity identifiers data about which we want to
     *                 read.
     * @param statsFilter contains filtering conditions using which we need to
     *                 filter data in database.
     * @return collection of records with data related to specified entities and filtered by
     *                 conditions in specified filter instance.
     * @throws VmtDbException in case data reading process has been broken due to
     *                 issues with DB connection.
     */
    @Nonnull
    List<R> getRecords(@Nonnull Set<String> entityIds, @Nonnull StatsFilter statsFilter)
                    throws VmtDbException;
}
