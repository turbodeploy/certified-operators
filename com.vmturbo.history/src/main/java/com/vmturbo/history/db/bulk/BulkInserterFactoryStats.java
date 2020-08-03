package com.vmturbo.history.db.bulk;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jooq.Table;

/**
 * This class is a container for stats objects for all the {@link BulkInserter}s created by a
 * given {@link BulkInserterFactory}.
 *
 * <p>Besides being able to obtain stats for individual inserters, this class can also manufacture
 * a stats object representing totals.</p>
 */
public class BulkInserterFactoryStats {

    private Map<Object, BulkInserterStats> statsByKey = new HashMap<>();
    private Map<Table<?>, BulkInserterStats> statsByInTable = new HashMap<>();
    private Map<Table<?>, BulkInserterStats> statsByOutTable = new HashMap<>();

    /**
     * Create a new instance initially containing all the provided {@link BulkInserterStats}
     * objects.
     *
     * @param inserterStats stats objects for bulk inserters
     */
    public BulkInserterFactoryStats(Collection<BulkInserterStats> inserterStats) {
        inserterStats.forEach(this::addInserterStats);
    }

    /**
     * Add the given {@link BulkInserterStats} instance to this container.
     *
     * @param stats stats objects to add
     */
    private void addInserterStats(BulkInserterStats stats) {
        statsByKey.put(stats.getKey(), stats);
        statsByInTable.put(stats.getInTable(), stats);
        statsByOutTable.put(stats.getOutTable(), stats);
    }

    /**
     * Get all the keys appearing in contained stats objects.
     *
     * @return stats object keys
     */
    public Set<Object> getKeys() {
        return statsByKey.keySet();
    }

    /**
     * Get all the distinct input tables used in contained stats objects.
     *
     * @return distinct stats input tables
     */
    public Set<Table<?>> getInTables() {
        return statsByInTable.keySet();
    }

    /**
     * Get all the distinct output tables used in contained stats objects.
     *
     * @return distinct stats output tables
     */
    public Set<Table<?>> getOutTables() {
        return statsByOutTable.keySet();
    }

    /**
     * Get the stats object for the given key.
     *
     * @param key key of desired stats object
     * @return stats object with that key
     */
    public BulkInserterStats getByKey(Object key) {
        return statsByKey.get(key);
    }

    /**
     * Get the stats object for the given input table.
     *
     * @param table input table of desired stats object
     * @return stats object with that input table
     */
    public BulkInserterStats getByInTable(Table<?> table) {
        return statsByInTable.get(table);
    }

    /**
     * Get the stats object with the given output table.
     *
     * @param table output table of desired stats object
     * @return stats object with that output table
     */
    public BulkInserterStats getByOutTable(Table<?> table) {
        return statsByOutTable.get(table);
    }

    /**
     * Get all the stats objects in this container.
     *
     * @return all the contained stats objects
     */
    private Collection<BulkInserterStats> getAllStats() {
        return statsByKey.values();
    }

    /**
     * Manufacture a stats object that reflects totals across all contained stats objects.
     *
     * @return stats object with totals
     */
    public BulkInserterStats getTotals() {
        // create an empty stats object to hold totals
        BulkInserterStats result = new BulkInserterStats(null, null, null);
        // add data from each contained stats object
        statsByKey.values().forEach(stat ->
            result.update(stat.getWritten(), stat.getBatches(), stat.getFailedBatches(),
                stat.getWritten(), stat.getLostTimeNanos()));
        return result;
    }
}
