package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.Entities.ENTITIES;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableRecord;
import org.mockito.stubbing.Answer;

/**
 * Class that simulates certain database operations in an in-memory structure, with mocking support
 * for bulk loader.
 */
public class DbMock {
    private final RecordStore store = new RecordStore();

    /**
     * A stubbing Answer suitable for a record insert or insertAll operation.
     *
     * <p>The given records are added to (or replace existing records in) the simulated database.</p>
     */
    public final Answer<Object> insert = invocation -> {
        Object recObj = invocation.getArguments()[0];
        if (recObj instanceof Record) {
            // one reccord => stub insert operation
            store.insert((TableRecord)recObj);
        } else if (recObj instanceof Collection) {
            // collection = > insertAll operation
            //noinspection unchecked
            store.insert((Collection<TableRecord>)recObj);
        } else {
            throw new IllegalArgumentException();
        }
        return null;
    };

    /**
     * Insert a record.
     *
     * @param record record to be inserted
     */
    public void insert(TableRecord<?> record) {
        store.insert(record);
    }

    /**
     * Get the record from the given table with the given key values.
     *
     * @param table table
     * @param key   key vlaues
     * @param <T>   underlying record type
     * @return selected record, or null if not found
     */
    @Nullable
    public <T extends Record> T getRecord(Table<T> table, List<Object> key) {
        return store.getRecord(table, key);
    }

    /**
     * Retrieve a record with a singleton key.
     *
     * <p>This is a convenience method that wraps the key value in an array and then calls
     * {@link #getRecord(Table, List)}}.</p>
     *
     * @param table table to query
     * @param key   key value
     * @param <T>   underlying record type
     * @return selected record, or null if not found
     */
    public <T extends Record> T getRecord(Table<T> table, Object key) {
        return store.getRecord(table, Stream.of(key).collect(Collectors.toList()));
    }


    /**
     * Get all the records from the given table.
     *
     * @param table table
     * @param <T>   underlying record type
     * @return list of records
     */
    public <T extends Record> Collection<T> getRecords(Table<T> table) {
        return store.getRecords(table);
    }

    /**
     * Retrieve records with the given keys.
     *
     * @param table table to retrieve from
     * @param keys  key value lists
     * @param <T>   underlying record type
     * @return list of records; not found records are excluded, so list may be shorter than key list
     */
    public <T extends Record> List<T> getRecords(Table<T> table, List<List<Object>> keys) {
        return keys.stream()
                .map(key -> store.getRecord(table, key))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Retrieve records with the given singleton keys.
     *
     * <p>This is a convenience method tha wraps each key in a list and rertrieves their records.
     *
     * @param table table to retrieve from
     * @param keys  list of singleton key values
     * @param <T>   underlying record type
     * @return list of records; not found records are excluded, so list may be shorter than key list
     */
    public <T extends Record> List<T> getRecords(Table<T> table, Object... keys) {
        return Stream.of(keys)
                .map(key -> getRecord(table, key))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get a list of the tables for which we captured records.
     *
     * @return the tables
     */
    public Collection<Table<?>> getTables() {
        return store.getTables();
    }

    /**
     * Record store to hold data for simulated database.
     */
    private static class RecordStore {
        // configure keys for whatever tables will be used with this mock. Each key is a list of
        // fields for the indicated table.
        Map<Table<?>, List<Field<?>>> keyFields = ImmutableMap.<Table<?>, List<Field<?>>>builder()
                .put(ENTITIES, key(ENTITIES.ID))
                .put(VM_STATS_LATEST, key(
                        VM_STATS_LATEST.SNAPSHOT_TIME, VM_STATS_LATEST.UUID, VM_STATS_LATEST.PRODUCER_UUID,
                        VM_STATS_LATEST.PROPERTY_TYPE, VM_STATS_LATEST.PROPERTY_SUBTYPE,
                        VM_STATS_LATEST.COMMODITY_KEY, VM_STATS_LATEST.RELATION))
                .put(PM_STATS_LATEST, key(
                        PM_STATS_LATEST.SNAPSHOT_TIME, PM_STATS_LATEST.UUID, PM_STATS_LATEST.PRODUCER_UUID,
                        PM_STATS_LATEST.PROPERTY_TYPE, PM_STATS_LATEST.PROPERTY_TYPE,
                        PM_STATS_LATEST.COMMODITY_KEY, PM_STATS_LATEST.RELATION))
                .build();

        private static List<Field<?>> key(Field<?>... values) {
            return Arrays.asList(values);
        }


        /**
         * The siluated database tables.
         */
        Map<Table<?>, // top-level map has an entry for each table with any records
                Map<List<Object>, // per-table map associates key lists with records (1:1)
                        Record>> records = new HashMap<>();

        /**
         * Insert a record into its table.
         *
         * @param record record to be inserted
         */
        void insert(TableRecord record) {
            Table<?> table = ((TableRecord<?>)record).getTable();
            final Map<List<Object>, Record> recs
                    = records.computeIfAbsent(table, t -> new HashMap<>());
            recs.put(getKey(record), record);
        }

        void insert(Collection<TableRecord> records) {
            records.forEach(this::insert);
        }

        <T extends Record> List<T> getRecords(Table<T> table) {
            @SuppressWarnings("unchecked")
            final List<T> result = (List<T>)new ArrayList<>(
                    this.records.getOrDefault(table, Collections.emptyMap()).values());
            return result;
        }

        <T extends Record> T getRecord(Table<T> table, List<Object> key) {
            final Map<List<Object>, Record> recs = records.computeIfPresent(table, (t, map) -> map);
            @SuppressWarnings("unchecked")
            final T rec = (T)(recs != null ? recs.getOrDefault(key, null) : null);
            return rec;
        }

        private List<Object> getKey(TableRecord record) {
            final List<Field<?>> fields = keyFields.get(record.getTable());
            return fields.stream()
                    .map(record::getValue)
                    .collect(Collectors.toList());
        }

        private Collection<Table<?>> getTables() {
            return records.keySet();
        }
    }
}
