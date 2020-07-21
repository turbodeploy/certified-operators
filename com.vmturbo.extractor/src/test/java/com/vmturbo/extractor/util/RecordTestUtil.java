package com.vmturbo.extractor.util;

import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CAPACITY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CONSUMED;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CURRENT;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_KEY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_PROVIDER;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_UTILIZATION;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.MetricType;

/**
 * Utility class for use in tests involving {@link Record} class.
 */
public class RecordTestUtil {
    private RecordTestUtil() {
    }

    /**
     * Create a record for the given table.
     *
     * @param table the table
     * @param data  data for table columns keyed by {@link Column} objects.
     * @return new record
     */
    public static Record createRecord(final Table table, Map<Column<?>, Object> data) {
        return createRecordByName(table, data.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), Entry::getValue)));
    }

    /**
     * Create a new record for the given table.
     *
     * @param table the table
     * @param data  data for table columns, keyed by column names
     * @return new record
     */
    public static Record createRecordByName(final Table table, Map<String, Object> data) {
        Record r = new Record(table);
        data.forEach(r::set);
        return r;
    }

    /**
     * Equip a mocked or spied-on record sink to record its records to a list.
     *
     * @param sink               mocked or spied-on record sink
     * @param alsoCallRealMethod true for a spy if you want the sink to process the record in
     *                           addition to capturing it
     * @return array into which records will be recorded as they are sent to the sink
     */
    public static List<Record> captureSink(Consumer<Record> sink, boolean alsoCallRealMethod) {
        List<Record> records = new ArrayList<>();
        doAnswer(inv -> {
            final Record r = inv.getArgumentAt(0, Record.class);
            if (r != null) {
                records.add(r);
            }
            if (alsoCallRealMethod) {
                inv.callRealMethod();
            }
            return null;
        }).when(sink).accept(any(Record.class));
        return records;
    }

    /**
     * Create a new metric record from column values.
     *
     * @param time         time value
     * @param oid          entity oid value
     * @param hash         entity hash value
     * @param type         commodity type value
     * @param commodityKey commodity key value
     * @param current      current used value (sold commodity metric)
     * @param capacity     current capacity value (sold commodity metric)
     * @param utilization  current utilization value (sold commodity metric)
     * @param consumed     current consumed value (bought commodity metric)
     * @param provider     providing entity (bought commodity metric)
     * @return a map representing the record data
     */
    public static Map<String, Object> createMetricRecordMap(
            final OffsetDateTime time, final long oid, final Long hash, final MetricType type,
            final String commodityKey, final Double current, final Double capacity, final Double utilization,
            final Double consumed, final Long provider) {
        return ImmutableList.<Pair<String, Object>>of(
                Pair.of(TIME.getName(), time),
                Pair.of(ENTITY_OID.getName(), oid),
                Pair.of(ENTITY_HASH.getName(), hash),
                Pair.of(COMMODITY_TYPE.getName(), type),
                Pair.of(COMMODITY_KEY.getName(), commodityKey),
                Pair.of(COMMODITY_CURRENT.getName(), current),
                Pair.of(COMMODITY_CAPACITY.getName(), capacity),
                Pair.of(COMMODITY_UTILIZATION.getName(), utilization),
                Pair.of(COMMODITY_CONSUMED.getName(), consumed),
                Pair.of(COMMODITY_PROVIDER.getName(), provider))
                .stream()
                .filter(pair -> pair.getRight() != null)
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    /**
     * Matcher to compare a record with an expected record.
     *
     * <p>Null-valued entries are not considered, and otherwise the match is based on
     * {@link Map#equals(Object)}</p>
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static class MapMatchesLaxly<K, V> extends BaseMatcher<Map<K, V>> {

        private final Map<?, ?> expected;
        private final ImmutableSet<K> wild;

        /**
         * Create a new matcher instance.
         *
         * @param expected expected record value
         * @param wild     list of keys to allow in actual map, even if missing in expected
         * @param <K>      key type
         * @param <V>      value type
         * @return new matcher
         */
        public static <K, V> MapMatchesLaxly<K, V> mapMatchesLaxly(Map<K, V> expected, K... wild) {
            return new MapMatchesLaxly<>(expected, wild);
        }

        MapMatchesLaxly(Map<?, ?> expected, K... wild) {
            this.expected = stripNullValues(expected);
            this.wild = ImmutableSet.<K>builder().add(wild).build();
        }

        @Override
        public boolean matches(final Object item) {
            boolean matches = item instanceof Map && expected.equals(stripWild(stripNullValues((Map<?, ?>)item)));
            if (!matches) {
                return matches;
            } else {
                return matches;
            }
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(String.format(
                    "Map is expected to include, in any order "
                            + "and perhaps with additional null-valued entries: {%s}",
                    expected.toString()));
        }

        private Map<?, ?> stripNullValues(Map<?, ?> map) {
            return map.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }

        private Map<?, ?> stripWild(Map<?, ?> map) {
            return map.entrySet().stream()
                    .filter(e -> !wild.contains(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }
}
