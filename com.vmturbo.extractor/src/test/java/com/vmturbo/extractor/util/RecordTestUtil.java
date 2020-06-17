package com.vmturbo.extractor.util;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.models.Table.Record;

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

        /**
         * Create a new matcher instance.
         *
         * @param expected expected record value
         * @param <K>      key type
         * @param <V>      value type
         * @return new matcher
         */
        public static <K, V> MapMatchesLaxly<K, V> mapMatchesLaxly(Map<K, V> expected) {
            return new MapMatchesLaxly<>(expected);
        }

        MapMatchesLaxly(Map<?, ?> expected) {
            this.expected = stripNullValues(expected);
        }

        @Override
        public boolean matches(final Object item) {
            return item instanceof Map && expected.equals(stripNullValues((Map<?, ?>)item));
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
    }
}
