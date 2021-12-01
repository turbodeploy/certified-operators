package com.vmturbo.common.protobuf.memory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

/**
 * Utility class helpers for histogram-related tests.
 */
public class HistogramHelpers {
    /**
     * Private constructor for utility class.
     */
    private HistogramHelpers() {

    }

    /**
     * histPattern.
     *          SIZE        COUNT TYPE
     *    368 Bytes           12 TOTAL
     * 1  176 Bytes            4 [C
     * 2   96 Bytes            4 java.lang.String
     * 3   96 Bytes            4 com.vmturbo.common.protobuf.memory.FastMemoryWalkerTest$TestObject
     */
    public static final Pattern histPattern = Pattern.compile(
        "\\s+(?<index>\\d+)?\\s+(?<size>[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)\\s+(?<units>\\S+)\\s+(?<instanceCount>\\S+)\\s+(?<klassName>\\S+)");

    /**
     * Create a new iterator for histogram results.
     *
     * @param string The string representing the histogram.
     * @return a new iterator for histogram results.
     */
    public static Iterable<HistogramRow> histResults(@Nonnull final String string) {
        return histResults(histPattern.matcher(string));
    }

    /**
     * Create a new iterator for histogram results.
     *
     * @param matcher the matcher to match on.
     * @return a new iterator for histogram results.
     */
    public static Iterable<HistogramRow> histResults(@Nonnull final Matcher matcher) {
        final Iterator<HistogramRow> it = new HistIterator(matcher);
        return () -> it;
    }

    /**
     * HistIterator.
     */
    public static class HistIterator implements Iterator<HistogramRow> {
        private final Matcher matcher;

        /**
         * Create a new HistIterator.
         *
         * @param matcher The matcher for finding histogram rows.
         */
        public HistIterator(@Nonnull final Matcher matcher) {
            this.matcher = Objects.requireNonNull(matcher);
        }

        @Override
        public boolean hasNext() {
            return matcher.find();
        }

        @Override
        public HistogramRow next() {
            return new HistogramRow(matcher);
        }
    }

    /**
     * HistogramRow.
     */
    public static class HistogramRow {
        final Optional<Integer> index;
        final double size;
        final String units;
        final int instanceCount;
        final String klassName;

        private HistogramRow(@Nonnull final Matcher matcher) {
            assertThat(matcher.groupCount(), greaterThanOrEqualTo(5)); // 4 or 5 fields, and match 0 is the entire string that matched
            index = Optional.ofNullable(matcher.group("index")).map(Integer::parseInt);
            size = Double.parseDouble(matcher.group("size"));
            units = matcher.group("units");
            instanceCount = Integer.parseInt(matcher.group("instanceCount").replace(",", ""));
            klassName = matcher.group("klassName");
        }
    }
}
