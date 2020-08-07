package com.vmturbo.common.protobuf.memory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.github.jamm.MemoryLayoutSpecification;
import org.junit.Test;

import com.vmturbo.common.protobuf.memory.MemoryVisitor.ClassHistogramSizeVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;

/**
 * Tests for {@link FastMemoryWalker} and associated {@link MemoryVisitor}s.
 */
public class FastMemoryWalkerTest {

    /**
     * TestObject.
     */
    private static class TestObject {
        private TestObject other;
        private String name;

        /**
         * Create new TestObject.
         *
         * @param name name
         */
        private TestObject(final String name) {
            other = null;
            this.name = name;
        }

        /**
         * Create new TestObject.
         *
         * @param other other
         * @param name name
         */
        private TestObject(TestObject other,
                          final String name) {
            this.other = other;
            this.name = name;
        }
    }

    /**
     * histPattern.
     *          SIZE        COUNT TYPE
     *    368 Bytes           12 TOTAL
     * 1  176 Bytes            4 [C
     * 2   96 Bytes            4 java.lang.String
     * 3   96 Bytes            4 com.vmturbo.common.protobuf.memory.FastMemoryWalkerTest$TestObject
     */
    private final Pattern histPattern = Pattern.compile(
        "\\s+(?<index>\\d+)?\\s+(?<size>[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)\\s+(?<units>\\S+)\\s+(?<instanceCount>\\S+)\\s+(?<klassName>\\S+)");

    // Foo --> Baz--> Quux
    //                /
    // Bar  ---------
    private final TestObject quux = new TestObject("this-is-a-much-longer-name-than-the-others");
    private final TestObject baz = new TestObject(quux, "baz");
    private final TestObject bar = new TestObject(quux, "bar");
    private final TestObject foo = new TestObject(baz, "foo");

    /**
     * testBasicSizesAndCounts.
     */
    @Test
    public void testBasicSizesAndCounts() {
        final TotalSizesAndCountsVisitor visitor =
            new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(visitor).traverse(quux);

        final long baseSize = MemoryLayoutSpecification.sizeOf(quux);
        final long nameSize = MemoryLayoutSpecification.sizeOf(quux.name);
        final long charArraySize = MemoryLayoutSpecification.sizeOf(quux.name.toCharArray());

        assertEquals(3, visitor.totalCount());
        assertEquals(charArraySize + baseSize + nameSize, visitor.totalSize());
    }

    /**
     * testHistogramSizesAndCounts.
     */
    @Test
    public void testHistogramSizesAndCounts() {
        final TotalSizesAndCountsVisitor visitor =
            new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(visitor).traverse(foo);

        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(hist).traverse(foo);

        assertEquals(visitor.totalCount(), hist.totalCount());
        assertEquals(visitor.totalSize(), hist.totalSize());
    }

    /**
     * testBasicHistogram.
     */
    @Test
    public void testBasicHistogram() {
        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(hist).traverse(foo, bar);
        final Set<String> expectedClasses =
            Arrays.asList(TestObject.class, char[].class, String.class).stream()
            .map(Class::getName)
            .collect(Collectors.toSet());

        for (HistogramRow hr : histResults(histPattern.matcher(hist.toString()))) {
            if (hr.index.isPresent()) {
                assertEquals(4, hr.instanceCount);
                assertThat(hr.klassName, isIn(expectedClasses));
            } else {
                // Total row
                assertEquals("TOTAL", hr.klassName);
                assertEquals(hist.totalSize(), (int)hr.size);
                assertEquals(hist.totalCount(), hr.instanceCount);
            }
        }
    }

    /**
     * testHistogramOrdering.
     */
    @Test
    public void testHistogramOrdering() {
        // Should be sorted by size
        // Indices should be in ascending order from 1
        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(hist).traverse(foo, bar);

        final AtomicInteger index = new AtomicInteger();
        double size = hist.totalSize();
        for (HistogramRow hr : histResults(histPattern.matcher(hist.toString()))) {
            hr.index.ifPresent(ndx -> assertEquals(index.get(), (int)ndx));
            assertThat(hr.size, lessThanOrEqualTo(size));

            index.incrementAndGet();
            size = hr.size;
        }
    }

    /**
     * testExclusionSet.
     */
    @Test
    public void testExclusionSet() {
        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.singleton(baz), 0, 100);
        new FastMemoryWalker(hist).traverse(foo);

        // We should not traverse below baz, which means we should get 2 TestObject instances
        // and one String and char[] instance (we traverse TO baz, but not to its descendants).
        // If we did not respect the exclusion set, we'd have 3 instances of all of them.
        final Map<String, Integer> expectedCounts = ImmutableMap.of(
            "TOTAL", 4,
            TestObject.class.getName(), 2,
            char[].class.getName(), 1,
            String.class.getName(), 1
        );
        for (HistogramRow hr : histResults(histPattern.matcher(hist.toString()))) {
            assertEquals((int)expectedCounts.get(hr.klassName), hr.instanceCount);
        }
    }

    /**
     * testExclusionDepth.
     */
    @Test
    public void testExclusionDepth() {
        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.singleton(baz), 1, 100);
        new FastMemoryWalker(hist).traverse(foo);

        // Since we reach baz at depth 1, and exclusionDepth is at depth 1,
        // we should continue walking its descendants (we only start excluding
        // when we exceed exclusionDepth).
        final Map<String, Integer> expectedCounts = ImmutableMap.of(
            "TOTAL", 9,
            TestObject.class.getName(), 3,
            char[].class.getName(), 3,
            String.class.getName(), 3
        );
        for (HistogramRow hr : histResults(histPattern.matcher(hist.toString()))) {
            assertEquals((int)expectedCounts.get(hr.klassName), hr.instanceCount);
        }
    }

    /**
     * testMaxDepth.
     */
    @Test
    public void testMaxDepth() {
        final ClassHistogramSizeVisitor hist =
            new ClassHistogramSizeVisitor(Collections.emptySet(), 100, 1);
        new FastMemoryWalker(hist).traverse(foo);

        // We should not traverse below depth 1, which is just the immediate children
        // of foo (baz and the name String). Baz's children and the name String's
        // character array are below depth 1.
        final Map<String, Integer> expectedCounts = ImmutableMap.of(
            "TOTAL", 3,
            TestObject.class.getName(), 2,
            char[].class.getName(), 0,
            String.class.getName(), 1
        );
        for (HistogramRow hr : histResults(histPattern.matcher(hist.toString()))) {
            assertEquals((int)expectedCounts.get(hr.klassName), hr.instanceCount);
        }
    }

    private static Iterable<HistogramRow> histResults(@Nonnull final Matcher matcher) {
        final Iterator<HistogramRow> it = new HistIterator(matcher);
        return () -> it;
    }

    /**
     * HistIterator.
     */
    private static class HistIterator implements Iterator<HistogramRow> {
        private final Matcher matcher;

        private HistIterator(@Nonnull final Matcher matcher) {
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
    private static class HistogramRow {
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