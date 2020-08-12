package com.vmturbo.components.common.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.Test;

/**
 * Tests for {@link MemoryMetricsManager}.
 */
public class MemoryMetricsManagerTest {

    /**
     * TestObject.
     */
    private static class TestObject {
        private TestObject other;
        private String name;

        /**
         * Constructor for TestObject.
         *
         * @param name The name.
         */
        private TestObject(final String name) {
            other = null;
            this.name = name;
        }

        /**
         * Constructor for TestObject.
         *
         * @param other The other TestObject.
         * @param name The name.
         */
        private TestObject(TestObject other,
                           final String name) {
            this.other = other;
            this.name = name;
        }
    }

    // Foo --> Baz--> Quux
    //                /
    // Bar  ---------
    //
    // Foo and Bar are roots
    private static final TestObject quux = new TestObject("quux");
    private static final TestObject baz = new TestObject(quux, "baz");
    private static final TestObject bar = new TestObject(quux, "bar");
    private static final TestObject foo = new TestObject(baz, "foo");

    /**
     * testSizesAndCounts.
     */
    @Test
    public void testSizesAndCounts() {
        final String result = MemoryMetricsManager.sizesAndCounts(foo).toString();
        assertThat(result, containsString("COUNT=9"));
        assertThat(result, containsString("SIZE="));
    }

    /**
     * testHistogram.
     */
    @Test
    public void testHistogram() {
        final String result = MemoryMetricsManager.histogram(foo).toString();
        assertThat(result, containsString("9 TOTAL"));
        assertThat(result, containsString("3 [C"));
        assertThat(result, containsString("3 com.vmturbo.components.common.metrics.MemoryMetricsManagerTest$TestObject"));
        assertThat(result, containsString("3 java.lang.String"));
    }

    /**
     * testMemoryGraph.
     */
    @Test
    public void testMemoryGraph() {
        final String result = MemoryMetricsManager.memoryGraph(foo);
        assertThat(result, containsString("Memory walk tabular results"));
        assertThat(result, containsString("TOTAL="));
        assertThat(result, containsString("Bytes"));
        assertThat(result, containsString("CHILD_COUNT=9"));
        assertThat(result, containsString("TOTAL"));
        assertThat(result, containsString("CHILD_COUNT"));
        assertThat(result, containsString("TYPE"));
        assertThat(result, containsString("PATH"));
    }

    /**
     * testMemoryGraphWithMinSize.
     */
    @Test
    public void testMemoryGraphWithMinSize() {
        final String result = MemoryMetricsManager.memoryGraph(foo, 0);
        assertThat(result, containsString("Memory walk tabular results"));
        assertThat(result, containsString("root"));
        assertThat(result, containsString("root.other"));
        assertThat(result, containsString("root.name"));
        assertThat(result, containsString("root.other.other"));
        assertThat(result, containsString("root.other.other.name.value"));
    }
}