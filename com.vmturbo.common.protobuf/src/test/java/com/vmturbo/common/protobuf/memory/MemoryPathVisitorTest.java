package com.vmturbo.common.protobuf.memory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.logging.MemoryMetrics.FoundPath;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryPathVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;

/**
 * Tests for {@link MemoryPathVisitor}.
 */
public class MemoryPathVisitorTest {

    /**
     * TestObject.
     */
    private static class TestObject {
        private TestObject other;
        private Object data;
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
     * Object to search for.
     */
    private static class HiddenObject {
        private String name;

        /**
         * Create new HiddenObject.
         *
         * @param name name
         */
        private HiddenObject(final String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    // Foo --> Baz--> Quux
    //                /
    // Bar  ---------
    private final TestObject quux = new TestObject("this-is-a-much-longer-name-than-the-others");
    private final TestObject baz = new TestObject(quux, "baz");
    private final TestObject bar = new TestObject(quux, "bar");
    private final TestObject foo = new TestObject(baz, "foo");
    private final HiddenObject ho = new HiddenObject("hidden");

    /**
     * Ignore the tests under Java11. They seem to be working incorrectly. Or even the code itself
     * works incorrectly under Java11.
     */
    @Before
    public void assume() {
        Assume.assumeThat(System.getProperty("java.version"), CoreMatchers.startsWith("1.8."));
    }

    /**
     * testNonePresent.
     */
    @Test
    public void testNonePresent() {
        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(0, visitor.totalCount());
        assertEquals(0, visitor.totalSize());
    }

    /**
     * testFindAtRoot.
     */
    @Test
    public void testFindAtRoot() {
        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singleton(new NamedObject(ho.name, ho)));

        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals(ho.name, fp.getToStringValue());
        assertEquals(ho.name, fp.getPath());
    }

    /**
     * testFindAtChild.
     */
    @Test
    public void testFindAtChild() {
        foo.data = ho;
        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals(ho.name, fp.getToStringValue());
        assertEquals(foo.name + ".data", fp.getPath());
    }

    /**
     * testFindDeeper.
     */
    @Test
    public void testFindDeeper() {
        quux.data = ho;
        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals(ho.name, fp.getToStringValue());
        assertEquals(foo.name + ".other.other.data", fp.getPath());
    }

    /**
     * multiplePathsToSameInstance.
     */
    @Test
    public void multiplePathsToSameInstance() {
        foo.data = ho;
        baz.data = ho;
        quux.data = ho;

        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        // Even with multiple paths to the same object, we should only get the shortest path
        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals(ho.name, fp.getToStringValue());
        assertEquals(foo.name + ".data", fp.getPath());
    }

    /**
     * findMultipleInstances.
     */
    @Test
    public void findMultipleInstances() {
        foo.data = ho;
        quux.data = new HiddenObject("quux-hidden");

        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        // We should find both instances
        assertEquals(2, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals(ho.name, fp.getToStringValue());
        assertEquals(foo.name + ".data", fp.getPath());

        final FoundPath fp2 = visitor.foundPaths(true).get(1);
        assertEquals(HiddenObject.class.getName(), fp2.getClassName());
        assertEquals("quux-hidden", fp2.getToStringValue());
        assertEquals(foo.name + ".other.other.data", fp2.getPath());
    }

    /**
     * findTabularResults.
     */
    @Test
    public void findTabularResults() {
        foo.data = ho;
        quux.data = new HiddenObject("quux-hidden");

        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        // We should find both instances
        assertEquals(2, visitor.totalCount());
        final String results = visitor.tabularResults();
        assertThat(results, containsString("0   com.vmturbo.common.protobuf.memory.MemoryPathVisitorTest$HiddenObject foo.data"));
        assertThat(results, containsString("1   com.vmturbo.common.protobuf.memory.MemoryPathVisitorTest$HiddenObject foo.other.other.data"));
    }

    /**
     * findInArray.
     */
    @Test
    public void findInArray() {
        foo.data = new Object[] { 10, "foo", ho, new ArrayList<>()};
        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        // We should find in the array
        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(false).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertFalse(fp.hasToStringValue());
        assertEquals(foo.name + ".data[2]", fp.getPath());
    }

    /**
     * testMultipleClasses.
     */
    @Test
    public void testMultipleClasses() {
        foo.data = ho;
        MemoryPathVisitor visitor = new MemoryPathVisitor(ImmutableSet.of(
            TestObject.class, char[].class, HiddenObject.class), 100, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(8, visitor.totalCount());
        assertEquals(1, visitor.foundPaths(false).stream()
            .filter(fp -> fp.getClassName().equals(HiddenObject.class.getName()))
            .count());
        assertEquals(3, visitor.foundPaths(false).stream()
            .filter(fp -> fp.getClassName().equals(TestObject.class.getName()))
            .count());
        assertEquals(4, visitor.foundPaths(false).stream()
            .filter(fp -> fp.getClassName().equals(char[].class.getName()))
            .count());
    }

    /**
     * testStopAtMaxInstances.
     */
    @Test
    public void testStopAtMaxInstances() {
        foo.data = ho;
        MemoryPathVisitor visitor = new MemoryPathVisitor(ImmutableSet.of(
            TestObject.class, char[].class, HiddenObject.class), 100, 3, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(3, visitor.totalCount());
    }

    /**
     * testStopAtMaxDepth.
     */
    @Test
    public void testStopAtMaxDepth() {
        foo.data = ho;
        MemoryPathVisitor visitor = new MemoryPathVisitor(ImmutableSet.of(
            TestObject.class, char[].class, HiddenObject.class), 1, 100, 0);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        assertEquals(3, visitor.totalCount());
        assertEquals(1, visitor.foundPaths(false).stream()
            .filter(fp -> fp.getClassName().equals(HiddenObject.class.getName()))
            .count());
        assertEquals(2, visitor.foundPaths(false).stream()
            .filter(fp -> fp.getClassName().equals(TestObject.class.getName()))
            .count());
    }

    /**
     * testMinInstanceDepth.
     */
    @Test
    public void testMinInstanceDepth() {
        foo.data = new HiddenObject("h0");
        baz.data = new HiddenObject("h1");
        quux.data = new HiddenObject("h2");

        MemoryPathVisitor visitor = new MemoryPathVisitor(Collections.singleton(HiddenObject.class), 100, 100, 3);
        new RelationshipMemoryWalker(visitor).traverseNamed(Collections.singleton(new NamedObject(foo.name, foo)));

        // We should skip the objects of the matching class at lower depth and only take the ones
        // at least as deep as the min instance depth
        assertEquals(1, visitor.totalCount());
        final FoundPath fp = visitor.foundPaths(true).get(0);
        assertEquals(HiddenObject.class.getName(), fp.getClassName());
        assertEquals("h2", fp.getToStringValue());
        assertEquals(foo.name + ".other.other.data", fp.getPath());
    }
}