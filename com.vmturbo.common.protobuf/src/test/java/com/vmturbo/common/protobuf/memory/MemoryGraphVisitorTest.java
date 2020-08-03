package com.vmturbo.common.protobuf.memory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryGraphVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemorySubgraph;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;
import com.vmturbo.common.protobuf.memory.RelationshipMemoryWalker.MemoryReferenceNode;

/**
 * MemoryGraphVisitorTest.
 */
public class MemoryGraphVisitorTest {

    /**
     * TestObject. Base class.
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
     * TestSubclass. Subclass.
     */
    private static class TestSubclass extends TestObject {
        final int[] myInts;

        /**
         * Create new TestSubclass.
         *
         * @param other other
         * @param name name
         */
        private TestSubclass(TestObject other, final String name) {
            super(other, name);
            myInts = new int[] { 1, 2, 3 };
        }
    }

    // Foo --> Baz--> Quux
    //                /
    // Bar  ---------
    private final TestObject quux = new TestObject("this-is-a-much-longer-name-than-the-others");
    private final TestObject baz = new TestObject(quux, "baz");
    private final TestObject bar = new TestObject(quux, "bar");
    private final TestObject foo = new TestObject(baz, "foo");

    private final Pattern headerPattern = Pattern.compile("\\s+TOTAL\\s+CHILD_COUNT\\s+TYPE\\s+PATH");
    private final Pattern pathPattern = Pattern.compile(
        "\\s+(?<depth>\\d+)\\s+(?<size>[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)\\s+(?<units>\\S+)\\s+(?<children>\\S+)\\s+(?<klassName>\\S+)\\s+(?<path>\\S+)");
    private final Pattern titlePattern = Pattern.compile("=(?<size>[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)\\s+(?<units>\\w+), CHILD_COUNT=(?<children>\\S+)]");

    /**
     * testShallow.
     */
    @Test
    public void testShallow() {
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singleton(new NamedObject(quux.name, quux)));

        TotalSizesAndCountsVisitor vis = new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(vis).traverse(quux);

        // Ensure we compute the same sizes and counts
        assertEquals(vis.totalCount(), visitor.totalCount());
        assertEquals(vis.totalSize(), visitor.totalSize());
    }

    /**
     * testDeeper.
     */
    @Test
    public void testDeeper() {
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singleton(new NamedObject(foo.name, foo)));

        TotalSizesAndCountsVisitor vis = new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(vis).traverse(foo);

        // Ensure we compute the same sizes and counts
        assertEquals(vis.totalCount(), visitor.totalCount());
        assertEquals(vis.totalSize(), visitor.totalSize());
    }

    /**
     * testMultipleRoots.
     */
    @Test
    public void testMultipleRoots() {
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Arrays.asList(new NamedObject(foo.name, foo), new NamedObject(bar.name, bar)));

        TotalSizesAndCountsVisitor vis = new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(vis).traverse(foo, bar);

        // Ensure we compute the same sizes and counts
        assertEquals(vis.totalCount(), visitor.totalCount());
        assertEquals(vis.totalSize(), visitor.totalSize());
    }

    /**
     * testNodeIterationNotRetainingChildren.
     */
    @Test
    public void testNodeIterationNotRetainingChildren() {
        final int logDepth = 1;
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, logDepth, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Arrays.asList(new NamedObject(foo.name, foo), new NamedObject(bar.name, bar)));

        int currentDepth = 0;
        long nodeCount = 0;
        for (MemoryReferenceNode ref : visitor) {
            assertThat(ref.getDepth(), is(greaterThanOrEqualTo(currentDepth)));
            assertThat(ref.getDepth(), is(lessThanOrEqualTo(logDepth))); // We should not exceed the log depth
            currentDepth = ref.getDepth();
            nodeCount++;
        }
        // Since we did not retain descendants, the retained nodes should be less than the total visited
        assertThat(nodeCount, lessThan(visitor.totalCount()));
    }

    /**
     * testNodeIterationRetainingChildren.
     */
    @Test
    public void testNodeIterationRetainingChildren() {
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 1, true);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Arrays.asList(new NamedObject(foo.name, foo), new NamedObject(bar.name, bar)));

        int currentDepth = 0;
        long nodeCount = 0;
        for (MemoryReferenceNode ref : visitor) {
            assertThat(ref.getDepth(), is(greaterThanOrEqualTo(currentDepth)));
            currentDepth = ref.getDepth();
            nodeCount++;
        }

        // Since we did retain descendants, the retained nodes should be equal to the total visited
        assertEquals(visitor.totalCount(), nodeCount);
    }

    /**
     * testDescendantsAssignedToShorterPath.
     */
    @Test
    public void testDescendantsAssignedToShorterPath() {
        // Foo --> Baz--> Quux
        //                /
        // Bar  ---------
        //
        // When passing both Foo and Bar as roots, Quux should be the child of Bar and NOT the child of Foo
        // because there is a shorter path from Bar --> Quux than from Foo --> Quux
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, true);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Arrays.asList(new NamedObject(foo.name, foo), new NamedObject(bar.name, bar)));

        for (MemoryReferenceNode node : visitor) {
            if (node.getObj() == quux) {
                assertThat(node.pathDescriptor(), containsString(bar.name));
                assertThat(node.pathDescriptor(), not(containsString(foo.name)));
                return;
            }
        }

        // We should have found quux and exited the test above. If not fail.
        fail();
    }

    /**
     * testPathForArrayElement.
     */
    @Test
    public void testPathForArrayElement() {
        final Object[] objects = new Object[] { foo };
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, true);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singletonList(new NamedObject("objects", objects)));

        for (MemoryReferenceNode node : visitor) {
            if (node.getObj() == quux) {
                assertEquals("objects[0].other.other", node.pathDescriptor());
                return;
            }
        }

        // We should have found quux and exited the test above. If not fail.
        fail();
    }

    /**
     * testSubraphIteration.
     */
    @Test
    public void testSubraphIteration() {
        final int logDepth = 0;
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, logDepth, true);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singletonList(new NamedObject(foo.name, foo)));

        final long subGraphCount = StreamSupport.stream(
            ((Iterable<MemorySubgraph>)visitor::subgraphIterator).spliterator(), false)
            .count();
        assertEquals(logDepth + 1, subGraphCount);
    }

    /**
     * testSubclasses.
     */
    @Test
    public void testSubclasses() {
        final TestSubclass sc = new TestSubclass(foo, "subclass");
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, true);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singletonList(new NamedObject("subclass", sc)));

        final List<String> names = StreamSupport.stream(visitor.spliterator(), false)
            .map(MemoryReferenceNode::getNameOrIndex)
            .collect(Collectors.toList());

        // We should have traversed both the local members of the subclass (myInt) and its superclass elements
        assertThat(names, hasItems("subclass", "myInts", "other", "name"));
    }

    /**
     * testTabularResults.
     */
    @Test
    public void testTabularResults() {
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Collections.singleton(new NamedObject(quux.name, quux)));
        final String results = visitor.tabularResults();
        assertEquals(3, headerCount(results));

        final String[] expectedPaths = new String[] {
            quux.name,
            quux.name + ".name",
            quux.name + ".name.value"
        };
        final String[] expectedClasses = new String[] {
            quux.getClass().getName(),
            String.class.getName(),
            (new char[] {}).getClass().getName()
        };

        int currentPathIndex = 0;
        for (PathResult pr : pathResults(pathPattern.matcher(results))) {
            assertEquals(currentPathIndex, pr.depth); // Test depth order
            assertEquals(expectedPaths[currentPathIndex], pr.path); // Test correct path
            assertEquals(expectedClasses[currentPathIndex], pr.klassName); // Test correct class name
            assertEquals(expectedPaths.length - currentPathIndex, pr.children); // Test correct child count

            currentPathIndex++;
        }
        assertEquals(expectedPaths.length, currentPathIndex);

        final PathIterator it = new PathIterator(pathPattern.matcher(results));
        if (it.hasNext()) {
            final Matcher titleMatcher = titlePattern.matcher(results);
            assertTrue(titleMatcher.find());
            final double titleSize = Double.parseDouble(titleMatcher.group("size"));
            assertEquals(it.next().size, titleSize, 0);
        } else {
            fail();
        }
    }

    /**
     * testTabularResultsMinSize.
     */
    @Test
    public void testTabularResultsMinSize() {
        final Object[] objList = new Object[2000];
        MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), 100, 100, 100, false);
        new RelationshipMemoryWalker(visitor).traverseNamed(
            Arrays.asList(new NamedObject("small", new Object()), new NamedObject("objList", objList)));

        // The only object that should be in the table is the "objList" and not the "small" object
        final String[] expectedPaths = new String[] {
            "objList",
        };
        final String[] expectedClasses = new String[] {
            (new Object[] {}).getClass().getName()
        };

        int currentPathIndex = 0;
        final String results = visitor.tabularResults(1000);
        for (PathResult pr : pathResults(pathPattern.matcher(results))) {
            assertEquals(currentPathIndex, pr.depth); // Test depth order
            assertEquals(expectedPaths[currentPathIndex], pr.path); // Test correct path
            assertEquals(expectedClasses[currentPathIndex], pr.klassName); // Test correct class name
            assertEquals(expectedPaths.length - currentPathIndex, pr.children); // Test correct child count

            // Units should be in kilobytes
            assertEquals("KB", pr.units);
            currentPathIndex++;
        }
        assertEquals(expectedPaths.length, currentPathIndex);
    }

    private int headerCount(@Nonnull final String results) {
        int count = 0;
        final Matcher matcher = headerPattern.matcher(results);
        while (matcher.find()) {
            count++;
        }

        return count;
    }

    private static Iterable<PathResult> pathResults(@Nonnull final Matcher matcher) {
        final Iterator<PathResult> it = new PathIterator(matcher);
        return () -> it;
    }

    /**
     * PathIterator.
     */
    private static class PathIterator implements Iterator<PathResult> {
        private final Matcher matcher;

        private PathIterator(@Nonnull final Matcher matcher) {
            this.matcher = Objects.requireNonNull(matcher);
        }

        @Override
        public boolean hasNext() {
            return matcher.find();
        }

        @Override
        public PathResult next() {
            return new PathResult(matcher);
        }
    }

    /**
     * PathResult.
     */
    private static class PathResult {
        final int depth;
        final double size;
        final String units;
        final int children;
        final String klassName;
        final String path;

        private PathResult(@Nonnull final Matcher matcher) {
            assertEquals(matcher.groupCount(), 7); // 6 fields, and match 0 is the entire string that matched
            depth = Integer.parseInt(matcher.group("depth"));
            size = Double.parseDouble(matcher.group("size"));
            units = matcher.group("units");
            children = Integer.parseInt(matcher.group("children").replace(",", ""));
            klassName = matcher.group("klassName");
            path = matcher.group("path");
        }
    }
}