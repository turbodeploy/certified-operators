package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for {@link CollectionUtils}.
 */
public class CollectionUtilsTest {

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests how dependent objects are sorted. It is expected that dependencies will have less index
     * value then dependent objects
     */
    @Test
    public void testDependencySorting() {
        final ObjectWithDependencies obj1 = new ObjectWithDependencies(1, Sets.newHashSet(2, 3));
        final ObjectWithDependencies obj2 = new ObjectWithDependencies(2, Sets.newHashSet(3));
        final ObjectWithDependencies obj3 = new ObjectWithDependencies(3, Collections.emptySet());
        final ObjectWithDependencies obj4 = new ObjectWithDependencies(4, Collections.emptySet());
        final ObjectWithDependencies obj5 = new ObjectWithDependencies(5, Sets.newHashSet(4));
        final ObjectWithDependencies obj6 = new ObjectWithDependencies(6, Sets.newHashSet(1));

        final List<ObjectWithDependencies> list = Arrays.asList(obj1, obj2, obj3, obj4, obj5, obj6);
        final List<ObjectWithDependencies> sorted =
                CollectionUtils.sortWithDependencies(list, ObjectWithDependencies::getId,
                        ObjectWithDependencies::getDependsOn);
        Assert.assertTrue(sorted.indexOf(obj1) > sorted.indexOf(obj2));
        Assert.assertTrue(sorted.indexOf(obj1) > sorted.indexOf(obj3));
        Assert.assertTrue(sorted.indexOf(obj2) > sorted.indexOf(obj3));
        Assert.assertTrue(sorted.indexOf(obj5) > sorted.indexOf(obj4));
        Assert.assertTrue(sorted.indexOf(obj6) > sorted.indexOf(obj1));
    }

    /**
     * Tests cyclic dependency in input collections. Exception is expected.
     */
    @Test
    public void testDepdenenctySortingCyclic() {
        final ObjectWithDependencies obj1 = new ObjectWithDependencies(1, Collections.singleton(2));
        final ObjectWithDependencies obj2 = new ObjectWithDependencies(2, Collections.singleton(3));
        final ObjectWithDependencies obj3 = new ObjectWithDependencies(3, Collections.singleton(1));
        expectedException.expect(IllegalArgumentException.class);
        final List<ObjectWithDependencies> list = Arrays.asList(obj1, obj2, obj3);
        CollectionUtils.sortWithDependencies(list, ObjectWithDependencies::getId,
                ObjectWithDependencies::getDependsOn);
    }

    /**
     * Test inconsistent dependencies configuration - an object depends in another object, that is
     * absent.
     */
    @Test
    public void testDepdenenctySortingBrokenDependency() {
        final ObjectWithDependencies obj1 = new ObjectWithDependencies(1, Collections.emptySet());
        final ObjectWithDependencies obj2 = new ObjectWithDependencies(2, Collections.singleton(3));
        expectedException.expect(IllegalArgumentException.class);
        final List<ObjectWithDependencies> list = Arrays.asList(obj1, obj2);
        CollectionUtils.sortWithDependencies(list, ObjectWithDependencies::getId,
                ObjectWithDependencies::getDependsOn);
    }

    /**
     * Tests sorting of a collection, where dependencies are represented as objects themselves.
     */
    @Test
    public void testDependencySortingDirectObjects() {
        final List<Long> arrayList = Arrays.asList(2L, 3L, 1L, 4L);
        final List<Long> sortedList = CollectionUtils.sortWithDependencies(arrayList,
                value -> value > 1L ? Collections.singleton(value - 1) : Collections.emptySet());
        Assert.assertEquals(Arrays.asList(1L, 2L, 3L, 4L), sortedList);
    }

    /**
     * Simple object with dependencies to run tests on.
     */
    private static class ObjectWithDependencies {
        private final int id;
        private final Set<Integer> dependsOn;

        ObjectWithDependencies(int id, @Nonnull Set<Integer> dependsOn) {
            this.id = id;
            this.dependsOn = Objects.requireNonNull(dependsOn);
        }

        public int getId() {
            return id;
        }

        public Set<Integer> getDependsOn() {
            return dependsOn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ObjectWithDependencies)) {
                return false;
            }
            ObjectWithDependencies that = (ObjectWithDependencies)o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
