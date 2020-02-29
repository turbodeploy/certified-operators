package com.vmturbo.components.common.identity;

import java.util.Arrays;
import java.util.Collections;
import java.util.PrimitiveIterator;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

/**
 * Generic base tests for OidSet implementations.
 */
public abstract class OidSetTest<T extends OidSet> {

    // override this method to generate OidSet instances for the test
    abstract protected T createOidSet(long[] sourceOids);

    @Test
    public void testContainsOid() {
        T testFilter = createOidSet(new long[]{1L,2L,3L});
        Assert.assertTrue(testFilter.contains(2L));
        Assert.assertFalse(testFilter.contains(5L));
    }

    @Test
    public void testContainsStringOids() {
        T testFilter = createOidSet(new long[]{1L,2L,3L});
        // empty list should be contained
        Assert.assertTrue(testFilter.contains(Collections.emptyList()));
        // non-long-compatible strings should NOT be contained
        Assert.assertFalse(testFilter.containsStringOids(Arrays.asList("V")));
        Assert.assertFalse(testFilter.containsStringOids(Arrays.asList("")));
        Assert.assertFalse(testFilter.containsStringOids(Arrays.asList("7.1")));
        // strings with valid longs should behave as expected
        Assert.assertTrue(testFilter.containsStringOids(Arrays.asList("1", "2", "3")));
        Assert.assertFalse(testFilter.containsStringOids(Arrays.asList("1", "2", "3", "4")));
    }

    @Test
    public void testContainsCollection() {
        T testFilter = createOidSet(new long[]{1L, 2L, 3L});
        Assert.assertTrue(testFilter.contains(Collections.emptyList()));
        Assert.assertTrue(testFilter.contains(Arrays.asList(1L, 2L)));
        Assert.assertTrue(testFilter.contains(Arrays.asList(1L, 2L, 3L)));
        Assert.assertFalse(testFilter.contains(Arrays.asList(1L, 5L)));
    }

    /**
     * Test if the oidset contains any elements of an input collection of Longs.
     */
    @Test
    public void testContainsAnyCollection() {
        T testFilter = createOidSet(new long[]{1L, 2L, 3L});
        Assert.assertFalse(testFilter.containsAny(Collections.emptyList()));
        Assert.assertTrue(testFilter.containsAny(Arrays.asList(1L, 2L)));
        Assert.assertTrue(testFilter.containsAny(Arrays.asList(1L, 2L, 3L)));
        Assert.assertTrue(testFilter.containsAny(Arrays.asList(1L, 5L)));
        Assert.assertFalse(testFilter.containsAny(Arrays.asList(4L, 5L)));
    }

    /**
     * Test if the oidset contains any elements of an input oidset
     */
    @Test
    public void testContainsAnyOidSet() {
        T testFilter = createOidSet(new long[]{1L, 2L, 3L});
        Assert.assertFalse(testFilter.containsAny(createOidSet(null)));
        Assert.assertTrue(testFilter.containsAny(createOidSet(new long[]{1L, 2L})));
        Assert.assertTrue(testFilter.containsAny(createOidSet(new long[]{1L, 2L, 3L})));
        Assert.assertTrue(testFilter.containsAny(createOidSet(new long[]{1L, 5L})));
        Assert.assertFalse(testFilter.containsAny(createOidSet(new long[]{4L, 5L})));
    }

    @Test
    public void testEmptySet() {
        T testFilter = createOidSet(new long[0]);
        Assert.assertFalse(testFilter.contains(5L));
    }

    @Test
    public void testNullConstructorParameter() {
        T testFilter = createOidSet(null);
        Assert.assertEquals(0, testFilter.size());
    }

    @Test
    public void testFilterPrimitiveArray() {
        T testFilter = createOidSet(new long[]{1L,2L,3L,4L});
        OidSet results = testFilter.filter(new long[]{2,3});
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2));
        Assert.assertFalse(results.contains(1));
    }

    @Test
    public void testFilterOidSet() {
        T testFilter = createOidSet(new long[]{1L,2L,3L,4L});
        T testCandidates = createOidSet(new long[]{2L,3L});
        OidSet results = testFilter.filter(testCandidates);
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2));
        Assert.assertFalse(results.contains(1));
    }

    @Test
    public void testFilterSet() {
        T testFilter = createOidSet(new long[]{1L,2L,3L,4L});
        Set<Long> testCandidates = Sets.newHashSet(2L,3L);
        Set<Long> results = testFilter.filter(testCandidates);
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2L));
        Assert.assertFalse(results.contains(1L));
    }

    @Test
    public void testIterator() {
        T testFilter = createOidSet(new long[]{1,2,3,4});
        PrimitiveIterator.OfLong iterator = testFilter.iterator();
        Assert.assertEquals(1L, iterator.nextLong());
        Assert.assertEquals(2L, iterator.nextLong());
        Assert.assertEquals(3L, iterator.nextLong());
        Assert.assertEquals(4L, iterator.nextLong());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testUnion() {
        T testSetA = createOidSet(new long[]{2, 3, 4});
        // set B has some overlap with A and some unique elements
        T testSetB = createOidSet(new long[]{1, 2, 4, 5});
        OidSet union = testSetA.union(testSetB);
        // the final output should contain the unique elements
        Assert.assertEquals(5, union.size());
        PrimitiveIterator.OfLong iterator = union.iterator();
        Assert.assertEquals(1L, iterator.nextLong());
        Assert.assertEquals(2L, iterator.nextLong());
        Assert.assertEquals(3L, iterator.nextLong());
        Assert.assertEquals(4L, iterator.nextLong());
        Assert.assertEquals(5L, iterator.nextLong());
        Assert.assertFalse(iterator.hasNext());
    }

    /**
     * test to ensure that the oid set hash code is order-independent.
     */
    @Test
    public void testHashCodeStability() {
        // make sure that the hash code is the same for two sets with the same contents regardless of
        // order.
        T testSetA = createOidSet(new long[]{1, 2, 3, 4});
        T testSetB = createOidSet(new long[]{4, 3, 2, 1});
        Assert.assertEquals(testSetA.hashCode(), testSetB.hashCode());
    }

    /**
     * test to ensure that the oid set equals() is order-independent.
     */
    @Test
    public void testEqualsStability() {
        T testSetA = createOidSet(new long[]{1, 2, 3, 4});
        T testSetB = createOidSet(new long[]{4, 3, 2, 1});
        Assert.assertEquals(testSetA, testSetB);
    }

    /**
     * test to ensure that the oid set equals() will fail if one set is contained within the other.
     */
    @Test
    public void testNotEquals() {
        T testSetA = createOidSet(new long[]{1, 2, 3, 4});
        T testSetB = createOidSet(new long[]{1, 2, 3});
        Assert.assertNotEquals(testSetA, testSetB);
    }
}
