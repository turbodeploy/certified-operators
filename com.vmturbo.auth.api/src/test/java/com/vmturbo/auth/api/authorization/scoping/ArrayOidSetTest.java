package com.vmturbo.auth.api.authorization.scoping;

import java.util.Arrays;
import java.util.Collections;
import java.util.PrimitiveIterator;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ArrayOidSetTest {

    @Test
    public void testContainsOid() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1L,2L,3L});
        Assert.assertTrue(testFilter.contains(2L));
        Assert.assertFalse(testFilter.contains(5L));
    }

    @Test
    public void testContainsCollection() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1L,2L,3L});
        Assert.assertTrue(testFilter.contains(Collections.emptyList()));
        Assert.assertTrue(testFilter.contains(Arrays.asList(1L, 2L)));
        Assert.assertTrue(testFilter.contains(Arrays.asList(1L, 2L, 3L)));
        Assert.assertFalse(testFilter.contains(Arrays.asList(1L, 5L)));
    }

    @Test
    public void testEmptySet() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[0]);
        Assert.assertFalse(testFilter.contains(5L));
    }

    @Test
    public void testFilterPrimitiveArray() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1L,2L,3L,4L});
        OidSet results = testFilter.filter(new long[]{2,3});
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2));
        Assert.assertFalse(results.contains(1));
    }

    @Test
    public void testFilterOidSet() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1L,2L,3L,4L});
        ArrayOidSet testCandidates = new ArrayOidSet(new long[]{2L,3L});
        OidSet results = testFilter.filter(testCandidates);
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2));
        Assert.assertFalse(results.contains(1));
    }

    @Test
    public void testFilterSet() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1L,2L,3L,4L});
        Set<Long> testCandidates = Sets.newHashSet(2L,3L);
        Set<Long> results = testFilter.filter(testCandidates);
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(2L));
        Assert.assertFalse(results.contains(1L));
    }

    @Test
    public void testIterator() {
        ArrayOidSet testFilter = new ArrayOidSet(new long[]{1,2,3,4});
        PrimitiveIterator.OfLong iterator = testFilter.iterator();
        Assert.assertEquals(1L, iterator.nextLong());
        Assert.assertEquals(2L, iterator.nextLong());
        Assert.assertEquals(3L, iterator.nextLong());
        Assert.assertEquals(4L, iterator.nextLong());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testUnion() {
        ArrayOidSet testSetA = new ArrayOidSet(new long[]{2, 3, 4});
        // set B has some overlap with A and some unique elements
        ArrayOidSet testSetB = new ArrayOidSet(new long[]{1, 2, 4, 5});
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
}
