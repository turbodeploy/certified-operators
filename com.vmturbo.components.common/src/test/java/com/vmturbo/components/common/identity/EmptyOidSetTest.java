package com.vmturbo.components.common.identity;

import static org.junit.Assert.assertFalse;

import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.identity.OidSet.EmptyOidSet;

/**
 * EmptyOidSet tests.
 */
public class EmptyOidSetTest {

    /**
     * Verify that the EmptyOidSet iterator is empty.
     */
    @Test
    public void testEmptyIterator() {
        EmptyOidSet eos = new EmptyOidSet();
        Iterator it = eos.iterator();
        assertFalse(it.hasNext());
    }

    /**
     * A union of empty set with anything else will produce the other.
     */
    @Test
    public void testUnion() {
        EmptyOidSet eos = new EmptyOidSet();
        OidSet other = new ArrayOidSet(ImmutableList.of(0L, 1L));
        Assert.assertEquals(other, eos.union(other));
    }

    /**
     * A union with a null reference will return the current set.
     */
    @Test
    public void testUnionNull() {
        EmptyOidSet eos = new EmptyOidSet();
        Assert.assertNotNull(eos.union(null));
    }

}
