package com.vmturbo.oid.identity;

import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.oid.identity.OidSet.EmptyOidSet;

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
        OidSet other = new ArrayOidSet(Arrays.asList(0L, 1L));
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
