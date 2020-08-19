package com.vmturbo.components.common.identity;

import static org.junit.Assert.assertFalse;

import java.util.Iterator;

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

}
