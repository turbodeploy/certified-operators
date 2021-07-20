package com.vmturbo.oid.identity;

/**
 * Tests for {@link ArrayOidSetTest}.
 */
public class ArrayOidSetTest extends OidSetTest<ArrayOidSet> {

    /**
     * create a new oid set.
     *
     * @param sourceOids The source oids.
     * @return A new {@link ArrayOidSet} containing the source oids.
     */
    @Override
    protected ArrayOidSet createOidSet(final long[] sourceOids) {
        return new ArrayOidSet(sourceOids);
    }
}
