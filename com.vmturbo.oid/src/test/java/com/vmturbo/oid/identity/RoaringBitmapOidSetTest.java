package com.vmturbo.oid.identity;

/**
 * Tests for {@link RoaringBitmapOidSet}.
 */
public class RoaringBitmapOidSetTest extends OidSetTest<RoaringBitmapOidSet> {

    /**
     * Create an oidSet.
     *
     * @param sourceOids The oids to add to the set.
     * @return A new {@link RoaringBitmapOidSet} containing the sourceOids.
     */
    @Override
    protected RoaringBitmapOidSet createOidSet(final long[] sourceOids) {
        return new RoaringBitmapOidSet(sourceOids);
    }
}
