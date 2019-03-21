package com.vmturbo.components.common.identity;

/**
 *
 */
public class RoaringBitmapOidSetTest extends OidSetTest<RoaringBitmapOidSet> {

    @Override
    protected RoaringBitmapOidSet createOidSet(final long[] sourceOids) {
        return new RoaringBitmapOidSet(sourceOids);
    }
}
