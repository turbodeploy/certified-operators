package com.vmturbo.components.common.identity;

/**
 *
 */
public class ArrayOidSetTest extends OidSetTest<ArrayOidSet> {

    @Override
    protected ArrayOidSet createOidSet(final long[] sourceOids) {
        return new ArrayOidSet(sourceOids);
    }
}
