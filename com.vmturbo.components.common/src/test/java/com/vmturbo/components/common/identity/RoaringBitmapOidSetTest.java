package com.vmturbo.components.common.identity;

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
public class RoaringBitmapOidSetTest extends OidSetTest<RoaringBitmapOidSet> {

    @Override
    protected RoaringBitmapOidSet createOidSet(final long[] sourceOids) {
        return new RoaringBitmapOidSet(sourceOids);
    }
}
