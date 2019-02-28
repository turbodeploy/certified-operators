package com.vmturbo.components.common.identity;

import java.util.Arrays;
import java.util.Collections;
import java.util.PrimitiveIterator;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;

/**
 *
 */
public class ArrayOidSetTest extends OidSetTest<ArrayOidSet> {

    @Override
    protected ArrayOidSet createOidSet(final long[] sourceOids) {
        return new ArrayOidSet(sourceOids);
    }
}
