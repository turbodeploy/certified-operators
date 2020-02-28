package com.vmturbo.common.protobuf.topology;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Tests the methods of {@link EnvironmentTypeUtil}.
 */
public class EnvironmentTypeUtilTest {
    /**
     * Tests {@link EnvironmentTypeUtil#match}.
     */
    @Test
    public void testMatchingCases() {
        Assert.assertTrue(EnvironmentTypeUtil.match(null, EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.match(null, EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.HYBRID, EnvironmentType.CLOUD));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.UNKNOWN_ENV, null));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.UNKNOWN_ENV,
                                                    EnvironmentType.UNKNOWN_ENV));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.UNKNOWN_ENV, EnvironmentType.HYBRID));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.CLOUD, null));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.CLOUD, EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.CLOUD, EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.CLOUD, EnvironmentType.CLOUD));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.CLOUD, EnvironmentType.ON_PREM));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.ON_PREM, null));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.ON_PREM, EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.ON_PREM, EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.match(EnvironmentType.ON_PREM, EnvironmentType.ON_PREM));
        Assert.assertFalse(EnvironmentTypeUtil.match(EnvironmentType.ON_PREM, EnvironmentType.CLOUD));
    }

    /**
     * Tests {@link EnvironmentTypeUtil#matchingPredicate}.
     */
    @Test
    public void testMatchingPredicateCases() {
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(null).test(EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(null).test(EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.HYBRID).test(EnvironmentType.CLOUD));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.UNKNOWN_ENV).test(null));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.UNKNOWN_ENV)
                                .test(EnvironmentType.UNKNOWN_ENV));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.UNKNOWN_ENV)
                                .test(EnvironmentType.HYBRID));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD).test(null));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD)
                                .test(EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD)
                                .test(EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD)
                                .test(EnvironmentType.CLOUD));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD)
                                .test(EnvironmentType.ON_PREM));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.ON_PREM).test(null));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.ON_PREM)
                                .test(EnvironmentType.UNKNOWN_ENV));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.ON_PREM)
                                .test(EnvironmentType.HYBRID));
        Assert.assertTrue(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.ON_PREM)
                                .test(EnvironmentType.ON_PREM));
        Assert.assertFalse(EnvironmentTypeUtil.matchingPredicate(EnvironmentType.ON_PREM)
                                .test(EnvironmentType.CLOUD));
    }
}
