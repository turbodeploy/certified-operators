package com.vmturbo.topology.processor.history.percentile;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for PercentileBuckets.
 */
public class PercentileBucketsTest {
    private static double delta = 0.001;

    /**
     * Test that correct bucket specification is parsed.
     */
    @Test
    public void testParseBucketSuccess() {
        PercentileBuckets pb = new PercentileBuckets("0,1,50,99,100");
        Assert.assertEquals(5, pb.size());
        Assert.assertEquals(Integer.valueOf(0), pb.index(0));
        Assert.assertEquals(Integer.valueOf(2), pb.index(10));
        Assert.assertEquals(Integer.valueOf(4), pb.index(100));
        Assert.assertEquals(0, pb.average(0), delta);
        Assert.assertEquals(0.5, pb.average(1), delta);
        Assert.assertEquals(25.5, pb.average(2), delta);
        Assert.assertEquals(99.5, pb.average(4), delta);
    }

    /**
     * Test that malformed bucket specification leads to default bucket distribution.
     */
    @Test
    public void testParseBucketFailureFormat() {
        PercentileBuckets pb = new PercentileBuckets("0,erkj,50,99,100");
        Assert.assertEquals(101, pb.size());
    }

    /**
     * Test that bucket specification that does not cover entire range leads to default bucket distribution.
     */
    @Test
    public void testParseBucketFailureRange() {
        PercentileBuckets pb = new PercentileBuckets("1,2,50,99,100");
        Assert.assertEquals(101, pb.size());
    }

}
