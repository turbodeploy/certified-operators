package com.vmturbo.cost.component.savings;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Algorithm-2 action list compression.
 */
public class AlgorithmTest {

    /**
     * Action list compression tests.
     */
    @Test
    public void compressList() {
        Assert.assertEquals(Arrays.asList(5d), Algorithm2.compressList(Arrays.asList(2d, 0d, 3d)));
        Assert.assertEquals(Arrays.asList(6d), Algorithm2.compressList(Arrays.asList(2d, 0d, 4d, 0d)));
        Assert.assertEquals(Arrays.asList(7d), Algorithm2.compressList(Arrays.asList(0d, 2d, 0d, 5d)));
        Assert.assertEquals(Arrays.asList(-9d, 8d), Algorithm2.compressList(Arrays.asList(-9d, 0d, 2d, 0d, 6d, 0d)));
        Assert.assertEquals(Arrays.asList(2d, -3d, 4d), Algorithm2.compressList(Arrays.asList(2d, -3d, 4d)));
        Assert.assertEquals(Arrays.asList(6d, -7d), Algorithm2.compressList(Arrays.asList(2d, 0d, 4d, -7d)));
        Assert.assertEquals(Arrays.asList(), Algorithm2.compressList(Arrays.asList(0d)));
        Assert.assertEquals(Arrays.asList(), Algorithm2.compressList(Arrays.asList()));
    }
}