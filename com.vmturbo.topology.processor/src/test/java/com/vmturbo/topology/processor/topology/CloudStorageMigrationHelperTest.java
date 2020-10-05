package com.vmturbo.topology.processor.topology;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for CloudStorageMigrationHelper class.
 */
public class CloudStorageMigrationHelperTest {

    /**
     * Test method getAzureOptimalDiskSizeGB. Return the optimal disk size given a disk size in GB.
     */
    @Test
    public void testGetAzureOptimalDiskSizeGB() {
        int[] input = new int[] {2, 5, 13, 25, 32, 50, 100, 200, 1000, 1024, 2000, 5000, 10000};
        int[] expectedOutput = new int[] {4, 8, 16, 32, 32, 64, 128, 256, 1024, 1024, 2048, 5120, 10240};

        for (int i = 0; i < input.length; i++) {
            float output = CloudStorageMigrationHelper.getAzureOptimalDiskSizeGB(input[i]);
            Assert.assertEquals(expectedOutput[i], output, 0.0);
        }
    }
}
