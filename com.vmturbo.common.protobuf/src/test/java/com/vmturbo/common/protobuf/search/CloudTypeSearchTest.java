package com.vmturbo.common.protobuf.search;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link CloudType} routines.
 */
public class CloudTypeSearchTest {
    /**
     * Test that conversion of probe type to CloudType works as expected.
     */
    @Test
    public void testProbeTypeConversion() {
        verifyProbeType("AWS", CloudType.AWS);
        verifyProbeType("AWS Billing", CloudType.AWS);
        verifyProbeType("AWS Cost", CloudType.AWS);
        verifyProbeType("AWS Lambda", CloudType.AWS);
        verifyProbeType("Azure Subscription", CloudType.AZURE);
        verifyProbeType("Azure Service Principal", CloudType.AZURE);
        verifyProbeType("Azure EA", CloudType.AZURE);
        verifyProbeType("Azure Cost", CloudType.AZURE);
        verifyProbeType("VCenter", null);
    }

    private void verifyProbeType(@Nonnull String probeType, CloudType expectedCloudType) {
        Assert.assertEquals(expectedCloudType, CloudType.fromProbeType(probeType).orElse(null));
    }
}
