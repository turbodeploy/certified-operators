package com.vmturbo.topology.processor.cost;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 *
 */
public class CloudCostUtilsTest {
    @Test
    public void testStorageTierLocalNameToId() {
        Assert.assertEquals("No change", CloudCostUtils.storageTierLocalNameToId("No change", null));
        Assert.assertEquals("aws::ST::ID", CloudCostUtils.storageTierLocalNameToId("id", SDKProbeType.AWS_COST));
        Assert.assertEquals("azure::ST::ID", CloudCostUtils.storageTierLocalNameToId("id", SDKProbeType.AZURE));
    }

    @Test
    public void testDatabaseTierLocalNameToId() {
        Assert.assertEquals("No change", CloudCostUtils.databaseTierLocalNameToId("No change", null));
        Assert.assertEquals("aws::DBPROFILE::id", CloudCostUtils.databaseTierLocalNameToId("id", SDKProbeType.AWS_COST));
        Assert.assertEquals("azure::DBPROFILE::id", CloudCostUtils.databaseTierLocalNameToId("id", SDKProbeType.AZURE));
    }
}
