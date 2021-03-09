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
    }

    @Test
    public void testAzureDatabaseTierLocalNameToId() {
        Assert.assertEquals("azure::DBPROFILE::Basic", CloudCostUtils
            .databaseTierLocalNameToId("Basic / 2048.0 MegaBytes", SDKProbeType.AZURE));
        Assert.assertEquals("azure::DBPROFILE::Standard_S6", CloudCostUtils
            .databaseTierLocalNameToId("S6 / 1024.0 MegaBytes", SDKProbeType.AZURE));
        Assert.assertEquals("azure::DBPROFILE::Premium_P11", CloudCostUtils
            .databaseTierLocalNameToId("P11 / 512.0 MegaBytes", SDKProbeType.AZURE));
    }
}
