package com.vmturbo.cost.calculation.topology;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Tests for AccountPricingData.
 */
public class AccountPricingDataTest {
    /**
     *  Verify that AccountPricingData.OS_TO_BASE_OS covers all OSType values.
     */
    @Test
    public void testOsToBaseOsMap() {
        for (OSType os : OSType.values()) {
            assertNotNull(AccountPricingData.OS_TO_BASE_OS.get(os));
        }
    }
}
