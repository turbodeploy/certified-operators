package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;

/**
 * test SMA with random input.
 */
public class RandomProviderList {
    /**
     * call SMA for regional, zonal and isf ris.
     */
    @Test
    public void testRandomInput() {
        int familyRange = 4;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
    }
}
