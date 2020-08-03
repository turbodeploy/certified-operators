package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

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
        SMAUtilsTest.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.REGIONAL, OSType.LINUX, familyRange);
        SMAUtilsTest.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.REGIONAL, OSType.WINDOWS, familyRange);
        SMAUtilsTest.testRandomInput(100, 6000, 100, 10, 4, 1, TypeOfRIs.ZONAL, OSType.WINDOWS, familyRange);
    }
}
