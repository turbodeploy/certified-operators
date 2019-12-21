package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;
/**
 * Testing scalability.
 */

public class Scalability80PercentRIsZonalWindows {
    /**
     * number of vms is 80% of the number of ris.
     */
    @Ignore
    @Test
    public void testScalingZonalWindows80PercentOfRIs() {
        int templates = 175;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.ZONAL;
        SMAPlatform platform = SMAPlatform.WINDOWS;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, riType, OS, familyRange
        SMATestUtils.testRandomInput(templates, 8000, 10000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 12800, 16000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 16000, 20000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 32000, 40000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 64000, 80000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 128000, 160000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 256000, 320000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 512000, 640000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 1024000, 1280000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 2048000, 2560000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 4096000, 5120000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 8192000, 10240000, families, zones, accounts, typeOfRIs, platform, familyRange);
    }

}
