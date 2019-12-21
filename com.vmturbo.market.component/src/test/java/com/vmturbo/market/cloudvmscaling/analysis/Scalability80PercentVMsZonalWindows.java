package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;
/**
 * Testing scalability.
 */

public class Scalability80PercentVMsZonalWindows {
    /**
     * number of ris is 80 percent of the number of vms. all ris are zonal windows.
     */
    @Ignore
    @Test
    public void testScalingZonalWindows80PercentOfVMs() {
        int templates = 175;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.ZONAL;
        SMAPlatform platform = SMAPlatform.WINDOWS;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, family range
        SMATestUtils.testRandomInput(templates, 10000, 8000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 16000, 12800, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 20000, 16000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 40000, 32000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 80000, 64000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 160000, 128000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 320000, 256000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 640000, 512000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 1280000, 1024000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 2560000, 2048000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 5120000, 4096000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 10240000, 8192000, families, zones, accounts, typeOfRIs, platform, familyRange);
    }

}
