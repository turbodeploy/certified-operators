package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;
/**
 * Testing scalability.
 */

public class Scalability2TimesVMsZonalWindows {
    /**
     * the number of ris is twice the number of vms.
     */
    @Ignore
    @Test
    public void testScalability2TimesVMsZonalWindows() {
        int templates = 175;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.ZONAL;
        SMAPlatform platform = SMAPlatform.WINDOWS;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, family range
        SMATestUtils.testRandomInput(templates, 10000, 20000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 16000, 32000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 20000, 40000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 40000, 80000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 80000, 160000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 160000, 320000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 320000, 640000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 640000, 1280000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 1280000, 2560000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 2560000, 5120000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 5120000, 10280000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(templates, 10240000, 20580000, families, zones, accounts, typeOfRIs, platform, familyRange);
    }
}
