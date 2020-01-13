package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Testing scalability.
 */

public class ScalabilityFixVMsRegionalWindows {
    /**
     * number of vms is fixed to 16000. all ris are regional windows.
     */
    @Ignore
    @Test
    public void testScalingFixVMsRegionalWindows() {

        int templates = 175;
        int vms = 16000;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.REGIONAL;
        OSType platform = OSType.WINDOWS;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, family range
        SMAUtilsTest.testRandomInput(templates, vms, 100, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 200, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 400, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 800, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 1600, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 3200, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 6400, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 12800, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 16000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 20000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 25600, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 38400, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 51200, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 76800, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 128000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 256000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 512000, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, vms, 1024000, families, zones, accounts, typeOfRIs, platform, familyRange);
    }
}
