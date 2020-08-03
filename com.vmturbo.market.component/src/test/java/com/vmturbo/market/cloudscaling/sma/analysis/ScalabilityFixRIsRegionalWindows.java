package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Testing scalability.
 */

public class ScalabilityFixRIsRegionalWindows {
    /**
     * fix the number of ris to 16000. all ris are regional windows.
     */
    @Ignore
    @Test
    public void testScalingFixRIsRegionalWindows() {

        int templates = 175;
        int ris = 16000;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.REGIONAL;
        OSType platform = OSType.WINDOWS;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS
        SMAUtilsTest.testRandomInput(templates, 100, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 1600, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 3200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 6400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 12800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 16000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 20000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 25600, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 38400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 51200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 76800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 128000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 256000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 512000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMAUtilsTest.testRandomInput(templates, 1024000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
    }


}
