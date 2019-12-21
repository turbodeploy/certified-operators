package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;
/**
 * Testing scalability.
 */

public class ScalabilityFixRIsISF {
    /**
     * fix the number of ris to 16000.
     */
    @Ignore
    @Test
    public void testScalingFixRIsISF() {
        int ris = 16000;
        int families = 26;
        int zones = 4;
        int accounts = 1;
        TypeOfRIs typeOfRIs = TypeOfRIs.REGIONAL;
        SMAPlatform platform = SMAPlatform.LINUX;
        int familyRange = 13;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testRandomInput(175, 100, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 1600, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 3200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 6400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 12800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 16000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 20000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 25600, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 38400, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 51200, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 76800, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 128000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 256000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 512000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
        SMATestUtils.testRandomInput(175, 1024000, ris, families, zones, accounts, typeOfRIs, platform, familyRange);
    }

}
