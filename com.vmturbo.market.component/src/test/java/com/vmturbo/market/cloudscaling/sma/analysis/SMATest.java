package com.vmturbo.market.cloudscaling.sma.analysis;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.jsonprocessing.JsonToSMAInputTranslator;

/**
 * This is to test many toy scenarios and making sure we are getting expected outputs.
 */
public class SMATest {
    private static final String dirPath = "src/test/resources/cloudvmscaling/";

    /**
     *  run sma for a single scenario.
     *
     * @param filename the json file with the info of vms, ris and templates.
     */
    private void testExactResult(String filename) {
        JsonToSMAInputTranslator jsonToSMAInputTranslator =
                new JsonToSMAInputTranslator();
        SMAInputContext smaInputContext = jsonToSMAInputTranslator.readsmaInput(dirPath + filename + ".i");
        List<SMAMatch> expectedouput = jsonToSMAInputTranslator.readsmaOutput(dirPath + filename + ".o.txt", smaInputContext);
        SMAOutputContext outputActualContext = StableMarriageAlgorithm.execute(new SMAInput(Collections.singletonList(smaInputContext))).getContexts().get(0);
        Assert.assertTrue(compareSMAMatches(outputActualContext.getMatches(), expectedouput));
    }


    /**
     * wrapper method to run the SMA for various scenarios.
     */
    @Test
    public void testAwsSMA() {

        testExactResult("3vm1ri.json");

        testExactResult("1vm1riPartialRI.json");

        testExactResult("2vm1rinorioptimisation.json");

        testExactResult("2vm1rinorioptimisation2.json");

        testExactResult("2vm1rinorioptimisationASG.json");

        /*
         * 2 vms. both belong to ASG. no common provider. THey should keep using the RI. Increase in
         * coverage is allowed.
         */
        testExactResult("2vm2riasgnocommonprovider.json");
        /*
         * 2 vms partially covered. the coupons will be redistributed. Post processing will solve it.
         */
        testExactResult("2vm1rireconfigure.json");
        /*
         * VM and RI are the same except zone, then no match.
         */
        testExactResult("1vm1riDifferentZoneNoMatch.json");

        /*
         * 2 vms and 2 ris and 2 templates. Each vm has only 1 provider which makes the matching
        straightforward.
         */
        testExactResult("2vm2ri.json");
        /*
         * 2 vms and 1 ri. One of the vm gets discounted.
         */
        testExactResult("2vm1ri.json");
        /*
         * 1 vm on a costlier template gets scaled down to a cheaper template. No RIs involved
         */
        testExactResult("1vm0ri2templates.json");
        /*
         * 1 vm on a costlier template gets scaled down to a cheaper template. No RIs involved.
         * Don't specify an empty RI json array.
         */
        testExactResult("1vm2templates.json");
        /*
         * 1 vm and 2 ris. one zonal one regional. the vm should prefer zonal.
         */
        testExactResult("regionvszonal.json");
        /*
         * 1 vm and 2 ris. one zonal one regional. the vm should prefer zonal.
         */
        testExactResult("1vm3ri1ZoneCompatible.json");
        /*
         * 2 vm and 2 ris. using count.
         */
        testExactResult("2vm2riusingcount.json");
        /*
         * 1 vm and 1 ISF ri. full coverage.
         */
        testExactResult("1vm1riInstanceSF.json");
        /*
         * 1 vm and 1 ISF ri. partial coverage.
         */
        testExactResult("1vm1riInstanceSFPartial.json");
        /*
         * 1 vm and 2 almost same ri's. Prefer RI which is on a costlier template.
         */
        testExactResult("1vm2rinodiff.json");
        /*
         * 2 identical vm and 1 ri. The name breaks the tie.
         */
        testExactResult("1vm2rinodiff.json");
        /*
         * 1 vm and 2 same ri's. The name breaks the tie.
         */
        testExactResult("1vm2riNameBreaksTie.json");

        // 2 ris merged together since they are identical.
        testExactResult("2vm2riMergeRI.json");

        // 2 instance size flexible ris merged together .
        testExactResult("2vm2riInstanceSFMergeRIs.json");

        // partial coverage should be preferred if it has better savings per coupon.
        testExactResult("partialoverfullinefficient.json");

    }

    /**
     * wrapper method to run the SMA for various scenarios.
     */
    @Test
    public void testAzureSMA() {
        testExactResult("Azure1vm2riLeastLicenseDiscountCost.json");
        testExactResult("Azure1vm2riScoping.json");
        testExactResult("Azure1vm2riOSTypeCost.json");
        testExactResult("Azure3vm3riScoping.json");
        testExactResult("Azure3vm6riScoping.json");
        testExactResult("Azure2vm2riScoping.json");
        testExactResult("Azure2vm2riSharedScoping.json");
        testExactResult("Azure2vm2riISFSingle.json");
        testExactResult("Azure1vm4riISFScoping.json");
        testExactResult("Azure1vm4rIsISFAndScoping.json");
        testExactResult("Azure3vms12risISFAndScoping.json");
        testExactResult("Azure1vm1riMatch.json");
        testExactResult("Azure1vm1riShareScopeMatch.json");
        testExactResult("Azure1vm1riSingleScopeNoMatch.json");
    }
    
    /**
     * test ASG.
     */
    @Test
    public void testSMAASG() {
        /*
            3 vms all belong to an ASG. the vm belong to different zones.
            but the RI is regional. and it goes to the cheapest template
            in the family of the RI.
         */
        testExactResult("3vm1riASG.json");

        testExactResult("6vm2ri2GrpASG.json");

        testExactResult("3vm0riASG.json");

        testExactResult("3vm1riASGPartialConverage.json");

    }

    /**
     * compare two lists of SMAMatch and make sure they have the same set of elements.
     *
     * @param matches1 first list
     * @param matches2 second list
     * @return true if the lists are same.
     */
    private boolean compareSMAMatches(List<SMAMatch> matches1, List<SMAMatch> matches2) {
        if (matches1.size() != matches2.size()) {
            return false;
        }
        for (SMAMatch match1 : matches1) {
            boolean found = false;
            for (SMAMatch match2 : matches2) {
                if (compareReservedInstance(match1.getReservedInstance(),
                        match2.getReservedInstance())
                        && Math.abs(match1.getDiscountedCoupons() - match2.getDiscountedCoupons()) < SMAUtils.BIG_EPSILON
                        && (match1.getVirtualMachine().getOid() == match2.getVirtualMachine().getOid())
                        && (match1.getTemplate().getOid() == match2.getTemplate().getOid())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare 2 RI's are same.
     * @param ri1 first RI
     * @param ri2 second RI
     * @return true if the RI's are same.
     */
    private boolean compareReservedInstance(SMAReservedInstance ri1, SMAReservedInstance ri2) {
        if (ri1 == null && ri2 == null) {
            return true;
        } else if (ri1 == null) {
            return false;
        } else if (ri2 == null) {
            return false;
        } else {
            return (ri1.getOid() == ri2.getOid());
        }
    }
}
