package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.jsonprocessing.JsonToSMAInputTranslator;

//import org.apache.commons.lang3.tuple.Pair;

/**
 * This is to test many toy scenarios and making sure we are getting expected outputs.
 */
public class SMATest {
    private static String dirPath = "src/test/resources/cloudvmscaling/";


    private static final Logger logger = LogManager.getLogger();

    /**
     *  run sma for a single scenario.
     *
     * @param filename the json file with the info of vms, ris and templates.
     */
    public void testExactResult(String filename) {
        JsonToSMAInputTranslator jsonToSMAInputTranslator = new JsonToSMAInputTranslator();
        Pair<SMAInput, SMAOutput> inputOutputPair = jsonToSMAInputTranslator.parseInputWithExpectedOutput(dirPath + filename);
        int index = 0;
        for (SMAInputContext inputContext : inputOutputPair.first.getContexts()) {
            SMAOutputContext outputActualContext = StableMarriagePerContext.execute(inputContext);
            SMAOutputContext outputExpectedContext = inputOutputPair.second.getContexts().get(index);
            Assert.assertTrue(compareSMAMatches(outputActualContext.getMatches(), outputExpectedContext.getMatches()));
        }
    }

    /**
     * run SMA and compare only the count of the vm-ri pairing.
     *
     * @param filename the json file with the input
     * @param count expected output.
     */
    public void testSMACount(String filename, int count) {
        JsonToSMAInputTranslator jsonToSMAInputTranslator = new JsonToSMAInputTranslator();
        SMAInput smaInput = jsonToSMAInputTranslator.parseInput(dirPath + filename);
        for (SMAInputContext inputContext : smaInput.getContexts()) {
            SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
            List<SMAMatch> actualMatches = outputContext.getMatches().stream()
                    .filter(a -> (a.getReservedInstance() != null)).collect(Collectors.toList());
            Assert.assertEquals(count, actualMatches.size());
        }
    }

    /**
     * wrapper method for testSMACount.
     */
    @Test
    public void testSMACount() {
        testSMACount("testStress.json", 30);
        testSMACount("realExample.json", 10);

    }

    /**
     * wrapper method to run the SMA for various scenarios.
     */
    @Test
    public void testSMA() {
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

        testExactResult("2vm2riMergeRI.json");

        testExactResult("2vm2riInstanceSFMergeRIs.json");

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

        //testExactResult("ASGMinimizeMoves.json");

        //testExactResult("ASGAccountMoves.json");

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
    public boolean compareSMAMatches(List<SMAMatch> matches1, List<SMAMatch> matches2) {
        if (matches1.size() != matches2.size()) {
            return false;
        }
        for (SMAMatch match1 : matches1) {
            boolean found = false;
            for (SMAMatch match2 : matches2) {
                if (compareReservedInstance(match1.getReservedInstance(),
                        match2.getReservedInstance()) &&
                        (match1.getDiscountedCoupons() == match2.getDiscountedCoupons()) &&
                        (match1.getVirtualMachine().getOid() == match2.getVirtualMachine().getOid()) &&
                        (match1.getTemplate().getOid() == match2.getTemplate().getOid())) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
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
