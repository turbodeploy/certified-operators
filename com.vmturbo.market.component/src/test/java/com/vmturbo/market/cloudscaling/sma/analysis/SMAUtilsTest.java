package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;

import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class has supporting methods for SMATest classes.
 */
public class SMAUtilsTest {


    /**
     * update the reserved instances by combining similar RI's.
     * Market component does this externally. This is just for test purposes.
     *
     * @param smaInputContext the input context
     */
    static void normalizeReservedInstance(SMAInputContext smaInputContext) {
        Map<String, List<SMATemplate>> familyNameToTemplates =
                StableMarriagePerContext.computeFamilyToTemplateMap(smaInputContext.getTemplates());
        StableMarriagePerContext.normalizeReservedInstances(smaInputContext.getReservedInstances(), familyNameToTemplates);
        Map<SMAReservedInstanceKey, List<SMAReservedInstance>> distinctRIs = new HashMap<>();
        for (SMAReservedInstance ri : smaInputContext.getReservedInstances()) {
            SMAReservedInstanceKey riContext = new SMAReservedInstanceKey(ri);
            List<SMAReservedInstance> instances = distinctRIs.get(riContext);
            if (instances == null) {
                distinctRIs.put(riContext, new ArrayList<>(Arrays.asList(ri)));

            } else {
                instances.add(ri);
            }
        }
        smaInputContext.getReservedInstances().clear();
        for (List<SMAReservedInstance> members : distinctRIs.values()) {
            if (members == null || members.size() < 1) {
                // this is an error. Can be handled as an error if required.
                continue;
            }
            Collections.sort(members, new SortByRIOID());
            SMAReservedInstance representative = members.get(0);
            for (SMAReservedInstance smaReservedInstance : members) {
                SMAReservedInstance newReservedInstance = new SMAReservedInstance(
                        smaReservedInstance.getOid(),
                        representative.getRiKeyOid(),
                        smaReservedInstance.getName(),
                        smaReservedInstance.getBusinessAccountId(),
                        smaReservedInstance.getApplicableBusinessAccounts(),
                        smaReservedInstance.getTemplate(),
                        smaReservedInstance.getZoneId(),
                        smaReservedInstance.getCount(),
                        smaReservedInstance.isIsf(),
                        smaReservedInstance.isShared(),
                        smaReservedInstance.isPlatformFlexible());
                smaInputContext.getReservedInstances().add(newReservedInstance);
            }
        }
    }

    /**
     * figure out if an RI is ISF based on zone, tenancy and os.
     *
     * @param zone    zone id of ri. SMAUtils.NO_ZONE for regional
     * @param tenancy the tenancy of the ri
     * @param os      the os type of the ri
     * @return true if the RI is ISF.
     */
    public static boolean computeInstanceSizeFlexibleForAWSRIs(long zone, Tenancy tenancy, OSType os) {
        if (tenancy == Tenancy.DEFAULT &&
                SMAUtils.LINUX_OS.contains(os) &&
                zone == SMAUtils.NO_ZONE) {
            return true;
        }
        return false;
    }

    /**
     * Class to define the context for a RI, and used to combine similar RIs.
     */
    public static class SMAReservedInstanceKey {
        // the least cost template in the family for ISF RI's
        private final SMATemplate normalizedTemplate;
        private final long zone;
        private final long businessAccount;

        /**
         * Constructor given an RI, construct its context.
         * @param ri the parent reserved instance.
         */
        public SMAReservedInstanceKey(final SMAReservedInstance ri) {
            this.normalizedTemplate = ri.getNormalizedTemplate();
            this.zone = ri.getZoneId();
            this.businessAccount = ri.getBusinessAccountId();
        }

        /*
         * Determine if two RIs are equivalent.  They are equivalent if same template, zone and business account.
         * If the RIs are regional, then zone == NO_ZONE.
         */
        @Override
        public int hashCode() {
            return Objects.hash(normalizedTemplate.getOid(), zone, businessAccount);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final SMAReservedInstanceKey that = (SMAReservedInstanceKey)obj;
            return normalizedTemplate.getOid() == that.normalizedTemplate.getOid() &&
                    zone == that.zone && businessAccount == that.businessAccount;
        }
    }

    /**
     *  Given two RIs, compare by OID.
     */
    public static class SortByRIOID implements Comparator<SMAReservedInstance> {
        /**
         * compare the OID's of the two RIs.
         * @param ri1 first RI
         * @param ri2 second RI
         * @return 1 if the first OID is bigger else -1.
         */
        @Override
        public int compare(SMAReservedInstance ri1, SMAReservedInstance ri2) {
            return (ri1.getOid() - ri2.getOid() > 0) ? 1 : -1;
        }
    }


    /**
     * creates random input and runs SMA with it.
     *
     * @param nTemplates         number of templates
     * @param nVirtualMachines   number of virtual machines
     * @param nReservedInstances number of reserved instances
     * @param nfamily            number of template families
     * @param nzones             number of availability zones
     * @param nbusinessAccounts  number of business accounts
     * @param typeOfRIs          Zonal if all RIs are zonal, Regional if all RIs are regional, or MIXED
     * @param os                 operating system
     * @param familyRange        maximum number of families allowed in a VM's provider list
     * @return SMAOutputContext
     */
    static SMAOutputContext testRandomInput(int nTemplates, int nVirtualMachines, int nReservedInstances,
                                            int nfamily, int nzones, int nbusinessAccounts,
                                            TypeOfRIs typeOfRIs, OSType os, int familyRange) {

        SMAInputContext inputContext = generateInput(nTemplates, nVirtualMachines, nReservedInstances,
                nfamily, nzones, nbusinessAccounts,
                typeOfRIs, os, familyRange);

        SMAOutputContext outputContext = generateOutput(inputContext);
        return outputContext;
    }

    /**
     * Single SMA run with an input that has ASG.
     * @param nTemplates the number of templates in the test
     * @param nfamily the number of families
     * @param nzones the number of zones
     * @param nbusinessAccount the number of business accounts
     * @param typeOfRIs all ris are regional or zonal or mixed
     * @param os  the os of the context to determine if the ri's are isf
     * @param familyRange the avg number of templates in each family.
     * @param asgCount the number of asgs
     * @param asgSize average number of vms in each asg
     * @return the output of the SMA for a single context.
     */
    public static SMAOutputContext testASG(int nTemplates,
                                           int nfamily, int nzones, int nbusinessAccount,
                                           TypeOfRIs typeOfRIs, OSType os, int familyRange, int asgCount, int asgSize) {

        SMAInputContext inputContext = generateASGInput(nTemplates,
                nfamily, nzones, nbusinessAccount,
                typeOfRIs, os, familyRange, asgCount, asgSize);

        SMAOutputContext outputContext = generateOutput(inputContext);
        return outputContext;
    }

    /**
     * Run SMA. Execute all actions. Run SMA again and make sure there is no scaling.
     * @param nTemplates the number of templates in the test
     * @param nfamily the number of families
     * @param nzones the number of zones
     * @param nbusinessAccount the number of business accounts
     * @param zonalRIs all ris are regional or zonal or mixed
     * @param os  the os of the context to determine if the ri's are isf
     * @param familyRange the avg number of templates in each family.
     * @param asgCount the number of asgs
     * @param asgSize average number of vms in each asg
     * @param loops the number of testcases where we failed to have stability.
     *              loops[i] keeps track of testcases which stabilized after i iterations
     */
    public static void testASGStability(int nTemplates,
                                        int nfamily, int nzones, int nbusinessAccount, TypeOfRIs zonalRIs,
                                        OSType os, int familyRange, int asgCount, int asgSize, int[] loops) {

        SMAInputContext inputContext = generateASGInput(nTemplates,
                nfamily, nzones, nbusinessAccount,
                zonalRIs, os, familyRange, asgCount, asgSize);
        int nVirtualMachines = inputContext.getVirtualMachines().size();
        int nReservedInstances = inputContext.getReservedInstances().size();
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        int mismatch = 0;
        int loop = 0;
        while (loop < 12) {
            loop++;
            List<SMAVirtualMachine> smaVirtualMachines = outputContext.getMatches().stream().map(a -> a.getVirtualMachine()).collect(Collectors.toList());
            List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAVirtualMachine oldVM = smaVirtualMachines.get(i);
                SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(),
                        oldVM.getName(),
                        oldVM.getGroupName(),
                        oldVM.getBusinessAccountId(),
                        outputContext.getMatches().get(i).getTemplate(),
                        oldVM.getProviders(),
                        //oldVM.getCurrentRICoverage(),
                        (float)outputContext.getMatches().get(i).getDiscountedCoupons(),
                        oldVM.getZoneId(),
                        outputContext.getMatches().get(i).getReservedInstance() == null ?
                                SMAUtils.BOGUS_RI :
                                outputContext.getMatches().get(i).getReservedInstance(),
                        oldVM.getOsType());
                newVirtualMachines.add(smaVirtualMachine);
            }
            List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
            List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
            for (int i = 0; i < nReservedInstances; i++) {
                SMAReservedInstance oldRI = oldReservedInstances.get(i);
                SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
                newReservedInstances.add(newRI);
            }
            SMAContext context = inputContext.getContext();
            SMAInputContext newInputContext = new SMAInputContext(context, newVirtualMachines, newReservedInstances, inputContext.getTemplates());
            SMAOutputContext newOutputContext = StableMarriagePerContext.execute(newInputContext);
            mismatch = 0;
            int coupons = 0;
            float newsaving = 0;
            int templatemismatch = 0;
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAMatch match = newOutputContext.getMatches().get(i);
                SMAVirtualMachine vm = match.getVirtualMachine();
                SMATemplate currentTemplate = vm.getCurrentTemplate();
                SMATemplate matchTemplate = match.getTemplate();
                if (currentTemplate.getOid() != matchTemplate.getOid() || (Math.round(vm.getCurrentRICoverage()) - match.getDiscountedCoupons() != 0)) {
                    //System.out.println(String.format("testStability mismatch VM=%s: currentTemplate=%s != matchTemplate=%s coverage=%s discount=%s",
                    //      vm.toStringShallow(), currentTemplate, matchTemplate, vm.getCurrentRICoverage(), match.getDiscountedCoupons()));
                    mismatch++;
                    coupons += Math.round(vm.getCurrentRICoverage()) - match.getDiscountedCoupons();
                    newsaving += (vm.getCurrentTemplate().getNetCost(vm.getBusinessAccountId(), vm.getOsType(), vm.getCurrentRICoverage()) -
                            match.getTemplate().getNetCost(vm.getBusinessAccountId(), vm.getOsType(), match.getDiscountedCoupons()));
                }
                if (currentTemplate.getOid() != matchTemplate.getOid()) {
                    templatemismatch++;
                }
            }
            System.out.println("coupons: " + coupons + " mismatch " + mismatch + " saving " + newsaving);
            outputContext = newOutputContext;
            inputContext = newInputContext;
            if (mismatch > 0) {
                int a = 1;
            }
            if (mismatch == 0) {
                break;
            }
            if (newsaving < -0.5) {
                loops[14]++;
                loops[12] += templatemismatch;
            }
            if (Math.abs(newsaving) < 0.5) {
                loops[13]++;
                loops[12] += templatemismatch;
            }
        }
        loops[loop]++;
        loops[0]++;

        for (int i = 0; i < 12; i++) {
            System.out.print("L" + i + " " + loops[i] + " ; ");
        }
        System.out.println("actual moves: " + loops[12] + " zero saving: " + loops[13] + "bad_saving" + loops[14]);
        Assert.assertEquals(0, mismatch);

    }

    /**
     * Given a map from family to number of templates, generate the templates and return a map of
     * family to templates.
     * sorted by coupons.
     *
     * @param os  the osType, which is needed to access cost.
     * @param familyToNumberOfTemplates map from family to number of templates
     *                                  return map from family to template
     * @param rand Random object with a seed based on current time.
     * @param nBusinessAccounts number of business accounts.  Needed to associate cost with each business account.
     * @return a map of family to templates sorted by coupons.
     */
    private static Map<String, List<SMATemplate>> computeFamilyToTemplateMap(OSType os,
                                                                             Map<Integer, Integer> familyToNumberOfTemplates,
                                                                             Random rand, int nBusinessAccounts) {

        // map from family to its templates
        Map<String, List<SMATemplate>> map = new HashMap<>();

        Set<Integer> families = familyToNumberOfTemplates.keySet();
        for (Integer nFamily : families) {
            Integer nTemplates = familyToNumberOfTemplates.get(nFamily);
            String family = "f" + nFamily;
            float familyCouponCost = (float)rand.nextInt(2) + ((float)rand.nextInt(100000) / 100000);
            List<SMATemplate> list = new ArrayList<>();
            for (int i = 0; i < nTemplates; i++) {
                // number of coupons are a power of 2.
                int numberOfCoupons = (int)Math.pow(2.0, i);
                // on-demand cost is a function of number of coupons.
                SMACost onDemandCost = new SMACost(SMAUtils.round((float)numberOfCoupons * familyCouponCost), 0);
                // only dealing with AWS, discounted cost is 0.
                // ??? discounted cost is RI cost.  This should refer to license component
                SMACost discountedCost = new SMACost(0, 0);
                String name = family + ".t" + i;
                SMATemplate smaTemplate = new SMATemplate(SMATestConstants.TEMPLATE_BASE +
                        (nFamily * 1000L) + i, name, family, numberOfCoupons, null);
                for (int j = 0; j < nBusinessAccounts; j++) {
                    smaTemplate.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE + j, os, onDemandCost);
                    smaTemplate.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE + j, os, discountedCost);
                }
                list.add(smaTemplate);
            }
            map.put(family, list);
        }
        // for debugging purposes.
        for (String family : map.keySet()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append(family).append(":\n");
            for (SMATemplate template : map.get(family)) {
                buffer.append(template.toString()).append("\n");
            }
        }
        //        System.out.println(buffer.toString());
        return map;
    }

    /*
     * Count number of templates in a size
     * Used for debugging
     */
    private static void countTemplates(List<SMATemplate> templates) {
        Map<Integer, Integer> countTemplateSizes = new HashMap<>();
        for (SMATemplate template : templates) {
            int coupons = template.getCoupons();
            Integer nTemplates = countTemplateSizes.get(coupons);
            if (nTemplates == null) {
                countTemplateSizes.put(coupons, 1);
            } else {
                countTemplateSizes.put(coupons, nTemplates + 1);
            }
        }
        System.out.println("coupons, number of templates");
        for (Integer key : countTemplateSizes.keySet()) {
            System.out.println(key + ", " + countTemplateSizes.get(key));

        }
    }

    /**
     * Test performance of worst case scenario for SMA when every RI proposes to every VM.
     * For AWS, because shared is true and platformFlexible is false for RIs.
     * @param nVirtualMachines number of virtual machines
     * @param nReservedInstances number of reserved instances
     */
    public static void testWorstCase(int nVirtualMachines, int nReservedInstances) {
        OSType os = OSType.UNKNOWN_OS;
        SMATemplate smaTemplate = new SMATemplate(SMATestConstants.TEMPLATE_BASE + 1,
                SMATestConstants.TEMPLATE_NAME_ONE,
                "family1", 1, null);
        for (int i = 0; i <  10; i++) {
            smaTemplate.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE + i, os, new SMACost(1, 0));
            smaTemplate.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE + i, os, new SMACost(0, 0));
        }
        List<SMATemplate> templates = Arrays.asList(smaTemplate);
        List<SMAVirtualMachine> virtualMachines = new ArrayList<>();
        for (int i = 0; i < nVirtualMachines; i++) {
            long vmOid = SMATestConstants.VIRTUAL_MACHINE_BASE + i;
            SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(vmOid, "vm_" + i,
                    SMAUtils.NO_GROUP_ID,
                    SMATestConstants.BUSINESS_ACCOUNT_BASE,
                    smaTemplate,
                    new ArrayList(Arrays.asList(smaTemplate)),
                    0,
                    SMATestConstants.ZONE_BASE,
                    SMAUtils.BOGUS_RI,
                    os);
            virtualMachines.add(smaVirtualMachine);
        }
        List<SMAReservedInstance> reservedInstances = new ArrayList<>();
        for (int i = 0; i < nReservedInstances; i++) {
            long riKeyId = SMATestConstants.RESERVED_INSTANCE_KEY_BASE + i;
            long riOid = SMATestConstants.RESERVED_INSTANCE_BASE + i;
            long baOid = SMATestConstants.BUSINESS_ACCOUNT_BASE + (i % (nReservedInstances
                    / SMATestConstants.BUSINESS_ACCOUNT_BASE));
            SMAReservedInstance reservedInstance = new SMAReservedInstance(riOid,
                    riKeyId,
                    "ri" + riOid,
                    baOid, Collections.emptySet(),
                    smaTemplate,
                    SMATestConstants.ZONE_BASE,
                    1,
                    true,
                    true,
                    false);
            reservedInstances.add(reservedInstance);
        }
        SMAContext context = new SMAContext(SMACSP.AWS,
                OSType.LINUX,
                SMATestConstants.REGION_BASE,
                SMATestConstants.BILLING_FAMILY_BASE + 1,
                Tenancy.DEFAULT);
        SMAInputContext inputContext = new SMAInputContext(context, virtualMachines,
                reservedInstances, templates);
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);

    }


    /**
     * Run SMA. Execute all actions. Run SMA again and make sure there is no scaling.
     * @param nTemplates the number of templates in the test
     * @param nVirtualMachines the number of virtual machines
     * @param nReservedInstances the number of reserved instances
     * @param nfamily the number of family
     * @param nzones the number of zones
     * @param nbusinessAccount the number of business accounts
     * @param zonalRIs all ris are regional or zonal or mixed
     * @param os the os of the context to determine if the ri's are isf
     * @param familyRange the avg number of templates in each family.
     * @param loops the number of testcases where we failed to have stability.
     *              loops[i] keeps track of testcases which stabilized after i iterations
     */
    public static void testStability(int nTemplates, int nVirtualMachines, int nReservedInstances,
                                     int nfamily, int nzones, int nbusinessAccount, TypeOfRIs zonalRIs,
                                     OSType os, int familyRange, int[] loops) {

        SMAInputContext inputContext = generateInput(nTemplates, nVirtualMachines, nReservedInstances,
                nfamily, nzones, nbusinessAccount,
                zonalRIs, os, familyRange);
        nReservedInstances = inputContext.getReservedInstances().size();
        nVirtualMachines = inputContext.getVirtualMachines().size();
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        int mismatch = 0;
        int loop = 0;
        while (loop < 11) {
            loop++;
            List<SMAVirtualMachine> smaVirtualMachines = outputContext.getMatches().stream().map(a -> a.getVirtualMachine()).collect(Collectors.toList());
            List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAVirtualMachine oldVM = smaVirtualMachines.get(i);
                SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(),
                        oldVM.getName(),
                        oldVM.getGroupName(),
                        oldVM.getBusinessAccountId(),
                        outputContext.getMatches().get(i).getTemplate(),
                        oldVM.getProviders(),
                        //oldVM.getCurrentRICoverage(),
                        (float)outputContext.getMatches().get(i).getDiscountedCoupons(),
                        oldVM.getZoneId(),
                        outputContext.getMatches().get(i).getReservedInstance() == null ?
                                SMAUtils.BOGUS_RI :
                                outputContext.getMatches().get(i).getReservedInstance(),
                        oldVM.getOsType());
                newVirtualMachines.add(smaVirtualMachine);
            }
            SMAContext context = inputContext.getContext();
            List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
            List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
            for (int i = 0; i < nReservedInstances; i++) {
                SMAReservedInstance oldRI = oldReservedInstances.get(i);
                SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
                newReservedInstances.add(newRI);
            }
            SMAInputContext newInputContext = new SMAInputContext(context, newVirtualMachines, newReservedInstances, inputContext.getTemplates());
            SMAOutputContext newOutputContext = StableMarriagePerContext.execute(newInputContext);
            mismatch = 0;
            int coupons = 0;
            float newsaving = 0;
            int templatemismatch = 0;
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAMatch match = newOutputContext.getMatches().get(i);
                SMAVirtualMachine vm = match.getVirtualMachine();
                SMATemplate currentTemplate = vm.getCurrentTemplate();
                SMATemplate matchTemplate = match.getTemplate();
                if (currentTemplate.getOid() != matchTemplate.getOid() || (Math.round(vm.getCurrentRICoverage()) - match.getDiscountedCoupons() != 0)) {
                    System.out.println(String.format("testStability mismatch VM=%s: "
                                    + "currentTemplate=%s != matchTemplate=%s "
                                    + "coverage=%s discount=%s",
                            vm.getName(), currentTemplate.getName(),
                            matchTemplate.getName(),
                            vm.getCurrentRICoverage(), match.getDiscountedCoupons()));
                    mismatch++;
                    coupons += Math.round(vm.getCurrentRICoverage()) - match.getDiscountedCoupons();
                    newsaving += (vm.getCurrentTemplate().getNetCost(vm.getBusinessAccountId(), os, vm.getCurrentRICoverage()) -
                            match.getTemplate().getNetCost(vm.getBusinessAccountId(), os, match.getDiscountedCoupons()));
                }
                if (currentTemplate.getOid() != matchTemplate.getOid()) {
                    templatemismatch++;
                }
            }
            System.out.println("coupons: " + coupons + " mismatch " + mismatch + " saving " + newsaving);
            outputContext = newOutputContext;
            inputContext = newInputContext;
            if (mismatch > 0) {
                int a = 1;
            }
            if (mismatch == 0) {
                break;
            }
            if (newsaving < -0.5) {
                loops[14]++;
                loops[12] += templatemismatch;
            }
            if (Math.abs(newsaving) < 0.5) {
                loops[13]++;
                loops[12] += templatemismatch;
            }
        }
        loops[loop]++;
        loops[0]++;

        for (int i = 0; i < 12; i++) {
            System.out.print("L" + i + " " + loops[i] + " ; ");
        }
        System.out.println("actual moves: " + loops[12] + " zero saving: " + loops[13] + "bad_saving" + loops[14]);
        Assert.assertEquals(0, mismatch);

    }

    /**
     * Given constraints, generate an SMAInputContext.
     *
     * @param nTemplates         number of templates
     * @param nVirtualMachines   number of virtual machines
     * @param nReservedInstances number of reserved instances
     * @param nfamily            of families
     * @param nzones             number of zones
     * @param nbusinessAccount   number of business accounts
     * @param typeOfRIs          are the RIs instance size flexible?
     * @param os                 operating system
     * @param familyRange        range of families: each VM has 1 - familyRange families in its set of providers.
     * @return SMAInputContext
     */
    private static SMAInputContext generateInput(int nTemplates, int nVirtualMachines, int nReservedInstances,
                                                 int nfamily, int nzones, int nbusinessAccount,
                                                 TypeOfRIs typeOfRIs, OSType os, int familyRange) {
        SMAContext context = new SMAContext(SMACSP.AWS,
                os,
                SMATestConstants.REGION_BASE + 1,
                SMATestConstants.BILLING_FAMILY_BASE + 1,
                Tenancy.DEFAULT);
        List<SMATemplate> smaTemplates = new ArrayList<>();
        List<SMAVirtualMachine> smaVirtualMachines = new ArrayList<>();
        List<SMAReservedInstance> smaReservedInstances = new ArrayList<>();
        Random rand = new Random(System.currentTimeMillis());

        // map from family to number of templates
        Map<Integer, Integer> familyToNumberOfTemplates = new HashMap<>();

        for (int i = 0; i < nTemplates; i++) {
            int family = rand.nextInt(nfamily);
            Integer count = familyToNumberOfTemplates.get(family);
            if (count == null) {
                familyToNumberOfTemplates.put(family, 1);
            } else {
                familyToNumberOfTemplates.put(family, count + 1);
            }
        }


        // map from family to sorted list of templates.
        Map<String, List<SMATemplate>> familyToTemplateMap =
            computeFamilyToTemplateMap(os, familyToNumberOfTemplates, rand, nbusinessAccount);
        smaTemplates = familyToTemplateMap.values().stream().flatMap(e -> e.stream()).collect(Collectors.toList());

        for (int i = 0; i < nVirtualMachines; i++) {
            //            Collections.shuffle(smaTemplates);
            SMATemplate currentTemplate = smaTemplates.get(rand.nextInt(nTemplates - 1));
            List<SMATemplate> providers = computeProviders(currentTemplate,
                    familyToTemplateMap,
                    familyRange,
                    rand);

            float currentRICoverage = 0;
            long vmOid = SMATestConstants.VIRTUAL_MACHINE_BASE + i;
            SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(vmOid, "vm_" + vmOid,
                    SMAUtils.NO_GROUP_ID,
                    SMATestConstants.BUSINESS_ACCOUNT_BASE + rand.nextInt(nbusinessAccount),
                    //smaTemplates.get(rand.nextInt(nTemplates - 1)),
                    providers.get(rand.nextInt(providers.size())),
                    providers,
                    currentRICoverage,
                    SMATestConstants.ZONE_BASE + rand.nextInt(nzones),
                    SMAUtils.BOGUS_RI,
                    os);
            smaVirtualMachines.add(smaVirtualMachine);
        }

        for (int i = 0; i < nReservedInstances; i++) {
            long riOid = SMATestConstants.RESERVED_INSTANCE_BASE + i;
            long riKeyOid = SMATestConstants.RESERVED_INSTANCE_KEY_BASE + i;
            long zone = (typeOfRIs == TypeOfRIs.REGIONAL ? SMAUtils.NO_ZONE : SMATestConstants.ZONE_BASE + rand.nextInt(nzones));
            long baOid = SMATestConstants.BUSINESS_ACCOUNT_BASE + rand.nextInt(nbusinessAccount);
            SMAReservedInstance smaReservedInstance = new SMAReservedInstance(riOid,
                riKeyOid, "ri" + riOid,
                baOid, Collections.emptySet(),
                smaTemplates.get(rand.nextInt(nTemplates)),
                zone,
                1,  // count
                computeInstanceSizeFlexibleForAWSRIs(zone, context.getTenancy(), context.getOs()),
                true,
                false);
            smaReservedInstances.add(smaReservedInstance);
        }
        SMAInputContext inputContext = new SMAInputContext(context, smaVirtualMachines, smaReservedInstances, smaTemplates);
        normalizeReservedInstance(inputContext);
        return inputContext;
    }

    /*
     * Return providers, a list of templates, with these constraints
     * 1) If template T, which is in family F, is in the providers than template T1 is in providers
     *    if T1.coupons > T.coupons.
     * 2) the number of families in the providers is [1 - familyRange]
     *
     *  @param currentTemplate the template the VM is allocated to
     *  @param familyToTemplateMap map from family to list of templates in the family: constraint
     *                             templates are sorted by coupons
     *  @param familyRange range of families in the providers.
     *  @param rand random number generator
     *  @return list of templates
     */
    private static List<SMATemplate> computeProviders(SMATemplate currentTemplate,
                                                      Map<String, List<SMATemplate>> familyToTemplateMap,
                                                      int familyRange,
                                                      Random rand) {
        List<SMATemplate> providers = new ArrayList<>();
        String family = currentTemplate.getFamily();
        List<String> providerFamilies = new ArrayList<>();
        providerFamilies.add(family);
        // list of templates for this family
        List<SMATemplate> templates = familyToTemplateMap.get(family);
        // index of current template in the list
        int templateIndex = templates.indexOf(currentTemplate);
        if (templateIndex == -1) {
            // WTF
            templateIndex = 0;
        }
        /*
         * Add to providers all templates in the family that have the same number or more
         * coupons as currentTemplate.
         */
        providers.addAll(templates.subList(templateIndex, templates.size()));

        // list of families
        boolean exception = false;
        List<String> families = new ArrayList<>(familyToTemplateMap.keySet());
        // randomly pick the number of families in the list of providers
        int numberOfFamilies = 0;
        try {
            numberOfFamilies = rand.nextInt(familyRange - 1);
        } catch (Exception e) {
            exception = true;
        }
        for (int i = 0; i < numberOfFamilies; i++) {
            int index = rand.nextInt(familyRange - 1);
            try {
                family = families.get(index);
            } catch (Exception e) {
                exception = true;
            }
            if (providerFamilies.contains(family)) {
                // if we already have the family, then skip it.
                continue;
            }
            providerFamilies.add(family);
            templates = familyToTemplateMap.get(family);
            int newIndex = templateIndex;
            if (templateIndex >= templates.size()) {
                // if current family does not have a appropriate size template, then skip the family.
                continue;
            }
            providers.addAll(templates.subList(newIndex, templates.size()));
        }

        return providers;
    }

    /**
     * The jitter test is to make sure that small changes in environment results in little actions.
     * We run SMA and excute all the actions. We then create a small change in the environment. We
     * then look at the number of changes and number of actions and make sure they are comaprable.
     * @param nTemplates the number of templates in the test
     * @param nVirtualMachines the number of virtual machines
     * @param nReservedInstances the number of reserved instances
     * @param nfamily the number of family
     * @param nzones the number of zones
     * @param nbusinessAccount the number of business accounts
     * @param zonalRIs all ris are regional or zonal or mixed
     * @param os the os of the context to determine if the ri's are isf
     * @param familyRange the avg number of templates in each family.
     */
    public static void testJitter(int nTemplates, int nVirtualMachines, int nReservedInstances,
                                  int nfamily, int nzones, int nbusinessAccount, TypeOfRIs zonalRIs,
                                  OSType os, int familyRange) {

        SMAInputContext inputContext = generateInput(nTemplates, nVirtualMachines, nReservedInstances,
                nfamily, nzones, nbusinessAccount, zonalRIs, os, familyRange);
        nReservedInstances = inputContext.getReservedInstances().size();
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        int loop = 0;
        while (loop < 2) {

            loop++;
            List<SMAVirtualMachine> smaVirtualMachines = outputContext.getMatches().stream().map(a -> a.getVirtualMachine()).collect(Collectors.toList());
            List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
            Random rand = new Random(System.currentTimeMillis());
            int jitter = 0;
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAVirtualMachine oldVM = smaVirtualMachines.get(i);
                if (rand.nextInt(1000) < 1 && oldVM.getProviders().size() > 1) {
                    oldVM.getProviders().remove(oldVM.getNaturalTemplate());
                    jitter++;
                }
                SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(), oldVM.getName(),
                        oldVM.getGroupName(),
                        oldVM.getBusinessAccountId(),
                        outputContext.getMatches().get(i).getTemplate(),
                        oldVM.getProviders(),
                        outputContext.getMatches().get(i).getDiscountedCoupons(),
                        oldVM.getZoneId(),
                        outputContext.getMatches().get(i).getReservedInstance() == null ?
                                SMAUtils.BOGUS_RI :
                                outputContext.getMatches().get(i).getReservedInstance(),
                        os);
                newVirtualMachines.add(smaVirtualMachine);
            }
            SMAContext context = inputContext.getContext();
            List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
            List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
            for (int i = 0; i < nReservedInstances; i++) {
                SMAReservedInstance oldRI = oldReservedInstances.get(i);
                SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
                newReservedInstances.add(newRI);
            }
            SMAInputContext newInputContext = new SMAInputContext(context, newVirtualMachines, newReservedInstances, inputContext.getTemplates());
            SMAOutputContext newOutputContext = StableMarriagePerContext.execute(newInputContext);
            int mismatch = 0;
            for (int i = 0; i < nVirtualMachines; i++) {
                SMAMatch match = newOutputContext.getMatches().get(i);
                SMAVirtualMachine vm = match.getVirtualMachine();
                SMATemplate currentTemplate = vm.getCurrentTemplate();
                SMATemplate matchTemplate = match.getTemplate();
                if ((currentTemplate.getOid() != matchTemplate.getOid()) || (Math.round(vm.getCurrentRICoverage()) - match.getDiscountedCoupons() != 0)) {
                    //System.out.println(String.format("testStability mismatch VM=%s: currentTemplate=%s != matchTemplate=%s coverage=%s discount=%s",
                    //        vm.toStringShallow(), currentTemplate, matchTemplate, vm.getCurrentRICoverage(), match.getDiscountedCoupons()));
                    mismatch++;
                }
            }
            inputContext = newInputContext;
            outputContext = newOutputContext;
            System.out.println("mismatch: " + mismatch + " jitter " + jitter);
        }
    }

    /**
     * Given an input context, call Stable Marriage Algorithm (SMA) and return the output context.
     *
     * @param inputContext the input to SMA
     * @return output context generated from SMA
     */
    private static SMAOutputContext generateOutput(SMAInputContext inputContext) {

        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        return outputContext;
    }

    private static SMAInputContext generateASGInput(int nTemplates,
                                                    int nfamily, int nzones, int nbusinessAccount,
                                                    TypeOfRIs typeOfRIs, OSType os, int familyRange, int asgCount, int asgSize) {
        SMAContext context = new SMAContext(SMACSP.AWS,
                os,
                SMATestConstants.REGION_BASE,
                SMATestConstants.BILLING_FAMILY_BASE + 1,
                Tenancy.DEFAULT);
        List<SMATemplate> smaTemplates = new ArrayList<>();
        List<SMAVirtualMachine> smaVirtualMachines = new ArrayList<>();
        List<SMAReservedInstance> smaReservedInstances = new ArrayList<>();
        Random rand = new Random(System.currentTimeMillis());

        // map from family to number of templates
        Map<Integer, Integer> familyToNumberOfTemplates = new HashMap<>();

        for (int i = 0; i < nTemplates; i++) {
            int family = rand.nextInt(nfamily);
            Integer count = familyToNumberOfTemplates.get(family);
            if (count == null) {
                familyToNumberOfTemplates.put(family, 1);
            } else {
                familyToNumberOfTemplates.put(family, count + 1);
            }
        }

        // map from family to sorted list of templates.
        Map<String, List<SMATemplate>> familyToTemplateMap = computeFamilyToTemplateMap(os, familyToNumberOfTemplates, rand, nbusinessAccount);
        smaTemplates = familyToTemplateMap.values().stream().flatMap(e -> e.stream()).collect(Collectors.toList());
        for (int i = 0; i < asgCount; i++) {
            int size = rand.nextInt(asgSize) + 1;
            int rsize = rand.nextInt(asgSize) + 1;
            SMATemplate riTemplate = smaTemplates.get(rand.nextInt(nTemplates - 1));
            Long businessAccount = SMATestConstants.BUSINESS_ACCOUNT_BASE + rand.nextInt(nbusinessAccount);
            Long vmZone = 10L + rand.nextInt(nzones);
            Long zone = (typeOfRIs == TypeOfRIs.REGIONAL ? SMAUtils.NO_ZONE : vmZone);
            List<SMATemplate> providers = computeProviders(riTemplate,
                    familyToTemplateMap,
                    familyRange,
                    rand);
            int providerSize = providers.size();
            float currentRICoverage = 0;
            for (int j = 0; j < size; j++) {
                SMATemplate memberCurrentTemplate = providers.get(rand.nextInt(providerSize));
                List<SMATemplate> memberProviders = new ArrayList(providers);
                memberProviders.remove(rand.nextInt(memberProviders.size()));
                long vmOid = SMATestConstants.VIRTUAL_MACHINE_BASE + (i * (asgSize + 1)) + j;
                SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(vmOid,
                        "vm_" + vmOid + "_" + j,
                        (SMATestConstants.GROUP_NAME_BASE + i),
                        businessAccount,
                        providers.get(rand.nextInt(providerSize)),
                        memberProviders,
                        currentRICoverage,
                        vmZone, SMAUtils.BOGUS_RI,
                        os);
                smaVirtualMachines.add(smaVirtualMachine);
            }
            for (int j = 0; j < rsize; j++) {
                Long riOid = SMATestConstants.RESERVED_INSTANCE_BASE + (i * (asgSize + 1)) + j;
                Long riKeyOid = SMATestConstants.RESERVED_INSTANCE_KEY_BASE + (i * (asgSize + 1)) + j;
                SMAReservedInstance smaReservedInstance = new SMAReservedInstance(riOid,
                    riKeyOid, "ri_" + riOid + "_" + j,
                    businessAccount,
                    Collections.emptySet(),
                    riTemplate,
                    zone,
                    1,  // count
                    computeInstanceSizeFlexibleForAWSRIs(zone, context.getTenancy(), context.getOs()),
                    true,
                    false);
                smaReservedInstances.add(smaReservedInstance);
            }
        }

        SMAInputContext inputContext = new SMAInputContext(context, smaVirtualMachines, smaReservedInstances, smaTemplates);
        normalizeReservedInstance(inputContext);
        return inputContext;
    }
}
