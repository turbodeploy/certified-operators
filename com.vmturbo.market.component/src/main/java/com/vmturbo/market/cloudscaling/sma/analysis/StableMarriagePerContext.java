package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics;
import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachineGroup;

/**
 * Given a context, run SMA, and update the context's matching field with the result.
 * Called from JUnit tests.
 * Question: if two providers are least cost, which is picked by the market.
 */
public class StableMarriagePerContext {

    private static final Logger logger = LogManager.getLogger();


    /**
     * Given a inputContext, run SMA.
     *
     * @param inputContext the input of SMA partitioned on a per context basis.
     * @return up date the matching in the inputContext.
     */
    public static SMAOutputContext execute(SMAInputContext inputContext) {

        logger.debug("stableMarriage start");
        final Stopwatch stopWatch = Stopwatch.createStarted();
        /*
         * Where to collect statistics
         */
        SMAStatistics statistics = new SMAStatistics();
        statistics.setContext(inputContext.getContext());
        final List<SMAVirtualMachine> virtualMachines = inputContext.getVirtualMachines();
        statistics.setNumberOfVirtualMachines(virtualMachines.size());
        statistics.setTotalVmCurrentCoupons(virtualMachines.stream().mapToInt(v -> v.getCurrentTemplate().getCoupons()).sum());
        statistics.setTotalVmNaturalCoupons(virtualMachines.stream().mapToInt(v -> v.getNaturalTemplate().getCoupons()).sum());
        /*
         * Map from the group name to the virtual machine groups (auto scaling group)
         */
        Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap =
                createVirtualMachineGroupMap(virtualMachines);
        /*
         *  List of templates; that is, providers
         */
        final List<SMATemplate> templates = inputContext.getTemplates();
        statistics.setNumberOfTemplates(templates.size());
        /*
         * Precompute map from family name to list of SMATemplates
         */
        Map<String, List<SMATemplate>> familyNameToTemplates = computeFamilyToTemplateMap(templates);
        statistics.setNumberOfFamilies(familyNameToTemplates.size());
        /*
         *  List of reserved instances.  There may be no RIs.
         */
        final List<SMAReservedInstance> reservedInstances = (inputContext.getReservedInstances() == null ?
            Collections.EMPTY_LIST : new ArrayList<>(inputContext.getReservedInstances()));
        statistics.setNumberOfReservedInstances(reservedInstances.size());
        statistics.setTotalRiCoupons(reservedInstances.stream().mapToInt(r -> r.getTemplate().getCoupons()).sum());
        statistics.setIsZonalRIs(computeTypeOfRIs(reservedInstances));

        /*
         * If instance size flexible (ISF), move all RIs in a
         * family to the smallest instance type in that family.
         * For ISF and non-ISF: update count and coverage appropriately.
         * Two RIs can be combined if their RI template, and zone match.
         * We already scale the isf to the cheapest template. so we can safely compare template.
         */
        normalizeReservedInstances(reservedInstances, familyNameToTemplates);
        statistics.setNumberOfNormalizedReservedInstances(reservedInstances.size());
        /*
         * Sort RIs based on the maximum saving it can achieve. Use name to break ties.
         * This is to minimize swaps and create consistency.
         */
        Collections.sort(reservedInstances, new SortByRIOID());
        final SMAContext context = inputContext.getContext();

        /*
         * Update RI Coverage for groups
         */
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            for (SMAVirtualMachineGroup group : virtualMachineGroupMap.values()) {
                reservedInstance.updateRICoveragePerGroup(group);
            }
        }

        /*
         * Queue that keeps track of all the RI's that are not engaged.
         * freeRISet is the set version of the queue.
         */
        Deque<SMAReservedInstance> freeRIs = new LinkedList<>();

        /*
         * Map that keeps track of all the unused coupons for used RIs.
         */
        Map<SMAReservedInstance, Integer> remainingCoupons = new HashMap<>();
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            int coupons = Math.round(reservedInstance.getNormalizedTemplate().getCoupons() *
                    reservedInstance.getTotalCount());
            if (coupons > 0) {
                remainingCoupons.put(reservedInstance, coupons);
                if (!freeRIs.contains(reservedInstance)) {
                    freeRIs.add(reservedInstance);
                }
            }
        }

        /*
         * build map from RI to list of VMs that can move to that RI. the couponToBestVM
         * attribute is updated for each ri.
        */
        createRIToVMsMap(virtualMachines,
                remainingCoupons, virtualMachineGroupMap,
                familyNameToTemplates,
                statistics);

        // map to keep track of the successful engagements so far.
        Map<SMAVirtualMachine, SMAMatch> currentEngagements = new HashMap<>();

        final Stopwatch stopWatch_iteration = Stopwatch.createStarted();
        /*
         * We run two rounds of iteration. In the first iteration we dont allow VMs to be partially
         * covered. We then run another round of stable marriage. At this point all the RIs have
         * just enough coupons to partially cover one more vm.
         */
        runIterations(freeRIs, remainingCoupons,
                currentEngagements, virtualMachineGroupMap, statistics, false);
        freeRIs.clear();

        /* Recompute the preference list of each ri
         * based on the remaining coupons. The list will change because some of the
         * vms cannot be fully covered with the remaining coupons and hence might not be on
         * the top of the list as before when it had more coupons and was able to cover fully.
         */
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            if (remainingCoupons.get(reservedInstance) > 0 &&
                    reservedInstance.getDiscountableVMs().size() > 0) {
                if (!freeRIs.contains(reservedInstance)) {
                    freeRIs.add(reservedInstance);
                }
                reservedInstance.getCouponToBestVM().clear();
                List<SMAVirtualMachine> vmList = new ArrayList(reservedInstance.getDiscountableVMs());
                reservedInstance.getDiscountableVMs().clear();
                sortAndUpdateVMList(vmList, remainingCoupons.get(reservedInstance), reservedInstance, virtualMachineGroupMap);
            }
        }
        runIterations(freeRIs, remainingCoupons, currentEngagements, virtualMachineGroupMap, statistics, true);

        long timeInMilliseconds = stopWatch_iteration.elapsed(TimeUnit.MILLISECONDS);
        logger.debug("stableMarriage iterations took {} ms.", timeInMilliseconds);
        statistics.setIterationTime(timeInMilliseconds);

        final Stopwatch stopWatch_postprocessing = Stopwatch.createStarted();
        /*
         * For all VMs, compute a SMAMatch
         */
        List<SMAMatch> matches = new ArrayList<>();
        matches.addAll(currentEngagements.values());
        matches.addAll(addGroupMemberEngagement(virtualMachineGroupMap, currentEngagements));

        statistics.setTotalDiscountedCoupons(matches.stream().mapToInt(m -> m.getDiscountedCoupons()).sum());

        // for all the VMs that does not have a matching move them to the natural template.
        Set<SMAVirtualMachine> set = matches.stream().map(a -> a.getVirtualMachine()).collect(Collectors.toSet());
        List<SMAVirtualMachine> virtualMachinesWithoutMatching = virtualMachines.stream().filter(vm -> !set.contains(vm)).collect(Collectors.toList());
        for (SMAVirtualMachine smaVirtualMachine : virtualMachinesWithoutMatching) {
            matches.add(new SMAMatch(smaVirtualMachine, smaVirtualMachine.getNaturalTemplate(),
                    null, 0));
        }

        statistics.setTotalVmDesiredStateCoupons(computeDesiredStateCoupons(matches));
        timeInMilliseconds = stopWatch_postprocessing.elapsed(TimeUnit.MILLISECONDS);
        logger.debug("stableMarriage post processing took {} ms.", timeInMilliseconds);
        statistics.setPostProcessTime(timeInMilliseconds);
        timeInMilliseconds = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        logger.debug("stableMarriage took {} ms.", timeInMilliseconds);
        statistics.setTime(timeInMilliseconds);

        /*
         * log the results
         */
        // compute the costs and savings
        statistics.computeSavings(virtualMachines, matches);
        // log the statistics
        logger.debug(statistics.toString());

        SMAOutputContext outputContext = new SMAOutputContext(context, matches);
        return outputContext;
    }

    /**
     * Given two SMAMatch, compare by virtual machine name.
     */

    public static class SortMatchesByVMOID implements Comparator<SMAMatch> {
        @Override
        public int compare(SMAMatch match1, SMAMatch match2) {
            return (match1.getVirtualMachine().getOid() - match2.getVirtualMachine().getOid() > 0) ? 1 : -1;
        }
    }

    /**
     * Determine if all the RIs in the list are zonal or regional or mixed.
     *
     * @param reservedInstances the list of reserved instances.
     * @return the RIs in the list are zonal or regional or mixed.
     */
    private static TypeOfRIs computeTypeOfRIs(List<SMAReservedInstance> reservedInstances) {
        int zonalRIs = 0;
        int regionalRIs = 0;
        for (SMAReservedInstance ri : reservedInstances) {
            if (ri.getZone() != SMAUtils.NO_ZONE) {
                zonalRIs++;
            } else {
                regionalRIs++;
            }
            if (zonalRIs > 0 && regionalRIs > 0) {
                break;
            }
        }
        if (zonalRIs == 0) {
            return TypeOfRIs.REGIONAL;
        } else if (regionalRIs == 0) {
            return TypeOfRIs.ZONAL;
        } else {
            return TypeOfRIs.MIXED;
        }
    }

    private static int computeDesiredStateCoupons(List<SMAMatch> matches) {
        int coupons = 0;
        for (SMAMatch match : matches) {
            coupons += match.getTemplate().getCoupons();
        }
        return coupons;
    }

    /**
     * ISF RIs are first scaled down to the template in the family with the fewest coupons.
     * We combine RI's that are same (businessAccount, normalizedTemplate zone).
     * We pick a representative that will go to SMA for all.
     * The other RIs are captured in the representative's members field.
     * The coupons are redistributed in the postprocessing step.
     *
     * @param reservedInstances the reserved instances to normalize. This is used to store output too.
     * @param familyNameToTemplates map from family name to the templates
     */
    public static void normalizeReservedInstances(List<SMAReservedInstance> reservedInstances,
                                                  Map<String, List<SMATemplate>> familyNameToTemplates) {
        Set<Long> businessAccounts = reservedInstances.stream().map(SMAReservedInstance::getBusinessAccount).collect(Collectors.toSet());
        Table<Long, String, SMATemplate> businessAccountToFamilyToSmallestTemplate = HashBasedTable.create();
        for (Map.Entry<String, List<SMATemplate>> entry : familyNameToTemplates.entrySet()) {
            String familyName = entry.getKey();
            List<SMATemplate> templatesInFamily = entry.getValue();
            SMATemplate smallestTemplateInFamily = templatesInFamily.get(0);
            for (SMATemplate template : templatesInFamily) {
                if (smallestTemplateInFamily.getCoupons() > template.getCoupons()) {
                    smallestTemplateInFamily = template;
                }
            }
            for (long account : businessAccounts) {
                businessAccountToFamilyToSmallestTemplate.put(account, familyName, smallestTemplateInFamily);
            }
        }
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            reservedInstance.normalizeTemplate(businessAccountToFamilyToSmallestTemplate.row(reservedInstance.getBusinessAccount()));
        }

        Map<SMAReservedInstanceKey, List<SMAReservedInstance>> distinctRIs = new HashMap<>();

        for (SMAReservedInstance ri : reservedInstances) {
            SMAReservedInstanceKey riContext = new SMAReservedInstanceKey(ri);
            List<SMAReservedInstance> instances = distinctRIs.get(riContext);
            if (instances == null) {
                distinctRIs.put(riContext, new ArrayList<>(Arrays.asList(ri)));

            } else {
                instances.add(ri);
            }
        }
        reservedInstances.clear();
        for (List<SMAReservedInstance> members : distinctRIs.values()) {
            if (members == null || members.size() < 1) {
                // this is an error. Can be handled as an error if required.
                continue;
            }
            Collections.sort(members, new SortByRIOID());
            SMAReservedInstance representative = members.get(0);
            float representativeTotalCount = 0;
            for (SMAReservedInstance ri : members) {
                representativeTotalCount = representativeTotalCount + ri.getTotalCount();
                ri.setTotalCount(0);
            }
            representative.setTotalCount(representativeTotalCount);
            reservedInstances.add(representative);
        }
    }

    /**
     * Create the engagement for the group members.
     *
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @param currentEngagements     the current engagements which has only the leader engagement.
     * @return matches for the group members
     */
    public static List<SMAMatch> addGroupMemberEngagement(
            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
            Map<SMAVirtualMachine, SMAMatch> currentEngagements) {
        /*
         * the natural template of all group members is updated with the leader natural template.
         * (only if the group has atleast 1 common provider)
         * Even if the leader does not have a matching the group members will all move to the natural
         * template.
         * For the groups which has a engagement we split the coupons among the members..We allocate
         * the coupons among the members in order. In order to be consistent
         * smaVirtualMachineGroup.getVirtualMachines() is already sorted by name. if there are
         * 10 coupons and 3 VMs each requiring 4 coupons one of the VM will be 50% discounted.
         */
        List<SMAMatch> matches = new ArrayList<>();
        for (SMAVirtualMachineGroup smaVirtualMachineGroup : virtualMachineGroupMap.values()) {
            SMAVirtualMachine groupLeader = smaVirtualMachineGroup.getGroupLeader();
            SMAMatch leaderEngagement = currentEngagements.get(groupLeader);
            int totalGroupCouponsRemaining = 0;
            int groupMemberRequiredCoupons = 0;
            if (leaderEngagement != null) {
                totalGroupCouponsRemaining = leaderEngagement.getDiscountedCoupons();
                groupMemberRequiredCoupons = leaderEngagement.getTemplate().getCoupons();
            }
            for (SMAVirtualMachine groupMember : smaVirtualMachineGroup.getVirtualMachines()) {
                /*
                 * if there is an engagement create engagement for all the group members.
                 */
                if (leaderEngagement != null) {
                    if (totalGroupCouponsRemaining > 0) {
                        int couponsAllocated = Math.min(groupMemberRequiredCoupons, totalGroupCouponsRemaining);
                        if (groupMember != groupLeader) {
                            matches.add(new SMAMatch(groupMember, leaderEngagement.getTemplate(),
                                    leaderEngagement.getReservedInstance(),
                                    couponsAllocated));
                        } else {
                            leaderEngagement.setDiscountedCoupons(
                                    couponsAllocated);
                        }
                        totalGroupCouponsRemaining = totalGroupCouponsRemaining -
                                couponsAllocated;
                    } else {
                        // there will be a match created later with no RI associated.
                        groupMember.setNaturalTemplate(leaderEngagement.getTemplate());
                    }
                }
            }
        }
        return matches;
    }

    /**
     * Create for each RI the list of VMs it can discount.
     *
     * @param virtualMachines        list of virtual machines
     * @param remainingCoupons       remaining coupons for each RI
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @param familyNameToTemplates  map from family name to list of SMATemplates
     * @param statistics the statistics of the SMA
     */
    public static void
    createRIToVMsMap(List<SMAVirtualMachine> virtualMachines,
                     Map<SMAReservedInstance, Integer> remainingCoupons,
                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                     Map<String, List<SMATemplate>> familyNameToTemplates,
                     SMAStatistics statistics) {

        final Stopwatch stopWatch = Stopwatch.createStarted();

        // Multimap from template to the VMs that has the template as one of its provider.
        Map<SMATemplate, List<SMAVirtualMachine>> templateToVmsMap = new HashMap<>();
        for (SMAVirtualMachine vm : virtualMachines) {
            if (vm.getGroupSize() >= 1) {
                // don't add VM is in ASG and not the leader
                for (SMATemplate template : vm.getGroupProviders()) {
                    List<SMAVirtualMachine> instances = templateToVmsMap.get(template);
                    if (instances == null) {
                        templateToVmsMap.put(template, new ArrayList<>(Arrays.asList(vm)));
                    } else {
                        instances.add(vm);
                    }
                }
            }
        }

        // The RI can discount a VM if the template of RI is one of the provider for VM.
        for (SMAReservedInstance ri : remainingCoupons.keySet()) {
            List<SMAVirtualMachine> virtualMachineList = new ArrayList<>();
            if (ri.isIsf()) {
                // all vms that has a provider from the ri template family.
                HashSet<SMAVirtualMachine> vmsWithProviderInFamily = new HashSet<>();
                List<SMATemplate> familyTemplates = familyNameToTemplates.get(ri.getNormalizedTemplate().getFamily());
                for (SMATemplate template : familyTemplates) {
                    List<SMAVirtualMachine> vmList = templateToVmsMap.get(template);
                    if (vmList != null) {
                        vmsWithProviderInFamily.addAll(templateToVmsMap.get(template));
                    }
                }
                virtualMachineList.addAll(vmsWithProviderInFamily);
            } else {
                // TODO vm.zonecompatible()
                // TODO create a interface for both VM and VMGroup
                List<SMAVirtualMachine> vmList = templateToVmsMap.get(ri.getNormalizedTemplate());
                if (vmList != null) {
                    virtualMachineList.addAll(vmList
                            .stream().filter(vm -> vm.zoneCompatible(ri, virtualMachineGroupMap))
                            .collect(Collectors.toList()));
                }
            }
            // TODO move attribute remaining coupons to SMAReservedInstance
            sortAndUpdateVMList(virtualMachineList, remainingCoupons.get(ri), ri, virtualMachineGroupMap);
        }

        long timeInMilliseconds = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        logger.debug("stableMarriage sorting took {} ms.", timeInMilliseconds);
        statistics.setSortTime(timeInMilliseconds);

    }

    /**
     * sort the virtual machines that can be discounted by a reserved instance.
     * Update the RI's couponToBestVM and discountableVMs fields.
     *
     * @param virtualMachineList     list of virtual machines
     * @param riCoupons              remaining coupons for each RI
     * @param ri                     the reserved instance
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     */
    private static void sortAndUpdateVMList(List<SMAVirtualMachine> virtualMachineList,
                                            Integer riCoupons, SMAReservedInstance ri,
                                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        Collections.sort(virtualMachineList, new RIPreference(riCoupons, ri, virtualMachineGroupMap));
        for (int i = 0; i < virtualMachineList.size(); i++) {
            SMAVirtualMachine vm = virtualMachineList.get(i);
            int coupons = ri.getTemplate().getCoupons() * vm.getGroupSize();
            if (ri.isIsf()) {
                coupons = vm.getMinCostProviderPerFamily(ri.getTemplate().getFamily()).getCoupons() * vm.getGroupSize();
            }
            ri.addVMToCouponToBestVM(coupons, i, vm);
        }
    }

    /**
     * create the virtualMachineGroup for the list of virtual machines and map it to the group oid.
     *
     * @param virtualMachines the list of virtual machines.
     * @return map from group oid to the newly created virtualMachineGroups
     */
    public static Map<String, SMAVirtualMachineGroup> createVirtualMachineGroupMap(
            List<SMAVirtualMachine> virtualMachines) {
        Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap = new HashMap<>();
        // collect all the vms for each ASG to create a smaVirtualMachineGroup
        Map<String, List<SMAVirtualMachine>> groupNameToVirtualMachineList = new HashMap<>();
        for (SMAVirtualMachine vm : virtualMachines) {
            if (!vm.getGroupName().equals(SMAUtils.NO_GROUP_ID)) {
                if (!groupNameToVirtualMachineList.containsKey(vm.getGroupName())) {
                    List<SMAVirtualMachine> smaVirtualMachineListForGroup = new ArrayList<>();
                    groupNameToVirtualMachineList.put(vm.getGroupName(), smaVirtualMachineListForGroup);
                }
                groupNameToVirtualMachineList.get(vm.getGroupName()).add(vm);
            }
        }
        for (Map.Entry<String, List<SMAVirtualMachine>> entry : groupNameToVirtualMachineList.entrySet()) {
            String groupName = entry.getKey();
            SMAVirtualMachineGroup smaVirtualMachineGroup = new SMAVirtualMachineGroup(groupName, entry.getValue());
            virtualMachineGroupMap.put(groupName, smaVirtualMachineGroup);
        }
        return virtualMachineGroupMap;
    }


    /**
     * Given an VM and the VM's profile, determine which SMAMatch the VM prefers.
     * At this point we have made sure that both the SMAMatches have same coverage.
     * So no cost saving is involved.
     * Preferences
     * 1) Zonal preference: if newRI is zonal and oldRI is regional, then true newRI
     * 2) Minimize moves:
     * a)  by profile: if profile(newRI) == consumptionProfile(VM) && profile(oldRI) != consumptionProfile(VM) then newRI
     * b)  by family: if family(newRI) == consumptionFamily(VM) && family(oldRI) != consumptionFamily(VM) then newRI
     * c)  by size: if abs(size(newRI) - size(allocated(VM)) < abs(size(oldRI) - size(allocated(VM)) then newRI
     * 3) Account preference: account(VM) == account(newRI) && account(VM) != account(oldRI), then true newRI
     * 4) Bill preference: prefer to stay on RI specified in the bill
     * 5) Name of RI
     *
     * @param vm                     virtual machine considering for engagement
     * @param oldEngagement          engagement the VM is already engaged to
     * @param newEngagement          current engagement considering to be engaged by VM
     * @return true if newEngagement, else false, that is, VM stays engaged to oldEngagement
     */
    protected static boolean preference(SMAVirtualMachine vm,
                                        SMAMatch oldEngagement,
                                        SMAMatch newEngagement) {
        try {
            Objects.requireNonNull(vm, "VM is null");
            Objects.requireNonNull(oldEngagement, "oldEngagement is null");
            Objects.requireNonNull(newEngagement, "newEngagement is null");
            logger.debug("preference(vm={}, profile={}, newRI={}, oldRI={})", vm.getName(),
                    oldEngagement.toString(), newEngagement.toString());

            SMAReservedInstance newRI = newEngagement.getReservedInstance();
            SMAReservedInstance oldRI = oldEngagement.getReservedInstance();

            float discountNewRI = newRI.getRICoverage(vm);
            float discountOldRI = oldRI.getRICoverage(vm);
            // if vm is already covered by one of the RI prefer that RI

            if (discountNewRI - discountOldRI > SMAUtils.EPSILON) {
                return true;
            }
            if (discountOldRI - discountNewRI > SMAUtils.EPSILON) {
                return false;
            }
            // Zonal preference
            // Beyond this point, both the RIs are Regional or both are Zonal.
            final Boolean zonalPreference = zonalPreference(newRI, oldRI);
            if (zonalPreference != null) {
                return zonalPreference;
            }

            // Last resort break tie with oid. t3a.large and t3.large happens
            // to have same on-demand cost too..
            return (oldRI.getOid() - newRI.getOid() > 0);

        } catch (Exception e) {
            logger.error("*** preference(vm={}, profile={}, newRI={}, oldRI={}) e={}",
                    (vm == null ? "null" : vm.getName()),
                    (vm == null || vm.getCurrentTemplate() == null ? "null" : vm.getCurrentTemplate()),
                    (newEngagement == null ? "null" : newEngagement),
                    (oldEngagement == null ? "null" : newEngagement),
                    e.getMessage());
        }
        logger.debug("preference() return false, no engagement is swapped");
        return false;
    }

    /**
     * this method is used to identify and prefer zonal ris as they are more restrictive.
     *
     * @param newRI new RI
     * @param oldRI old RI
     * @return true if newRI is zonal and oldRI is not, false if oldRI is zonal and newRI is not, null otherwise
     */
    private static Boolean zonalPreference(final SMAReservedInstance newRI,
                                           final SMAReservedInstance oldRI) {
        logger.debug("preference() zonal preference: newRI.isRegionScoped()={} " +
                "oldRI.isRegionScoped()={}", newRI.isRegionScoped(), oldRI.isRegionScoped());
        if (!newRI.isRegionScoped() && oldRI.isRegionScoped()) {
            return true;
        } else if (newRI.isRegionScoped() && !oldRI.isRegionScoped()) {
            return false;
        }
        return null;
    }

    /**
     * Prefer oldEngagement if costImprovement is negative.
     * Prefer newEngagement if costImprovement is positive.
     * Prefer newEngagement if costImprovement is 0 and the preference returns true.
     *
     * @param oldEngagement          the engagement of the group leader. null if currently not engaged.
     * @param newEngagement          current engagement considering to be engaged by VM
     * @return true if newEngagement is better, else false, that is, VM stays engaged to oldEngagement
     */
    public static boolean isCurrentRIBetterThanOldRI(SMAMatch oldEngagement,
                                                     SMAMatch newEngagement) {
        SMAVirtualMachine virtualMachine = newEngagement.getVirtualMachine();
        float costImprovement = costImprovement(virtualMachine.getBusinessAccount(),
                (float)newEngagement.getDiscountedCoupons() / (float)virtualMachine.getGroupSize(),
                newEngagement.getTemplate(),
                (oldEngagement == null) ?
                        0 : (float)oldEngagement.getDiscountedCoupons() / (float)virtualMachine.getGroupSize(),
                (oldEngagement == null) ?
                        virtualMachine.getNaturalTemplate() : oldEngagement.getTemplate());
        if (oldEngagement == null && costImprovement <
                (0.1 * virtualMachine.getGroupSize() *
                    virtualMachine.getNaturalTemplate().getOnDemandTotalCost(virtualMachine.getBusinessAccount()))) {
            return false;
        }
        if (costImprovement < -1.0 * SMAUtils.EPSILON) {
            return false;
        } else if (costImprovement > SMAUtils.EPSILON) {
            return true;
        } else {
            if (oldEngagement == null) {
                return false;
            } else {
                return preference(virtualMachine, oldEngagement, newEngagement);
            }
        }
    }

    /**
     * Run stable marriage iteration and generate engagements.
     *
     * @param freeRIs                the RI queue the algorithm uses to propose
     * @param remainingCoupons       map to keep track of coupons remaining for each RI
     * @param currentEngagements     map from vm to its current engagement. This is the map that gets
     *                               populated and acts as a return parameter.
     * @param virtualMachineGroupMap map from group name to virtual machines in that group
     * @param statistics             datastructure used to maintain statistics
     * @param includePartialCoverage allow partial coverage on when this is true.
     */

    public static void runIterations(Deque<SMAReservedInstance> freeRIs,
                                     Map<SMAReservedInstance, Integer> remainingCoupons,
                                     Map<SMAVirtualMachine, SMAMatch> currentEngagements,
                                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                     SMAStatistics statistics, boolean includePartialCoverage) {
        while (!freeRIs.isEmpty()) {
            statistics.incrementIterations();
            SMAReservedInstance currentRI = freeRIs.poll();
            Integer currentRICoupons = (currentRI == null) ? 0 : remainingCoupons.get(currentRI);

            // if the last matched VM was only partially filled allocated the coupons to that VM first.
            SMAVirtualMachine lastDiscountedVM = currentRI.getLastDiscountedVM();
            if (lastDiscountedVM != null && currentRICoupons > 0) {
                SMAMatch lastMatch = currentEngagements.get(lastDiscountedVM);
                if (lastMatch != null && lastMatch.getReservedInstance() == currentRI) {
                    int totalCouponsReq = lastMatch.getTemplate().getCoupons() * lastDiscountedVM.getGroupSize();
                    int additionalCouponReq = totalCouponsReq - lastMatch.getDiscountedCoupons();
                    int allocatableCoupons = Math.min(currentRICoupons, additionalCouponReq);
                    if (allocatableCoupons > 0) {
                        currentRICoupons = currentRICoupons - allocatableCoupons;
                        remainingCoupons.put(currentRI, currentRICoupons);
                        lastMatch.setDiscountedCoupons(lastMatch.getDiscountedCoupons() + allocatableCoupons);
                    }
                }
            }
            if (currentRICoupons > 0 && !currentRI.isCouponToBestVMEmpty()) {
                logger.debug("stableMarriage new RI={} remainingCoupons={}", currentRI.getName(),
                        currentRICoupons);
                SMAVirtualMachine currentVM = currentRI.findBestVMIndexFromCouponToBestVM(currentRICoupons, includePartialCoverage);
                if (currentVM == null) {
                    continue;
                }
                int groupSize = currentVM.getGroupSize();

                /*
                 * the destination template is the template the vm will move to if the RI is matched
                 * to this VM. In case of non ISF it will be the Ri template.
                 * For ISF it will be the cheapest provider of the vm that belong to the RI family.
                 */
                SMATemplate destinationTemplate = currentRI.getNormalizedTemplate();
                if (currentRI.isIsf()) {
                    destinationTemplate = currentVM.getMinCostProviderPerFamily(currentRI.getNormalizedTemplate().getFamily());
                    if (destinationTemplate == null) {
                        logger.error("stableMarriage destinationTemplate can't be null " +
                                        "currentRI={} currentVM={} riTemplate={}", currentRI, currentVM,
                                currentRI.getNormalizedTemplate());
                        continue;
                    }
                }
                // the number of coupons the VM will require if it has to be fully discounted.
                Integer currentVMCouponRequest = destinationTemplate.getCoupons() * groupSize;
                if (currentVMCouponRequest == 0) {
                    logger.error("stableMarriage currentVMCouponRequest can't be zero " +
                            "destinationTemplate={} groupSize={} ", destinationTemplate, groupSize);
                    continue;
                }

                Integer discountedCoupons = Math.min(currentVMCouponRequest,
                        currentRICoupons);
                SMAMatch oldEngagement = currentEngagements.get(currentVM);
                /* discounted coupons are the actual coupons used in this engagement.
                 * it will be the minimum of what is required and what is available.
                 */
                if (!freeRIs.contains(currentRI)) {
                    freeRIs.addFirst(currentRI);
                }
                currentRI.removeVMFromCouponToBestVM(currentVMCouponRequest);
                SMAMatch newEngagement = new SMAMatch(currentVM, destinationTemplate,
                        currentRI, discountedCoupons);
                Boolean isCurrentRIBetter = isCurrentRIBetterThanOldRI(oldEngagement,
                        newEngagement);
                statistics.incrementPreferenceCalls();
                if (isCurrentRIBetter) {
                    if (oldEngagement == null) {
                        statistics.incrementNewEngagements();
                        logger.debug("stableMarriage new engagement: " +
                                        "VM={} pair(RI={}, coupons={})",
                                currentVM.getName(), currentRI.getName(), discountedCoupons);
                        currentEngagements.put(currentVM, newEngagement);
                        currentRI.setLastDiscountedVM(currentVM);
                        // update remaining coupons
                        int currentRIRemainingCoupons = currentRICoupons - discountedCoupons;
                        remainingCoupons.put(currentRI, currentRIRemainingCoupons);
                    } else {
                        // engagement already exists. swap engagement
                        SMAReservedInstance oldRI = oldEngagement.getReservedInstance();
                        statistics.incrementSwaps();
                        // break engagement;
                        Integer oldEngagementCoupons = oldEngagement.getDiscountedCoupons();
                        logger.debug("stableMarriage swap: " +
                                        "VM={} pair(RI={}, coupons={}) oldRI={}",
                                currentVM.getName(), currentRI.getName(), discountedCoupons,
                                oldRI.getName());
                        currentEngagements.remove(currentVM);
                        currentEngagements.put(currentVM, newEngagement);
                        // update remaining coupons of oldRI and currentRI
                        int currentLeftOverCoupons = currentRICoupons - discountedCoupons;
                        remainingCoupons.put(currentRI, currentLeftOverCoupons);
                        remainingCoupons.put(oldRI, remainingCoupons.get(oldRI) + oldEngagementCoupons);
                        currentRI.setLastDiscountedVM(currentVM);
                        if (!freeRIs.contains(oldRI)) {
                            freeRIs.add(oldRI);
                        }
                    }
                }

            }
        }
    }


    /**
     * compare the net cost based on available coupons, the template On-demand, discounted cost.
     *
     * @param businessAccountId  business account to get rates for.
     * @param currentCoupons  current available coupons
     * @param currentTemplate the current template
     * @param oldCoupons      old available coupons
     * @param oldTemplate     the old template
     * @return the effective savings of resize from oldTemplate to currentTemplate
     */

    public static float costImprovement(long businessAccountId, float currentCoupons, SMATemplate currentTemplate,
                                        float oldCoupons, SMATemplate oldTemplate) {
        return (oldTemplate.getNetCost(businessAccountId, oldCoupons) -
            currentTemplate.getNetCost(businessAccountId, currentCoupons));
    }


    /**
     * Given a list of SMATemplates partition them into a map by family.
     *
     * @param templates list of SMATemplates
     * @return map from family to list of SMATemplates in that family
     */
    public static Map<String, List<SMATemplate>> computeFamilyToTemplateMap(List<SMATemplate> templates) {
        Map<String, List<SMATemplate>> map = new HashMap<>();
        // compute list of families
        Set<String> families = templates.stream().map(SMATemplate::getFamily).collect(Collectors.toSet());
        for (String family : families) {
            List<SMATemplate> list = templates.stream().filter(t -> t.getFamily().contentEquals(family))
                    .collect(Collectors.toList());
            map.put(family, list);
        }
        return map;
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
     *  Comparator used to sort the list of VMs that can be discounted by a reserved instance.
     */
    public static class RIPreference implements Comparator<SMAVirtualMachine> {
        // number of coupons remaining for the reservedInstance
        private int coupons;
        // the reserved instance of interest.
        private SMAReservedInstance reservedInstance;
        // map containing ASG information.
        private Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap;

        /**
         * Constuctor for the comparator.
         * @param coupons number of coupons remaining for the reservedInstance
         * @param reservedInstance the reserved instance of interest.
         * @param virtualMachineGroupMap map containing ASG information.
         */
        public RIPreference(int coupons, SMAReservedInstance reservedInstance,
                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
            this.coupons = coupons;
            this.reservedInstance = reservedInstance;
            this.virtualMachineGroupMap = virtualMachineGroupMap;
        }

        /**
         * compare vm1 and vm2 and determine reservedInstance prefers vm1 or vm2.
         * @param vm1 first vm
         * @param vm2 second vm
         * @return -1 if reservedInstance prefers vm1. return 1 if reservedInstance prefers vm2.
         */
        @Override
        public int compare(SMAVirtualMachine vm1, SMAVirtualMachine vm2) {
            float netSavingVm1 = computeSaving(vm1);
            float netSavingVm2 = computeSaving(vm2);

            String riFamily = reservedInstance.getNormalizedTemplate().getFamily();
            float couponsVm1 = (float)reservedInstance.getNormalizedTemplate().getCoupons() * vm1.getGroupSize();
            float couponsVm2 = (float)reservedInstance.getNormalizedTemplate().getCoupons() * vm2.getGroupSize();
            if (reservedInstance.isIsf()) {
                couponsVm1 = (float)Math.min(coupons, vm1.getMinCostProviderPerFamily(riFamily).getCoupons() * vm1.getGroupSize());
                couponsVm2 = (float)Math.min(coupons, vm2.getMinCostProviderPerFamily(riFamily).getCoupons() * vm2.getGroupSize());
            }
            float netSavingVm1PerCoupon = netSavingVm1 / couponsVm1;
            float netSavingVm2PerCoupon = netSavingVm2 / couponsVm2;

            netSavingVm1PerCoupon = (float)Math.round(netSavingVm1PerCoupon *
                    SMAUtils.ROUNDING) / SMAUtils.ROUNDING;
            netSavingVm2PerCoupon = (float)Math.round(netSavingVm2PerCoupon *
                    SMAUtils.ROUNDING) / SMAUtils.ROUNDING;

            // Pick VM with higher savings per coupon.
            if ((netSavingVm1PerCoupon - netSavingVm2PerCoupon) > SMAUtils.EPSILON) {
                return -1;
            }
            if ((netSavingVm2PerCoupon - netSavingVm1PerCoupon) > SMAUtils.EPSILON) {
                return 1;
            }

            if ((couponsVm1 - couponsVm2) > SMAUtils.EPSILON) {
                return -1;
            }
            if ((couponsVm2 - couponsVm1) > SMAUtils.EPSILON) {
                return 1;
            }

            // pick VM with higher RI coverage.
            float riCoverageVm1 = reservedInstance.getRICoverage(vm1);
            float riCoverageVm2 = reservedInstance.getRICoverage(vm2);
            // if vm is already covered by one of the RI prefer that RI

            if ((riCoverageVm1 - riCoverageVm2) > SMAUtils.EPSILON) {
                return -1;
            }
            if ((riCoverageVm2 - riCoverageVm1) > SMAUtils.EPSILON) {
                return 1;
            }

            // pick vm with lesser moves
            if (!reservedInstance.isIsf()) {
                int vm1TemplateMoves = templateMoves(vm1, reservedInstance.getNormalizedTemplate());
                int vm2TemplateMoves = templateMoves(vm2, reservedInstance.getNormalizedTemplate());
                if (vm1TemplateMoves < vm2TemplateMoves) {
                    return -1;
                } else if (vm1TemplateMoves > vm2TemplateMoves) {
                    return 1;
            }
            } else {
                int vm1TemplateMoves = templateMoves(vm1, vm1.getMinCostProviderPerFamily(riFamily));
                int vm2TemplateMoves = templateMoves(vm2, vm2.getMinCostProviderPerFamily(riFamily));
                if (vm1TemplateMoves < vm2TemplateMoves) {
                    return -1;
                } else if (vm1TemplateMoves > vm2TemplateMoves) {
                    return 1;
                }
            }
            // pick vm in the same family
            int vm1FamilyMoves = familyMoves(vm1, riFamily);
            int vm2FamilyMoves = familyMoves(vm2, riFamily);
            if (vm1FamilyMoves < vm2FamilyMoves) {
                return -1;
            } else if (vm1FamilyMoves > vm2FamilyMoves) {
                return 1;
            }


            //return breakTie(vm1, vm2);
            // We could break tie yet. Last resort is to use oid.
            if (vm1.getOid() - vm2.getOid() > 0) {
                return -1;
            } else {
                return 1;
            }
        }

        /**
         * the number of moves required for the vm/vmGroup to change to the new template.
         *
         * @param vm                  the VM of interest
         * @param template            new  template
         * @return move count
         */
        int templateMoves(final SMAVirtualMachine vm,
                                         final SMATemplate template) {
            int moves = 0;
            if (vm.getGroupSize() > 1) {
                for (SMAVirtualMachine member : virtualMachineGroupMap
                        .get(vm.getGroupName()).getVirtualMachines()) {
                    moves += member.getCurrentTemplate().equals(template) ? 0 : 1;
                }
                return moves;
            } else {
                return vm.getCurrentTemplate().equals(template) ? 0 : 1;
            }
        }


        /**
         * the number of family change for the vm/vmGroup to change to the new template.
         *
         * @param vm                  the VM of interest
         * @param family            new  template family
         * @return move count
         */
        int familyMoves(final SMAVirtualMachine vm,
                                       final String family) {
            int moves = 0;
            if (vm.getGroupSize() > 1) {
                for (SMAVirtualMachine member : virtualMachineGroupMap
                        .get(vm.getGroupName()).getVirtualMachines()) {
                    moves += member.getCurrentTemplate().getFamily().equals(family) ? 0 : 1;
                }
                return moves;
            } else {
                return vm.getCurrentTemplate().getFamily().equals(family) ? 0 : 1;
            }
        }

        /**
         * Compute the savings obtained by matching the virtual machine with reservedInstance.
         *
         * @param vm virtual machine of interest.
         * @return  the savings obtained by matching the virtual machine with reservedInstance.
         */
        float computeSaving(SMAVirtualMachine vm) {
            List<SMAVirtualMachine> vmList = new ArrayList(Arrays.asList(vm));
            if (vm.getGroupSize() > 1) {
                vmList = virtualMachineGroupMap.get(vm.getGroupName()).getVirtualMachines();
            }
            SMATemplate riTemplate = reservedInstance.getNormalizedTemplate();
            float netSavingvm = 0;
            for (SMAVirtualMachine member : vmList) {
                float onDemandCostvm = member.getNaturalTemplate().getOnDemandTotalCost(vm.getBusinessAccount());
                /*  ISF : VM with t3.large and a ISF RI in t2 family. VM can move to t2.large.
                 *  t2.large need 10 coupons. But we have only 6 coupons.
                 *  riTemplate.getFamily() returns t2.
                 *  getMinCostProviderPerFamily().get("t2") returns t2.large
                 *  netCost will be ondemandCost * .4 + discountedCost 0.6 of the t2.large template.
                 *  Note that for AWS the discountedCost will be 0;
                 */
                /*  Non-ISF VM with t3.large and a RI is t2.large. VM has to move to t2.large.
                 *  t2.large need 10 coupons.
                 *  riTemplate returns t2.large.
                 *  afterMoveCost will be ondemandCost * 0 + discountedCost * 1 of the t2.large template.
                 *  Note that for AWS the discountedCost will be 0; So afterMoveCost will be 0;
                 */
                float afterMoveCostvm = reservedInstance.isIsf() ?
                        member.getMinCostProviderPerFamily(riTemplate.getFamily()).getNetCost(vm.getBusinessAccount(),
                            (float)coupons / vm.getGroupSize()) :
                        riTemplate.getNetCost(vm.getBusinessAccount(), (float)coupons / vm.getGroupSize());

                netSavingvm += (onDemandCostvm - afterMoveCostvm);
            }
            return netSavingvm;

        }
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
            this.zone = ri.getZone();
            this.businessAccount = ri.getBusinessAccount();
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
}
