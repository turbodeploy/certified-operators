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
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine.CostContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachineGroup;

/**
 * Given a context, run SMA, and generate the SMAOutputContext.
 */
public class StableMarriagePerContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Run the StableMarriage algorithm until it converges.
     * So we might have to run the algorithm multiple times for it to converge.
     * @param inputContext the input of SMA partitioned on a per context basis.
     * @return the matching in the inputContext.
     */
    public static SMAOutputContext execute(final SMAInputContext inputContext) {
        // Set the scaleUp boolean for virtual machines. This has to be done before we start
        // iterations because the current template will be updated after that and it will
        // mess up this calculation.

        for (SMAVirtualMachine vm : inputContext.getVirtualMachines()) {
            vm.setScaleUp(vm.getProviders() != null
                    && !vm.getProviders().stream()
                    .anyMatch(a -> a.getOid() == vm.getCurrentTemplate().getOid()));
        }
        long mismatch = 1;
        int iterations = 0;
        SMAInputContext modifiableInputContext = new SMAInputContext(inputContext);
        SMAOutputContext outputContext = null;
        while (mismatch > 0 && iterations < SMAUtils.MAX_ITERATIONS) {
            outputContext = executeOnce(modifiableInputContext);
            mismatch = computeMismatch(outputContext, modifiableInputContext);
            modifiableInputContext = new SMAInputContext(modifiableInputContext, outputContext);
            iterations++;
        }
        updateOutputWithActualVirtualMachines(outputContext, inputContext);
        return outputContext;
    }

    /**
     * Update the vm's in the outPutContext with the actual vms.
     * @param outputContext the output context to be modified
     * @param inputContext the unmodified inout context
     */
    public static void updateOutputWithActualVirtualMachines(SMAOutputContext outputContext,
                                                             SMAInputContext inputContext) {
        Collections.sort(inputContext.getVirtualMachines(), new SortVmByOID());
        Collections.sort(outputContext.getMatches(), new SortMatchesByVMOID());
        for (int i = 0; i < outputContext.getMatches().size(); i++) {
            outputContext.getMatches().get(i)
                    .setVirtualMachine(inputContext.getVirtualMachines().get(i));
        }
    }

    /**
     * Calculate the number of mismatch between the outputContext and inputContext. mismatch occurs if
     * template or RI coverage change.
     * @param outputContext the output context
     * @param inputContext the input context
     * @return the number of VMs whose template or RI coverage changed during the SMA.
     */
    public static long computeMismatch(SMAOutputContext outputContext, SMAInputContext inputContext) {
        long mismatch = 0;
        for (SMAMatch match : outputContext.getMatches()) {
            SMAVirtualMachine vm = match.getVirtualMachine();
            SMATemplate currentTemplate = vm.getCurrentTemplate();
            SMATemplate matchTemplate = match.getTemplate();
            if (currentTemplate.getOid() != matchTemplate.getOid()
                    || (Math.abs(vm.getCurrentRICoverage()
                    - match.getDiscountedCoupons()) > SMAUtils.EPSILON)) {
                mismatch++;
            }
        }
        return mismatch;
    }

    /**
     * Given a inputContext, run SMA.
     *
     * @param inputContext the input of SMA partitioned on a per context basis.
     * @return the generated SMAOutputContext .
     */
    public static SMAOutputContext executeOnce(SMAInputContext inputContext) {
        final List<SMAVirtualMachine> virtualMachines = inputContext.getVirtualMachines();
        /*
         * Map from the group name to the virtual machine groups (auto scaling group)
         */
        Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap =
                createVirtualMachineGroupMap(virtualMachines);
        /*
         *  List of templates; that is, providers
         */
        final List<SMATemplate> templates = inputContext.getTemplates();
        /*
         * Precompute map from family name to list of SMATemplates
         */
        Map<String, List<SMATemplate>> familyNameToTemplates = computeFamilyToTemplateMap(templates);
        /*
         *  List of reserved instances.  There may be no RIs.
         */
        final List<SMAReservedInstance> reservedInstances = (inputContext.getReservedInstances() == null ?
            Collections.EMPTY_LIST : new ArrayList<>(inputContext.getReservedInstances()));
        /*
         * If instance size flexible (ISF), move all RIs in a
         * family to the smallest instance type in that family.
         * For ISF and non-ISF: update count and coverage appropriately.
         * Two RIs can be combined if their RI template, and zone match.
         * We already scale the isf to the cheapest template. so we can safely compare template.
         */
        normalizeReservedInstances(reservedInstances, familyNameToTemplates);
        /*
         * Sort RIs based on OID. This is to create consistency.
         */
        Collections.sort(reservedInstances, new SortByRIOID());
        final SMAContext context = inputContext.getContext();
        /*
         * Update RI Coverage for groups.
         */
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            for (SMAVirtualMachineGroup group : virtualMachineGroupMap.values()) {
                reservedInstance.updateRICoveragePerGroup(group);
            }
        }
        /*
         * Queue that keeps track of all the RI's that are not engaged.
         */
        Deque<SMAReservedInstance> freeRIs = new LinkedList<>();
        /*
         * Map that keeps track of all the unused coupons for RIs.
         */
        Map<SMAReservedInstance, Float> remainingCoupons = new HashMap<>();
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            float coupons = (reservedInstance.getNormalizedTemplate().getCoupons()
                    * reservedInstance.getNormalizedCount());
            if (coupons > SMAUtils.EPSILON) {
                remainingCoupons.put(reservedInstance, coupons);
                if (!freeRIs.contains(reservedInstance)) {
                    freeRIs.add(reservedInstance);
                }
            }
        }
        /*
         * build map from  to list of VMs that can move to that RI.
         * the discountableVMsPartitionedByCoupon attribute is updated for each RI.
        */
        createRIToVMsMap(virtualMachines,
                remainingCoupons, virtualMachineGroupMap,
                familyNameToTemplates,
                inputContext.getSmaConfig().isReduceDependency());

        // map to keep track of the successful engagements so far.
        Map<SMAVirtualMachine, SMAMatch> currentEngagements = new HashMap<>();

        /*
         * This is the main function in SMA. This is where RIs get matched to VMs.
         */
        runIterations(freeRIs, remainingCoupons,
                currentEngagements, virtualMachineGroupMap,
                inputContext.getSmaConfig().isReduceDependency());

        /*
         * For all VMs, compute a SMAMatch
         */
        List<SMAMatch> matches = new ArrayList<>();
        matches.addAll(currentEngagements.values());
        // Add the SMAMatch for ASG group members.
        matches.addAll(addGroupMemberEngagement(virtualMachineGroupMap, currentEngagements));

        // for all the VMs that does not have a matching move them to the natural template.
        Set<SMAVirtualMachine> set = matches.stream().map(a -> a.getVirtualMachine()).collect(Collectors.toSet());
        List<SMAVirtualMachine> virtualMachinesWithoutMatching = virtualMachines.stream().filter(vm -> !set.contains(vm)).collect(Collectors.toList());
        for (SMAVirtualMachine smaVirtualMachine : virtualMachinesWithoutMatching) {
            matches.add(new SMAMatch(smaVirtualMachine, smaVirtualMachine.getNaturalTemplate(),
                    null, 0));
        }

        // generate the SMAOutputContext
        SMAOutputContext outputContext = new SMAOutputContext(context, matches);
        return outputContext;
    }

    /**
     * Given two SMAMatch, compare by virtual machine oid.
     */
    public static class SortMatchesByVMOID implements Comparator<SMAMatch> {
        @Override
        public int compare(SMAMatch match1, SMAMatch match2) {
            return (match1.getVirtualMachine().getOid() - match2.getVirtualMachine().getOid() > 0) ? 1 : -1;
        }
    }

    /**
     * Given two VirtualMachines, compare by virtual machine oid.
     */
    public static class SortVmByOID implements Comparator<SMAVirtualMachine> {
        @Override
        public int compare(SMAVirtualMachine vm1, SMAVirtualMachine vm2) {
            return (vm1.getOid() - vm2.getOid() > 0) ? 1 : -1;
        }
    }

    /**
     * ISF RIs are first scaled down to the template in the family with the fewest coupons.
     * We combine RI's that are same (businessAccount, normalizedTemplate zone).
     * We pick a representative that will go to SMA for all.
     * The other RIs are captured in the representative's members field.
     * Market component will take care of redistributing the coupons.
     *
     * @param reservedInstances the reserved instances to normalize. This is used to store output too.
     * @param familyNameToTemplates map from family name to the templates
     */
    public static void normalizeReservedInstances(List<SMAReservedInstance> reservedInstances,
                                                  Map<String, List<SMATemplate>> familyNameToTemplates) {
        Map<String, SMATemplate> familyNameToSmallestTemplate = new HashMap<>();
        for (Map.Entry<String, List<SMATemplate>> entry : familyNameToTemplates.entrySet()) {
            String familyName = entry.getKey();
            List<SMATemplate> templatesInFamily = entry.getValue();
            SMATemplate smallestTemplateInFamily = templatesInFamily.get(0);
            for (SMATemplate template : templatesInFamily) {
                if (smallestTemplateInFamily.getCoupons() > template.getCoupons() + SMAUtils.BIG_EPSILON) {
                    smallestTemplateInFamily = template;
                }
            }
            familyNameToSmallestTemplate.put(familyName, smallestTemplateInFamily);

        }
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            reservedInstance.normalizeTemplate(familyNameToSmallestTemplate
                    .get(reservedInstance.getTemplate().getFamily()));
        }

        Map<Long, List<SMAReservedInstance>> distinctRIs = new HashMap<>();
        for (SMAReservedInstance ri : reservedInstances) {
            Long riKeyOid = ri.getRiKeyOid();
            List<SMAReservedInstance> instances = distinctRIs.get(riKeyOid);
            if (instances == null) {
                distinctRIs.put(riKeyOid, new ArrayList<>(Arrays.asList(ri)));

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
                representativeTotalCount = representativeTotalCount + ri.getNormalizedCount();
                ri.setNormalizedCount(0);
            }
            representative.setNormalizedCount(representativeTotalCount);
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
            float totalGroupCouponsRemaining = 0;
            float groupMemberRequiredCoupons = 0;
            if (leaderEngagement != null) {
                totalGroupCouponsRemaining = leaderEngagement.getDiscountedCoupons();
                groupMemberRequiredCoupons = leaderEngagement.getTemplate().getCoupons();
            }
            for (SMAVirtualMachine groupMember : smaVirtualMachineGroup.getVirtualMachines()) {
                /*
                 * if there is an engagement create engagement for all the group members.
                 */
                if (leaderEngagement != null) {
                    if (totalGroupCouponsRemaining > SMAUtils.EPSILON) {
                        float couponsAllocated = Math.min(groupMemberRequiredCoupons, totalGroupCouponsRemaining);
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
                        matches.add(new SMAMatch(groupMember, leaderEngagement.getTemplate(),
                                null,
                                0f));
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
     * @param reduceDependency if true will reduce relinquishing
     */
    public static void
    createRIToVMsMap(List<SMAVirtualMachine> virtualMachines,
                     Map<SMAReservedInstance, Float> remainingCoupons,
                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                     Map<String, List<SMATemplate>> familyNameToTemplates,
                     boolean reduceDependency) {
        // Multimap from template to the VMs that has the template as one of its provider.
        Map<SMATemplate, List<SMAVirtualMachine>> templateToVmsMap = new HashMap<>();
        for (SMAVirtualMachine vm : virtualMachines) {
            if (vm.getGroupSize() >= 1) {
                // don't add VM is in ASG and not the leader
                for (SMATemplate template : vm.getGroupProviders()) {
                    templateToVmsMap.putIfAbsent(template, new ArrayList<>());
                    templateToVmsMap.get(template).add(vm);
                }
            }
        }

        // The RI can discount a VM if the template of RI is one of the provider for VM.
        for (SMAReservedInstance ri : remainingCoupons.keySet()) {
            List<SMAVirtualMachine> virtualMachineList = new ArrayList<>();
            if (ri.isIsf()) {
                // all VMs that has a provider from the RI template family.
                Set<SMAVirtualMachine> vmsWithProviderInFamily = new HashSet<>();
                List<SMATemplate> familyTemplates = familyNameToTemplates.get(ri.getNormalizedTemplate().getFamily());
                for (SMATemplate template : familyTemplates) {
                    List<SMAVirtualMachine> vmList = templateToVmsMap.get(template);
                   if (vmList != null) {
                       vmsWithProviderInFamily.addAll(vmList.stream()
                           // take into account scoping or RI
                           .filter(vm -> vm.mayBeCoveredByRI(ri))
                               .collect(Collectors.toSet()));
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
                            // take into account scoping of RI.
                            .filter(vm -> vm.mayBeCoveredByRI(ri))
                            .collect(Collectors.toList()));
                }
            }
            // TODO move attribute remaining coupons to SMAReservedInstance
            sortAndUpdateVMList(virtualMachineList, remainingCoupons.get(ri), ri, virtualMachineGroupMap, reduceDependency);
        }

    }

    /**
     * sort the virtual machines that can be discounted by a reserved instance.
     * Update the RI's discountableVMsPartitionedByCoupon.
     *
     * @param virtualMachineList     list of virtual machines
     * @param riCoupons              remaining coupons for each RI
     * @param ri                     the reserved instance
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @param reduceDependency if true will reduce relinquishing
     */
    private static void sortAndUpdateVMList(List<SMAVirtualMachine> virtualMachineList,
                                            Float riCoupons, SMAReservedInstance ri,
                                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                            boolean reduceDependency) {
        Collections.sort(virtualMachineList, new RIPreference(riCoupons, ri, virtualMachineGroupMap, reduceDependency));
        virtualMachineList.stream().forEach(vm -> ri.addVMToDiscountableVMs(vm, false));
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
            List<SMATemplate> groupProviderList = findProviderIntersection(entry.getValue());
            if (!groupProviderList.isEmpty()) {
                SMAVirtualMachineGroup smaVirtualMachineGroup = new SMAVirtualMachineGroup(groupName,
                        entry.getValue(), groupProviderList);
                virtualMachineGroupMap.put(groupName, smaVirtualMachineGroup);
            }
        }
        return virtualMachineGroupMap;
    }

    /**
     * Find intersection of providers of all vms in the group.
     * @param virtualMachines list of member virtual machines.
     *
     * @return the intersection of providers of all vms in the group.
     */
    public static List<SMATemplate>  findProviderIntersection(List<SMAVirtualMachine> virtualMachines) {
        if (virtualMachines.isEmpty()) {
            return new ArrayList<>();
        }
        List<SMATemplate> groupProviderList = virtualMachines.get(0).getGroupProviders();
        for (SMAVirtualMachine virtualMachine : virtualMachines) {
            if (virtualMachine != virtualMachines.get(0)) {
                Set<SMATemplate> memberGroupProviders = new HashSet<>(virtualMachine
                        .getGroupProviders());
                // update the groupLeader provider with the intersection of all member providers.
                groupProviderList = groupProviderList.stream()
                        .filter(memberGroupProviders::contains)
                        .collect(Collectors.toList());
            }
        }
        return groupProviderList;
    }


    /**
     * Given an VM newEngagement with newRI and oldEngagement with oldRI determine which
     * SMAMatch the VM prefers.
     * At this point we have made sure that both the SMAMatches have same coverage.
     *
     * @param vm                     virtual machine considering for engagement
     * @param oldEngagement          engagement the VM is already engaged to
     * @param newEngagement          current engagement considering to be engaged by VM
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @return true if newEngagement, else false, that is, VM stays engaged to oldEngagement
     */
    protected static boolean preference(SMAVirtualMachine vm,
                                        SMAMatch oldEngagement,
                                        SMAMatch newEngagement,
                                        Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        try {
            Objects.requireNonNull(vm, "VM is null");
            Objects.requireNonNull(oldEngagement, "oldEngagement is null");
            Objects.requireNonNull(newEngagement, "newEngagement is null");
            logger.debug("preference(vm={}, profile={}, newRI={}, oldRI={})", vm.getName(),
                    oldEngagement.toString(), newEngagement.toString());

            SMAReservedInstance newRI = newEngagement.getReservedInstance();
            SMAReservedInstance oldRI = oldEngagement.getReservedInstance();

            float newRiCoverage = newRI.getRICoverage(vm);
            float oldRiCoverage = oldRI.getRICoverage(vm);

            // if vm is already covered by one of the RI prefer that RI
            if (newRiCoverage - oldRiCoverage > SMAUtils.EPSILON) {
                return true;
            }
            if (oldRiCoverage - newRiCoverage > SMAUtils.EPSILON) {
                return false;
            }

            // non-ISF preference
            // Beyond this point, both the RIs are Regional or both are Zonal.
            final Boolean nonISFPreference = nonISFPreference(newRI, oldRI);
            if (nonISFPreference != null) {
                return nonISFPreference;
            }

            // Single scoping preference.  In Azure an RI can be shared, which is EA scope,
            // or single, which is business account scope.
            final Boolean scopePreference = scopePreference(newRI, oldRI);
            if (scopePreference != null) {
                return scopePreference;
            }

            SMATemplate newTemplate = newEngagement.getTemplate();
            SMATemplate oldTemplate = oldEngagement.getTemplate();

            // Minimize moves based on VM's current allocated template and the new and old RI
            // template.  Beyond this point, VM's allocated template does not match either
            // the new or current RI

            int newTemplateMoves = templateMoves(vm, newTemplate, virtualMachineGroupMap);
            int oldTemplateMoves = templateMoves(vm, oldTemplate, virtualMachineGroupMap);
            if (newTemplateMoves < oldTemplateMoves) {
                return true;
            } else if (newTemplateMoves > oldTemplateMoves) {
                return false;
            }
            int newFamilyMoves = familyMoves(vm, newTemplate.getFamily(), virtualMachineGroupMap);
            int oldFamilyMoves = familyMoves(vm, oldTemplate.getFamily(), virtualMachineGroupMap);
            if (newFamilyMoves < oldFamilyMoves) {
                return true;
            } else if (newFamilyMoves > oldFamilyMoves) {
                return false;
            }

            // pick the RI which has lower ondemand cost.

            float newTemplateTotalCost =
                newTemplate.getOnDemandTotalCost(vm.getCostContext());
            float oldTemplateTotalCost =
                oldTemplate.getOnDemandTotalCost(vm.getCostContext());

            if (SMAUtils.round(newTemplateTotalCost - oldTemplateTotalCost)
                    > SMAUtils.EPSILON) {
                return false;
            } else if (SMAUtils.round(oldTemplateTotalCost - newTemplateTotalCost)
                    > SMAUtils.EPSILON) {
                return true;
            }

            // Last resort break tie with oid. t3a.large and t3.large happens
            // to have same on-demand cost too..
            return (oldRI.getOid() - newRI.getOid() > 0);

        } catch (Exception e) {
            logger.error("preference(vm={}, profile={}, newRI={}, oldRI={}) e={}",
                    (vm == null ? "null" : vm.getName()),
                    (vm == null || vm.getCurrentTemplate() == null ? "null" : vm.getCurrentTemplate()),
                    (newEngagement == null ? "null" : newEngagement),
                    (oldEngagement == null ? "null" : newEngagement),
                    e.getMessage());
        }
        return false;
    }

    /**
     * This method is used to identify and prefer non-ISF RIs over ISF RIs, because non-ISF RIs
     * are more restrictive.
     *
     * @param newRI new RI
     * @param oldRI old RI
     * @return true if newRI is non-ISF and oldRI is ISF, false if oldRI is non-ISF and newRI is ISF, null otherwise
     */
    private static Boolean nonISFPreference(final SMAReservedInstance newRI,
                                            final SMAReservedInstance oldRI) {
        Boolean preference = null;
        if (!newRI.isIsf() && oldRI.isIsf()) {
            preference = true;
        } else if (newRI.isIsf() && !oldRI.isIsf()) {
            preference = false;
        }
        logger.trace("nonISFPreference: newRI.isIsf()={} oldRI.isIsf()={} return={}",
            newRI.isIsf(), oldRI.isIsf(), preference);
        return preference;
    }

    /**
     * This method is used to identify and prefer single scoped RIs over shared scope, because
     * single scoped is more restrictive.
     *
     * @param newRI new RI
     * @param oldRI old RI
     * @return true if newRI is single scoped  and oldRI is not,
     *         false if oldRI is single scoped and newRI is not, null otherwise
     */
    private static Boolean scopePreference(final SMAReservedInstance newRI,
                                           final SMAReservedInstance oldRI) {
        Boolean preference = null;
        if (!newRI.isShared() && oldRI.isShared()) {
            preference = true;
        } else if (newRI.isShared() && !oldRI.isShared()) {
            preference = false;
        }
        logger.trace("scopePreference: newRI.isShared={} oldRI.isShared()={}, return={}",
            newRI.isShared(), oldRI.isShared(), preference);
        return preference;
    }

    /**
     * Prefer oldEngagement if costImprovement is negative.
     * Prefer newEngagement if costImprovement is positive.
     * Prefer newEngagement if costImprovement is 0 and the preference returns true.
     *
     * @param oldEngagement          the engagement of the group leader. null if currently not engaged.
     * @param newEngagement          current engagement considering to be engaged by VM
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @return true if newEngagement is better, else false, that is, VM stays engaged to oldEngagement
     */
    public static boolean isCurrentRIBetterThanOldRI(SMAMatch oldEngagement,
                                                     SMAMatch newEngagement,
                                                     Map<String, SMAVirtualMachineGroup>
                                                             virtualMachineGroupMap) {
        SMAVirtualMachine virtualMachine = newEngagement.getVirtualMachine();
        float costImprovement = costImprovement(virtualMachine.getCostContext(),
                newEngagement.getDiscountedCoupons() / (float)virtualMachine.getGroupSize(),
                newEngagement.getTemplate(),
                (oldEngagement == null) ?
                        0 : oldEngagement.getDiscountedCoupons() / (float)virtualMachine.getGroupSize(),
                (oldEngagement == null) ?
                        virtualMachine.getNaturalTemplate() : oldEngagement.getTemplate());
        costImprovement = SMAUtils.round(costImprovement);
        if (oldEngagement == null && costImprovement <
                (SMAUtils.BIG_EPSILON * virtualMachine.getGroupSize()
                        * virtualMachine.getNaturalTemplate()
                        .getOnDemandTotalCost(virtualMachine.getCostContext()))) {
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
                return preference(virtualMachine, oldEngagement,
                        newEngagement, virtualMachineGroupMap);
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
     * @param reduceDependency  if true will reduce relinquishing
     */

    public static void runIterations(Deque<SMAReservedInstance> freeRIs,
                                     Map<SMAReservedInstance, Float> remainingCoupons,
                                     Map<SMAVirtualMachine, SMAMatch> currentEngagements,
                                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                     boolean reduceDependency) {
        while (!freeRIs.isEmpty()) {
            SMAReservedInstance currentRI = freeRIs.poll();
            float currentRICoupons = (currentRI == null) ? 0f : remainingCoupons.get(currentRI);

            // if the last matched VM was only partially filled allocated the coupons to that VM first.
            SMAVirtualMachine lastDiscountedVM = currentRI.getLastDiscountedVM();
            if (lastDiscountedVM != null && currentRICoupons > SMAUtils.EPSILON) {
                SMAMatch lastMatch = currentEngagements.get(lastDiscountedVM);
                if (lastMatch != null && lastMatch.getReservedInstance() == currentRI) {
                    float totalCouponsReq = lastMatch.getTemplate().getCoupons() * lastDiscountedVM.getGroupSize();
                    float additionalCouponReq = totalCouponsReq - lastMatch.getDiscountedCoupons();
                    float allocatableCoupons = Math.min(currentRICoupons, additionalCouponReq);
                    if (allocatableCoupons > SMAUtils.EPSILON) {
                        currentRICoupons = currentRICoupons - allocatableCoupons;
                        remainingCoupons.put(currentRI, currentRICoupons);
                        lastMatch.setDiscountedCoupons(lastMatch.getDiscountedCoupons() + allocatableCoupons);
                    }
                }
            }
            if (currentRICoupons > SMAUtils.EPSILON && !currentRI.isDisountableVMsEmpty()) {
                SMAVirtualMachine currentVM = currentRI
                        .findBestDiscountableVM(currentRICoupons, virtualMachineGroupMap, reduceDependency);
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
                        logger.error("SMA destinationTemplate can't be null " +
                                        "currentRI={} currentVM={} riTemplate={}", currentRI, currentVM,
                                currentRI.getNormalizedTemplate());
                        continue;
                    }
                }
                // the number of coupons the VM will require if it has to be fully discounted.
                float currentVMCouponRequest = destinationTemplate.getCoupons() * groupSize;
                if (currentVMCouponRequest < SMAUtils.BIG_EPSILON) {
                    logger.error("SMA currentVMCouponRequest can't be zero " +
                            "destinationTemplate={} groupSize={} ", destinationTemplate, groupSize);
                    continue;
                }
                /* discounted coupons are the actual coupons used in this engagement.
                 * it will be the minimum of what is required and what is available.
                 */
                float discountedCoupons = Math.min(currentVMCouponRequest,
                        currentRICoupons);
                SMAMatch oldEngagement = currentEngagements.get(currentVM);
                if (!freeRIs.contains(currentRI)) {
                    freeRIs.addFirst(currentRI);
                }
                currentRI.removeVMFromDiscountableVMs(currentVMCouponRequest);
                SMAMatch newEngagement = new SMAMatch(currentVM, destinationTemplate,
                        currentRI, discountedCoupons);
                Boolean isCurrentRIBetter = isCurrentRIBetterThanOldRI(oldEngagement,
                        newEngagement, virtualMachineGroupMap);
                if (isCurrentRIBetter) {
                    if (oldEngagement == null) {
                        currentEngagements.put(currentVM, newEngagement);
                        currentRI.setLastDiscountedVM(currentVM);
                        // update remaining coupons
                        float currentRIRemainingCoupons = currentRICoupons - discountedCoupons;
                        remainingCoupons.put(currentRI, currentRIRemainingCoupons);
                    } else {
                        // engagement already exists. swap engagement
                        SMAReservedInstance oldRI = oldEngagement.getReservedInstance();
                        // break engagement;
                        currentEngagements.remove(currentVM);
                        currentEngagements.put(currentVM, newEngagement);
                        // update remaining coupons of oldRI and currentRI
                        float currentLeftOverCoupons = currentRICoupons - discountedCoupons;
                        remainingCoupons.put(currentRI, currentLeftOverCoupons);
                        float oldEngagementCoupons = oldEngagement.getDiscountedCoupons();
                        remainingCoupons.put(oldRI, remainingCoupons.get(oldRI) + oldEngagementCoupons);
                        oldRI.restoreSkippedVMs();
                        currentRI.setLastDiscountedVM(currentVM);
                        if (!freeRIs.contains(oldRI)) {
                            freeRIs.add(oldRI);
                        }
                    }
                } else if (currentRICoupons < currentVMCouponRequest - SMAUtils.EPSILON) {
                    currentRI.addToSkippedVMs(currentVM);
                }

            }
        }
    }


    /**
     * compare the net cost based on available coupons, the template On-demand, discounted cost.
     *
     * @param costContext     instance containing all the parameters for cost lookup
     * @param currentCoupons  current available coupons
     * @param currentTemplate the current template
     * @param oldCoupons      old available coupons
     * @param oldTemplate     the old template
     * @return the effective savings of resize from oldTemplate to currentTemplate
     */

    public static float costImprovement(CostContext costContext,
                                        float currentCoupons, SMATemplate currentTemplate,
                                        float oldCoupons, SMATemplate oldTemplate) {
        return (oldTemplate.getNetCost(costContext, oldCoupons)
            - currentTemplate.getNetCost(costContext, currentCoupons));
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
        private float coupons;
        // the reserved instance of interest.
        private SMAReservedInstance reservedInstance;
        // map containing ASG information.
        private Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap;
        /*
         * what mode the SMA is running.
         */
        private boolean reduceDependency;

        /**
         * Constuctor for the comparator.
         * @param coupons number of coupons remaining for the reservedInstance
         * @param reservedInstance the reserved instance of interest.
         * @param virtualMachineGroupMap map containing ASG information.
         * @param reduceDependency if true will reduce relinquishing
         */
        public RIPreference(float coupons, SMAReservedInstance reservedInstance,
                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                            boolean reduceDependency) {
            this.coupons = coupons;
            this.reservedInstance = reservedInstance;
            this.virtualMachineGroupMap = virtualMachineGroupMap;
            this.reduceDependency = reduceDependency;
        }

        /**
         * compare vm1 and vm2 and determine reservedInstance prefers vm1 or vm2.
         * @param vm1 first vm
         * @param vm2 second vm
         * @return -1 if reservedInstance prefers vm1. return 1 if reservedInstance prefers vm2.
         */
        @Override
        public int compare(SMAVirtualMachine vm1, SMAVirtualMachine vm2) {

            // to reduce dependency do the cost comparison is done after current coverage comparison.
            // this will prevent relinquishing.
            if (reduceDependency) {
                int currentCoverageComparison = currentCoverageComparison(vm1, vm2, reservedInstance);
                if (currentCoverageComparison != 0) {
                    return currentCoverageComparison;
                }
            }

            int costComparison = reservedInstance
                    .compareCost(vm1, vm2, virtualMachineGroupMap, coupons);
            if (costComparison != 0) {
                return costComparison;
            }

            if (!reduceDependency) {
                int currentCoverageComparison = currentCoverageComparison(vm1, vm2, reservedInstance);
                if (currentCoverageComparison != 0) {
                    return currentCoverageComparison;
                }
            }


            String riFamily = reservedInstance.getNormalizedTemplate().getFamily();
            // pick vm with lesser moves
            if (!reservedInstance.isIsf()) {
                int vm1TemplateMoves = templateMoves(vm1, reservedInstance.getNormalizedTemplate(),
                        virtualMachineGroupMap);
                int vm2TemplateMoves = templateMoves(vm2, reservedInstance.getNormalizedTemplate(),
                        virtualMachineGroupMap);
                if (vm1TemplateMoves < vm2TemplateMoves) {
                    return -1;
                } else if (vm1TemplateMoves > vm2TemplateMoves) {
                    return 1;
                }
            } else {
                int vm1TemplateMoves = templateMoves(vm1, vm1.getMinCostProviderPerFamily(riFamily),
                        virtualMachineGroupMap);
                int vm2TemplateMoves = templateMoves(vm2, vm2.getMinCostProviderPerFamily(riFamily),
                        virtualMachineGroupMap);
                if (vm1TemplateMoves < vm2TemplateMoves) {
                    return -1;
                } else if (vm1TemplateMoves > vm2TemplateMoves) {
                    return 1;
                }
            }

            // pick vm in the same family
            int vm1FamilyMoves = familyMoves(vm1, riFamily, virtualMachineGroupMap);
            int vm2FamilyMoves = familyMoves(vm2, riFamily, virtualMachineGroupMap);
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


    }

    /**
     * prefer the vm that has the higher coverage from the RI to reduce dependency.
     * @param vm1 first vm
     * @param vm2 second vm
     * @param reservedInstance            reserved instance
     * @return prefer the vm with higher coverage.
     */
    private static int currentCoverageComparison(SMAVirtualMachine vm1, SMAVirtualMachine vm2,
                                                 SMAReservedInstance reservedInstance) {
        float riCoverageVm1 = reservedInstance.getRICoverage(vm1);
        float riCoverageVm2 = reservedInstance.getRICoverage(vm2);
        // if vm is already covered by one of the RI prefer that RI.
        // If both vms are covered by this RI then prefer the vm which is not scaling up.

        if (riCoverageVm1 > SMAUtils.EPSILON && riCoverageVm2 > SMAUtils.EPSILON) {
            if (vm2.isScaleUp() && !vm1.isScaleUp()) {
                return -1;
            }
            if (!vm2.isScaleUp() && vm1.isScaleUp()) {
                return 1;
            }
        }



        if ((riCoverageVm1 - riCoverageVm2) > SMAUtils.EPSILON) {
            return -1;
        }
        if ((riCoverageVm2 - riCoverageVm1) > SMAUtils.EPSILON) {
            return 1;
        }
        return 0;
    }


    /**
     * the number of moves required for the vm to change to the new template.
     *
     * @param vm                  the VM of interest
     * @param template            new  template
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @return move count
     */
    private static int templateMoves(final SMAVirtualMachine vm,
                                     final SMATemplate template,
                                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        int moves = 0;
        if (vm.getGroupSize() > 1) {
            for (SMAVirtualMachine member : virtualMachineGroupMap
                    .get(vm.getGroupName()).getVirtualMachines()) {
                moves += member.getCurrentTemplate().getOid() == template.getOid() ? 0 : 1;
            }
            return moves;
        } else {
            return vm.getCurrentTemplate().getOid() == template.getOid() ? 0 : 1;
        }
    }


    /**
     * the number of family change for the vm to change to the new template.
     *
     * @param vm                  the VM of interest
     * @param family              family of new template
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @return move count
     */
    private static int familyMoves(final SMAVirtualMachine vm,
                                   final String family,
                                   Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        // Minimize moves: check family equality
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
}
