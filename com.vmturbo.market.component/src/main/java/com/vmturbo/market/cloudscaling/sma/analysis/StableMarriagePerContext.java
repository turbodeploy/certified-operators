package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.ArrayList;
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

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.market.cloudscaling.sma.entities.SMACloudCostCalculator;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachineGroup;

/**
 * Given a context, run SMA, and generate the SMAOutputContext.
 */
public class StableMarriagePerContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Run the StableMarriage algorithm.
     *
     * @param inputContext the input of SMA partitioned on a per context basis.
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     *
     * @return the matching in the inputContext.
     */
    public static SMAOutputContext execute(final SMAInputContext inputContext,
                                           Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                            SMACloudCostCalculator cloudCostCalculator) {
        final List<SMAVirtualMachine> virtualMachines = inputContext.getVirtualMachines();

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


        final SMAContext context = inputContext.getContext();
        /*
         * Queue that keeps track of all the RI's that are not engaged.
         */
        Deque<SMAReservedInstance> freeRIs = new LinkedList<>();
        /*
         * Map that keeps track of all the unused coupons for RIs.
         */
        Map<SMAReservedInstance, CloudCommitmentAmount> remainingCoupons = new HashMap<>();
        for (SMAReservedInstance reservedInstance : reservedInstances) {
            CloudCommitmentAmount coupons = reservedInstance.getCommitmentAmount();
            if (CommitmentAmountCalculator.isPositive(coupons, SMAUtils.EPSILON)) {
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
                inputContext.getSmaConfig().isReduceDependency(), cloudCostCalculator);

        // map to keep track of the successful engagements so far.
        Map<SMAVirtualMachine, SMAMatch> currentEngagements = new HashMap<>();

        /*
         * This is the main function in SMA. This is where RIs get matched to VMs.
         */
        runSMA(freeRIs, remainingCoupons,
                currentEngagements, virtualMachineGroupMap,
                inputContext.getSmaConfig().isReduceDependency(), cloudCostCalculator);

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
                    null, CommitmentAmountCalculator.ZERO_COVERAGE));
        }

        // generate the SMAOutputContext
        SMAOutputContext outputContext = new SMAOutputContext(context, matches);
        Collections.sort(outputContext.getMatches(), new SortMatchesByVMOID());
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
            CloudCommitmentAmount totalGroupCouponsRemaining = CommitmentAmountCalculator.ZERO_COVERAGE;
            CloudCommitmentAmount groupMemberRequiredCoupons = CommitmentAmountCalculator.ZERO_COVERAGE;
            if (leaderEngagement != null) {
                totalGroupCouponsRemaining = leaderEngagement.getDiscountedCoupons();
                groupMemberRequiredCoupons = leaderEngagement.getTemplate().getCommitmentAmount();
            }
            for (SMAVirtualMachine groupMember : smaVirtualMachineGroup.getVirtualMachines()) {
                /*
                 * if there is an engagement create engagement for all the group members.
                 */
                if (leaderEngagement != null) {
                    if (CommitmentAmountCalculator.isPositive(totalGroupCouponsRemaining, SMAUtils.EPSILON)) {
                        CloudCommitmentAmount couponsAllocated = CommitmentAmountCalculator.min(groupMemberRequiredCoupons, totalGroupCouponsRemaining);
                        if (groupMember != groupLeader) {
                            matches.add(new SMAMatch(groupMember, leaderEngagement.getTemplate(),
                                    leaderEngagement.getReservedInstance(),
                                    couponsAllocated));
                        } else {
                            leaderEngagement.setDiscountedCoupons(
                                    couponsAllocated);
                        }
                        totalGroupCouponsRemaining = CommitmentAmountCalculator.subtract(totalGroupCouponsRemaining,
                                couponsAllocated);
                    } else {
                        matches.add(new SMAMatch(groupMember, leaderEngagement.getTemplate(),
                                null,
                                CommitmentAmountCalculator.ZERO_COVERAGE));
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
     * @param reduceDependency if true will reduce relinquishing
     */
    public static void
    createRIToVMsMap(List<SMAVirtualMachine> virtualMachines,
                     Map<SMAReservedInstance, CloudCommitmentAmount> remainingCoupons,
                     Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                     Map<String, List<SMATemplate>> familyNameToTemplates,
                     boolean reduceDependency,
                     SMACloudCostCalculator cloudCostCalculator) {
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
            sortAndUpdateVMList(virtualMachineList, remainingCoupons.get(ri), ri, virtualMachineGroupMap, reduceDependency, cloudCostCalculator);
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
            CloudCommitmentAmount riCoupons, SMAReservedInstance ri,
                                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                            boolean reduceDependency,
                                            SMACloudCostCalculator cloudCostCalculator) {
        Collections.sort(virtualMachineList, new RIPreference(riCoupons, ri, virtualMachineGroupMap, reduceDependency, cloudCostCalculator));
        virtualMachineList.stream().forEach(vm -> ri.addVMToDiscountableVMs(vm, false));
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
                                        Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                                        SMACloudCostCalculator cloudCostCalculator) {
        try {
            Objects.requireNonNull(vm, "VM is null");
            Objects.requireNonNull(oldEngagement, "oldEngagement is null");
            Objects.requireNonNull(newEngagement, "newEngagement is null");
            logger.debug("preference(vm={}, profile={}, newRI={}, oldRI={})", vm.getName(),
                    oldEngagement.toString(), newEngagement.toString());

            SMAReservedInstance newRI = newEngagement.getReservedInstance();
            SMAReservedInstance oldRI = oldEngagement.getReservedInstance();

            float newRiCoverage = newRI.getRICoverage(vm, cloudCostCalculator);
            float oldRiCoverage = oldRI.getRICoverage(vm, cloudCostCalculator);

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
                cloudCostCalculator.getNetCost(vm, newTemplate, CommitmentAmountCalculator.ZERO_COVERAGE, SMAUtils.UNKNOWN_OID);
            float oldTemplateTotalCost =
                cloudCostCalculator.getNetCost(vm, oldTemplate, CommitmentAmountCalculator.ZERO_COVERAGE, SMAUtils.UNKNOWN_OID);

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
                                                             virtualMachineGroupMap,
                                                     SMACloudCostCalculator cloudCostCalculator) {
        SMAVirtualMachine virtualMachine = newEngagement.getVirtualMachine();
        float costImprovement = costImprovement(virtualMachine,
                CommitmentAmountCalculator.multiplyAmount(newEngagement.getDiscountedCoupons(), 1.0f/(float)virtualMachine.getGroupSize()),
                newEngagement.getTemplate(),
                (oldEngagement == null) ?
                        CommitmentAmountCalculator.ZERO_COVERAGE :
                        CommitmentAmountCalculator.multiplyAmount(oldEngagement.getDiscountedCoupons(), 1.0f/(float)virtualMachine.getGroupSize()),
                (oldEngagement == null) ?
                        virtualMachine.getNaturalTemplate() : oldEngagement.getTemplate(), cloudCostCalculator,
                (oldEngagement == null) ? SMAUtils.UNKNOWN_OID : oldEngagement.getReservedInstance().getOid(),
                newEngagement.getReservedInstance().getOid());
        costImprovement = SMAUtils.round(costImprovement);
        if (costImprovement < -1.0 * SMAUtils.EPSILON) {
            return false;
        } else if (costImprovement > SMAUtils.EPSILON) {
            return true;
        } else {
            if (oldEngagement == null) {
                // a uncovered vm should never scale to another template to use an RI if savings is 0.
                // If they are in the same template then they can use the RI even if savings is 0.
                return virtualMachine.getCurrentTemplate().getOid() == newEngagement.getTemplate().getOid();
            } else {
                return preference(virtualMachine, oldEngagement,
                        newEngagement, virtualMachineGroupMap, cloudCostCalculator);
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

    public static void runSMA(Deque<SMAReservedInstance> freeRIs,
                              Map<SMAReservedInstance, CloudCommitmentAmount> remainingCoupons,
                              Map<SMAVirtualMachine, SMAMatch> currentEngagements,
                              Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                              boolean reduceDependency,
                              SMACloudCostCalculator smaCloudCostCalculator) {
        while (!freeRIs.isEmpty()) {
            SMAReservedInstance currentRI = freeRIs.poll();
            CloudCommitmentAmount currentRICoupons = (currentRI == null) ? CommitmentAmountCalculator.ZERO_COVERAGE : remainingCoupons.get(currentRI);

            // if the last matched VM was only partially filled allocated the coupons to that VM first.
            SMAVirtualMachine lastDiscountedVM = currentRI.getLastDiscountedVM();
            if (lastDiscountedVM != null && CommitmentAmountCalculator.isPositive(currentRICoupons, SMAUtils.EPSILON)) {
                SMAMatch lastMatch = currentEngagements.get(lastDiscountedVM);
                if (lastMatch != null && lastMatch.getReservedInstance() == currentRI) {
                    CloudCommitmentAmount totalCouponsReq = CommitmentAmountCalculator.multiplyAmount(lastMatch.getTemplate().getCommitmentAmount(),
                            lastDiscountedVM.getGroupSize());
                    CloudCommitmentAmount additionalCouponReq =
                            CommitmentAmountCalculator.subtract(totalCouponsReq, lastMatch.getDiscountedCoupons());
                    CloudCommitmentAmount allocatableCoupons = CommitmentAmountCalculator.min(currentRICoupons, additionalCouponReq);
                    if (CommitmentAmountCalculator.isPositive(allocatableCoupons, SMAUtils.EPSILON)) {
                        currentRICoupons = CommitmentAmountCalculator.subtract(currentRICoupons,allocatableCoupons);
                        remainingCoupons.put(currentRI, currentRICoupons);
                        lastMatch.setDiscountedCoupons(CommitmentAmountCalculator.sum(
                                lastMatch.getDiscountedCoupons(), allocatableCoupons));
                    }
                }
            }
            if (CommitmentAmountCalculator.isPositive(currentRICoupons, SMAUtils.EPSILON) && !currentRI.isDiscountableVMsEmpty()) {
                SMAVirtualMachine currentVM = currentRI
                        .findBestDiscountableVM();
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
                CloudCommitmentAmount currentVMCouponRequest = CommitmentAmountCalculator.multiplyAmount(
                        destinationTemplate.getCommitmentAmount(),groupSize);
                if (!CommitmentAmountCalculator.isPositive(currentVMCouponRequest, SMAUtils.EPSILON)) {
                    logger.error("SMA currentVMCouponRequest can't be zero " +
                            "destinationTemplate={} groupSize={} ", destinationTemplate, groupSize);
                    continue;
                }
                /* discounted coupons are the actual coupons used in this engagement.
                 * it will be the minimum of what is required and what is available.
                 */
                CloudCommitmentAmount discountedCoupons = CommitmentAmountCalculator.min(currentVMCouponRequest,
                        currentRICoupons);
                SMAMatch oldEngagement = currentEngagements.get(currentVM);
                if (!freeRIs.contains(currentRI)) {
                    freeRIs.addFirst(currentRI);
                }
                currentRI.removeVMFromDiscountableVMs();
                SMAMatch newEngagement = new SMAMatch(currentVM, destinationTemplate,
                        currentRI, discountedCoupons);
                Boolean isCurrentRIBetter = isCurrentRIBetterThanOldRI(oldEngagement,
                        newEngagement, virtualMachineGroupMap, smaCloudCostCalculator);
                if (isCurrentRIBetter) {
                    if (oldEngagement == null) {
                        currentEngagements.put(currentVM, newEngagement);
                        currentRI.setLastDiscountedVM(currentVM);
                        // update remaining coupons
                        CloudCommitmentAmount currentRIRemainingCoupons =
                                CommitmentAmountCalculator.subtract(currentRICoupons, discountedCoupons);
                        remainingCoupons.put(currentRI, currentRIRemainingCoupons);
                    } else {
                        // engagement already exists. swap engagement
                        SMAReservedInstance oldRI = oldEngagement.getReservedInstance();
                        // break engagement;
                        currentEngagements.remove(currentVM);
                        currentEngagements.put(currentVM, newEngagement);
                        // update remaining coupons of oldRI and currentRI
                        CloudCommitmentAmount currentLeftOverCoupons =
                                CommitmentAmountCalculator.subtract(currentRICoupons, discountedCoupons);
                        remainingCoupons.put(currentRI, currentLeftOverCoupons);
                        CloudCommitmentAmount oldEngagementCoupons = oldEngagement.getDiscountedCoupons();
                        remainingCoupons.put(oldRI,
                                CommitmentAmountCalculator.sum(remainingCoupons.get(oldRI), oldEngagementCoupons));
                        oldRI.restoreSkippedVMs();
                        currentRI.setLastDiscountedVM(currentVM);
                        if (!freeRIs.contains(oldRI)) {
                            freeRIs.add(oldRI);
                        }
                    }
                } else if (CommitmentAmountCalculator.isPositive(
                        CommitmentAmountCalculator.subtract(currentVMCouponRequest, currentRICoupons),
                        SMAUtils.EPSILON)) {//currentVMCouponRequest - currentRICoupons > EPSILON
                    currentRI.addToSkippedVMs(currentVM);
                }

            }
        }
    }


    /**
     * compare the net cost based on available coupons, the template On-demand, discounted cost.
     *
     * @param currentCoupons  current available coupons
     * @param currentTemplate the current template
     * @param oldCoupons      old available coupons
     * @param oldTemplate     the old template
     * @return the effective savings of resize from oldTemplate to currentTemplate
     */

    public static float costImprovement(SMAVirtualMachine virtualMachine,
            CloudCommitmentAmount currentCoupons, SMATemplate currentTemplate,
            CloudCommitmentAmount oldCoupons, SMATemplate oldTemplate, SMACloudCostCalculator cloudCostCalculator,
                                        long oldRI, long newRI) {
        float oldCost = cloudCostCalculator.getNetCost(virtualMachine,oldTemplate,oldCoupons,oldRI);
        float newCost = cloudCostCalculator.getNetCost(virtualMachine, currentTemplate, currentCoupons, newRI);
        return (oldCost - newCost);

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
        private CloudCommitmentAmount coupons;
        // the reserved instance of interest.
        private SMAReservedInstance reservedInstance;
        // map containing ASG information.
        private Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap;
        /*
         * what mode the SMA is running.
         */
        private boolean reduceDependency;

        private SMACloudCostCalculator cloudCostCalculator;

        /**
         * Constuctor for the comparator.
         * @param coupons number of coupons remaining for the reservedInstance
         * @param reservedInstance the reserved instance of interest.
         * @param virtualMachineGroupMap map containing ASG information.
         * @param reduceDependency if true will reduce relinquishing
         */
        public RIPreference(CloudCommitmentAmount coupons, SMAReservedInstance reservedInstance,
                            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                            boolean reduceDependency, SMACloudCostCalculator cloudCostCalculator) {
            this.coupons = coupons;
            this.reservedInstance = reservedInstance;
            this.virtualMachineGroupMap = virtualMachineGroupMap;
            this.reduceDependency = reduceDependency;
            this.cloudCostCalculator = cloudCostCalculator;
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
                int currentCoverageComparison = currentCoverageComparison(vm1, vm2, reservedInstance, cloudCostCalculator);
                if (currentCoverageComparison != 0) {
                    return currentCoverageComparison;
                }
            }

            int costComparison = cloudCostCalculator
                    .compareCost(vm1, vm2, virtualMachineGroupMap, coupons, reservedInstance);
            if (costComparison != 0) {
                return costComparison;
            }

            if (!reduceDependency) {
                int currentCoverageComparison = currentCoverageComparison(vm1, vm2, reservedInstance, cloudCostCalculator);
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
                                                 SMAReservedInstance reservedInstance,
            SMACloudCostCalculator smaCloudCostCalculator) {
        float riCoverageVm1 = reservedInstance.getRICoverage(vm1, smaCloudCostCalculator);
        float riCoverageVm2 = reservedInstance.getRICoverage(vm2, smaCloudCostCalculator);
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
