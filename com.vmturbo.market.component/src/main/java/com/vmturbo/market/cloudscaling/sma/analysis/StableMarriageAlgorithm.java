package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;

/**
 * Stable Marriage Algorithm.
 */

public class StableMarriageAlgorithm {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Given a SMAInput, generate the SMAOutput.
     *
     * @param input the list of input contexts
     * @return the SMA output
     */
    public static SMAOutput execute(@Nonnull SMAInput input) {
        Objects.requireNonNull(input, "SMA execute() input is null!");
        final Stopwatch stopWatch = Stopwatch.createStarted();
        long actionCount = 0;
        List<SMAOutputContext> outputContexts = new ArrayList<>();
        for (SMAInputContext inputContext : input.getContexts()) {
            SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
            /*
            vm1 was in ri1 and template1. vm1 got discounted by ri2.
            post processing took the ri2 from vm1.we scale vm1 back to template1.
            This is a RI optimisation action.
            We will have to run postprocessing twice. On the 2nd round vm1 will
            be identified as a VM that lost coupons while staying in same template.
            */
            boolean actionNegated = true;
            int iterations = 0;
            while (actionNegated && iterations < SMAUtils.MAX_ITERATIONS) {
                actionNegated = postProcessing(outputContext);
                iterations++;
            }
            outputContexts.add(outputContext);
            for (SMAMatch match : outputContext.getMatches()) {
                if ((match.getVirtualMachine().getCurrentTemplate().getOid() != match.getTemplate().getOid())
                        || (Math.abs(match.getVirtualMachine().getCurrentRICoverage()
                        - match.getDiscountedCoupons()) > SMAUtils.EPSILON)) {
                    actionCount++;
                }
            }
        }
        long timeInMilliseconds = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("created {} outputContexts with {} actions in {}ms", outputContexts.size(), actionCount, timeInMilliseconds);
        SMAOutput output = new SMAOutput(outputContexts);
        if (logger.isDebugEnabled()) {
            for (SMAOutputContext outputContext : output.getContexts()) {
                logger.debug("SMA {}", outputContext);
            }
        }
        return output;
    }

    /**
     * post processing step to redistribute the coupons among the mathces.
     * The net savings remain unchanged. All investment RI  optimisation are negated.
     *
     * @param outputContext the output context of interest.
     * @return true is at-least 1 RI optimization was negated.
     */
    public static boolean postProcessing(SMAOutputContext outputContext) {
        boolean actionNegated = false;
        Map<Long, List<SMAMatch>> matchesWithOutgoingCoupons = new HashMap<>();
        Map<Long, List<SMAMatch>> matchesWithIncomingCoupons = new HashMap<>();
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            SMAReservedInstance projectedRI = smaMatch.getReservedInstance();
            SMAReservedInstance sourceRI = smaMatch.getVirtualMachine().getCurrentRI();
            // investment optimisation action. outgoing  coupons
            // from getVirtualMachine().getCurrentRI().
            if (isOutgoing(smaMatch)) {
                Long rikeyoid = sourceRI.getRiKeyOid();
                // for every rikeyoid we create atleast an empty matchesWithOutgoingCoupons list
                // and matchesWithIncomingCoupons list.
                matchesWithOutgoingCoupons.putIfAbsent(rikeyoid, new ArrayList<>());
                matchesWithIncomingCoupons.putIfAbsent(rikeyoid, new ArrayList<>());
                matchesWithOutgoingCoupons.get(rikeyoid).add(smaMatch);
            } else if (isIncoming(smaMatch)) {
                // incoming coupons. all the coupons accumulated are confirmed
                // to be not available to this VM before SMA. Could have moved to the same RI.
                // but even then we would have gained more coupons.
                Long rikeyoid = projectedRI.getRiKeyOid();
                // for every rikeyoid we create atleast an empty matchesWithOutgoingCoupons list
                // and matchesWithIncomingCoupons list.
                matchesWithOutgoingCoupons.putIfAbsent(rikeyoid, new ArrayList<>());
                matchesWithIncomingCoupons.putIfAbsent(rikeyoid, new ArrayList<>());
                matchesWithIncomingCoupons.get(rikeyoid).add(smaMatch);

            }
        }
        // for each investment RI optimisation get the lost RIs and assign them back
        for (Entry<Long, List<SMAMatch>> riKeyIdMatchPair : matchesWithOutgoingCoupons.entrySet()) {
            Long riKeyId = riKeyIdMatchPair.getKey();
            List<SMAMatch> outgoingCouponMatches = riKeyIdMatchPair.getValue();
            for (SMAMatch outgoingCouponMatch : outgoingCouponMatches) {
                // If the VM scales down then coupons required should be based on the
                // coupons required for the scaled down template. It is legit to lose coupons
                // if the vm scales down and is 100% covered. For eg a vm in t2.large with 10/16
                // coverage can go to t2.medium and have 8/8 coverage.
                float coupons_required = Math.min(outgoingCouponMatch.getVirtualMachine()
                        .getCurrentRICoverage(), outgoingCouponMatch.getTemplate().getCoupons())
                        - outgoingCouponMatch.getDiscountedCoupons();
                for (SMAMatch incomingCouponMatch : matchesWithIncomingCoupons.get(riKeyId)) {
                    float coupons_grabbed;
                    if (incomingCouponMatch.getVirtualMachine().getCurrentRI() == null
                            || incomingCouponMatch.getVirtualMachine().getCurrentRI()
                            .getRiKeyOid() != riKeyId) {
                        // If the VM is not previously covered
                        // by this ri then all coupons can be taken.
                        coupons_grabbed = Math.min(coupons_required, incomingCouponMatch.getDiscountedCoupons());
                    } else {
                        // If the VM is previously covered by the same RI
                        // then we can take upto the coupons which was previously covered.
                        // Since covered by same RI incomingCouponMatch is
                        // discounted more than previously discounted.
                        coupons_grabbed = Math.min(coupons_required,
                                incomingCouponMatch.getDiscountedCoupons()
                                        - incomingCouponMatch.getVirtualMachine()
                                        .getCurrentRICoverage());
                    }
                    if (coupons_grabbed > SMAUtils.EPSILON) {
                        actionNegated = true;
                        coupons_required = coupons_required - coupons_grabbed;
                        incomingCouponMatch.setDiscountedCoupons(incomingCouponMatch.getDiscountedCoupons()
                                - coupons_grabbed);
                    }
                    if (coupons_required < SMAUtils.EPSILON) {
                        break;
                    }
                }
                // adjust the outgoingCouponMatch based on the coupons_required. Ideally coupons_required
                // should be 0 and the VM should retain all the coupons it already had. If not it will
                // retain coupons to be 100% covered if it had scaled down.
                outgoingCouponMatch.setDiscountedCoupons(Math.min(outgoingCouponMatch.getVirtualMachine()
                        .getCurrentRICoverage(), outgoingCouponMatch.getTemplate().getCoupons()) - coupons_required);
                outgoingCouponMatch.setReservedInstance(outgoingCouponMatch.getVirtualMachine()
                        .getCurrentRI());
            }
        }

        moveUncoveredVMBackToNaturalTemplate(outputContext);

        return actionNegated;

    }

    /**
     * Move uncovered vm back to natural template after post processing.
     *
     * @param outputContext the output context of interest.
     */
    private static void moveUncoveredVMBackToNaturalTemplate(SMAOutputContext outputContext) {
        // get all the matches that involve RIs. Group together the ASG.
        Map<String, Set<SMAMatch>> matchByASG = new HashMap<>();
        Set<SMAMatch> nonASGMatch = new HashSet();
        Map<Long, Float> leftoverCoupons = new HashMap<>();
        Map<Long, SMAReservedInstance> riKeyOidToSMAReservedInstance = new HashMap<>();
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            String groupName = smaMatch.getVirtualMachine().getGroupName();
            if (groupName.equals(SMAUtils.NO_GROUP_ID)) {
                nonASGMatch.add(smaMatch);
            } else {
                matchByASG.putIfAbsent(groupName, new HashSet<>());
                matchByASG.get(groupName).add(smaMatch);
            }
        }
        // for non ASG vms compute the savings again and see if it still makes sense to
        // stay in the RI template or its better to move to natural template.

        // If the natural template is better then move..If the natural template saving
        // is same as the saving with coverage and the vm is already on the ri template
        // it better be using up the RIs.
        for (SMAMatch smaMatch : nonASGMatch) {
            if (smaMatch.getReservedInstance() != null) {
                float saving = smaMatch.getReservedInstance()
                        .computeSaving(smaMatch.getVirtualMachine(),
                                new HashMap<>(), smaMatch.getDiscountedCoupons());
                if (saving <  -1 * SMAUtils.EPSILON || (saving <  SMAUtils.EPSILON &&
                        smaMatch.getVirtualMachine().getCurrentTemplate().getOid() != smaMatch.getTemplate().getOid())) {
                    float current_leftover = leftoverCoupons.getOrDefault(smaMatch
                            .getReservedInstance().getRiKeyOid(),0f);
                    current_leftover += smaMatch.getDiscountedCoupons();
                    leftoverCoupons.put(smaMatch.getReservedInstance().getRiKeyOid(),
                            current_leftover);
                    riKeyOidToSMAReservedInstance.put(smaMatch.getReservedInstance().getRiKeyOid(),
                            smaMatch.getReservedInstance());
                    smaMatch.setReservedInstance(null);
                    smaMatch.setDiscountedCoupons(0);
                    smaMatch.setTemplate(smaMatch.getVirtualMachine().getNaturalTemplate());
                }
            }
        }
        // for asg first make sure atleast one of the member is discounted. If so
        // compute the saving if all the members move to the natural template vs
        // all member stay in the ri template and get discounted.
        for (Set<SMAMatch> smaMatches : matchByASG.values()) {
            float saving = 0;
            Optional<SMAMatch> matchWithCoverage = smaMatches.stream()
                    .filter(a -> a.getReservedInstance() != null).findFirst();
            List<SMAVirtualMachine> virtualMachines = smaMatches.stream()
                    .map(a -> a.getVirtualMachine()).collect(Collectors.toList());
            List<SMATemplate> groupProviderList = StableMarriagePerContext
                    .findProviderIntersection(virtualMachines);
            if (!groupProviderList.isEmpty()) {
                virtualMachines.stream().forEach(a -> a.setGroupProviders(groupProviderList));
            } else {
                continue;
            }
            // at least one member is convered.
            if (matchWithCoverage.isPresent()) {
                for (SMAMatch smaMatch : smaMatches) {
                    saving += matchWithCoverage.get().getReservedInstance()
                            .computeSaving(smaMatch.getVirtualMachine(),
                                    new HashMap<>(), smaMatch.getDiscountedCoupons());
                }
                if (saving <  -1 * SMAUtils.EPSILON || (saving <  SMAUtils.EPSILON &&
                        matchWithCoverage.get().getVirtualMachine().getCurrentTemplate().getOid()
                                != matchWithCoverage.get().getTemplate().getOid())) {
                    SMAReservedInstance coveredRI = matchWithCoverage.get().getReservedInstance();
                    riKeyOidToSMAReservedInstance.put(coveredRI.getRiKeyOid(),
                            coveredRI);
                    for (SMAMatch smaMatch : smaMatches) {
                        float current_leftover = leftoverCoupons.getOrDefault(coveredRI
                                .getRiKeyOid(),0f);
                        current_leftover += smaMatch.getDiscountedCoupons();
                        leftoverCoupons.put(coveredRI.getRiKeyOid(),
                                current_leftover);
                        smaMatch.setReservedInstance(null);
                        smaMatch.setDiscountedCoupons(0);
                        smaMatch.setTemplate(smaMatch.getVirtualMachine().getNaturalTemplate());
                    }
                }
            }
        }
        allocateLeftOverCoupons(outputContext, leftoverCoupons, riKeyOidToSMAReservedInstance);
    }

    /**
     * Allocate the coupons relinquished in moveUncoveredVMBackToNaturalTemplate stage.
     * @param outputContext the output context of interest.
     * @param leftoverCoupons map from Ri to coupons that are left after moveUncoveredVMBackToNaturalTemplate
     */
    private static void allocateLeftOverCoupons(SMAOutputContext outputContext,
                                                Map<Long, Float> leftoverCoupons,
                                                Map<Long, SMAReservedInstance> riKeyOidToSMAReservedInstance) {
        // first allocate coupons to the VMs which are already partially covered by this RI. Thus we dont
        // generate a new action.
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            if (smaMatch.getReservedInstance() != null) {
                float current_coupons = smaMatch.getDiscountedCoupons();
                float coupons_required = smaMatch.getTemplate().getCoupons()
                        - smaMatch.getDiscountedCoupons();
                float coupons_leftover = leftoverCoupons.getOrDefault(smaMatch
                        .getReservedInstance().getRiKeyOid(), 0f);
                if (coupons_required > SMAUtils.EPSILON && coupons_leftover > SMAUtils.EPSILON) {
                    float coupons_swapped = Math.min(coupons_required, coupons_leftover);
                    smaMatch.setDiscountedCoupons(current_coupons + coupons_swapped);
                    leftoverCoupons.put(smaMatch.getReservedInstance().getRiKeyOid(),
                            coupons_leftover - coupons_swapped);
                }
            }
        }
        // if still coupons are left over allocate them to the VMs that are still in the same template.
        // This will be ok since we are anyway recommending the VM to be moved to this RI.
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            if (smaMatch.getReservedInstance() == null) {
                float coupons_required = smaMatch.getTemplate().getCoupons();
                Optional<SMAReservedInstance> riWithCouponsLeft = findDiscountableRI(smaMatch,
                        riKeyOidToSMAReservedInstance, leftoverCoupons);
                float coupons_leftover = riWithCouponsLeft.isPresent()
                        ? leftoverCoupons.get(riWithCouponsLeft.get().getRiKeyOid())
                        : 0f;
                if (coupons_required > SMAUtils.EPSILON && coupons_leftover > SMAUtils.EPSILON) {
                        float coupons_swapped = Math.min(coupons_required, coupons_leftover);
                        smaMatch.setDiscountedCoupons(coupons_swapped);
                        smaMatch.setReservedInstance(riWithCouponsLeft.get());
                        leftoverCoupons.put(riWithCouponsLeft.get().getRiKeyOid(),
                                coupons_leftover - coupons_swapped);
                }
            }
        }
    }

    /**
     * Find the RI that can discount an undiscounted VM while staying at the current template.
     * @param smaMatch the match which is currently not associated with any RI
     * @param riKeyOidToSMAReservedInstance the map from ri key oid to SMAReservedInstance
     * @param leftoverCoupons map from Ri to coupons that are left after moveUncoveredVMBackToNaturalTemplate
     * @return an RI with leftover coupons that can discount the vm in smaMatch
     */
    private static Optional<SMAReservedInstance> findDiscountableRI(SMAMatch smaMatch,
                                           Map<Long, SMAReservedInstance> riKeyOidToSMAReservedInstance,
                                           Map<Long, Float> leftoverCoupons) {
        for (SMAReservedInstance reservedInstance : riKeyOidToSMAReservedInstance.values()) {
            if (leftoverCoupons.get(reservedInstance.getRiKeyOid()) < SMAUtils.EPSILON) {
                continue;
            }
            if (reservedInstance.isIsf()) {
                if (smaMatch.getTemplate().getFamily().equals(reservedInstance.getTemplate().getFamily())
                        && smaMatch.getVirtualMachine().mayBeCoveredByRI(reservedInstance)) {
                    return Optional.of(reservedInstance);
                }
            } else {
                if (smaMatch.getTemplate().getOid() == reservedInstance.getTemplate().getOid()
                        && smaMatch.getVirtualMachine().mayBeCoveredByRI(reservedInstance)
                        && smaMatch.getVirtualMachine().zoneCompatible(reservedInstance, new HashMap<>())) {
                    return Optional.of(reservedInstance);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * determine if the virtual machine in the smaMatch lost coverage while staying in same template.
     *
     * @param smaMatch smaMatch of interest
     * @return true if the virtual machine in the smaMatch lost coverage.
     */
    private static boolean isOutgoing(SMAMatch smaMatch) {
        SMAReservedInstance projectedRI = smaMatch.getReservedInstance();
        SMAReservedInstance sourceRI = smaMatch.getVirtualMachine().getCurrentRI();
        SMAVirtualMachine virtualMachine = smaMatch.getVirtualMachine();
        return (sourceRI != null
                && ((!sourceRI.isIsf() && virtualMachine.getCurrentTemplate().getOid()
                == smaMatch.getTemplate().getOid()) // same template for non ISF
                || (sourceRI.isIsf() && virtualMachine.getCurrentTemplate().getFamily()
                .equals(smaMatch.getTemplate().getFamily()))) // same family for ISF.
                && (projectedRI == null // lost coverage. vm did not use up any other RI.
                || ((sourceRI.getRiKeyOid() == projectedRI.getRiKeyOid())
                && (virtualMachine.getCurrentRICoverage()
                - smaMatch.getDiscountedCoupons() > SMAUtils.BIG_EPSILON)) //same RI lesser coupons
        ));
    }

    /**
     * determine if the virtual machine in the smaMatch gained coverage.
     * A vm moving from another RI is considered gained coverage even if the coverage is less.
     *
     * @param smaMatch smaMatch of interest
     * @return true if the virtual machine in the smaMatch gained coverage.
     */
    private static boolean isIncoming(SMAMatch smaMatch) {
        SMAReservedInstance projectedRI = smaMatch.getReservedInstance();
        SMAReservedInstance sourceRI = smaMatch.getVirtualMachine().getCurrentRI();
        SMAVirtualMachine virtualMachine = smaMatch.getVirtualMachine();
        return (projectedRI != null
                && (sourceRI == null //a vm which had 0 coverage now has some coverage.
                || sourceRI.getRiKeyOid() != projectedRI.getRiKeyOid() //a vm covered by another RI.
                || (smaMatch.getDiscountedCoupons()
                - virtualMachine.getCurrentRICoverage() > SMAUtils.EPSILON) //gained coverage from same RI.
        ));
    }
}



