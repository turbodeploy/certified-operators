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
            postProcessing(outputContext);
            outputContexts.add(outputContext);
            for (SMAMatch match : outputContext.getMatches()) {
                if ((match.getVirtualMachine().getCurrentTemplate() != match.getTemplate())
                        || (Math.abs(match.getVirtualMachine().getCurrentRICoverage()
                        - match.getProjectedRICoverage()) > SMAUtils.EPSILON)) {
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
     */
    public static void postProcessing(SMAOutputContext outputContext) {
        Map<Long, List<SMAMatch>> matchesWithOutgoingCoupons = new HashMap<>();
        Map<Long, List<SMAMatch>> matchesWithIncomingCoupons = new HashMap<>();
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            // effective discounted coupons is initialised to discounted coupons.
            smaMatch.setProjectedRICoverage(smaMatch.getDiscountedCoupons());
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
                float coupons_required = outgoingCouponMatch.getVirtualMachine()
                        .getCurrentRICoverage()
                        - outgoingCouponMatch.getProjectedRICoverage();
                for (SMAMatch incomingCouponMatch : matchesWithIncomingCoupons.get(riKeyId)) {
                    float coupons_grabbed;
                    if (incomingCouponMatch.getVirtualMachine().getCurrentRI() == null
                            || incomingCouponMatch.getVirtualMachine().getCurrentRI()
                            .getRiKeyOid() != riKeyId) {
                        // If the VM is not previously covered
                        // by this ri then all coupons can be taken.
                        coupons_grabbed = Math.min(coupons_required, incomingCouponMatch.getProjectedRICoverage());
                    } else {
                        // If the VM is previously covered by the same RI
                        // then we can take upto the coupons which was previously covered.
                        // Since covered by same RI incomingCouponMatch is
                        // discounted more than previously discounted.
                        coupons_grabbed = Math.min(coupons_required,
                                incomingCouponMatch.getProjectedRICoverage()
                                        - incomingCouponMatch.getVirtualMachine()
                                        .getCurrentRICoverage());
                    }
                    if (coupons_grabbed > SMAUtils.EPSILON) {
                        coupons_required = coupons_required - coupons_grabbed;
                        incomingCouponMatch.setProjectedRICoverage(incomingCouponMatch.getProjectedRICoverage()
                                - coupons_grabbed);
                    }
                    if (coupons_required < SMAUtils.EPSILON) {
                        break;
                    }
                }
                outgoingCouponMatch.setProjectedRICoverage(outgoingCouponMatch
                        .getVirtualMachine().getCurrentRICoverage() - coupons_required);
                outgoingCouponMatch.setReservedInstance(outgoingCouponMatch.getVirtualMachine()
                        .getCurrentRI());
            }
        }

        // get all the matches that involve RIs. Group together the ASG.
        Map<String, Set<SMAMatch>> matchByASG = new HashMap<>();
        Set<SMAMatch> nonASGMatch = new HashSet();
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
        for (SMAMatch smaMatch : nonASGMatch) {
            if (smaMatch.getReservedInstance() != null) {
                float saving = smaMatch.getReservedInstance()
                        .computeSaving(smaMatch.getVirtualMachine(),
                                new HashMap<>(), smaMatch.getProjectedRICoverage());
                if (saving < SMAUtils.EPSILON) {
                    smaMatch.setReservedInstance(null);
                    smaMatch.setProjectedRICoverage(0);
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
            // at least one member is convered.
            if (matchWithCoverage.isPresent()) {
                for (SMAMatch smaMatch : smaMatches) {
                    saving += matchWithCoverage.get().getReservedInstance()
                            .computeSaving(smaMatch.getVirtualMachine(),
                                    new HashMap<>(), smaMatch.getProjectedRICoverage());
                }
                if (saving < SMAUtils.EPSILON) {
                    for (SMAMatch smaMatch : smaMatches) {
                        smaMatch.setReservedInstance(null);
                        smaMatch.setProjectedRICoverage(0);
                        smaMatch.setTemplate(smaMatch.getVirtualMachine().getNaturalTemplate());
                    }
                }
            }
        }

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
                && virtualMachine.getCurrentTemplate().getOid()
                == smaMatch.getTemplate().getOid() // same template.
                && (projectedRI == null // lost coverage. vm did not use up any other RI.
                || ((sourceRI.getRiKeyOid() == projectedRI.getRiKeyOid())
                && (virtualMachine.getCurrentRICoverage()
                - smaMatch.getProjectedRICoverage() > SMAUtils.EPSILON)) //same RI lesser coupons
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
                || (smaMatch.getProjectedRICoverage()
                - virtualMachine.getCurrentRICoverage() > SMAUtils.EPSILON) //gained coverage from same RI.
        ));
    }
}



