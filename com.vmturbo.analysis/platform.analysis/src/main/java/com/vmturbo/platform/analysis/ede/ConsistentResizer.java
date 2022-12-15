package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.ede.ConsistentScalingNumber.fromNormalizedNumber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.PartialResize;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.RawMaterialMetadata;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.Trader;

public class ConsistentResizer {
    static final Logger logger = LogManager.getLogger(Resizer.class);

    private Map<String, ResizingGroup> resizingGroups;

    public ConsistentResizer() {
        resizingGroups = new HashMap<>();
    }

    /**
     * Add an untaken Resize action to the appropriate scaling group for later evaluation.
     * @param resize Resize action
     * @param engage True if the Resize would have been generated even if it weren't in a scaling
     *               group.
     * @param pairs Pairs containing the raw material commodity and the supplying Trader
     * @param rawMaterialDescriptor An optional descriptor of the raw materials for the resize.
     */
    void addResize(final Resize resize, final boolean engage,
                   final Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> pairs,
                   @Nonnull final Optional<RawMaterials> rawMaterialDescriptor) {
        String key = makeResizeKey(resize.getResizedCommoditySpec(), resize.getSellingTrader());
        ResizingGroup rg = resizingGroups.get(key);
        if (rg == null) {
            rg = new ResizingGroup();
            resizingGroups.put(key, rg);
        }
        rg.addResize(resize, engage, pairs, rawMaterialDescriptor);
    }

    /**
     * For each resize action for each commodity in each scaling group, find a suitable new
     * capacity and generate Resize actions for each.  If the underlying raw materials are
     * insufficient to allow consistent scaling, the new capacity will be either reduced, or if
     * the resulting new capacity would be less than the existing capacities of the group
     * members, the resizes will be dropped.
     * @param actions action list to add add Resizes to
     */
    public void resizeScalingGroupCommodities(final List<Action> actions) {
        resizingGroups.values().stream().forEach(rg -> rg.generateResizes(actions));
    }

    /**
     * Create a key based on the resized commodity and group ID.  This is used to place Resize
     * actions that need to be consistently resized in per-group per-commodity buckets.
     * @param resizedCommodity commoditySpecification used for generating the key.
     * @param trader the trader from which we extract the scalingGroupId.
     * @return key
     */
    private static String makeResizeKey(final CommoditySpecification resizedCommodity,
                                        final Trader trader) {
        return trader.getScalingGroupId() + resizedCommodity.getType();
    }

    /*
     * A ResizingGroup is used to maintain a list of PartialResizes for a scaling group.  It also
     * tracks the maximum capacity for each group as entries are added.
     */
    class ResizingGroup {
        private List<PartialResize> resizes = new ArrayList<>();
        private ConsistentScalingNumber maxCapacity = ConsistentScalingNumber.ZERO;
        private ConsistentScalingNumber maxOldCapacity = ConsistentScalingNumber.ZERO;

        // These are keyed by suppliers/rawMaterial.  counts tracks the number of traders
        // co-residing on each rawMaterial. availableHeadroom tracks the current headroom for
        // each raw material.  Add partial resizes are added to this group, the old capacity of
        // the resized commodity is returned to the headroom count.  Therefore the
        // availableHeadroom maps keeps a running total of the available headroom.
        private Map<CommoditySold, Integer> counts = new HashMap<>();
        private Map<CommoditySold, ConsistentScalingNumber> availableHeadroom = new HashMap<>();
        private Map<CommoditySold, RawMaterialMetadata> rawMaterialToMetadataMapping = new HashMap<>();
        private ConsistentScalingNumber capacityIncrement;

        public ResizingGroup() {
        }

        /**
         * Add an untaken Resize action to this scaling group while tracking limits and
         * maximum capacity.  It also maintains a list of Resizes that share the same supplier
         * so that we can avoid over allocation of its raw materials.
         * *
         * @param resize Resize action to add
         * @param engage True if this Resize was initiated due to ROI calculations and the new
         *               capacity is different from the original capacity.
         * @param pairs Map containing the raw material commodity and the supplying Trader
         * @param rawMaterialDescriptor An optional descriptor of the raw materials for the resize.
         */
        void addResize(final Resize resize, final boolean engage,
                       final Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> pairs,
                       @Nonnull final Optional<RawMaterials> rawMaterialDescriptor) {
            PartialResize pr = new PartialResize(resize, engage, pairs, rawMaterialDescriptor);
            // Ideally the capacityIncrement should be identical for all members of the scaling group.
            // However, if the providers have different speeds, this could lead to a different capacityIncrement
            // in consistent units. Pick the smallest to try to get the most consistent results across
            // market cycles when nothing changes. Long-term, the capacity increments should actually
            // be consistent in the consistent units. Today they are only consistent in normalized units.
            if (capacityIncrement == null) {
                capacityIncrement = fromNormalizedNumber(
                    resize.getResizedCommodity().getSettings().getCapacityIncrement(),
                    pr.getConsistentScalingFactor());
            } else {
                capacityIncrement = ConsistentScalingNumber.min(capacityIncrement,
                    fromNormalizedNumber(resize.getResizedCommodity().getSettings().getCapacityIncrement(),
                        pr.getConsistentScalingFactor()));
            }

            resizes.add(pr);
        }

        void generateResizes(final List<Action> actions) {
            // If the group has a single Resize and the Resize was triggered by ROI, then treat
            // this as a normal Resize.  This will cover keyed commodities such as VStorage, which
            // we do not attempt to consistently resize.  If it's the only Resize in the group
            // and it was not triggered by ROI, then drop the action.
            if (resizes.size() == 1) {
                PartialResize pr = resizes.get(0);
                if (pr.isResizeDueToROI()) {
                    Resizer.takeAndAddResizeAction(actions, pr.getResize());
                }
                return;
            }
            CommoditySold congestedRawMaterial = null;
            ConsistentScalingNumber minUpperBound = ConsistentScalingNumber.MAX_VALUE;
            ConsistentScalingNumber maxLowerBound = ConsistentScalingNumber.ZERO;

            // iterate over the partial resizes and recompute the available overhead.
            for (PartialResize partial : resizes) {
                final double csf = partial.getConsistentScalingFactor();
                final ConsistentScalingNumber newCapacity =
                    fromNormalizedNumber(partial.getResize().getNewCapacity(),
                        partial.getProviderConsistentScalingFactor());
                final ConsistentScalingNumber oldCapacity = partial.getConsistentScalingOldCapacity();
                CommoditySoldSettings resizedCommSettings = partial.getResize().getResizedCommodity().getSettings();
                minUpperBound = ConsistentScalingNumber.min(minUpperBound,
                    fromNormalizedNumber(resizedCommSettings.getCapacityUpperBound(), csf));
                maxLowerBound = ConsistentScalingNumber.max(maxLowerBound,
                    fromNormalizedNumber(resizedCommSettings.getCapacityLowerBound(), csf));
                maxCapacity = ConsistentScalingNumber.max(maxCapacity, newCapacity);
                if (newCapacity.isGreaterThan(oldCapacity)) {
                    // We only set the max old capacity for resize ups, because we never want to set
                    // a new capacity below the original capacity of a resize up.  If this condition
                    // occurs, we abort all resizes in the scaling group.
                    maxOldCapacity = ConsistentScalingNumber.max(maxOldCapacity, oldCapacity);
                }

                partial.getRawMaterials().forEach((rawMaterialInfo, rawMaterialCommMapping) -> {
                    CommoditySold commSold = rawMaterialCommMapping.first;
                    Trader seller = rawMaterialCommMapping.second;
                    // Need to identify entities that reside on the same supplier so that we don't reuse
                    // excess capacity.  Release the existing resources.  After the max capacity is known
                    // we will remove that amount and determine whether there are sufficient resources.
                    counts.merge(commSold, 1, Integer::sum);
                    availableHeadroom.compute(commSold, (k, v) -> {
                        if (v == null) {
                            // New entry. Since we want to keep track of the amount of headroom available
                            // in each rawMaterial, the initial entry in the map needs to add the current
                            // amount of headroom in the rawMaterial.  After that, we return the capacity
                            // of the resized commodity back to the rawMaterial.
                            // Get seller consistentScalingFactor only if given partial resize requires
                            // consistentScalingFactor.
                            final float sellerConsistentScalingFactor = partial.requiresConsistentScalingFactor()
                                ? seller.getSettings().getConsistentScalingFactor() : 1f;
                            ConsistentScalingNumber headroom = commSold == null
                                ? ConsistentScalingNumber.MAX_VALUE
                                : fromNormalizedNumber(
                                partial.getResize().getResizedCommodity().getSettings().getUtilizationUpperBound()
                                    * (commSold.getEffectiveCapacity() - commSold.getQuantity()),
                                    sellerConsistentScalingFactor);
                            rawMaterialToMetadataMapping.put(commSold, rawMaterialInfo);
                            return headroom.plus(oldCapacity);
                        } else {
                            // Update existing entry
                            return v.plus(oldCapacity);
                        }
                    });
                });
            }
            if (maxLowerBound.isGreaterThan(minUpperBound)) {
                logger.error("Skipping resize generation for {} because the lowerBounds exceeds the upperBound.",
                        resizes.get(0).getResize().getSellingTrader().getScalingGroupId());
                return;
            }
            // make sure we dont exceed the upperbound
            maxCapacity = ConsistentScalingNumber.min(maxCapacity, minUpperBound);
            // make sure we meet the lowerbound
            // update maxLowerBound to the smallest multiple of capacityIncrement larger than
            // current maxLowerBound value to make sure finalNewCapacity is always larger than lower
            // bound when resizing down. Ensure this updated lower bound is not above the upper bound.
            maxLowerBound = maxLowerBound.dividedBy(capacityIncrement)
                .approxCeiling()
                .times(capacityIncrement);
            maxLowerBound = ConsistentScalingNumber.min(maxLowerBound, minUpperBound);
            maxCapacity = ConsistentScalingNumber.max(maxCapacity, maxLowerBound);

            /*
             * Subtract the new max capacity from each raw material. If any resulting headroom is
             * less than zero, then track the greatest deficit.  Since we are potentially adjusting
             * for multiple resizes, divide the adjustment by the number of resizes sharing the
             * same raw material.
             */
            ConsistentScalingNumber maxCapacityAdjustment = ConsistentScalingNumber.ZERO;
            ConsistentScalingNumber maxCapacityAdjustmentForNonExecutableAction = ConsistentScalingNumber.ZERO;
            for (Map.Entry<CommoditySold, ConsistentScalingNumber> e : availableHeadroom.entrySet()) {
                int numConsumers = counts.get(e.getKey());
                /* Determine how much headroom would be available if we scaled all members of the
                 * scaling group to their new capacities.  If this is less than zero, then there
                 * is insufficient capacity, and we will need to reduce the new capacity.
                 *
                 * For example, if the raw material has 100 headroom, and we have two group members
                 * consuming from it whose resizes are as follows:
                 *
                 *   1. Resize 20 -> 100 (increase 80)
                 *   2. Resize 30 -> 100 (increase 70)
                 *
                 * After releasing the existing capacities, we have 150 total headroom, and we need
                 * to allocate 200 for the new capacity.  The headroom calculation below will result
                 * in -50, so we need to adjust the resize amount by (-50 / 2) = -25.  That will
                 * result in a new capacity of (100 - 25) = 75.  The new resizes will then be:
                 *
                 *   1. Resize 20 -> 75 (increase 55)
                 *   2. Resize 30 -> 75 (increase 45)
                 */
                ConsistentScalingNumber headroom =
                    availableHeadroom.get(e.getKey()).minus(
                        maxCapacity.timesFactor(numConsumers));
                if (headroom.isLessThan(ConsistentScalingNumber.ZERO)) {
                    // Need to adjust. This will always be a negative number.  By maximum here, we
                    // are referring to the largest amount that we will need to decrease the new
                    // capacity.
                    ConsistentScalingNumber availableHeadroomPerConsumer = headroom.dividedByFactor(numConsumers);
                    // for resize down actions maxOldCapacity is zero
                    if (maxOldCapacity.equals(ConsistentScalingNumber.ZERO))
                    {
                        // for a resize down action if the computed headroom for any host is less
                        // than zero abort the resize action for the consistent scaling
                        // group as the host does not have enough available headroom to accomodate
                        // the VM's resize
                        logger.warn("Skipping resize generation for {} because of insuffucient headroom",
                                resizes.get(0).getResize().getSellingTrader().getScalingGroupId());
                        return;

                    }
                    if (availableHeadroomPerConsumer.isLessThan(maxCapacityAdjustment)) {
                        maxCapacityAdjustment = availableHeadroomPerConsumer;
                        congestedRawMaterial = e.getKey();
                    }
                    // update capacityAdjustment for non-executable action generation.
                    if (availableHeadroomPerConsumer
                            .isLessThan(maxCapacityAdjustmentForNonExecutableAction)
                            && rawMaterialToMetadataMapping.get(e.getKey()).isHardConstraint()) {
                        maxCapacityAdjustmentForNonExecutableAction = availableHeadroomPerConsumer;
                    }
                }
            }

            ConsistentScalingNumber newCapacity = maxCapacity.plus(maxCapacityAdjustment);
            ConsistentScalingNumber unboundedNewCapacity = maxCapacity
                    .plus(maxCapacityAdjustmentForNonExecutableAction);

            boolean isUnboundedCapDifferent = !newCapacity.approxEquals(unboundedNewCapacity);
            // if the newCapacity is lower than the maxLowerBound, stop generating an action.
            if (newCapacity.isLessThan(maxLowerBound)) {
                if (isUnboundedCapDifferent && unboundedNewCapacity.isGreaterThan(maxLowerBound)) {
                    newCapacity = unboundedNewCapacity;
                } else {
                    logger.warn("Skipping resize generation for {} because the newCapacity of "
                                    + "{} exceeds the maxlowerBound of {}.",
                            resizes.get(0).getResize().getSellingTrader().getScalingGroupId(),
                            newCapacity, maxLowerBound);
                    return;
                }

            }

            // If consistent resize actions are non-executable resize up with unboundedNewCapacity
            // as new capacity.
            final boolean isNonExecutableResizeUp =
                    isUnboundedCapDifferent && maxOldCapacity.isLessThan(unboundedNewCapacity);

            if (maxOldCapacity.isGreaterThan(newCapacity)) {
                // if we are unable to generate an executable scale UP, try generating a
                // non-executable action that fits the bounds.
                if (isNonExecutableResizeUp) {
                    newCapacity = unboundedNewCapacity;
                } else {
                    // The consistent new capacity is less than one of the individual old capacities
                    // that needed to size up, so abort all resizes in the scaling group.
                    for (PartialResize pr : resizes) {
                        Trader trader = pr.getResize().getSellingTrader();
                        if (trader.isDebugEnabled() || logger.isTraceEnabled()) {
                            logger.info(
                                    "Not resizing consistent scaling member {} due to insufficient {} on {}",
                                    trader.getDebugInfoNeverUseInCode(),
                                    pr.getResize().getResizedCommoditySpec().getDebugInfoNeverUseInCode(),
                                    pr.getRawMaterialToSellerMapping().get(congestedRawMaterial).getDebugInfoNeverUseInCode());
                        }
                    }
                    return;
                }
            }
            ConsistentScalingNumber tempNewCapacity = newCapacity;

            // Note that there could be corner cases where after the filter, in the same consistent
            // scaling group, some entities have resize actions, while others remain same capacity.
            List<PartialResize> newResizes = resizes.stream()
                    // Drop Resize if the capacity difference between tempNewCapacity and old capacity
                    // is less than the configured capacity increment
                    .filter(pr -> isCapacityIncrementSatisfied(pr.getConsistentScalingOldCapacity(), tempNewCapacity))
                    .collect(Collectors.toList());
            // If capacityIncrement is not satisfied by any executable resizes based on tempNewCapacity,
            // and if isNonExecutableResizeUp is true, filter resize actions based on unboundedNewCapacity.
            // If existing, these resize actions with unboundedNewCapacity as new capacity will be
            // eventually marked as non-executable in ActionClassifier.
            if (newResizes.isEmpty() && isNonExecutableResizeUp) {
                newResizes = resizes.stream()
                        .filter(pr -> isCapacityIncrementSatisfied(pr.getConsistentScalingOldCapacity(), unboundedNewCapacity))
                        .collect(Collectors.toList());
                newCapacity = unboundedNewCapacity;
            }

            final ConsistentScalingNumber integralIncrementCount = newCapacity
                .dividedBy(capacityIncrement)
                .approxFloor();
            final ConsistentScalingNumber finalNewCapacity = integralIncrementCount.times(capacityIncrement);
            // prevent scale down when target is not eligible for resize down.
            if (resizes.stream()
                    .anyMatch(pr -> pr.getConsistentScalingOldCapacity().isGreaterThan(finalNewCapacity)
                        && !pr.getResize().getSellingTrader().getSettings().isEligibleForResizeDown())) {
                return;
            }
            newResizes.stream()
                // Do not override resize ups if the new capacity is less than the existing capacity
                .filter(pr -> {
                    final ConsistentScalingNumber newConsistentScalingCapacity = ConsistentScalingNumber
                        .fromNormalizedNumber(pr.getResize().getNewCapacity(), pr.getProviderConsistentScalingFactor());
                    return !(pr.getConsistentScalingOldCapacity().isLessThan(newConsistentScalingCapacity)
                        && pr.getConsistentScalingOldCapacity().isGreaterThan(finalNewCapacity));
                })
                // Sort by oldCapacity, descending, in order to free up as much on the supplier
                // as possible as early as possible.
                .sorted((a, b) -> (int)(b.getConsistentScalingOldCapacity()
                    .minus(a.getConsistentScalingOldCapacity()))
                    .inConsistentUnits())
                .forEach(pr -> {
                    pr.getResize().setNewCapacity(finalNewCapacity
                        // We inverse-scale from consistent scaling number to normalized MHz using
                        // the ORIGINAL consistentScalingFactor on the selling trader. This is to
                        // be consistent with TopologyConverter.commSoldTOtoCommSoldDTO later on
                        // where we further inverse-scale from normalized MHz to milli-cores using
                        // the ORIGINAL cpuScalingFactor.
                        .inNormalizedUnits(pr.getConsistentScalingFactor()));
                    Resizer.takeAndAddResizeAction(actions, pr.getResize());
                });
        }

        /**
         * Check if the difference between given old capacity and new capacity is larger than or equal
         * to the configured capacityIncrement.
         *
         * @param oldCapacity Given old capacity of resize action.
         * @param newCapacity Given new capacity of resize action.
         * @return True if the difference between given old capacity and new capacity is larger than
         * or equal to capacityIncrement.
         */
        private boolean isCapacityIncrementSatisfied(@Nonnull final ConsistentScalingNumber oldCapacity,
                                                     @Nonnull final ConsistentScalingNumber newCapacity) {
            return oldCapacity
                    .minus(newCapacity)
                    .abs()
                    .isGreaterThanOrApproxEqual(capacityIncrement);
        }
    }
}
