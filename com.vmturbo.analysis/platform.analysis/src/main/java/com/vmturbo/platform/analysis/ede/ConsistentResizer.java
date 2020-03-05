package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.PartialResize;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.sdk.common.util.Pair;

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
     * @param p Pair containing the raw material commodity and the supplying Trader
     */
    void addResize(final Resize resize, final boolean engage,
                   final Pair<CommoditySold, Trader> p) {
        String key = makeResizeKey(resize.getResizedCommoditySpec(), resize.getSellingTrader());
        ResizingGroup rg = resizingGroups.get(key);
        if (rg == null) {
            rg = new ResizingGroup();
            resizingGroups.put(key, rg);
        }
        rg.addResize(resize, engage, p);
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
     * @param resizedCommodity
     * @param scalingGroupId
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
        private double maxCapacity = 0;
        private double maxOldCapacity = 0;

        // These are keyed by suppliers/rawMaterial.  counts tracks the number of traders
        // co-residing on each rawMaterial. availableHeadroom tracks the current headroom for
        // each raw material.  Add partial resizes are added to this group, the old capacity of
        // the resized commodity is returned to the headroom count.  Therefore the
        // availableHeadroom maps keeps a running total of the available headroom.
        private Map<CommoditySold, Integer> counts = new HashMap<>();
        private Map<CommoditySold, Double> availableHeadroom = new HashMap<>();
        private double capacityIncrement = 0L;

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
         * @param p a Pair containing the raw material and supplying Trader
         */
        void addResize(final Resize resize, final boolean engage,
                       final Pair<CommoditySold, Trader> p) {
            PartialResize pr = new PartialResize(resize, engage, p);
            CommoditySold commSold = pr.getRawMaterial();
            // Since all scaling group members share the same settings, we can get the capacity
            // increment out of the first resize in the group that has a valid one.
            if (capacityIncrement == 0L) {
                capacityIncrement = resize.getResizedCommodity()
                    .getSettings().getCapacityIncrement();
            }
            final double newCapacity = resize.getNewCapacity();
            final double oldCapacity = resize.getOldCapacity();
            maxCapacity = Math.max(maxCapacity, newCapacity);
            if (newCapacity > oldCapacity) {
                // We only set the max old capacity for resize ups, because we never want to set
                // a new capacity below the original capacity of a resize up.  If this condition
                // occurs, we abort all resizes in the scaling group.
                maxOldCapacity = Math.max(maxOldCapacity, oldCapacity);
            }

            // Need to identify entities that reside on the same supplier so that we don't reuse
            // excess capacity.  Release the existing resources.  After the max capacity is known
            // we will remove that amount and determine whether there are sufficient resources.
            counts.merge(commSold, 1, Integer::sum);
            availableHeadroom.compute(commSold, (k, v) -> {
                if (v == null) {
                    // New entry.  Since we want to keep track of the amount of headroom available
                    // in each rawMaterial, the initial entry in the map needs to add the current
                    // amount of headroom in the rawMaterial.  After that, we return the capacity
                    // of the resized commodity back to the rawMaterial.
                    double headroom = commSold == null ? Double.MAX_VALUE :
                        resize.getResizedCommodity().getSettings().getUtilizationUpperBound() *
                            (commSold.getEffectiveCapacity() - commSold.getQuantity());

                    return headroom + oldCapacity;
                } else {
                    // Update existing entry
                    return v + oldCapacity;
                }
            });

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

            /*
             * Subtract the new max capacity from each raw material. If any resulting headroom is
             * less than zero, then track the greatest deficit.  Since we are potentially adjusting
             * for multiple resizes, divide the adjustment by the number of resizes sharing the
             * same raw material.
             */
            double maxCapacityAdjustment = 0.0f;
            for (Map.Entry<CommoditySold, Double> e : availableHeadroom.entrySet()) {
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
                double headroom = availableHeadroom.get(e.getKey()) - maxCapacity * numConsumers;
                if (headroom < 0) {
                    // Need to adjust.  This will always be a negative number.  By maximum here, we
                    // are referring to the largest amount that we will need to decrease the new
                    // capacity.
                    maxCapacityAdjustment = Math.min(maxCapacityAdjustment,
                            headroom / numConsumers);
                }
            }

            double newCapacity = maxCapacity + maxCapacityAdjustment;
            if (maxOldCapacity > newCapacity) {
                // The consistent new capacity is less than one of the individual old capacities
                // that needed to size up, so abort all resizes in the scaling group.
                for (PartialResize pr : resizes) {
                    Trader trader = pr.getResize().getSellingTrader();
                    if (trader.isDebugEnabled() || logger.isTraceEnabled()) {
                        logger.info(
                            "Not resizing consistent scaling member {} due to insufficient {} on {}",
                            trader.getDebugInfoNeverUseInCode(),
                            pr.getResize().getResizedCommoditySpec().getDebugInfoNeverUseInCode(),
                            pr.getSupplier().getDebugInfoNeverUseInCode());
                    }
                }
                return;
            }

            final double finalNewCapacity = newCapacity;
            resizes.stream()
                .map(PartialResize::getResize)
                // Drop Resize if the capacity difference is less than the configured
                // capacity increment
                .filter(resize ->
                    Math.abs(resize.getOldCapacity() - finalNewCapacity) >= capacityIncrement)
                // Do not override resize ups if the new capacity is less than the existing capacity
                .filter(resize -> !(resize.getOldCapacity() < resize.getNewCapacity()
                    && resize.getOldCapacity() > finalNewCapacity))
                // Sort by oldCapacity, descending, in order to free up as much on the supplier
                // as possible as early as possible.
                .sorted((a, b) -> (int)(b.getOldCapacity() - a.getOldCapacity()))
                .forEach(resize -> {
                    resize.setNewCapacity(finalNewCapacity);
                    Resizer.takeAndAddResizeAction(actions, resize);
                });
        }
    }
}
