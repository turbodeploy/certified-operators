package com.vmturbo.platform.analysis.utilities;

import java.util.HashMap;
import java.util.Map;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.economy.CommoditySold;

/**
 * A resizeActionStateTracker tracks the state of the usages of consumers from resold rawMaterials before the impact
 * of a resize is percolated down the supply chain.
 *
 *   App            App
 *    |C:100         |C:26
 *  c1|U:20          |U:20
 * Container     Container
 *    |C:100         |C:100
 *  c2|U:70          |U:26
 *   Pod            Pod
 *    |C:100         |C:100
 *  c3|U:70          |U:26
 *   VM             VM
 * When a commodity sold by a container that scales, we percolate the impact of the resize down the supply-chain.
 * commStateMapping holds: {c2, 70}, {c3, 70}
 */
public final class ResizeActionStateTracker {
    Map<CommoditySold, Pair<Double, Double>> commStateMapping;

    /**
     * Constructor.
     */
    public ResizeActionStateTracker() {
        this.commStateMapping = new HashMap<>();
    }

    /**
     * Add entry.
     *
     * @param commSold the commodity sold.
     * @param qnty the quantity.
     * @param peak the peak.
     */
    public void addEntry(CommoditySold commSold, Double qnty, Double peak) {
        this.commStateMapping.put(commSold, new Pair<Double, Double>(qnty, peak));
    }

    /**
     * Get the quantity.
     *
     * @param commSold the commodity sold.
     * @return the quantity.
     */
    public double getQuantity(CommoditySold commSold) {
        return commStateMapping.get(commSold).first;
    }

    /**
     * Get the peak.
     *
     * @param commSold the commodity sold.
     * @return the peak.
     */
    public double getPeakQuantity(CommoditySold commSold) {
        return commStateMapping.get(commSold).second;
    }

    /**
     * Get the entry value pair.
     *
     * @param commSold the commodity sold.
     * @return the pair.
     */
    public Pair<Double, Double> getUsageEntry(CommoditySold commSold) {
        return commStateMapping.get(commSold);
    }

} // end ResizeActionStateTracker class
