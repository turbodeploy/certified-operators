package com.vmturbo.platform.analysis.actions;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.ConsistentScalingNumber;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

public class PartialResize {
    private final Resize resize_;
    private final boolean resizeDueToROI_;
    private final Map<CommoditySold, Trader> rawMaterialsMap_ = new HashMap<>();

    private final double consistentScalingFactor;
    private final double providerConsistentScalingFactor;
    private ConsistentScalingNumber consistentScalingOldCapacity = null;

    public PartialResize(Resize resize, final boolean resizeDueToROI,
                         Map<CommoditySold, Trader> rawMaterialAndSupplier,
                         @Nonnull final Optional<RawMaterials> rawMaterialDescriptor) {
        this.resize_ = resize;
        this.resizeDueToROI_ = resizeDueToROI;
        rawMaterialsMap_.putAll(rawMaterialAndSupplier);
        final Optional<RawMaterials> filteredRawMaterials = rawMaterialDescriptor
            .filter(RawMaterials::requiresConsistentScalingFactor);

        consistentScalingFactor = filteredRawMaterials
            .map(d -> (double)resize.getSellingTrader().getSettings().getConsistentScalingFactor())
            .orElse(1.0);
        providerConsistentScalingFactor = filteredRawMaterials
            .map(this::computeProviderConsistentScalingFactor)
            .orElse(1.0);
    }

    public Resize getResize() {
        return resize_;
    }

    public boolean isResizeDueToROI() {
        return resizeDueToROI_;
    }

    public Map<CommoditySold, Trader> getRawMaterials() {
        return rawMaterialsMap_;
    }

    /**
     * Get the ConsistentScalingFactor for use on all resize capacity quantities
     * EXCEPT for newCapacity (ie use on oldCapacity, capacityIncrement, capacityBounds, etc.).
     *
     * @return The consistentScalingFactor from the trader whose commodity is being resized.
     */
    public double getConsistentScalingFactor() {
        return consistentScalingFactor;
    }

    /**
     * Get the consistentScalingFactor for use on the newCapacity quantity.
     *
     * @return The consistentScalingFactor from the trader providing to the trader being resized.
     *         When there is more than one provider based on raw materials, the provider is picked
     *         by the commodity best matching the commodity being resized.
     */
    public double getProviderConsistentScalingFactor() {
        return providerConsistentScalingFactor;
    }

    /**
     * Get a {@link ConsistentScalingNumber} representing the old capacity for the
     * contained resize action.
     *
     * @return a {@link ConsistentScalingNumber} representing the old capacity for the
     *         contained resize action.
     */
    public ConsistentScalingNumber getConsistentScalingOldCapacity() {
        if (consistentScalingOldCapacity == null) {
            consistentScalingOldCapacity = ConsistentScalingNumber.fromNormalizedNumber(
                resize_.getOldCapacity(), consistentScalingFactor);
        }
        return consistentScalingOldCapacity;
    }

    /**
     * Look up the consistent scaling factor on the provider to the trader being resized.
     * This is important in case the trader was moved before generating the resize because
     * the new capacity on the resize should be coordinated with the new provider, not
     * its original provider.
     * <p/>
     * The market has no concept of “principal” or “primary” raw material but we can get
     * what we need with some simple rules.
     * <p/>
     * 1. When there are co-materials (Container use case):
     *    Find the commodity with equivalent type from the resize’s raw materials and use the CSF from that.
     *    If there is no equivalent commodity, use the old supplier’s CSF.
     * 2. When there are not co-materials (VM use case):
     *    Use the CSF from the first commodity you see.
     *
     * @param rawMaterials The raw materials for the commodity being scaled.
     * @return The consistent scaling factor for the trader
     */
    private double computeProviderConsistentScalingFactor(@Nonnull final RawMaterials rawMaterials) {
        final int resizeCommBaseType = resize_.getResizedCommoditySpec().getBaseType();
        return rawMaterialsMap_.entrySet().stream()
            .filter(entry -> {
                final CommoditySold rawMaterial = entry.getKey();
                final Trader provider = entry.getValue();
                final int indexSold = provider.getCommoditiesSold().indexOf(rawMaterial);
                if (indexSold >= 0) {
                    final int rawMaterialBaseType = provider.getBasketSold().get(indexSold).getBaseType();
                    return rawMaterialBaseType == resizeCommBaseType
                        || !hasCoMaterials(rawMaterialBaseType, rawMaterials);
                }
                return false;
            }).findAny()
            // If no appropriate raw material provider is found. Return the CSF of the old provider.
            .map(entry -> (double)entry.getValue().getSettings().getConsistentScalingFactor())
            .orElse(consistentScalingFactor);
    }

    /**
     * Check whether a rawMaterialBaseType has a co-material on the RawMaterials.
     * When a particular raw material has no co-material it is the only possible match
     * in our search. (ie VM VCPU has no co-materials and only one raw material).
     *
     * @param rawMaterialBaseType The base type of the raw material commodity to check.
     * @param rawMaterials The raw materials for the resize.
     * @return Whether the particular raw material base type has a co-material.
     */
    private boolean hasCoMaterials(final int rawMaterialBaseType,
                                   @Nonnull final RawMaterials rawMaterials) {
        for (int i = 0; i < rawMaterials.getMaterials().length; i++) {
            if (rawMaterials.getMaterials()[i] == rawMaterialBaseType) {
                return rawMaterials.getCoMaterials()[i];
            }
        }
        return false;
    }
}
