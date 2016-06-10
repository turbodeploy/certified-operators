package com.vmturbo.platform.analysis.economy;

import java.util.function.DoubleBinaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Holds the values in Commodity Resize Dependency Map that
 * is keyed by commodity sold type. It has a dependent commodity (bought or sold) that has
 * to be adjusted in event of a resize of the sold commodity.
 * <p>
 * E.g., when we resize the vMem capacity (sold) of a VM, we also need to resize the quantity
 * of the Memory bought by the VM, as well as the Memory Provisioned bought by the VM. Or,
 * when in the future we resize Memory (sold) of a host, we also need to resize the Memory
 * Provisioned (sold) of the host.
 *
 */
public final class CommodityResizeSpecification {

    // Fields

    // The dependent commodity type
    private final @NonNull int dependentCommodityType_;
    // The limit function used to adjust its value in case of resize
    private final @NonNull DoubleBinaryOperator limitFunction_;

    // Constructors

    /**
     * Build dependent commodity information.
     *
     * @param dependentCommodityType The type of the commodity bought
     *                whose quantity should be adjusted when resizing the commodity sold.
     * @param limitFunction The limit function to be used to adjust it in case of resize.
     */
    public CommodityResizeSpecification(@NonNull int dependentCommodityType,
                                        @NonNull DoubleBinaryOperator limitFunction) {
        dependentCommodityType_ = dependentCommodityType;
        limitFunction_ = limitFunction;
    }

    /**
     *
     * @return The type of the commodity bought whose quantity should be adjusted
     *          when resizing the commodity sold.
     */
    public int getCommodityType() {
        return dependentCommodityType_;
    }

    @NonNull
    /**
     *
     * @return The limit function to be used to adjust the commodity bought in case of resize.
     */
    public DoubleBinaryOperator getLimitFunction() {
        return limitFunction_;
    }
}
