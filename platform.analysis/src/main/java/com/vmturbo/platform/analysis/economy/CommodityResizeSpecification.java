package com.vmturbo.platform.analysis.economy;

import java.util.function.DoubleBinaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class holds the values in Commodity Resize Dependency Map that
 * is keyed by commodity sold. It holds a dependent commodity that has
 * to be adjusted in event of a resize of the sold commodity.
 *
 */
public final class CommodityResizeSpecification {

    // Fields

    // The dependent commodity type
    private final @NonNull Long dependentCommodityType_;
    // The limit function used to adjust its value in case of resize
    private final @NonNull DoubleBinaryOperator limitFunction_;

    // Constructors

    /**
     * Build dependent commodity information.
     *
     * @param dependentCommodityType The commodity type.
     * @param limitFunction The limit function to be used to adjust it in case of resize.
     */
    public CommodityResizeSpecification(@NonNull Long dependentCommodityType,
                                        @NonNull DoubleBinaryOperator limitFunction) {
        dependentCommodityType_ = dependentCommodityType;
        limitFunction_ = limitFunction;
    }

    @NonNull
    public Long getCommodityType() {
        return dependentCommodityType_;
    }

    @NonNull
    public DoubleBinaryOperator getLimitFunction() {
        return limitFunction_;
    }
}
