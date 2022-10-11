package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;
import com.vmturbo.platform.analysis.utilities.DoubleNaryOperator;

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
public final class CommodityResizeSpecification implements Serializable {

    // Fields

    // The dependent commodity type
    private final @NonNull int dependentCommodityType_;
    // The function used to adjust its value in case of resize up
    private final @NonNull DoubleNaryOperator incrementFunction_;
    // The limit function used to adjust its value in case of resize down
    private final @NonNull DoubleNaryOperator decrementFunction_;

    // Constructors

    /**
     * Build dependent commodity information.
     *
     * @param dependentCommodityType The type of the commodity bought
     *                whose quantity should be adjusted when resizing the commodity sold.
     * @param incrementFunction The limit function to be used to adjust it in case of resize up.
     * @param decrementFunction The limit function to be used to adjust it in case of resize down.
     */
    public CommodityResizeSpecification(@NonNull int dependentCommodityType,
                                        @NonNull DoubleNaryOperator incrementFunction,
                                        @NonNull DoubleNaryOperator decrementFunction) {
        dependentCommodityType_ = dependentCommodityType;
        incrementFunction_ = incrementFunction;
        decrementFunction_ = decrementFunction;
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
     * Returns the function to be used to adjust the commodity bought in case of resize up.
     */
    public DoubleNaryOperator getIncrementFunction() {
        return incrementFunction_;
    }

    @NonNull
    /**
     * Returns the limit function to be used to adjust the commodity bought in case of resize down.
     */
    public DoubleNaryOperator getDecrementFunction() {
        return decrementFunction_;
    }

    /**
     * Tests whether two CommodityResizeSpecifications are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly CommodityResizeSpecification this,@ReadOnly Object other) {
        if (other == null || !(other instanceof CommodityResizeSpecification))
            return false;
        CommodityResizeSpecification otherResizeSpec = (CommodityResizeSpecification)other;
        return otherResizeSpec.getCommodityType() == dependentCommodityType_
                        && otherResizeSpec.getIncrementFunction() == incrementFunction_
                        && otherResizeSpec.getIncrementFunction() == decrementFunction_;
    }

    /**
     * Returns a strong hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(dependentCommodityType_)
                        .putInt(incrementFunction_.hashCode())
                        .putInt(decrementFunction_.hashCode()).hash().asInt();
    }
}
