package com.vmturbo.platform.analysis.economy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Ordering;

/**
 * A set of commodity specifications a trader may try to buy or sell.
 *
 * <p>
 *  It is intended to be associated with a {@link Market} (baskets bought) or a seller (baskets
 *  sold).
 * </p>
 */
public class Basket implements Comparable<@NonNull @ReadOnly Basket> {
    // Fields

    // The numerical representations of the commodity specifications comprising this basket.
    // These are used only internally for performance. It must be sorted in ascending order.
    // It must not contain duplicate elements.
    private  @NonNull CommoditySpecification @NonNull [] commodityTypes_;

    // Constructors

    /**
     * Constructs a new Basket containing the given commodity specifications.
     *
     * @param commodityTypes The commodity specifications that will become the contents of the new basket.
     *                       They are copied.
     */
    public Basket(CommoditySpecification... commodityTypes) {
        commodityTypes_ = new CommoditySpecification[commodityTypes.length];
        for(int i = 0 ; i < commodityTypes.length ; ++i) {
            commodityTypes_[i] = commodityTypes[i];
        }
        Arrays.sort(commodityTypes_);
        // TODO: assert that elements of commodityTypes_ are unique.
    }

    // Methods

    /**
     * Returns whether a buyer shopping for {@code this} basket, can be satisfied by a given basket.
     *
     * <p>
     *  e.g. a buyer buying CPU with 4 cores and memory, can be satisfied by a seller selling CPU
     *  with 8 cores, memory and some access commodities.
     * </p>
     *
     * @param other the Basket to be tested against {@code this}.
     * @return {@code true} if {@code this} basket is satisfied by {@code other}.
     */
    @Pure
    public final boolean isSatisfiedBy(@ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * Returns an unmodifiable list of the commodity specifications comprising {@code this} basket.
     */
    @Pure
    public final @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySpecification> getCommoditySpecifications(@ReadOnly Basket this) {
        CommoditySpecification[] result = new CommoditySpecification[commodityTypes_.length];
        for(int i = 0 ; i < commodityTypes_.length ; ++i) {
            //result[i] = new CommoditySpecification(commodityTypes_[i]);
        }
        return Collections.unmodifiableList(Arrays.asList(result));
    }

    @Override
    @Pure
    public final int compareTo(@NonNull @ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        return Ordering.natural().lexicographical().compare(Arrays.asList(commodityTypes_), Arrays.asList(other.commodityTypes_));
    }

    @Pure
    public final int size(@ReadOnly Basket this) {
        return commodityTypes_.length;
    }

    @Pure
    public final boolean isEmpty(@ReadOnly Basket this) {
        return size() == 0;
    }

    @Pure
    public final int indexOf(@ReadOnly Basket this, CommoditySpecification elementToSearchFor) {
        // The elements of commodityTypes_ are unique so the first match will be the only match.
        return Math.max(-1,Arrays.binarySearch(commodityTypes_, elementToSearchFor));
    }

    @Pure
    public final int lastIndexOf(@ReadOnly Basket this, CommoditySpecification elementToSearchFor) {
        // The elements of commodityTypes_ are unique so the first match will be the only match.
        return indexOf(elementToSearchFor);
    }

    @Pure
    public final boolean contains(@ReadOnly Basket this, CommoditySpecification elementToSearchFor) {
        return indexOf(elementToSearchFor) != -1;
    }

} // end Basket interface
