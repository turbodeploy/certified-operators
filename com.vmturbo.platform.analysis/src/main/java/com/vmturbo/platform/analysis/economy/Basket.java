package com.vmturbo.platform.analysis.economy;

import static com.google.common.primitives.Longs.lexicographicalComparator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A set of commodity (type, quality) pairs a trader may try to buy or sell.
 *
 * <p>
 *  They are intended to be associated with a {@link Market} (baskets bought) or a seller (baskets
 *  sold).
 * </p>
 */
public class Basket implements Comparable<@NonNull @ReadOnly Basket> {
    // Fields
    private  @NonNull long[] commodityTypes_; // the numerical representations of the commodity types
        // comprising this basket. These are used only internally for performance. It must be sorted
        // in ascending order.

    // Constructors

    /**
     * Constructs a new Basket containing the given commodity types.
     *
     * @param commodityTypes The commodity types that will become the contents of the new basket.
     *                       They are copied.
     */
    public Basket(CommodityType... commodityTypes) {
        commodityTypes_ = new long[commodityTypes.length];
        for(int i = 0 ; i < commodityTypes.length ; ++i) {
            commodityTypes_[i] = commodityTypes[i].numericalRepresentation();
        }
        Arrays.sort(commodityTypes_);
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
     * Returns an unmodifiable list of the commodity types comprising {@code this} basket.
     */
    @Pure
    public final @NonNull @ReadOnly List<@NonNull @ReadOnly CommodityType> getCommodityTypes(@ReadOnly Basket this) {
        CommodityType[] result = new CommodityType[commodityTypes_.length];
        for(int i = 0 ; i < commodityTypes_.length ; ++i) {
            //result[i] = new CommodityType(commodityTypes_[i]);
        }
        return Collections.unmodifiableList(Arrays.asList(result));
    }

    @Override
    @Pure
    public final int compareTo(@NonNull @ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        return lexicographicalComparator().compare(commodityTypes_, other.commodityTypes_);
    }

} // end Basket interface
