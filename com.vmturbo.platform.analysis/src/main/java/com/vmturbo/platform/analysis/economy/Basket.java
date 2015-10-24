package com.vmturbo.platform.analysis.economy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Ordering;

/**
 * A set of commodity specifications specifying the commodities a trader may try to buy or sell.
 *
 * <p>
 *  It is intended to be associated with a {@link Market} (baskets bought) or a seller (baskets
 *  sold).
 * </p>
 *
 * <p>
 *  Baskets are immutable objects.
 * </p>
 */
public final class Basket implements Comparable<@NonNull @ReadOnly Basket> {
    // Fields

    // An array holding the commodity specifications comprising this basket.
    // It must be sorted in ascending order and not contain duplicate elements.
    private final @NonNull @ReadOnly CommoditySpecification @NonNull @ReadOnly [] contents_;

    // Constructors

    /**
     * @see #Basket(Collection)
     */
    public Basket(@NonNull CommoditySpecification... contents) {
        this(Arrays.asList(contents));
    }

    /**
     * Constructs a new Basket containing the given commodity specifications.
     *
     * @param contents The commodity specifications that will become the contents of the new basket.
     *                 They are included themselves and not copied. Duplicate arguments are included
     *                 only once in the basket (those that come after the first in contents are
     *                 ignored).
     */
    public Basket(@NonNull @ReadOnly Collection<@NonNull @ReadOnly CommoditySpecification> contents) {
        // may need to change the following to a more efficient implementation.
        TreeSet<CommoditySpecification> set = new TreeSet<>(contents);
        contents_ = set.toArray(new CommoditySpecification[set.size()]);
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
     * <p>
     *  The current implementation returns {@code true} iff for every commodity specification in
     *  {@code this} there is a corresponding commodity specification in other that satisfies it.
     *  It assumes commodity types are sorted with type as the primary key.
     * </p>
     *
     * @param other the Basket to be tested against {@code this}.
     * @return {@code true} if {@code this} basket is satisfied by {@code other}.
     */
    @Pure
    public final boolean isSatisfiedBy(@ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        int otherIndex = 0;
        for(CommoditySpecification specification : contents_) {
            while(otherIndex < other.contents_.length && !specification.isSatisfiedBy(other.contents_[otherIndex])) {
                ++otherIndex;
            }
            if(otherIndex >= other.contents_.length)
                return false;
            ++otherIndex;
        }
        return true;
    }

    /**
     * Returns an unmodifiable list of the commodity specifications comprising {@code this} basket
     * in ascending order.
     */
    @Pure
    public final @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySpecification> getCommoditySpecifications(@ReadOnly Basket this) {
        return Collections.unmodifiableList(Arrays.asList(contents_));
    }

    // Some methods of Collection and List are included here, but not the complete implementation
    // of the interfaces.

    /**
     * @see Collection#size()
     */
    @Pure
    public final int size(@ReadOnly Basket this) {
        return contents_.length;
    }

    /**
     * @see Collection#isEmpty()
     */
    @Pure
    public final boolean isEmpty(@ReadOnly Basket this) {
        return size() == 0;
    }

    /**
     * @see List#indexOf(Object)
     */
    @Pure
    public final int indexOf(@ReadOnly Basket this, @NonNull @ReadOnly CommoditySpecification specificationToSearchFor) {
        // The elements of contents_ are unique so the first match will be the only match.
        return Math.max(-1,Arrays.binarySearch(contents_, specificationToSearchFor));
    }

    /**
     * @see List#lastIndexOf(Object)
     */
    @Pure
    public final int lastIndexOf(@ReadOnly Basket this, @NonNull @ReadOnly CommoditySpecification specificationToSearchFor) {
        // The elements of contents_ are unique so the first match will be the only match.
        return indexOf(specificationToSearchFor);
    }

    /**
     * @see Collection#contains(Object)
     */
    @Pure
    public final boolean contains(@ReadOnly Basket this, @NonNull @ReadOnly CommoditySpecification specificationToSearchFor) {
        return indexOf(specificationToSearchFor) != -1;
    }

    /**
     * Returns a new basket that contains all the commodity specifications in {@code this} plus the
     * given one.
     *
     * @param specificationToAdd The commodity specification that should be added to the new basket.
     *                           If it is already in the basket, it will be ignored.
     * @return A new basket that contains all of {@code this} basket's contents plus specificationToAdd.
     */
    @Pure
    public final @NonNull Basket add(@ReadOnly Basket this, CommoditySpecification specificationToAdd) {
        // TODO: improve efficiency. Perhaps even reuse instance if already contained.
        return new Basket(ObjectArrays.concat(contents_, specificationToAdd));
    }

    /**
     * Returns a new basket that contains all the commodity specifications in {@code this} except
     * the given one.
     *
     * @param specificationToRemove The commodity specification that should be removed from the new
     *                              basket. If it was not in the basket, it will be ignored.
     * @return A new basket that contains all of {@code this} basket's contents plus specificationToAdd.
     */
    @Pure
    public final @NonNull Basket remove(@ReadOnly Basket this, CommoditySpecification specificationToRemove) {
        // TODO: improve efficiency. Perhaps even reuse instance if already contained.
        @NonNull List<@NonNull CommoditySpecification> newContents = new ArrayList<>(getCommoditySpecifications());
        newContents.remove(specificationToRemove);

        return new Basket(newContents); // will remove the duplicate
    }

    // TODO: might be a good idea to define equals as well, although this shouldn't be necessary.

    /**
     * A total ordering on the Baskets to allow sorting and insertion into maps.
     *
     * <p>
     *  They are lexicographically compared using CommoditySpecification's natural ordering.
     * </p>
     */
    @Override
    @Pure
    public final int compareTo(@ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        // may need a more efficient implementation here.
        return Ordering.natural().lexicographical().compare(Arrays.asList(contents_), Arrays.asList(other.contents_));
    }

    /**
     * Returns a string representation of {@code this} basket as a list of commodity specifications
     * of the form [cs1, cs2, ... , csN].
     *
     * <p>
     *  This is included mostly for use by the assert* methods of JUnit so that they produce
     *  comprehensible error messages.
     * </p>
     */
    @Override
    public String toString() {
        return Arrays.deepToString(contents_);
    }

} // end Basket interface
