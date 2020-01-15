package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

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
// TODO: investigate the effect of having two different commodity specifications with the same type
// in the same basket.
public final class Basket implements Comparable<@NonNull @ReadOnly Basket>, Iterable<@NonNull @ReadOnly CommoditySpecification>, Serializable {
    // Fields

    // An array holding the commodity specifications comprising this basket.
    // It must be sorted in ascending order and not contain duplicate elements.
    private final @NonNull @ReadOnly CommoditySpecification @NonNull @ReadOnly [] contents_;

    // Constructors

    /**
     * @see #Basket(Collection)
     */
    public Basket(@NonNull @ReadOnly CommoditySpecification... contents) {
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

    /**
     * Constructs a new Basket containing the commodity specifications sold by origBasket
     *
     * @param origBasket The basket whose commodity specifications that will become the contents of
     *                   the new basket.
     */
    public Basket(@NonNull @ReadOnly Basket origBasket) {
        this(Arrays.asList(origBasket.contents_));
    }

    // Methods

    /**
     * Returns whether a buyer shopping for {@code this} basket, is a subset of a given basket.
     *
     * <p>
     *  The current implementation returns {@code true} iff for every commodity specification in
     *  {@code this} there is a corresponding commodity specification in other that equals it in type.
     *  It assumes commodity types are sorted with type as the primary key.
     * </p>
     *
     * @param other the Basket to be tested against {@code this}.
     * @return {@code true} if {@code this} basket is a subset of the {@code other}.
     */
    @Pure
    public final boolean isSatisfiedBy(@ReadOnly Basket this, @NonNull @ReadOnly Basket other) {
        int otherIndex = 0;
        for(CommoditySpecification specification : contents_) {
            while(otherIndex < other.contents_.length && !specification.equals(other.contents_[otherIndex])) {
                ++otherIndex;
            }
            if (otherIndex >= other.contents_.length) {
                return false;
            }
            ++otherIndex;
        }
        return true;
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
     * @see List#get(int)
     */
    @Pure
    public final CommoditySpecification get(@ReadOnly Basket this, int index) {
        return contents_[index];
    }

    /**
     * @see List#indexOf(Object)
     */
    @Pure
    public final int indexOf(@ReadOnly Basket this, @NonNull @ReadOnly CommoditySpecification specificationToSearchFor) {
        // The elements of contents_ are unique so the first match will be the only match.
        int i = Math.max(-1,Arrays.binarySearch(contents_, specificationToSearchFor));

        return i;
    }

    /**
     * @see List#indexOf(Object)
     */
    @Pure
    public final int indexOf(@ReadOnly Basket this, @NonNull @ReadOnly int type) {
        // TODO: make indexOf return 2 values min and the maxIndex. All comm's btw these indices will be of this type
        // The elements of contents_ are unique so the first match will be the only match.
        return Math.max(-1,Arrays.binarySearch(contents_,type, (x,y)->((CommoditySpecification)x).getType() - (Integer)y));
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
     * @see List#indexOf(Object)
     */
    @Pure
    public final int indexOfBaseType(@ReadOnly Basket this, @NonNull @ReadOnly int baseType) {
        // search for index corresponding to the baseType, we can not use binary search because
        // the basket is sorted only based on type not the baseType.
        for (int i = 0; i < contents_.length; i++) {
            if (contents_[i].getBaseType() == baseType) {
                return i;
            }
        }
        return -1;
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
     * @return A new basket that contains all of {@code this} basket's contents except specificationToRemove.
     */
    @Pure
    public final @NonNull Basket remove(@ReadOnly Basket this, CommoditySpecification specificationToRemove) {
        // TODO: improve efficiency. Perhaps even reuse instance if already contained.
        @NonNull List<@NonNull CommoditySpecification> newContents = new ArrayList<>(size());

        for (CommoditySpecification specification : contents_) {
            if (!specification.equals(specificationToRemove)) {
                newContents.add(specification);
            }
        }

        return new Basket(newContents); // will remove the duplicate
    }

    // TODO: might be a good idea to define equals as well, although this shouldn't be necessary.

    /**
     * @see Iterable#iterator()
     */
    @Override
    @SideEffectFree
    public Iterator<@NonNull @ReadOnly CommoditySpecification> iterator(@ReadOnly Basket this) {
        return new Iterator<@NonNull @ReadOnly CommoditySpecification>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < contents_.length;
            }

            @Override
            public @NonNull @ReadOnly CommoditySpecification next() {
                return contents_[index++];
            }
        }; // end Iterator implementation
    }

    /**
     * @see Iterable#iterator()
     */
    @SideEffectFree
    public Iterator<@NonNull @ReadOnly CommoditySpecification> reverseIterator(@ReadOnly Basket this) {
        return new Iterator<@NonNull @ReadOnly CommoditySpecification>() {
            private int index = contents_.length - 1;

            @Override
            public boolean hasNext() {
                return index > -1;
            }

            @Override
            public @NonNull @ReadOnly CommoditySpecification next() {
                return contents_[index--];
            }
        }; // end Iterator implementation
    }

    /**
     * Returns a sequential Stream of {@link CommoditySpecification}'s with this collection as its source.
     */
    @SideEffectFree
    public Stream<@NonNull @ReadOnly CommoditySpecification> stream(@ReadOnly Basket this) {
        return Arrays.stream(contents_);
    }

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
    public String toString(@ReadOnly Basket this) {
        return Arrays.deepToString(contents_);
    }

    /**
     * Returns a string representation of {@code this} basket as a list of commodity
     * specifications in the form of debug info, e.g. [VCPU|, VMEM|, Flow|Flow-1, ...]
     *
     * @return a string representation of the list of commodities in this basket for debugging
     */
    public String toDebugString(@ReadOnly Basket this) {
        return Arrays.stream(contents_).map(CommoditySpecification::getDebugInfoNeverUseInCode)
                .collect(Collectors.toList()).toString();
    }

    /**
     * Tests whether two Baskets are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Basket this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Basket)) {
            return false;
        }
        Basket otherBasket = (Basket)other;
        // if number of commoditySpecification in the basket does not match, return false immediately
        if (otherBasket.size() != size()) {
            return false;
        }
        // if any commoditySpecification in the basket does not match, return false immediately
        for (CommoditySpecification commSpec : otherBasket.contents_) {
            int specIndex = indexOf(commSpec);
            if (specIndex < 0 || !get(specIndex).equals(commSpec)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        Hasher hasher = Hashing.md5().newHasher();
        // use each commoditySpecification's hash code to generate the hash code for basket
        forEach(commSpec -> {
            hasher.putInt(commSpec.hashCode());
        });

        int i = hasher.hash().asInt();
        return i;
    }

    /**
     * Return the debug info for the commodity at the specified index.  If the input index is
     * beyond the list of the commodities in this basket, then an empty string is returned.
     *
     * @param index the index of the commodity which debug info to be returned
     * @return the debug info of the specified commodity
     */
    public String getCommodityDebugInfoAt(final int index) {
        if (index < contents_.length) {
            return contents_[index].getDebugInfoNeverUseInCode();
        }
        return StringUtils.EMPTY;
    }
} // end Basket interface
