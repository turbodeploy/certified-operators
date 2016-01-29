package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.function.Function;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;

/**
 * A detailed, exact statement of particulars about a commodity a buyer needs to buy (but may not be
 * currently buying) or a seller is able to sell (but may not be currently selling).
 *
 * <p>
 *  When a buyer sets out to buy a set of commodities he needs to specify what kind of commodities
 *  he needs e.g. he may need CPU and not Storage and he may also need a certain quality of it like
 *  e.g. a 4 core CPU might be enough, but a 2 core CPU might be insufficient.
 * </p>
 * <p>
 *  Likewise, when a seller sets out to sell stuff, he needs a way to specify what kind of
 *  commodities are available to prospective buyers. e.g. he may be able to offer CPU with 1 to 8
 *  cores and Memory.
 * </p>
 * <p>
 *  In order for a buyer to be satisfied by a seller, each commodity specification of the buyer must
 *  be satisfied by one commodity specification of the seller.
 * </p>
 *  By default, commodities are assumed to be additive, meaning if a new buyer starts buying the
 *  commodity then the sold commodity quantity is the previous quantity plus the quantity bought by
 *  the new buyer, and if a buyer stops buying the commodity (leaves the trader) then the new quantity
 *  of the sold commodity is the previous value minus the quantity that the leaving buyer bought.
 *  It is possible to specify a different behavior using a Function.
 *  TODO(Shai): elaborate
 * <p>
 *  CommoditySpecification objects are immutable.
 * </p>
 */
public final class CommoditySpecification implements Comparable<CommoditySpecification> {
    // Fields
    private final int type_; // must be non-negative.
    private final int qualityLowerBound_; // must be non-negative and less than or equal to qualityUpperBound_.
    private final int qualityUpperBound_; // must be non-negative and greater than or equal to qualityLowerBound_.
    private final Function<List<Double>, Double> operator_;

    // Constructors

    /**
     * Constructs a new CommoditySpecification with the given type and the maximum quality range,
     * and with the default quantity sold behavior.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     */
    public CommoditySpecification(int type) {
        this(type, 0, Integer.MAX_VALUE, null);
    }

    /**
     * Constructs a new CommoditySpecification with the given type and quality bounds, and with the
     * default quantity sold behavior.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     * @param qualityLowerBound The lowest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and less than or equal to qualityUpperBound.
     * @param qualityUpperBound The highest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and greater than or equal to qualityUpperBound.
     */
    public CommoditySpecification(int type, int qualityLowerBound, int qualityUpperBound) {
        this(type, qualityLowerBound, qualityUpperBound, null);
    }

    /**
     * Constructs a new CommoditySpecification with the given type and the maximum quality range,
     * and with the given quantity calculation function.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     * @param operator A function used to calculate the quantity sold by a {@link Commodity} of this
     *             type. If null, assume the quantity sold is additive.
     */
    public CommoditySpecification(int type, Function<List<Double>, Double> operator) {
        this(type, 0, Integer.MAX_VALUE, operator);
    }

    /**
     * Constructs a new CommoditySpecification with the given type, quality bounds and with the
     * given quantity calculation function.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     * @param qualityLowerBound The lowest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and less than or equal to qualityUpperBound.
     * @param qualityUpperBound The highest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and greater than or equal to qualityUpperBound.
     * @param operator A function used to calculate the quantity sold by a {@link Commodity} of this
     *             type. If null, assume the quantity sold is additive.
     */
    public CommoditySpecification(int type, int qualityLowerBound, int qualityUpperBound,
            @NonNull Function<List<Double>, Double> operator) {
        checkArgument(type >= 0);
        checkArgument(0 <= qualityLowerBound);
        checkArgument(qualityLowerBound <= qualityUpperBound);

        type_ = type;
        qualityLowerBound_ = qualityLowerBound;
        qualityUpperBound_ = qualityUpperBound;
        operator_ = operator;
    }

    // Methods

    /**
     * Returns the type of the commodity provided or requested.
     *
     * <p>
     *  Its a numerical representation of the type. An ID of sorts that may e.g. correspond to CPU
     *  or Memory, but the correspondence is not important to the economy and kept (potentially)
     *  outside the economy.
     * </p>
     */
    @Pure
    public int getType(@ReadOnly CommoditySpecification this) {
        return type_;
    }

    /**
     * Returns the lower bound on the quality provided or requested.
     *
     * <p>
     *  Together with upper bound, they create a quality range for a commodity sold or bought.
     * </p>
     *
     * @see #isSatisfiedBy(CommoditySpecification)
     */
    @Pure
    public int getQualityLowerBound(@ReadOnly CommoditySpecification this) {
        return qualityLowerBound_;
    }

    /**
     * Returns the upper bound on the quality provided or requested.
     *
     * <p>
     *  Together with lower bound, they create a quality range for a commodity sold or bought.
     * </p>
     *
     * @see #isSatisfiedBy(CommoditySpecification)
     */
    @Pure
    public int getQualityUpperBound(@ReadOnly CommoditySpecification this) {
        return qualityUpperBound_;
    }

    /**
     * Check whether this commodity specification uses the default (additive) quantity update or
     * it has its own function.
     * @return true when the commodity specification is additive, flase when it uses its own function
     */
    @Pure
    public boolean isAdditive() {
        return operator_ == null;
    }
    /**
     * Returns the quantity (peakQuantity) sold of a commodity of this type, given the quantities
     * (peakQuantities) bought by the buyers of the commodity. Uses the operator specified when
     * constructing this commodity specification.
     * @param quantities the quantities
     * @return
     */
    @Pure
    public Double quantity(List<Double> quantities) {
        return operator_ != null ? operator_.apply(quantities) : null;
    }

    /**
     * Tests whether {@code this} commodity specification is satisfied by another commodity
     * specification.
     *
     * <p>
     *  Intuitively {@code this} commodity specification should specify a commodity bought and the
     *  other commodity specification should specify a commodity sold. A true return value would
     *  mean that the corresponding buyer can buy the specified commodity from the respective seller.
     * </p>
     *
     * <p>
     *  The current implementation returns true if the types match exactly and the quality ranges
     *  overlap, which is a symmetric relation, but in general this relation doesn't have to be
     *  symmetric.
     * </p>
     *
     * @param other The commodity specification that should satisfy {@code this} specification.
     * @return {@code true} if {@code this} specification is satisfied by the other specification.
     *         {@code false} otherwise.
     */
    @Pure
    public boolean isSatisfiedBy(@ReadOnly CommoditySpecification this, @NonNull @ReadOnly CommoditySpecification other) {
        return type_ == other.type_ && qualityLowerBound_ <= other.qualityUpperBound_
                                    && qualityUpperBound_ >= other.qualityLowerBound_;
    }

    /**
     * A total ordering on the CommoditySpecifications to allow sorting and insertion into maps.
     *
     * <p>
     *  They are lexicographically compared first on the type, then on the lower quality bound and
     *  last on the upper quality bound.
     * </p>
     */
    @Override
    @Pure
    public int compareTo(@ReadOnly CommoditySpecification this, @NonNull @ReadOnly CommoditySpecification other) {
        if(type_ != other.type_)
            return type_ - other.type_;
        else if(qualityLowerBound_ != other.qualityLowerBound_)
            return qualityLowerBound_ - other.qualityLowerBound_;
        else
            return qualityUpperBound_ - other.qualityUpperBound_;
    }

    /**
     * Tests whether two CommoditySpecifications are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly CommoditySpecification this, @ReadOnly Object other) {
        if(other == null || !(other instanceof CommoditySpecification))
            return false;
        CommoditySpecification csOther = (CommoditySpecification)other;
        return type_ == csOther.type_ && qualityLowerBound_ == csOther.qualityLowerBound_
                                      && qualityUpperBound_ == csOther.qualityUpperBound_;
    }

    /**
     * Returns a strong hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(type_).putInt(qualityLowerBound_)
            .putInt(qualityUpperBound_).hash().asInt(); // no particular reason to use md5.
    }

    /**
     * Returns a string representation of {@code this} commodity specification as a triple of the
     * form <type, qualityLowerBound, qualityUpperBound>. When upper bound is the maximum value
     * we print the string MAX_VALUE, as in <17, 10, MX_VALUE>.
     *
     * <p>
     *  This is included mostly for use by the assert* methods of JUnit so that they produce
     *  comprehensible error messages.
     * </p>
     */
    @Override
    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append('<');
        b.append(type_);
        b.append(", ");
        b.append(qualityLowerBound_);
        b.append(", ");
        b.append(qualityUpperBound_ == Integer.MAX_VALUE ?
                "MAX_VALUE" : qualityUpperBound_);
        b.append(isAdditive() ? "" : ", OP");
        b.append('>');
        return b.toString();
    }

} // end CommoditySpecification class
