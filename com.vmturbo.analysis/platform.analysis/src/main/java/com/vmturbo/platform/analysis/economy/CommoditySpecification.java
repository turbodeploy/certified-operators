package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

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
 * <p>
 *  CommoditySpecification objects are immutable.
 * </p>
 */
public final class CommoditySpecification implements Comparable<CommoditySpecification> {
    // Fields
    private final int type_; // must be non-negative.
    private final int baseType_; // must be non-negative.
    private final int qualityLowerBound_; // must be non-negative and less than or equal to qualityUpperBound_.
    private final int qualityUpperBound_; // must be non-negative and greater than or equal to qualityLowerBound_.
    // TODO: (Jun 22, 2016) This field is intended to be temporarily used for debugging in the initial stages of M2. To avoid making drastic change in
    // market2, we use a setter and getter to set and get this field instead of putting it part of the constructor even though it should
    // only be set once and never changed.
    // If in future we want to keep it, we should make it part of the constructor, remove the setter, also modify all places that call it
    // and the corresponding tests.
    private String debugInfoNeverUseInCode_; // a field keeps information about the eclass and key of the commodity.

    // ID used for generating uniqueCommodityTypes that are to be sold by clones to guaranteedBuyers
    private static int newCommTypeID_ = Integer.MAX_VALUE;
    // Constructors

    /**
     * Constructs a new CommoditySpecification with the given type and the maximum quality range.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     */
    public CommoditySpecification(int type) {
        checkArgument(type >= 0, "type = " + type);

        type_ = type;
        baseType_ = type;
        qualityLowerBound_ = 0;
        qualityUpperBound_ = Integer.MAX_VALUE;
    }

    /**
     * Constructs a new CommoditySpecification with the given type and quality bounds.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     * @param qualityLowerBound The lowest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and less than or equal to qualityUpperBound.
     * @param qualityUpperBound The highest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and greater than or equal to qualityUpperBound.
     */
    public CommoditySpecification(int type, int baseType, int qualityLowerBound, int qualityUpperBound) {
        checkArgument(type >= 0, "type = " + type);
        checkArgument(0 <= qualityLowerBound, "qualityLowerBound = " + qualityLowerBound);
        checkArgument(qualityLowerBound <= qualityUpperBound,
            "qualityLowerBound = " + qualityLowerBound + ", qualityUpperBound = " + qualityUpperBound);

        type_ = type;
        baseType_ = baseType;
        qualityLowerBound_ = qualityLowerBound;
        qualityUpperBound_ = qualityUpperBound;
    }

    /**
     * Constructs a new CommoditySpecification with the given type and quality bounds.
     *
     * @param baseType The type of commodity sold or bought as an int.
     * @param qualityLowerBound The lowest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and less than or equal to qualityUpperBound.
     * @param qualityUpperBound The highest quality of this commodity a buyer can accept or a seller
     *             provide. It must be non-negative and greater than or equal to qualityUpperBound.
     */
    public CommoditySpecification(int baseType, int qualityLowerBound, int qualityUpperBound) {
        this(newCommTypeID_, baseType, qualityLowerBound, qualityUpperBound);
        newCommTypeID_--;
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

    @Pure
    public int getBaseType(@ReadOnly CommoditySpecification this) {
        return baseType_;
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
        b.append('>');
        return b.toString();
    }

    /**
     * Sets the debugInfo field. It contains information about the eclass and the key of the commodity.
     * @param debugInfo a string contains eclass|key of the commodity.
     */
    public @NonNull CommoditySpecification setDebugInfoNeverUseInCode(@NonNull String debugInfo) {
        debugInfoNeverUseInCode_ = debugInfo;
        return this;
    }

    /**
     * Returns the debugInfo field.
     */
    public String getDebugInfoNeverUseInCode(@ReadOnly CommoditySpecification this) {
        return debugInfoNeverUseInCode_;
    }
} // end CommoditySpecification class
