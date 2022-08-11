package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;

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
 *  he needs e.g. he may need CPU and not Storage
 * </p>
 * <p>
 *  Likewise, when a seller sets out to sell stuff, he needs a way to specify what kind of
 *  commodities are available to prospective buyers. e.g. he may be able to offer CPU and Memory.
 * </p>
 * <p>
 *  In order for a buyer to be satisfied by a seller, each commodity specification of the buyer must
 *  be equal in type to one commodity specification of the seller.
 * </p>
 * <p>
 *  CommoditySpecification objects are immutable.
 * </p>
 */
public final class CommoditySpecification implements Comparable<CommoditySpecification>, Serializable {
    // Fields
    private final int type_; // must be non-negative.
    private final int baseType_; // must be non-negative.
    private final boolean isCloneWithNewType_; // whether the clone of the commoditySpecification should be assigned a new type
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
     * Constructs a new CommoditySpecification with the given type.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     */
    public CommoditySpecification(int type) {
        checkArgument(type >= 0, "type = %s", type);

        type_ = type;
        baseType_ = type;
        isCloneWithNewType_ = false;
    }

    /**
     * Constructs a new CommoditySpecification with the given type.
     *
     * @param type The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *             memory but the exact correspondence is kept by an outside source.
     *             It must be non-negative.
     */
    public CommoditySpecification(int type, int baseType) {
        checkArgument(type >= 0, "type = %s", type);

        type_ = type;
        baseType_ = baseType;
        isCloneWithNewType_ = false;
    }

    /**
     * Constructs a new CommoditySpecification with the given type.
     *
     * @param type in an integer that represents the combination of both type of commodity and key.
     * @param baseType The type of commodity sold or bought as an int. It may represent e.g. CPU or
     *                 memory but the exact correspondence is kept by an outside source.
     *                 It must be non-negative.
     * @param isCloneWithNewType Whether the commodity specification should be given a new type when
     *  clone
     */
    public CommoditySpecification(int type, int baseType,
                                  boolean isCloneWithNewType) {
        checkArgument(type >= 0, "type = %s", type);

        type_ = type;
        baseType_ = baseType;
        isCloneWithNewType_ = isCloneWithNewType;
    }

    /**
     * Constructs a new CommoditySpecification with the given baseType.
     *
     * <p>
     * It returns a CommoditySpecification with a different type each time.
     * It changes global state and can't be used in a multi-threaded environment.
     * </p>
     *
     * @param baseType The type of commodity sold or bought as an int.
     * @param isCloneWithNewType Whether the commodity specification should be given a new type when clone
     */
    public CommoditySpecification(int baseType, boolean isCloneWithNewType) {
        this(newCommTypeID_, baseType, isCloneWithNewType);
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
     * Returns the true if cloning {@code this} needs to create a commoditySpecification with new type.
     *
     */
    @Pure
    public boolean isCloneWithNewType(@ReadOnly CommoditySpecification this) {
        return isCloneWithNewType_;
    }

    /**
     * A total ordering on the CommoditySpecifications to allow sorting and insertion into maps.
     *
     * <p>
     *  They are lexicographically compared on the type.
     * </p>
     */
    @Override
    @Pure
    public int compareTo(@ReadOnly CommoditySpecification this, @NonNull @ReadOnly CommoditySpecification other) {
        return type_ - other.type_;
    }

    /**
     * Tests whether two CommoditySpecifications are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly CommoditySpecification this, @ReadOnly Object other) {
        if(!(other instanceof CommoditySpecification)) {
            return false;
        }
        CommoditySpecification csOther = (CommoditySpecification)other;
        return type_ == csOther.type_;
    }

    /**
     * Returns a strong hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(type_).hash().asInt(); // no particular reason to use md5.
    }

    /**
     * Returns a string representation of {@code this} commodity specification
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

    /**
     * Return a new commSpec with new commodity id.
     *
     * @param newCommodityId new commodity id
     * @return a new commSpec
     */
    public CommoditySpecification createCommSpecWithNewCommodityId(final int newCommodityId) {
        final CommoditySpecification newCommSpec = new CommoditySpecification(newCommodityId,
            getBaseType(), isCloneWithNewType());
        newCommSpec.setDebugInfoNeverUseInCode(getDebugInfoNeverUseInCode());
        return newCommSpec;
    }
} // end CommoditySpecification class
