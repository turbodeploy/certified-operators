package com.vmturbo.platform.analysis.economy;

import static com.vmturbo.platform.analysis.economy.NumericCommodityType.*;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * The type of a commodity. e.g. CPU or Memory.
 *
 * <p>
 *  Together with the quality, this uniquely identifies a commodity sold from a single trader.
 * </p>
 */
public final class CommoditySpecification implements Comparable<CommoditySpecification> {

    // Fields
    private final String kind_; // the human readable kind of this commodity. e.g. CPU or Memory
    private final String unitOfMeasurement_; // the unit of measurement used for commodities of this kind.
    private final long numericalRepresentation_;

    // Constructors

    /**
     * Constructs a new CommoditySpecification object with the specified properties.
     *
     * @param kind The kind of the new CommoditySpecification. e.g. CPU or Memory.
     * @param unitOfMeasurement The unit of measurement used to measure quantities of commodities of
     *                          this kind.
     * @param numericalRepresentation The numerical representation of the new commodity specification.
     */
    CommoditySpecification(@NonNull @ReadOnly String kind, @NonNull @ReadOnly String unitOfMeasurement, long numericalRepresentation) {
        kind_ = kind;
        unitOfMeasurement_ = unitOfMeasurement;
        numericalRepresentation_ = numericalRepresentation;
    }

    // Methods


    /**
     * Returns the kind of {@code this} commodity specification. e.g. Memory or CPU.
     */
    public @NonNull @ReadOnly String getKind(@ReadOnly CommoditySpecification this) {
        return kind_;
    }

    /**
     * Returns the unit of measurement used to express quantities and capacities for commodities of this
     * type. e.g. MHz or MB
     */
    public @NonNull @ReadOnly String getUnit(@ReadOnly CommoditySpecification this) {
        return unitOfMeasurement_;
    }

    /**
     * Returns the lower bound on the quality provided or requested.
     */
    public int getQualityLowerBound(@ReadOnly CommoditySpecification this) {
        return (int)extractQualityLowerBound(numericalRepresentation_);
    }

    /**
     * Returns the upper bound on the quality provided or requested.
     */
    public int getQualityUpperBound(@ReadOnly CommoditySpecification this) {
        return (int)extractQualityUpperBound(numericalRepresentation_);
    }

    /**
     * Returns the numerical representation of {@code this} commodity specification, used for efficient operations.
     */
    long numericalRepresentation(@ReadOnly CommoditySpecification this) {
        return numericalRepresentation_;
    }

    public boolean isSatisfiedBy(@ReadOnly CommoditySpecification this, @NonNull @ReadOnly CommoditySpecification other) {
        return isSatisfiedBy(this.numericalRepresentation_, other.numericalRepresentation_);
    }

    static boolean isSatisfiedBy(long requestedType, long providedType) {
        return extractKind(requestedType) == extractKind(providedType)
            && extractQualityLowerBound(requestedType) <= extractQualityUpperBound(providedType)
            && extractQualityUpperBound(requestedType) >= extractQualityLowerBound(providedType);
    }

    @Override
    public int compareTo(CommoditySpecification other) {
        return (int)(this.numericalRepresentation_ - other.numericalRepresentation_);
    }

} // end CommoditySpecification enumeration
