package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

abstract class NumericCommodityType {
    // Fields
    static final byte KIND_BITS = 10;
    static final byte QUALITY_BITS = (Long.BYTES*8 - KIND_BITS) / 2;
    static final long KIND_MIN_VALUE = 0;
    static final long KIND_MAX_VALUE = 1 << KIND_BITS;
    static final long QUALITY_MIN_VALUE = 0;
    static final long QUALITY_MAX_VALUE = 1 << QUALITY_BITS;
    private static final long UPPER_QUALITY_MASK = (1L << 2*QUALITY_BITS) - 1;
    private static final long LOWER_QUALITY_MASK = (1L << QUALITY_BITS) - 1;

    // Constructors

    // Methods
    /**
     * Composes a numerical representation of a commodity specification from a numerical representation of
     * its kind and quality bounds.
     *
     * @param numericalKind A numerical representation of the kind of commodity.
     * @param qualityLowerBound The lower bound imposed on the quality of the commodity.
     * @param qualityUpperBound The upper bound imposed on the quality of the commodity.
     * @return The numerical representation of the commodity specification.
     */
    static long composeNumericalRepresentation(short numericalKind, int qualityLowerBound, int qualityUpperBound) {
        checkArgument(0 <= numericalKind && numericalKind < (1 << KIND_BITS));
        checkArgument(0 <= qualityLowerBound && qualityLowerBound < (1 << QUALITY_BITS));
        checkArgument(0 <= qualityUpperBound && qualityUpperBound < (1 << QUALITY_BITS));
        checkArgument(qualityLowerBound <= qualityUpperBound);
        return (numericalKind << 2*QUALITY_BITS) | (qualityUpperBound << QUALITY_BITS) | qualityLowerBound;
    }

    /**
     * Extracts the quality lower bound field from a long representing a commodity specification.
     *
     * @param numericalRepresentation the numerical representation of a commodity specification.
     * @return the quality lower bound extracted.
     */
    static long extractQualityLowerBound(long numericalRepresentation) {
        return numericalRepresentation & LOWER_QUALITY_MASK;
    }

    /**
     * Extracts the quality upper bound field from a long representing a commodity specification.
     *
     * @param numericalRepresentation the numerical representation of a commodity specification.
     * @return the quality upper bound extracted.
     */
    static long extractQualityUpperBound(long numericalRepresentation) {
        return (numericalRepresentation & UPPER_QUALITY_MASK) >> QUALITY_BITS;
    }

    /**
     * Extracts the numerical representation of the commodity kind from a numerical representation
     * of a commodity specification.
     *
     * @param numericalRepresentation The numerical representation of a commodity specification.
     * @return The extracted numerical representation of the commodity kind.
     */
    static long extractKind(long numericalRepresentation) {
        return numericalRepresentation >> 2*QUALITY_BITS;
    }

} // end class NumericCommodityType
