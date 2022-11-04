package com.vmturbo.mediation.azure.pricing.util;

import javax.annotation.Nonnull;

/**
 * Exception thrown by PriceConverter when there is an error.
 */
public class PriceConversionException extends Exception {
    /**
     * Construct a new PriceConversionException with a string message.
     *
     * @param error an error message describing the problem.
     */
    public PriceConversionException(@Nonnull final String error) {
        super(error);
    }
}
