package com.vmturbo.group.validation;

import javax.annotation.Nonnull;

import com.vmturbo.group.common.InvalidParameterException;

/**
 * Interface for classes that handle input sanitization.
 *
 * <p>Sanitizers are expected to return a copy of the input object, that has correct fields:
 * - defaults are added where needed but missing
 * - any values that need to be checked are sanitized.
 * </p>
 *
 * @param <T> the type of objects to be sanitized.
 */
public interface InputSanitizer<T> {

    /**
     * Returns the input value after sanitizing it.
     *
     * @param input the input value ot be sanitized.
     * @return the input value after sanitization.
     * @throws InvalidParameterException if the input was invalid and could not be sanitized.
     */
    @Nonnull
    T sanitize(@Nonnull T input) throws InvalidParameterException;
}
