package com.vmturbo.stitching;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * An interface that represents either an entity property or field of an entity to be used for
 * matching.
 *
 * @param <R> the type returned by the getMatchingValue method.
 */
public interface MatchingPropertyOrField<R> {

    /**
     * Returns collection of matching values for specified {@link StitchingEntity} instance.
     *
     * @param entity entity for which we want to get values of the matching
     *                 property.
     * @return collection of matching values.
     */
    @Nonnull
    Collection<R> getMatchingValue(@Nonnull StitchingEntity entity);
}
