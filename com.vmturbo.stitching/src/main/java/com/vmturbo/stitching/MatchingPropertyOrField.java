package com.vmturbo.stitching;

import java.util.Optional;

/**
 * An interface that represents either an entity property or field of an entity to be used for
 * matching.
 *
 * @param <RETURN_TYPE> the type returned by the getMatchingValue method.
 */
public interface MatchingPropertyOrField<RETURN_TYPE> {

    Optional<RETURN_TYPE> getMatchingValue(StitchingEntity entity);
}