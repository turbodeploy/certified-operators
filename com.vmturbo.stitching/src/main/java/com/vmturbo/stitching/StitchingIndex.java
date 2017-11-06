package com.vmturbo.stitching;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * A {@link StitchingIndex} is used to accelerate the matching process during stitching.
 *
 * Stitching attempts to find entities that match by testing if the "signatures" of the entities match.
 * A naive implementation would test every (internal_entity, external_entity) pair which would
 * require a quadratic number of comparisons. Quadratic algorithms do not scale. In order to accelerate
 * the process of finding matches, we require stitching operations to provide an index.
 *
 * This index is used by first building an index of all internal entity signatures, then for each
 * external entity signature, look up all matching internal signatures using this index.
 *
 * We choose to build the index on internal signatures because we expect the number of internal
 * signatures to be smaller than the number of external signatures because internal entities come
 * from only one target while external signatures come from an unbounded number of targets.
 *
 * The specific implementation details of an index are left up to the implementor but implementors
 * should aim to provide as close to a constant-time lookup as possible for all internal signatures
 * that match a given external signature.
 *
 * INTERNAL_SIGNATURE_TYPE and EXTERNAL_SIGNATURE_TYPE to permit maximum flexibility so that. Consider,
 * for example, if the internal signature is a single integer and the external signature is a list
 * of integers with it being considered a match when any of the internal in the external signature
 * equalling the internal signature.
 *
 * @param <INTERNAL_SIGNATURE_TYPE> The data type of the internal signature.
 * @param <EXTERNAL_SIGNATURE_TYPE> The data type of the external signature.
 */
public interface StitchingIndex<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> {

    /**
     * Add an internal signature to the index.
     *
     * @param internalSignature The internal signature to be inserted into the index.
     */
    void add(@Nonnull final INTERNAL_SIGNATURE_TYPE internalSignature);

    /**
     * Return a stream of all internal signatures that match the given external signatures.
     *
     * Typically, this will match only a single entity but a stream is returned to permit
     * one-to-many matching.
     *
     * @param externalSignature The external signature to match against internal signatures.
     * @return The internal signatures matching the given external signature.
     */
    Stream<INTERNAL_SIGNATURE_TYPE> findMatches(@Nonnull final EXTERNAL_SIGNATURE_TYPE externalSignature);

    /**
     * A {@link DefaultStitchingIndex} is a stitching index that accelerates lookups of internal entity
     * matches for external entities when the INTERNAL_SIGNATURE_TYPE and EXTERNAL_SIGNATURE_TYPE
     * are the same types and the signatures are considered a match exactly when those types are equal.
     *
     * This index is only suitable for one-to-one matching of internal and external entities.
     *
     * @param <INTERNAL_SIGNATURE_TYPE> The data type of the internal signature.
     * @param <EXTERNAL_SIGNATURE_TYPE> The data type of the external signature.
     */
    class DefaultStitchingIndex<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE>
        implements StitchingIndex<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> {

        private final Map<INTERNAL_SIGNATURE_TYPE, INTERNAL_SIGNATURE_TYPE> index;

        public DefaultStitchingIndex(final int expectedIndexSize) {
            index = new HashMap<>(expectedIndexSize);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void add(@Nonnull INTERNAL_SIGNATURE_TYPE internalSignature) {
            index.put(internalSignature, internalSignature);
        }

        /**
         * {@inheritDoc}
         *
         * An getExternalSignature will be considered a match when it is an exact match
         * of an internal signature by equality.
         */
        @Override
        public Stream<INTERNAL_SIGNATURE_TYPE> findMatches(@Nonnull EXTERNAL_SIGNATURE_TYPE externalSignature) {
            // Note that this requires INTERNAL_SIGNATURE_TYPE and EXTERNAL_SIGNATURE_TYPE to be of the same type.
            final INTERNAL_SIGNATURE_TYPE matchData = index.get(externalSignature);
            return (matchData == null) ? Stream.empty() : Stream.of(matchData);
        }
    }
}
