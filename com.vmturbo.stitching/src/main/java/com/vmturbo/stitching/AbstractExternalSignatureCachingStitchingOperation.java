package com.vmturbo.stitching;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;

/**
 * Stitching Operation that caches external signatures between calls to
 * initializeOperationBeforeStitching.
 *
 * @param <InternalSignatureT> the internal signature type of the operation.
 * @param <ExternalSignatureT> the external signature type of the operation.
 */
public abstract class AbstractExternalSignatureCachingStitchingOperation<InternalSignatureT,
        ExternalSignatureT> implements StitchingOperation<InternalSignatureT, ExternalSignatureT> {
    private static final Logger logger = LogManager.getLogger();

    private final Map<ExternalSignatureT, Collection<StitchingEntity>>
            externalSignatureToStitchingEntityMap = new HashMap<>();

    @Override
    public void initializeOperationBeforeStitching(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        externalSignatureToStitchingEntityMap.clear();
    }

    /**
     * Get set of external signatures for an external entity. External signatures will be matched
     * against internal signatures using a map from signature to internal entity to identify
     * entities that should be stitched with each other.
     * The {@link EntityType} of the external entities will be guaranteed to match the entity type
     * returned by {@link #getExternalEntityType()}. If {@link #getExternalEntityType()} returned
     * {@link Optional#empty()}, this method will never be called.
     * External entities are defined as the entities discovered by targets other than the target
     * that invoked the stitching operation.
     * Return an empty collection to skip this {@link StitchingEntity} when stitching.
     *
     * @param externalEntity The external entity whose signature should be retrieved.
     * @return The collection of signatures for the external entity. Return an empty collection
     *         to skip considering this entity during matching.
     */
    protected abstract Collection<ExternalSignatureT> getExternalSignature(
            @Nonnull StitchingEntity externalEntity);

    @Nonnull
    @Override
    public Map<ExternalSignatureT, Collection<StitchingEntity>> getExternalSignatures(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory, long targetId) {
        // if caching is disabled, clear the cache
        if (!isCachingEnabled()) {
            externalSignatureToStitchingEntityMap.clear();
        }
        // if the cache is empty, populate it with a map of external signature to a list of
        // entities that have that signature
        if (externalSignatureToStitchingEntityMap.isEmpty()) {
            logger.debug("Generating external signatures for operation {}",
                    getOperationName());
            // Generate the map of external signatures from the entities is the scope, or, if
            // there is no scope, use all entities of the correct entity type
            getScope(stitchingScopeFactory, targetId).orElseGet(
                    () -> stitchingScopeFactory.entityTypeScope(getExternalEntityType().get()))
                    .entities()
                    .forEach(entity -> getExternalSignature(entity).forEach(
                            signature -> externalSignatureToStitchingEntityMap.computeIfAbsent(
                                    signature, key -> Sets.newHashSet()).add(entity)));
        } else {
            logger.debug("Returning cached external signatures for operation {}",
                    getOperationName());
        }
        return externalSignatureToStitchingEntityMap;
    }

    @VisibleForTesting
    public boolean isCachingEnabled() {
        return true;
    }
}
