package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.journal.JournalableOperation;

/**
 * An operation that edits entity properties and relationships and removes entities during stitching
 * in order to compose a well-formed topology.
 *
 * StitchingOperations are generally written per-probe category. For example, one for Storage, one for Fabric,
 * one for HyperConverged, etc. with parameters used to tune the stitching behavior. Operations are chosen
 * based on probe category and type.
 *
 * {@link StitchingOperation}s are run on a per-target basis.
 *
 * {@link StitchingOperation}s should be stateless. Any state operations accumulate should be stored
 * outside of the operation itself and that state should be managed with the understanding that an
 * operation may be run in multiple pipelines (and multiple kinds of pipelines ie live/plan) in parallel.
 *
 * A {@link StitchingOperation} consists of two parts - matching and processing.
 * 1. Matching: In matching, internal entities (entities discovered by the probe that initiates the operation)
 *    of the type associated with the operation, are matched with external entities (entities discovered by
 *    targets OTHER than the target that initiates the operation) of the type specified by
 *    {@link #getExternalEntityType()}.
 *
 *    Matching works by first building an map of the signatures for all internal entities.
 *    Then, the signature for each external entity is used as a lookup into the index to
 *    find matching internal entities.
 *
 *    The matches are then processed.
 * 2. Processing: During processing, the operation makes updates and removes entities in the topology.
 *    Updates to the properties and commodities of an entity may be made immediately, but updates to
 *    the relationship of an entity (as described in {@link TopologicalChangelog}) are applied only
 *    AFTER the {@link #stitch(Collection, StitchingChangesBuilder)} call returns.
 *
 * @param <INTERNAL_SIGNATURE_TYPE> The type of the signature by which internal entities will be matched
 *                                  with external entities.
 * @param <EXTERNAL_SIGNATURE_TYPE> The type of the signature by which external entities will be matched
 *                                  with internal entities.
 */
public interface StitchingOperation<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE>
    extends JournalableOperation {

    /**
     * Get the scope for this {@link StitchingOperation}. The {@link StitchingScope} returned determines
     * which entities are provided as input to the {@link #stitch(Collection, StitchingChangesBuilder)}
     * method for this {@link StitchingOperation}. For example, imagine you have a storage probe that
     * wants to stitch with hypervisor storages but not some other storages (say from a datastore
     * browsing probe) that may just be proxy storages.  The scope is applied by StitchingManager when
     * generating candidates for stitching.  After the StitchingIndex is build for internal entities,
     * only external entities in the scope are considered for creating StitchingPoints.
     * See {@link StitchingScopeFactory} for further details.
     *
     * @param stitchingScopeFactory The factory to use to construct the {@link StitchingScope} for this
     *                                {@link StitchingOperation}.
     * @param targetId the target id of the internal target we are trying to stitch.
     * @return The {@link StitchingScope} to use for this {@link StitchingOperation}.  Returning
     * Optional.empty indicates no scope is set and candidates for matching can come from all
     * external probe targets.
     */
    @Nonnull
    Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
            long targetId);

    /**
     * Return a Map of external signature to a collection of StitchingEntities that have that
     * signature. The expectation is that the caller will call this method once per operation
     * during main stitching and avoid having to recompute the external signatures for each target
     * being stitched, which is wasteful.
     *
     * @param stitchingScopeFactory The factory to use to construct the {@link StitchingScope} for this
     *                                {@link StitchingOperation}.
     * @param signatureCache The cache of external signatures for use in looking up any existing
     *                       signatures for this operation.
     * @param targetId the target id of the internal target we are trying to stitch.
     * @return Map of external signatures to Collections of StitchingEntities that match the
     * signatures.
     */
    @Nonnull
    Map<EXTERNAL_SIGNATURE_TYPE, Collection<StitchingEntity>> getExternalSignatures(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
            @Nonnull ExternalSignatureCache signatureCache, long targetId);

    /**
     * The {@link EntityType} of the internal entities to be stitched.
     * Operations are specific to pairs of entity types (internal and external).
     *
     * Internal entities are defined as the entities discovered by the target invoking the stitching operation.
     *
     * @return The internal {@link EntityType} for the entities to be stitched by this operation.
     */
    @Nonnull EntityType getInternalEntityType();

    /**
     * The optional {@link EntityType} of the external entities to be stitched with the internal entities.
     *
     * External entities are defined as the entities discovered by targets other than the target
     * that invoked the stitching operation.
     *
     * Return {@link Optional#empty()} to indicate that no external entities are required by this stitching
     * operation. An example of a stitching operation that does not stitch with external entities is a
     * derived calculation.
     *
     * @return The optional {@link EntityType} of the external entities to be stitched with the internal entities.
     *         If {@link Optional#empty()} is returned, it indicates that the operation should not attempt
     *         to find any matching external entities.
     */
    @Nonnull Optional<EntityType> getExternalEntityType();

    /**
     * Get the internal signature for an internal entity. Internal signatures will be matched against
     * external signatures using a map from signature to internal entity to identify entities that
     * should be stitched with each other.
     *
     * The {@link EntityType} of the internal entity will be guaranteed to match the entity type returned
     * by {@link #getInternalEntityType()}.
     *
     * Internal entities are defined as the entities discovered by the target invoking the stitching operation.
     *
     * Return {@link Optional#empty()} to skip considering this entity during matching.
     *
     * @param internalEntity The internal entity whose signature should be retrieved.
     * @return The signature for the internal entity. Return {@link Optional#empty()} to skip considering
     *         this entity during matching.
     */
    Collection<INTERNAL_SIGNATURE_TYPE> getInternalSignature(@Nonnull StitchingEntity internalEntity);

    /**
     * Stitch a collection of {@link StitchingPoint}s.
     *
     * For a given stitching point, stitching may update the internal entity, external entities,
     * and entities reachable from these entities.
     *
     * Stitching may modify properties, modify entity relationships, or remove entities from the topology.
     * Stitching MAY NOT create entirely new entities.
     *
     * The precise semantics of stitching processing are specific to that operation, but in general
     * stitching is used to unify information discovered by targets that are unaware of each other
     * or any user or system settings until stitching occurs.
     *
     * Any updates to relationships in the entities in the {@link StitchingPoint}s must be noted in
     * the returned {@link TopologicalChangelog} so that the graph and certain other acceleration structures
     * that track entities and relationships can be updated for further stitching.
     *
     * @param stitchingPoints The collection of {@link StitchingPoint}s that should be stitched.
     * @param resultBuilder A builder for the result containing the changes this operation wants to make
     *                      to the entities and their relationships. The operation should use this builder
     *                      to create the result it returns.
     * @return A {@link TopologicalChangelog} that describes the result of stitching. The result should be built using
     *         the {@link StitchingChangesBuilder} provided as input.
     */
    @Nonnull
    TopologicalChangelog<StitchingEntity> stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                                 @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder);
}
