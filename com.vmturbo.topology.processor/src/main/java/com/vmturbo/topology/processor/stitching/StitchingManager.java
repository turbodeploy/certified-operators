package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * The {@link StitchingManager} coordinates stitching operations in the TopologyProcessor.
 *
 * Stitching is the process of taking the graphs of service entities discovered from individual
 * targets and specific probes which are originally unconnected with each other and applying certain
 * operations to connect them together into a single unified graph.
 *
 * Stitching is applied prior to the topology broadcast to ensure that other components in the XL
 * system only ever see valid and connected topologies. Individual stitching operations are run
 * on a per-target basis.
 *
 * Each stitching operation consists of two related parts:
 * 1. Matching internal entities with external entities in order to find stitching points -
 *    Internal entities are the entities discovered by the target for which the individual
 *    stitching operation is being run while external entities are entities discovered by
 *    targets OTHER than the one for which the operation is being run. The matching part of
 *    the operation examines information on individual internal entities and compares that
 *    to information on individual external entities in order to say whether the internal
 *    entity should be stitched with the external entity.
 * 2. Processing - Processing takes information from the internal and external entities
 *    that were matched in the matching phase and combines that information in some fashion.
 *    Processing may also look up system settings in order to modify or correct discovered
 *    information. This combination may result in modifying topological relationships in a
 *    way that connects originally unconnected subgraphs.
 *
 * Note that some stitching operations may only wish to stitch with internal entities and does
 * not need to match with external entities. These operations that stitch alone are often
 * referred to as "Calculations" or "Derived Metrics". These sorts of operations may
 * take information discovered by multiple targets of the same probe and combine them or use
 * settings to modify discovered information without needing any information from external entities.
 */
public class StitchingManager {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A metric that tracks duration of preparation for stitching.
     */
    private static final DataMetricSummary STITCHING_PREPARATION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_preparation_duration_seconds")
        .withHelp("Duration of construction for data structures in preparation for stitching.")
        .build()
        .register();

    /**
     * A metric that tracks duration of execution for stitching.
     */
    private static final DataMetricSummary STITCHING_EXECUTION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_execution_duration_seconds")
        .withHelp("Duration of execution of all stitching operations.")
        .build()
        .register();

    /**
     * A store of the operations to be applied during stitching.
     */
    final StitchingOperationStore stitchingOperationStore;

    /**
     * Create a new {@link StitchingManager} instance.
     *
     * @param stitchingOperationStore The store of the operations to be applied during stitching.
     */
    public StitchingManager(@Nonnull final StitchingOperationStore stitchingOperationStore) {
        this.stitchingOperationStore = Objects.requireNonNull(stitchingOperationStore);
    }

    /**
     * Stitch the individual graphs discovered by individual targets together to produce a unified topology.
     * Stitching is conducted by running {@link StitchingOperation}s on a per-target basis.
     * See comments on the {@link StitchingManager} class for further details.
     *
     * @param entityStore The store of all discovered entities.
     * @param targetStore The store of all targets.
     * @return A {@link StitchingContext} that contains the results of applying the stitching
     *         operations to the entities in the {@link TargetStore}. This context can be used
     *         to construct a {@link com.vmturbo.topology.processor.topology.TopologyGraph}.
     */
    @Nonnull
    public StitchingContext stitch(@Nonnull final EntityStore entityStore,
                                   @Nonnull final TargetStore targetStore) {

        final DataMetricTimer preparationTimer = STITCHING_PREPARATION_DURATION_SUMMARY.startTimer();
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        preparationTimer.observe();

        return stitch(stitchingContext, targetStore);
    }

    @VisibleForTesting
    StitchingContext stitch(@Nonnull final StitchingContext stitchingContext,
                            @Nonnull final TargetStore targetStore) {
        logger.info("Applying {} stitching operations for {} probes.",
            stitchingOperationStore.operationCount(), stitchingOperationStore.probeCount());

        final DataMetricTimer executionTimer = STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();
        stitchingOperationStore.getAllOperations().stream()
            .forEach(probeOperation ->
                targetStore.getProbeTargets(probeOperation.probeId).forEach(target ->
                    applyOperationForTarget(probeOperation.stitchingOperation,
                        stitchingContext,
                        target.getId())));
        executionTimer.observe();

        // Return the stitching context which has had all the results of the stitching operations
        // applied to it.
        return stitchingContext;
    }

    /**
     * Apply a specific stitching operation for a specific target.
     *
     * @param operation The operation to apply.
     * @param stitchingContext The stitching context containing the data necessary for stitching.
     * @param targetId The id of the target that is being stitched via the operation.
     */
    private void applyOperationForTarget(@Nonnull final StitchingOperation<?, ?> operation,
                                         @Nonnull final StitchingContext stitchingContext,
                                         final long targetId) {
        try {
            Optional<EntityType> externalType = operation.getExternalEntityType();

            final StitchingOperationResult results = externalType.isPresent() ?
                applyStitchWithExternalEntitiesOperation(operation, stitchingContext, targetId, externalType.get()) :
                applyStitchAloneOperation(operation, stitchingContext, targetId);

            stitchingContext.applyStitchingResult(results);
        } catch (RuntimeException e) {
            logger.error("Unable to apply stitching operation " + operation.getClass().getSimpleName() +
                " due to exception: ", e);
        }
    }

    /**
     * Apply a stitching operation in which internal entities do not need to be matched
     * with external entities. Instead, process each internal entity alone.
     *
     * @param operation The stitching operation to apply.
     * @param stitchingContext The stitching context containing acceleration structures for looking
     *                         up entities during stitching.
     * @param targetId The id of the target for which this stitching operation is being applied.
     * @return The results generated by the stitching operation. These results will
     *         be applied to mutate the {@link StitchingContext} and its associated
     *         {@link TopologyStitchingGraph}.
     */
    private StitchingOperationResult applyStitchAloneOperation(
        @Nonnull final StitchingOperation<?, ?> operation,
        @Nonnull final StitchingContext stitchingContext,
        final long targetId) {

        final EntityType internalEntityType = operation.getInternalEntityType();
        final List<StitchingPoint> stitchingPoints = stitchingContext.internalEntities(internalEntityType, targetId)
            .filter(internalEntity -> operation.getInternalSignature(internalEntity).isPresent())
            .map(StitchingPoint::new)
            .collect(Collectors.toList());

        return operation.stitch(stitchingPoints, stitchingContext.getStitchingGraph());
    }

    /**
     * Apply a stitching operation for a target that matches internal and external entities.
     *
     * @param operation The operation for stitching.
     * @param stitchingContext The stitching context containing acceleration structures for looking
     *                         up entities during stitching.
     * @param targetId The id of the target for which this stitching operation is being applied.
     * @param externalEntityType The {@link EntityType} of the external entities to be stitched with
     *                           the internal entities discovered by the target with the given targetId.
     * @param <INTERNAL_SIGNATURE_TYPE> The type of the signature of the internal entities.
     * @param <EXTERNAL_SIGNATURE_TYPE> The type of the signature of the external entities.
     * @return The results generated by the stitching operation. These results will
     *         be applied to mutate the {@link StitchingContext} and its associated
     *         {@link TopologyStitchingGraph}.
     */
    private <INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE>
    StitchingOperationResult applyStitchWithExternalEntitiesOperation(
        @Nonnull final StitchingOperation<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> operation,
        @Nonnull final StitchingContext stitchingContext,
        final long targetId,
        @Nonnull final EntityType externalEntityType) {

        // First create a map of signatures to their corresponding builders for all internal entities.
        // We will use this map later to look up the internal entities by their signature.
        // Be sure to use an identity hash map because it is important that signatures that are equal
        // by their equals method map to different keys for lookup purposes here.
        final IdentityHashMap<INTERNAL_SIGNATURE_TYPE, EntityDTO.Builder> signaturesToBuilders =
            new IdentityHashMap<>();
        final EntityType internalEntityType = operation.getInternalEntityType();

        stitchingContext
            .internalEntities(internalEntityType, targetId)
            .forEach(internalEntity -> operation.getInternalSignature(internalEntity)
                .ifPresent(internalSignature -> signaturesToBuilders.put(internalSignature, internalEntity)));

        // Now construct an index that can quickly calculate which internal signatures that an
        // external signature matches. Note that the internal implementation of the index are
        // determined by the operation itself but its contract is to be able to be able to
        // provide as close to a constant-time lookup as possible for all internal signatures
        // that match a given external signature.
        final StitchingIndex<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> stitchingIndex =
            operation.createIndex(signaturesToBuilders.size());
        signaturesToBuilders.keySet().forEach(stitchingIndex::add);

        // Compute a map of all internal entities to their matching external entities using
        // the index provided by the operation.
        final Stream<EntityDTO.Builder> externalEntities =
            stitchingContext.externalEntities(externalEntityType, targetId);
        final MatchMap matchMap = new MatchMap(signaturesToBuilders.size());
        externalEntities.forEach(externalEntity -> operation.getExternalSignature(externalEntity)
            .ifPresent(externalSignature -> stitchingIndex.findMatches(externalSignature)
                .forEach(internalSignature ->
                    matchMap.addMatch(signaturesToBuilders.get(internalSignature), externalEntity))));

        // Process the matches.
        return operation.stitch(matchMap.createStitchingPoints(), stitchingContext.getStitchingGraph());
    }

    /**
     * Maintains a mapping of (internal entity) -> (list of matching external entities) identified
     * during the matching phase of a stitching operation.
     *
     * Unused by stitch alone operations because external entities are ignored in such operations.
     */
    private static class MatchMap {
        private final Map<EntityDTO.Builder, List<Builder>> matches;

        public MatchMap(final int expectedSize) {
            matches = new IdentityHashMap<>(expectedSize);
        }

        /**
         * Add a match to this {@link MatchMap}. A match consists of an internal entity and all matching
         * external entities.
         *
         * @param internalEntity The internal entity part of the match.
         * @param externalEntity The external entity part of the match.
         */
        public void addMatch(@Nonnull final EntityDTO.Builder internalEntity,
                             @Nonnull final EntityDTO.Builder externalEntity) {
            final List<EntityDTO.Builder> partners =
                matches.computeIfAbsent(internalEntity, key -> new ArrayList<>());

            partners.add(externalEntity);
        }

        /**
         * Create stitching points for each match in this {@link MatchMap}.
         *
         * @return The collection of stitching points for the matches collected in this {@link MatchMap}.
         */
        public Collection<StitchingPoint> createStitchingPoints() {
            return matches.entrySet().stream()
                .map(entry -> new StitchingPoint(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }
    }
}
