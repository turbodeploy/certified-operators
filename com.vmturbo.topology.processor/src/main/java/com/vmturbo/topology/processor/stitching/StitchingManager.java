package com.vmturbo.topology.processor.stitching;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingPhase;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalTargetEntrySupplier;
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
 *
 * Stitching occurs in several phases.
 * 1. PreStitching: This phase happens prior to the regular stitching phase. During this phase, certain
 *    calculations are run that may update certain metrics gathered from a target or unify multiple instances of
 *    a single entity discovered by multiple targets into a single instance (as in the case of shared storage).
 *    Settings are not available in this phase. {@link PreStitchingOperation}s are run
 *    during this phase.
 * 2. Stitching: This phase happens after PreStitching but before
 *    {@link com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline} phases such as group resolution,
 *    policy application, or setting calculations. Settings are not available in this phase.
 *    {@link StitchingOperation}s are run during this phase.
 * 3. PostStitching: This phase happens after the
 *    {@link com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline} phases listed above.
 *    Settings are available in this phase but the relationships in the
 *    {@link TopologyGraph<TopologyEntity>} being stitched cannot be mutated by this phase.
 *    {@link PostStitchingOperation}s are run during this phase.
 */
public class StitchingManager {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A metric that tracks duration of execution for stitching.
     */
    private static final DataMetricSummary STITCHING_EXECUTION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_execution_duration_seconds")
        .withHelp("Duration of execution of all stitching operations.")
        .build()
        .register();

    /**
     * A metric that tracks duration of execution for PreStitching.
     */
    private static final DataMetricSummary PRE_STITCHING_EXECUTION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_pre_stitching_execution_duration_seconds")
        .withHelp("Duration of execution of all pre-stitching operations.")
        .build()
        .register();

    /**
     * A metric that tracks duration of execution for PostStitching.
     */
    private static final DataMetricSummary POST_STITCHING_EXECUTION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_post_stitching_execution_duration_seconds")
        .withHelp("Duration of execution of all post-stitching operations.")
        .build()
        .register();

    /**
     * A store of the operations to be applied during the Stitching phase.
     */
    private final StitchingOperationStore stitchingOperationStore;

    /**
     * A store of the {@link PreStitchingOperation}s to be applied during the pre-stitching phase.
     */
    private final PreStitchingOperationLibrary preStitchingOperationLibrary;

    /**
     * A store of the {@link PostStitchingOperation}s to be applied during the post-stitching phase.
     */
    private final PostStitchingOperationLibrary postStitchingOperationLibrary;

    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final CpuCapacityStore cpuCapacityStore;

    /**
     * Create a new {@link StitchingManager} instance.
     * @param stitchingOperationStore The store of the operations to be applied during the main stitching phase.
     * @param preStitchingOperationLibrary The store of pre stitching operations to be applied during pre-stitching.
     * @param postStitchingOperationLibrary The store of post stitching operations to be applied during post-stitching.
     * @param probeStore The store of probes known to the topology processor.
     * @param targetStore The store of available targets known to the topology processor.
     * @param cpuCapacityStore RPC handle to call the CPU Capacity Service
     */
    public StitchingManager(@Nonnull final StitchingOperationStore stitchingOperationStore,
                            @Nonnull final PreStitchingOperationLibrary preStitchingOperationLibrary,
                            @Nonnull final PostStitchingOperationLibrary postStitchingOperationLibrary,
                            @Nonnull final ProbeStore probeStore,
                            @Nonnull final TargetStore targetStore,
                            @Nonnull final CpuCapacityStore cpuCapacityStore) {
        this.stitchingOperationStore = Objects.requireNonNull(stitchingOperationStore);
        this.preStitchingOperationLibrary = Objects.requireNonNull(preStitchingOperationLibrary);
        this.postStitchingOperationLibrary = Objects.requireNonNull(postStitchingOperationLibrary);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.cpuCapacityStore = cpuCapacityStore;
    }

    /**
     * Stitch the individual graphs discovered by individual targets together to produce a unified topology.
     * Stitching is conducted by running {@link StitchingOperation}s on a per-target basis.
     * See comments on the {@link StitchingManager} class for further details.
     *
     * @param stitchingContext The context containing the entities to be stitched.
     * @param stitchingJournal The stitching journal used to track changes.
     * @return A {@link StitchingContext} that contains the results of applying the stitching
     *         operations to the entities in the {@link TargetStore}. This context can be used
     *         to construct a {@link TopologyGraph <TopologyEntity>}.
     */
    @Nonnull
    public StitchingContext stitch(@Nonnull final StitchingContext stitchingContext,
                                   @Nonnull final IStitchingJournal<StitchingEntity> stitchingJournal) {
        final StitchingOperationScopeFactory preStitchScopeFactory = new StitchingOperationScopeFactory(
            stitchingContext, probeStore, targetStore);

        stitchingJournal.recordTargets(
            new StitchingJournalTargetEntrySupplier(targetStore, probeStore, stitchingContext)::getTargetEntries);

        preStitch(preStitchScopeFactory, stitchingJournal);
        mainStitch(preStitchScopeFactory, stitchingJournal);

        return stitchingContext;
    }

    /**
     * Remove from the topology proxy entities that were not stitched and not marked
     * keepStandalone = true by their discovering probe.
     *
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that this method will operate on.
     * @param stitchingJournal The stitching journal used to track changes.
     */
    private void cleanupUnstitchedProxyEntities(
        final StitchingOperationScopeFactory scopeFactory,
        final IStitchingJournal<StitchingEntity> stitchingJournal) {
        stitchingJournal.recordMessage(
            "--------------- START: Cleanup of unstitched proxy entities ---------------");
        final StitchingResultBuilder resultBuilder =
            new StitchingResultBuilder(scopeFactory.getStitchingContext());
        scopeFactory.globalScope().entities()
            .filter(StitchingEntity::removeIfUnstitched)
            .forEach(stitchingEntity -> resultBuilder.queueEntityRemoval(stitchingEntity));
        TopologicalChangelog<StitchingEntity> results = resultBuilder.build();
        results.getChanges().forEach(change -> change.applyChange(stitchingJournal));
        stitchingJournal.recordMessage(
            "--------------- END: Cleanup of unstitched proxy entities ---------------");
    }

    /**
     * Apply post-stitching operations to the {@link GraphWithSettings}. See {@link PostStitchingOperation} for
     * details on what happens during post-stitching.
     *
     * @param graphWithSettings An object containing both the topology graph and associated settings.
     * @param stitchingJournal The journal to use to trace changes made during stitching.
     * {@link TopologyGraph<TopologyEntity>} and settings to be used during post-stitching.
     */
    public void postStitch(@Nonnull final GraphWithSettings graphWithSettings,
                           @Nonnull final IStitchingJournal<TopologyEntity> stitchingJournal) {
        logger.info("Applying {} post-stitching operations.",
            postStitchingOperationLibrary.getPostStitchingOperations().size());
        final DataMetricTimer executionTimer = POST_STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();

        final PostStitchingOperationScopeFactory scopeFactory = new PostStitchingOperationScopeFactory(
            graphWithSettings.getTopologyGraph(), probeStore, targetStore, cpuCapacityStore);
        final EntitySettingsCollection settingsCollection = graphWithSettings.constructEntitySettingsCollection();

        stitchingJournal.markPhase(StitchingPhase.POST_STITCHING);
        postStitchingOperationLibrary.getPostStitchingOperations().stream()
            .forEach(postStitchingOperation -> applyPostStitchingOperation(postStitchingOperation, scopeFactory,
                    settingsCollection, stitchingJournal));

        executionTimer.observe();
    }

    /**
     * Apply PreStitching phase to the entities in the stitching context.
     *
     * @param scopeFactory A factory to be used for calculating the entities within a given scope of a
     *                     {@link PreStitchingOperation}.
     * @param stitchingJournal The stitching journal used to track changes.
     */
    @VisibleForTesting
    void preStitch(@Nonnull final StitchingOperationScopeFactory scopeFactory,
                   @Nonnull final IStitchingJournal<StitchingEntity> stitchingJournal) {
        logger.info("Applying {} pre-stitching operations.",
            preStitchingOperationLibrary.getPreStitchingOperations().size());
        final DataMetricTimer executionTimer = PRE_STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();

        stitchingJournal.markPhase(StitchingPhase.PRE_STITCHING);
        preStitchingOperationLibrary.getPreStitchingOperations().stream()
            .forEach(preStitchingOperation -> applyPreStitchingOperation(
                preStitchingOperation, scopeFactory, stitchingJournal));

        executionTimer.observe();
    }

    /**
     * Apply the stitching phase to the entities in the stitching context.
     *
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the stitching operation should operate on.
     * @param stitchingJournal The stitching journal used to track changes.
     */
    @VisibleForTesting
    void mainStitch(@Nonnull final StitchingOperationScopeFactory scopeFactory,
                    @Nonnull final IStitchingJournal<StitchingEntity> stitchingJournal) {
        logger.info("Applying {} stitching operations for {} probes.",
                stitchingOperationStore.operationCount(), stitchingOperationStore.probeCount());

        stitchingJournal.markPhase(StitchingPhase.MAIN_STITCHING);
        final DataMetricTimer executionTimer = STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();
        stitchingOperationStore.getAllOperations().stream()
                .sorted(probeStore.getProbeOrdering())
                .forEach(probeOperation ->
                        targetStore.getProbeTargets(probeOperation.probeId).forEach(
                                target -> applyOperationForTarget(probeOperation.stitchingOperation,
                                        scopeFactory, stitchingJournal, target.getId(),
                                        probeOperation.probeId)));
        cleanupUnstitchedProxyEntities(scopeFactory, stitchingJournal);
        executionTimer.observe();
    }

    /**
     * Apply a specific {@link PreStitchingOperation}.
     *
     * @param preStitchingOperation The pre-stitching operation to be applied.
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the pre-stitching operation should operate on.
     * @param stitchingJournal The stitching journal used to track changes.
     */
    private void applyPreStitchingOperation(@Nonnull final PreStitchingOperation preStitchingOperation,
                                            @Nonnull final StitchingOperationScopeFactory scopeFactory,
                                            @Nonnull final IStitchingJournal<StitchingEntity> stitchingJournal) {
        try {
            final Stream<StitchingEntity> entities =
                preStitchingOperation.getScope(scopeFactory).entities();
            final StitchingResultBuilder resultBuilder =
                new StitchingResultBuilder(scopeFactory.getStitchingContext());
            stitchingJournal.recordOperationBeginning(preStitchingOperation);

            final TopologicalChangelog<StitchingEntity> results = preStitchingOperation.performOperation(
                entities, resultBuilder);
            results.getChanges().forEach(change -> change.applyChange(stitchingJournal));
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                logger.error("Unable to apply pre-stitching operation: {} due to grpc error: {}",
                        preStitchingOperation.getClass().getSimpleName(), e.getMessage());
            } else {
                logger.error("Unable to apply pre-stitching operation: {} due to exception:",
                        preStitchingOperation.getClass().getSimpleName(), e);
            }
            stitchingJournal.recordOperationException("Unable to apply pre-stitching operation " +
                preStitchingOperation.getClass().getSimpleName() + " due to exception: ", e);
        } finally {
            stitchingJournal.recordOperationEnding();
        }
    }

    /**
     * Apply a specific {@link PostStitchingOperation}.
     *  @param postStitchingOperation The post-stitching operation to be applied.
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the post-stitching operation should operate on.
     * @param settingsCollection A collection of settings for entities permitting the by-name lookup of a setting
     * @param stitchingJournal The stitching journal used to track changes.
     */
    private void applyPostStitchingOperation(@Nonnull final PostStitchingOperation postStitchingOperation,
                                             @Nonnull final PostStitchingOperationScopeFactory scopeFactory,
                                             @Nonnull final EntitySettingsCollection settingsCollection,
                                             @Nonnull final IStitchingJournal<TopologyEntity> stitchingJournal) {
        try {
            final Stream<TopologyEntity> entities =
                postStitchingOperation.getScope(scopeFactory).entities();
            final PostStitchingResultBuilder resultBuilder = new PostStitchingResultBuilder();
            stitchingJournal.recordOperationBeginning(postStitchingOperation);

            final TopologicalChangelog<TopologyEntity> results = postStitchingOperation.performOperation(
                entities, settingsCollection, resultBuilder);
            results.getChanges().forEach(change -> change.applyChange(stitchingJournal));
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                logger.error("Unable to apply post-stitching operation: {} due to grpc error: {}",
                        postStitchingOperation.getClass().getSimpleName(), e.getMessage());
            } else {
                logger.error("Unable to apply post-stitching operation: {} due to exception: ",
                        postStitchingOperation.getClass().getSimpleName(), e);
            }
            stitchingJournal.recordOperationException("Unable to apply post-stitching operation " +
                postStitchingOperation.getClass().getSimpleName() + " due to exception: ", e);
        } finally {
            stitchingJournal.recordOperationEnding();
        }
    }

    /**
     * Apply a specific stitching operation for a specific target.
     *
     * If an exception occurs for the operation-target pair, the rest of the operation results
     * are skipped. If some of the results were already applied when the exception is thrown,
     * those results continue to be applied and are not rolled back, but the rest of the results
     * for this operation-target pair are abandoned.
     *
     * @param operation The operation to apply.
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the stitching operation should operate on.
     * @param stitchingJournal The stitching journal used to track changes.
     * @param targetId The id of the target that is being stitched via the operation.
     * @param probeId The id of the probe associated with the target that is being stitched via the operation.
     */
    private void applyOperationForTarget(@Nonnull final StitchingOperation<?, ?> operation,
                                         @Nonnull final StitchingOperationScopeFactory scopeFactory,
                                         @Nonnull final IStitchingJournal<StitchingEntity> stitchingJournal,
                                         final long targetId, final long probeId) {
        try {
            Optional<EntityType> externalType = operation.getExternalEntityType();
            stitchingJournal.recordOperationBeginning(operation, operationDetailsForTarget(probeId, targetId));
            final TopologicalChangelog<StitchingEntity> results = externalType.map(extType ->
                    applyStitchWithExternalEntitiesOperation(operation, scopeFactory, targetId, extType))
                    .orElseGet(() -> applyStitchAloneOperation(operation, scopeFactory, targetId));
            results.getChanges().forEach(change -> change.applyChange(stitchingJournal));
        } catch (RuntimeException e) {
            logger.error("Unable to apply stitching operation " + operation.getClass().getSimpleName() +
                " due to exception: ", e);
            stitchingJournal.recordOperationException("Unable to apply stitching operation " +
                operation.getClass().getSimpleName() + " due to exception: ", e);
        } finally {
            stitchingJournal.recordOperationEnding();
        }
    }

    /**
     * Get a collection of details this probe and target to record in the stitching journal when
     * beginning a new stitching operation.
     *
     * @param probeId The ID of the probe associated with the target.
     * @param targetId The target for which the stitching operation is being run.
     * @return String messages detailing the probe and target associated with the stitching operation.
     */
    private Collection<String> operationDetailsForTarget(long probeId, long targetId) {
        final String probeDetails = probeStore.getProbe(probeId)
            .map(info -> info.getProbeCategory() + "/" + info.getProbeType() + "/" + probeId)
            .orElse(Long.toString(probeId));
        final String targetDetails = targetStore.getTarget(targetId)
            .map(info -> info.getDisplayName() + "/" + targetId)
            .orElse(Long.toString(targetId));

        return Arrays.asList(probeDetails, targetDetails);
    }

    /**
     * Apply a stitching operation in which internal entities do not need to be matched
     * with external entities. Instead, process each internal entity alone.
     *
     * @param operation The stitching operation to apply.
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the stitching operation should operate on.
     * @param targetId The id of the target for which this stitching operation is being applied.
     * @return The results generated by the stitching operation. These results will
     *         be applied to mutate the {@link StitchingContext} and its associated
     *         {@link TopologyStitchingGraph}.
     */
    private TopologicalChangelog applyStitchAloneOperation(
        @Nonnull final StitchingOperation<?, ?> operation,
        @Nonnull final StitchingOperationScopeFactory scopeFactory,
        final long targetId) {

        final EntityType internalEntityType = operation.getInternalEntityType();
        // if a scope is provided, create a stream of stitching entities from the scope, otherwise
        // use the internal entities from the stitching context of the correct entity type with this
        // targetId
        Stream<StitchingEntity> scopeEntities = operation.getScope(scopeFactory)
                .map(scope -> scope.entities()
                        .filter(stitchEntity -> stitchEntity.getTargetId() == targetId))
                .orElseGet(() -> scopeFactory.getStitchingContext()
                        .internalEntities(internalEntityType, targetId)
                        .map(StitchingEntity.class::cast));
        final List<StitchingPoint> stitchingPoints = scopeEntities
                .filter(internalEntity -> !operation.getInternalSignature(internalEntity).isEmpty())
                .map(StitchingPoint::new)
                .collect(Collectors.toList());

        final StitchingResultBuilder resultBuilder =
                new StitchingResultBuilder(scopeFactory.getStitchingContext());
        return operation.stitch(stitchingPoints, resultBuilder);
    }

    /**
     * Apply a stitching operation for a target that matches internal and external entities.
     *
     * @param operation The operation for stitching.
     * @param scopeFactory The factory for use in generating the scopes to be used to create the scope
     *                     of entities that the stitching operation should operate on.
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
    TopologicalChangelog<StitchingEntity> applyStitchWithExternalEntitiesOperation(
        @Nonnull final StitchingOperation<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> operation,
        @Nonnull final StitchingOperationScopeFactory scopeFactory,
        final long targetId,
        @Nonnull final EntityType externalEntityType) {

        // First create a map of signatures to their corresponding builders for all internal entities.
        // We will use this map later to look up the internal entities by their signature.
        // Be sure to use an identity hash map because it is important that signatures that are equal
        // by their equals method map to different keys for lookup purposes here.
        final IdentityHashMap<INTERNAL_SIGNATURE_TYPE, StitchingEntity> signaturesToEntities =
            new IdentityHashMap<>();
        final EntityType internalEntityType = operation.getInternalEntityType();

        scopeFactory.getStitchingContext()
                .internalEntities(internalEntityType, targetId)
                        .forEach(internalEntity -> operation.getInternalSignature(internalEntity)
                                        .forEach(internalSignature -> signaturesToEntities
                                                        .put(internalSignature, internalEntity)));

        // Now construct an index that can quickly calculate which internal signatures that an
        // external signature matches. Note that the internal implementation of the index are
        // determined by the operation itself but its contract is to be able to be able to
        // provide as close to a constant-time lookup as possible for all internal signatures
        // that match a given external signature.
        final StitchingIndex<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> stitchingIndex =
            operation.createIndex(signaturesToEntities.size());
        signaturesToEntities.keySet().forEach(stitchingIndex::add);

        // Compute a map of all internal entities to their matching external entities using
        // the index provided by the operation.
        // Exclude entities that come from the same target as the one being stitched.
        final Stream<StitchingEntity> externalEntities =
                operation.getScope(scopeFactory)
                        .map(f -> f.entities()
                                        .map(StitchingEntity.class::cast)
                                        .filter(e -> e.getTargetId() != targetId))
                        .orElseGet(() -> scopeFactory.getStitchingContext()
                                .externalEntities(externalEntityType, targetId)
                                .map(StitchingEntity.class::cast));
        final MatchMap matchMap = new MatchMap(signaturesToEntities.size());
        externalEntities.forEach(externalEntity -> operation.getExternalSignature(externalEntity)
                        .forEach(externalSignature -> {
                            stitchingIndex.findMatches(externalSignature)
                                            .forEach(internalSignature -> {
                                                matchMap.addMatch(signaturesToEntities
                                                                                .get(internalSignature),
                                                                externalEntity);
                                            });
                        }));

        // Process the matches.
        final StitchingResultBuilder resultBuilder =
                new StitchingResultBuilder(scopeFactory.getStitchingContext());
        return operation.stitch(matchMap.createStitchingPoints(), resultBuilder);
    }

    /**
     * Maintains a mapping of (internal entity) -> (list of matching external entities) identified
     * during the matching phase of a stitching operation.
     *
     * Unused by stitch alone operations because external entities are ignored in such operations.
     */
    private static class MatchMap {
        private final Map<StitchingEntity, Collection<StitchingEntity>> matches;

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
        public void addMatch(@Nonnull final StitchingEntity internalEntity,
                             @Nonnull final StitchingEntity externalEntity) {
            final Collection<StitchingEntity> matchList =
                matches.computeIfAbsent(internalEntity, key -> new HashSet<>());

            matchList.add(externalEntity);
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
