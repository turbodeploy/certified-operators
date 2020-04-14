package com.vmturbo.topology.processor.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.ListStringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToListStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.ListStringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary.StitchingUnknownProbeException;
import com.vmturbo.stitching.StringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringToListStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.StringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringToStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.journal.JournalableOperation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeOrdering;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * A store of stitching operations for use during stitching.
 *
 * When a probe registers with the TopologyProcessor, we automatically look up the {@link StitchingOperation}s
 * to use for that probe in the {@link StitchingOperationLibrary} and then save an entry internally between
 * the probeId and its corresponding {@link StitchingOperation}s.
 */
@ThreadSafe
public class StitchingOperationStore {
    private final Logger logger = LogManager.getLogger();

    /**
     * A map from probeId to the list of stitching operations that that probe
     * registered with.
     */
    private final Map<Long, List<StitchingOperation<?, ?>>> operations = new HashMap<>();

    private final StitchingOperationLibrary stitchingOperationLibrary;

    public StitchingOperationStore(@Nonnull final StitchingOperationLibrary stitchingOperationLibrary) {
        this.stitchingOperationLibrary = Objects.requireNonNull(stitchingOperationLibrary);
    }

    /**
     * Add the operations for a probe to the store. Operations are looked up by the type and category
     * of the probe.
     *
     * If the store already contains operations for the probe with the given ID, those operations
     * will be overwritten.
     *
     * @param probeId The id of the probe whose operations should be added.
     * @param probeInfo Info for the probe whose operations should be added. The type and category
     *                  fields of the info must be populated.
     * @throws ProbeException If no info or operations can be found for the probe with the given id.
     */
    public void setOperationsForProbe(long probeId, @Nonnull final ProbeInfo probeInfo,
                                      @Nonnull ProbeOrdering probeOrdering)
        throws ProbeException {
        setOperationsForProbe(probeId, probeInfo,
                probeOrdering.getCategoriesForProbeToStitchWith(probeInfo));
    }

    /**
     * Add the operations for a probe to the store. Operations are looked up by the type and category
     * of the probe.
     *
     * If the store already contains operations for the probe with the given ID, those operations
     * will be overwritten.
     *
     * @param probeId The id of the probe whose operations should be added.
     * @param probeInfo Info for the probe whose operations should be added. The type and category
     *                  fields of the info must be populated.
     * @param probeScope Set of ProbeCategory that gives the ProbeCategories that the probe of
     *                   this type stitches with.
     * @throws ProbeException If no info or operations can be found for the probe with the given id.
     */
    public void setOperationsForProbe(long probeId, @Nonnull final ProbeInfo probeInfo,
                                      @Nonnull final Set<ProbeCategory> probeScope)
            throws ProbeException {
        try {
            final ProbeCategory category = ProbeCategory.create(probeInfo.getProbeCategory());
            // TODO:  Right now, we have some probes (Azure, AWS) that only need to stitch with
            // themselves.  If we leave probeScope empty, they will stitch with every category which
            // could lead to problems (for example, an Azure region getting stitched onto the
            // region from the Azure volumes probe instead of the other way around).  So if
            // we have a data driven probe with no scope to stitch with, assume it is one of these
            // cases and sets probe scope to the probe's category.  This will go away once we
            // allow the probe scope to be set in a data driven manner.
            List<StitchingOperation<?, ?>> operations =
                    createStitchingOperationsFromProbeInfo(probeInfo,
                            probeScope.isEmpty() ? ImmutableSet.of(category) : probeScope);
            List<StitchingOperation<?, ?>> customOperations = stitchingOperationLibrary.stitchingOperationsFor(
                    probeInfo.getProbeType(), category);

            if (isCustomStitchingOperationsApplicable(operations, customOperations)) {
                operations.addAll(customOperations);
            } else {
                logger.warn("Skipping {} custom stitching operations for {} since there are data" +
                                " driven stitch operations for a subset of the custom stitching" +
                                " operations' entity types.", customOperations, probeInfo);
            }
            setOperationsForProbe(probeId, operations);

            logger.info("Selected {} stitching operations for probe {} ({}) in category {}.",
                operations.stream()
                    .map(JournalableOperation::getOperationName)
                    .collect(Collectors.joining(", ")),
                probeId,
                probeInfo.getProbeType(),
                probeInfo.getProbeCategory());
        } catch (StitchingUnknownProbeException e) {
            throw new ProbeException("Error looking up probe stitching operations", e);
        }
    }

    /**
     * Determines if both the probe data driven and  custom stitching operations can be run.
     * This will be allowed only if there is no clash between internal and external entity types
     * in their respective stitching operations.
     *
     * @param dataDrivenOperations - Operations defined in probe's supply chain definition.
     * @param customOperations - Custom operations set up for the probe
     * @return True if both the probe data driven and  custom stitching operations can be run.
     */
    private boolean isCustomStitchingOperationsApplicable(final List<StitchingOperation<?, ?>> dataDrivenOperations,
                                                          final List<StitchingOperation<?, ?>> customOperations) {
        if (dataDrivenOperations.isEmpty() || customOperations.isEmpty()) {
            return true;
        }
        final Set<EntityType> probeDrivenEntityTypes = getAllEntityTypes(dataDrivenOperations);
        final Set<EntityType> customOpEntityTypes = getAllEntityTypes(customOperations);
        return (Sets.intersection(probeDrivenEntityTypes, customOpEntityTypes).isEmpty());
    }

    /**
     * Returns a set of internal and external entity types referenced by the passed set of
     * stitching operations.
     *
     * @param operations Set of stitching operations
     * @return Set of entity types
     */
    private static Set<EntityType> getAllEntityTypes(final List<StitchingOperation<?, ?>> operations) {
        Set<EntityType> entityTypes = new HashSet<>();
        operations
                .forEach(o -> {
                    entityTypes.add(o.getInternalEntityType());
                    if (o.getExternalEntityType().isPresent()) {
                        entityTypes.add(o.getExternalEntityType().get());
                    }
                });
        return entityTypes;
    }

    private List<StitchingOperation<?, ?>> createStitchingOperationsFromProbeInfo(
            @Nonnull final ProbeInfo probeInfo, @Nonnull final Set<ProbeCategory> probeScope) {
        return probeInfo.getSupplyChainDefinitionSetList().stream()
                .filter(TemplateDTO::hasMergedEntityMetaData)
                .map(tDTO -> createStitchingOperation(tDTO, probeScope))
                .collect(Collectors.toList());
    }

    private StitchingOperation<?, ?> createStitchingOperation(
            @Nonnull final TemplateDTO templateDTO, @Nonnull final Set<ProbeCategory> probeScope) {
        MergedEntityMetadata memd = templateDTO.getMergedEntityMetaData();
        if (memd.getMatchingMetadata().getReturnType() == ReturnType.STRING) {
            if (memd.getMatchingMetadata().getExternalEntityReturnType() == ReturnType.STRING) {
                return new StringToStringDataDrivenStitchingOperation(
                        new StringToStringStitchingMatchingMetaDataImpl(
                                templateDTO.getTemplateClass(), memd), probeScope);
            } else {
                return new StringToListStringDataDrivenStitchingOperation(
                        new StringToListStringStitchingMatchingMetaDataImpl(
                                templateDTO.getTemplateClass(), memd), probeScope);

            }
        } else {
            if (memd.getMatchingMetadata().getExternalEntityReturnType() == ReturnType.STRING) {
                return new ListStringToStringDataDrivenStitchingOperation(
                        new ListStringToStringStitchingMatchingMetaDataImpl(
                                templateDTO.getTemplateClass(), memd), probeScope);
            } else {
                return new ListStringToListStringDataDrivenStitchingOperation(
                        new ListStringToListStringStitchingMatchingMetaDataImpl(
                                templateDTO.getTemplateClass(), memd), probeScope);
            }

        }
    }

    @VisibleForTesting
    synchronized void setOperationsForProbe(final long probeId,
                                            @Nonnull final List<StitchingOperation<?, ?>> probeStitchingOperations) {
        operations.put(probeId, Objects.requireNonNull(probeStitchingOperations));
    }

    /**
     * Remove the stitching operations for a probe.
     *
     * @param probeId The id of the probe whose operations should be removed.
     * @return The removed operations for the probe with the given ID.
     *         If the store has no operations for the probe, returns {@link Optional#empty()}.
     */
    public synchronized Optional<List<StitchingOperation<?, ?>>> removeOperationsForProbe(long probeId) {
        return Optional.ofNullable(operations.remove(probeId))
            .map(Collections::unmodifiableList);
    }

    /**
     * Get the stitching operations for a probe.
     *
     * @param probeId The id of the probe whose operations should be retrieved.
     * @return The operations for the probe with the given ID.
     *         If the store has no operations for the probe, returns {@link Optional#empty()}.
     */
    public synchronized Optional<List<StitchingOperation<?, ?>>> getOperationsForProbe(long probeId) {
        return Optional.ofNullable(operations.get(probeId))
            .map(Collections::unmodifiableList);
    }

    /**
     * Return all operations for all probes.
     * Operations are returned in ascending order of priority (lowest priority first).
     *
     * @return The collection of all {@link ProbeStitchingOperation}s in the store.
     */
    @Nonnull
    public synchronized Collection<ProbeStitchingOperation> getAllOperations() {
        return operations.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream()
                .map(operation -> new ProbeStitchingOperation(entry.getKey(), operation)))
            .collect(Collectors.toList());
    }

    /**
     * The number of probes with stitching operations in the store.
     *
     * @return the number of probes with stitching operations in the store.
     */
    public int probeCount() {
        return operations.size();
    }

    /**
     * The number of overall operations in the store.
     *
     * @return the number of overall operations in the store.
     */
    public synchronized int operationCount() {
        return operations.values().stream()
            .mapToInt(List::size)
            .sum();
    }

    /**
     * Clear all the stitching operations.
     */
    public void clearOperations() {
        operations.clear();
    }

    /**
     * A class that associates a probeId with the stitching operations that should be applied
     * for that probe.
     */
    @Immutable
    public static class ProbeStitchingOperation {
        public final Long probeId;
        public final StitchingOperation<?, ?> stitchingOperation;

        public ProbeStitchingOperation(@Nonnull final Long probeId,
                                       @Nonnull final StitchingOperation<?, ?> stitchingOperation) {
            this.probeId = probeId;
            this.stitchingOperation = stitchingOperation;
        }

        public String toString(@Nonnull final ProbeStore probeStore) {
            return stitchingOperation.getOperationName() + "[" +
                probeStore.getProbe(probeId).map(ProbeInfo::getProbeCategory).orElse("") + "/" +
                probeStore.getProbe(probeId).map(ProbeInfo::getProbeType).orElse("") + "]";
        }
    }
}
