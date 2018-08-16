package com.vmturbo.topology.processor.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

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
                probeOrdering.getCategoriesForProbeToStitchWith(probeId));
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
            List<StitchingOperation<?, ?>> operations =
                    createStitchingOperationsFromProbeInfo(probeInfo,
                            probeScope);
            if (operations.isEmpty()) {
                operations = stitchingOperationLibrary.stitchingOperationsFor(
                        probeInfo.getProbeType(), category);
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
    }
}
