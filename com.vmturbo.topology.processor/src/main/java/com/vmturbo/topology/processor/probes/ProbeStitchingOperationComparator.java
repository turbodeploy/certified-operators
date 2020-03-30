package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;

/**
 * Comparator for sorting {@link ProbeStitchingOperation}s.
 *
 * Used to ensure a total ordering of operations based on their probe category and type.
 * Uses the union-find algorithm to construct groups of categories and types of the same position.
 * See https://en.wikipedia.org/wiki/Disjoint-set_data_structure for more details on this algorithm.
 */
public class ProbeStitchingOperationComparator implements Comparator<ProbeStitchingOperation> {

    private static final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;
    private final List<ProbeCategory> categoryPositions;
    private final List<SDKProbeType> typePositions;

    /**
     * Create a new {@link ProbeStitchingOperationComparator} for sorting
     * {@link ProbeStitchingOperation}s.
     *
     * @param probeStore The probe store used for looking up the probe info for an operation.
     * @param probeCategoriesStitchBeforeMap The constraints placed on which categories have to stitch before
     *                                       which others.
     * @param probeTypesStitchBeforeMap The constraints placed on which probe types have to stitch before
     *                                  which others.
     * @throws ProbeException If the DFS detects a cycle in stitching dependencies.
     */
    ProbeStitchingOperationComparator(
        @Nonnull final ProbeStore probeStore,
        @Nonnull final Map<ProbeCategory, Set<ProbeCategory>> probeCategoriesStitchBeforeMap,
        @Nonnull final Map<SDKProbeType, Set<SDKProbeType>> probeTypesStitchBeforeMap)
        throws ProbeException {

        Objects.requireNonNull(probeCategoriesStitchBeforeMap);
        Objects.requireNonNull(probeTypesStitchBeforeMap);

        this.probeStore = Objects.requireNonNull(probeStore);
        categoryPositions = buildPositions(probeCategoriesStitchBeforeMap, ProbeCategory.values());
        typePositions = buildPositions(probeTypesStitchBeforeMap, SDKProbeType.values());
    }

    /**
     * Run a reverse topological sort to identify sort positions for the category or type.
     * This assures a full ordering for all stitching operations.
     *
     * @param stitchingDependencies Constraints on what must be stitched after what.
     *                              If Storage has an entry in the map to [HYPERVISOR, Cloud Management],
     *                              then that means that HYPERVISOR and Cloud Management are dependencies
     *                              of Storage and must be stitched before Storage.
     * @param values All the categories or types to topologically sort.
     * @param <T> The category or type data type.
     * @return A map from category or type to its stitching position.
     * @throws ProbeException If the DFS detects a cycle in stitching dependencies.
     */
    private <T> List<T> buildPositions(
        @Nonnull final Map<T, Set<T>> stitchingDependencies,
        @Nonnull final T[] values) throws ProbeException {

        final List<T> positions = new ArrayList<>(values.length);
        final List<T> startedVisits = new ArrayList<>(values.length);

        // Perform DFS. Iterate in order of the values array to ensure we get a
        // consistent topological ordering across sorts.
        for (T categoryOrType : values) {
            visit(categoryOrType, positions, startedVisits, stitchingDependencies);
        }

        logger.debug("Sorted positions: {}",
            () -> positions.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]")));
        return positions;
    }

    /**
     * Perform a DFS visit to a node in the directed graph.
     *
     * @param categoryOrType The category of type of the node to be visited.
     * @param positions All the nodes.
     * @param startedVisits All of the nodes whose visits have been started at one point.
     * @param dependencies The list of constraints (edges between nodes).
     * @param <T> The category or type data type.
     * @throws ProbeException If the DFS detects a cycle in stitching dependencies.
     */
    private <T> void visit(@Nonnull final T categoryOrType,
                           @Nonnull final List<T> positions,
                           @Nonnull final List<T> startedVisits,
                           @Nonnull final Map<T, Set<T>> dependencies) throws ProbeException {
        if (positions.contains(categoryOrType)) {
            // We have already visited this branch. Check this before checking cycles to ensure
            // we don't erroneously detect a cycle.
            return;
        } else if (startedVisits.contains(categoryOrType)) {
            // Cycle detected. We cannot topologically sort when cycles are present.
            throw new ProbeException("Cycle present in probe stitching dependencies.");
        }

        // Visit all nodes reachable from this one
        startedVisits.add(categoryOrType);
        for (T dependency : dependencies.getOrDefault(categoryOrType, Collections.emptySet())) {
            visit(dependency, positions, startedVisits, dependencies);
        }

        positions.add(categoryOrType);
    }

    /**
     * Identify which of two operations should stitch first based on both category
     * and type constraints. If the operations are indifferent as to which stitches first,
     * the operation that appears first alphabetically will stitch first.
     *
     * @param op1 The first operation to compare
     * @param op2 The second operation to compare
     * @return The result of the comparison. -1 if op1 < op2, 1 if op2 < op1, and 0 if op1 == op2
     */
    @Override
    public int compare(ProbeStitchingOperation op1, ProbeStitchingOperation op2) {
        final StitchingPosition position1 = getPosition(op1);
        final StitchingPosition position2 = getPosition(op2);
        final int comparison = position1.compareTo(position2);

        // In the case where we are indifferent to stitching position based on category and type,
        // sort alphabetically in order to be able to generate a consistent ordering.
        if (comparison == 0) {
            return op1.stitchingOperation.getOperationName().compareTo(
                op2.stitchingOperation.getOperationName());
        }

        return comparison;
    }

    /**
     * Get the position for a stitching operation.
     * Returns Integer.MAX_VALUE (stitch last) for any operations associated with unrecognized probe
     * categories or types.
     *
     * @param op The operation whose position should be retrieved.
     * @return The stitching position for the operation.
     */
    private StitchingPosition getPosition(@Nonnull final ProbeStitchingOperation op) {
        if (op.probeId == null) {
            logger.error("Null probe ID on operation", op.stitchingOperation.getOperationName());
            return StitchingPosition.MAX_POSITION;
        }

        return probeStore.getProbe(op.probeId).map(info -> {
            // ProbeCategory.create returns UNKNOWN for an unknown probe category
            final ProbeCategory category = ProbeCategory.create(info.getProbeCategory());
            final int categoryPosition = (category == ProbeCategory.UNKNOWN) ?
                Integer.MAX_VALUE :
                positionFor(categoryPositions.indexOf(category));

            // SDKProbeType.create returns null for an unknown probe type
            final SDKProbeType type = SDKProbeType.create(info.getProbeType());
            final int typePosition = positionFor(typePositions.indexOf(type));

            return new StitchingPosition(categoryPosition, typePosition);
        }).orElse(StitchingPosition.MAX_POSITION);
    }

    /**
     * Get the position associated with an index.
     *
     * @param index The index of the position in the array.
     * @return Returns the original index if >= 0 and the max int if less than 0.
     */
    private static int positionFor(final int index) {
        if (index < 0) {
            return Integer.MAX_VALUE;
        }

        return index;
    }

    /**
     * A stitching position represents the category and type positions for a probe category and type respectively.
     * Useful for sorting probe stitching operations where constraints are placed on the order
     * in which stitching operations are run based on probe category and type.
     */
    private static class StitchingPosition implements Comparable<StitchingPosition> {
        private final int categoryPosition;
        private final int typePosition;

        private static final StitchingPosition MAX_POSITION = new StitchingPosition(
            Integer.MAX_VALUE, Integer.MAX_VALUE);

        /**
         * Create a new {@link StitchingPosition} for use in sorting stitching operations.
         *
         * @param categoryPosition The position for the operation's probe category.
         * @param typePosition The position for the operation's probe type.
         */
        StitchingPosition(final int categoryPosition, final int typePosition) {
            this.categoryPosition = categoryPosition;
            this.typePosition = typePosition;
        }

        @Override
        public int compareTo(StitchingPosition otherPosition) {
            if (otherPosition == null) {
                // Should never happen
                logger.error("Comparison to null operation");
                return -1;
            }

            int relativeCategory = Integer.compare(categoryPosition, otherPosition.categoryPosition);
            if (relativeCategory == 0) {
                return Integer.compare(typePosition, otherPosition.typePosition);
            }
            return relativeCategory;
        }

        @Override
        public String toString() {
            return String.format("%d/%d", categoryPosition, typePosition);
        }
    }
}
