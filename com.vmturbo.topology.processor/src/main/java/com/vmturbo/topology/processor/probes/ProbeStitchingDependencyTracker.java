package com.vmturbo.topology.processor.probes;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Tracks stitching order dependency among probes.  Can return a set of {@link ProbeCategory} or
 * {@link SDKProbeType} that need to stitch before a passed in probe category or type.
 */
public class ProbeStitchingDependencyTracker {

    private final static Logger logger = LogManager.getLogger();

    /**
     * Map that takes a {@link ProbeCategory} and returns a Set of {@link ProbeCategory}s that need
     * to stitch before it.
     */
    private final Map<ProbeCategory, Set<ProbeCategory>> probeCategoriesStitchBeforeMap =
        Maps.newHashMap();

    /**
     * Map that takes a {@link SDKProbeType} and returns a Set of {@link SDKProbeType}s objects that
     * need to stitch before it.
     */
    private final Map<SDKProbeType, Set<SDKProbeType>> probeTypesStitchBeforeMap =
        Maps.newHashMap();

    /**
     * Create a ProbeStitchingDependencyTracker from a dependency map that has no cycles.
     *
     * @param probeCategoriesDependencyMap A map from {@link ProbeCategory} to a StitchingEntry
     *                                     object containing all the probe categories that need to
     *                                     stitch before it.
     * @param probeTypesDependencyMap A map from {@link SDKProbeType} to a StitchingEntry object
     *                                containing all the probe types that need to stitch before it.
     */
    private ProbeStitchingDependencyTracker(
            @Nonnull final Map<ProbeCategory, StitchingEntry<ProbeCategory>>
                probeCategoriesDependencyMap,
            @Nonnull final Map<SDKProbeType, StitchingEntry<SDKProbeType>>
                probeTypesDependencyMap) {
        constructStitchingDependencyTracker(probeCategoriesDependencyMap,
            probeCategoriesStitchBeforeMap);
        constructStitchingDependencyTracker(probeTypesDependencyMap,
            probeTypesStitchBeforeMap);
    }

    private <CATEGORY_OR_TYPE> void constructStitchingDependencyTracker(
            @Nonnull final Map<CATEGORY_OR_TYPE, StitchingEntry<CATEGORY_OR_TYPE>> dependencyMap,
            @Nonnull final Map<CATEGORY_OR_TYPE, Set<CATEGORY_OR_TYPE>> stitchBeforeMap) {
        Objects.requireNonNull(dependencyMap);
        dependencyMap.entrySet().stream()
            .forEach(nextEntry -> stitchBeforeMap
                .computeIfAbsent(nextEntry.getKey(), key -> Sets.newHashSet())
                .addAll(nextEntry.getValue().getStitchAfter()));
        stitchBeforeMap.keySet().forEach(
            key -> stitchBeforeMap.put(key, getTransitiveClosure(key, stitchBeforeMap)));
    }

    /**
     * Compute the transitive closure of a {@link CATEGORY_OR_TYPE} in stitchBeforeMap.  The
     * transitive closure of a vertex in a graph is the set of all vertices reachable from that
     * vertex.  See https://en.wikipedia.org/wiki/Transitive_closure#In_graph_theory. For example,
     * if Storage has a map entry of Hypervisor, Hyper_Converged and Hyper_Converged has an entry of
     * Hypervisor, Cloud_Management, then the transitive closure for Storage would be Hypervisor,
     * Hyper_Converged, Cloud_Management meaning Storage must stitch after all those categories are
     * stitched.  Note that we take advantage of the fact that stitchBeforeMap has no cycles in it.
     *
     * @param categoryOrType The {@link CATEGORY_OR_TYPE} to compute the transitive closure of.
     * @return {@link Set} giving the ProbeCategory objects in the transitive closure.
     */
    private <CATEGORY_OR_TYPE> Set<CATEGORY_OR_TYPE> getTransitiveClosure(
            @Nonnull final CATEGORY_OR_TYPE categoryOrType,
            @Nonnull final Map<CATEGORY_OR_TYPE, Set<CATEGORY_OR_TYPE>> stitchBeforeMap) {
        Set<CATEGORY_OR_TYPE> retVal = Sets.newHashSet();
        Set<CATEGORY_OR_TYPE> categoriesOrTypesToAdd = stitchBeforeMap.get(categoryOrType);
        logger.trace("Probe category or type {} has category or type {}", categoryOrType,
            categoriesOrTypesToAdd);
        if (categoriesOrTypesToAdd != null) {
            categoriesOrTypesToAdd.stream()
                .forEach(probeCat -> retVal.addAll(getTransitiveClosure(probeCat,
                    stitchBeforeMap)));
            retVal.addAll(categoriesOrTypesToAdd);
        }
        logger.trace("getTransitiveClosure for {} returning {}", categoryOrType, retVal);
        return retVal;

    }

    /**
     * Return the set of probe categories that must stitch before this one.
     *
     * @param probeCategory {@link ProbeCategory} to get the dependencies of.
     * @return Set of probe categories that must stitch before this one.
     */
    public Set<ProbeCategory> getProbeCategoriesThatStitchBefore(
            ProbeCategory probeCategory) {
        return probeCategoriesStitchBeforeMap.getOrDefault(probeCategory, Collections.emptySet());
    }

    /**
     * Return the set of prpbe types that must stitch before this one.
     *
     * @param probeType {@link SDKProbeType} to get the dependencies of.
     * @return Set of probe types that must stitch before this one.
     */
    public Set<SDKProbeType> getProbeTypesThatStitchBefore(
            SDKProbeType probeType) {
        return probeTypesStitchBeforeMap.getOrDefault(probeType, Collections.emptySet());
    }

    /**
     * Create a new comparator for sorting {@code StitchingOperation}s. Sort order is determined
     * by the dependency maps tracked by this class.
     *
     * @param probeStore Store for probes to look up probe information.
     * @return a new comparator for sorting {@code StitchingOperation}s.
     * @throws ProbeException If the DFS detects a cycle in stitching dependencies.
     */
    public ProbeStitchingOperationComparator createOperationComparator(
        @Nonnull final ProbeStore probeStore) throws ProbeException {
        return new ProbeStitchingOperationComparator(probeStore,
            probeCategoriesStitchBeforeMap, probeTypesStitchBeforeMap);
    }

    /**
     * Get a Builder for building a {@link ProbeStitchingDependencyTracker}.
     * @return Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Get the default ProbeStitchingDependencyTracker
     *
     * @return {@link ProbeStitchingDependencyTracker} that gives default probe category or type
     * stitching order.
     */
    @Nullable public static ProbeStitchingDependencyTracker getDefaultStitchingDependencyTracker() {
        try {
            return newBuilder()
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.STORAGE_BROWSING).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.STORAGE_BROWSING).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.NETWORK).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.FABRIC).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.FABRIC).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.APPLICATION_SERVER).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.APPLICATION_SERVER).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.APPLICATION_SERVER).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .requireThatProbeCategory(ProbeCategory.DATABASE_SERVER).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.DATABASE_SERVER).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.GUEST_OS_PROCESSES).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.GUEST_OS_PROCESSES).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.GUEST_OS_PROCESSES).stitchAfter(ProbeCategory.CLOUD_NATIVE)
                .requireThatProbeCategory(ProbeCategory.GUEST_OS_PROCESSES).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeCategory(ProbeCategory.GUEST_OS_PROCESSES).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .requireThatProbeCategory(ProbeCategory.ORCHESTRATOR).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.ORCHESTRATOR).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.BILLING).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.CUSTOM).stitchAfter(ProbeCategory.GUEST_OS_PROCESSES)
                .requireThatProbeCategory(ProbeCategory.CUSTOM).stitchAfter(ProbeCategory.APPLICATION_SERVER)
                // probe type ordering, we only support the probe types comparison if they are
                // belonged to same category.
                .requireThatProbeType(SDKProbeType.CLOUD_FOUNDRY).stitchAfter(SDKProbeType.PIVOTAL_OPSMAN)
                .build();
        } catch (ProbeException e) {
            logger.error("Dependency cycle while building default " +
                "ProbeStitchingDependencyTracker", e);
            return null;
        }
    }

    public static class Builder {

        private final Map<ProbeCategory, StitchingEntry<ProbeCategory>> probeCategoryDependencyMap =
            Maps.newHashMap();

        private final Map<SDKProbeType, StitchingEntry<SDKProbeType>> probeTypeDependencyMap =
            Maps.newHashMap();

        private Builder() { }

        /**
         * Create or get the StitchingEntry object that will enable us to add a {@link ProbeCategory}
         * stitching order dependency.
         *
         * @param probeCategory the {@link ProbeCategory} that we want to restrict to stitching
         *                      after some other {@link ProbeCategory} .
         * @return StitchingEntry for probe categories that will encapsulate all the probe
         * categories that need to stitch before it.
         */
        public StitchingEntry<ProbeCategory> requireThatProbeCategory(
                @Nonnull final ProbeCategory probeCategory) {
            return probeCategoryDependencyMap.computeIfAbsent(probeCategory,
                probeCat -> new StitchingEntry<>(this));
        }

        /**
         * Create or get the StitchingEntry object that will enable us to add a {@link SDKProbeType}
         * stitching order dependency.
         *
         * @param probetype the {@link SDKProbeType} that we want to restrict to stitching
         *                      after some other {@link SDKProbeType}.
         * @return StitchingEntry for probe types that will encapsulate all the probe types that
         * need to stitch before it.
         */
        public StitchingEntry<SDKProbeType> requireThatProbeType(
                @Nonnull final SDKProbeType probetype) {
            return probeTypeDependencyMap.computeIfAbsent(probetype,
                probeCat -> new StitchingEntry<>(this));
        }

        /**
         * Iterate over the probe {@link CATEGORY_OR_TYPE} and see if we have any cycles.
         *
         * @return true if there is a cycle, false if not.
         */
        private <CATEGORY_OR_TYPE> boolean checkForCycles(
                @Nonnull final Map<CATEGORY_OR_TYPE, StitchingEntry<CATEGORY_OR_TYPE>>
                    dependencyMap) {
            boolean cycleExists = false;
            Set<CATEGORY_OR_TYPE> allVisited = Sets.newHashSet();
            for (CATEGORY_OR_TYPE probeCat : dependencyMap.keySet()) {
                // if we haven't already covered this probe category or type, check it for cycles by
                // doing a depth first search with this as root
                if (allVisited.contains(probeCat)) {
                    continue;
                }
                Set<CATEGORY_OR_TYPE> visited = Sets.newHashSet();
                cycleExists = checkForCycles(probeCat, visited, dependencyMap);
                if (cycleExists) {
                    return true;
                }
                // keep track of all the probe categories or types we've visited to optimize
                // performance by avoiding searching the same portion of the forest twice.
                allVisited.addAll(visited);
            }
            return cycleExists;
        }

        /**
         * Check if there is a cycle starting from a particular {@link CATEGORY_OR_TYPE} in the
         * dependency map.
         *
         * @param root The starting probe category or type.
         * @param visited The set of probe categories or types that have already been visited.
         * @return
         */
        private <CATEGORY_OR_TYPE> boolean checkForCycles(
                @Nonnull CATEGORY_OR_TYPE root,
                @Nonnull Set<CATEGORY_OR_TYPE> visited,
                @Nonnull Map<CATEGORY_OR_TYPE, StitchingEntry<CATEGORY_OR_TYPE>> dependencyMap) {
            StitchingEntry<CATEGORY_OR_TYPE> stitchingEntry = dependencyMap.get(root);
            boolean cycleExists = false;
            if (visited.contains(root)) {
                logger.error("Cycle detected while building ProbeStitchingDependencyTracker."
                    + "  Cycle begins at probe category or type {}", root);
                return true;
            }
            visited.add(root);
            if (stitchingEntry != null) {
                for (CATEGORY_OR_TYPE probeCat : stitchingEntry.getStitchAfter()) {
                    cycleExists = checkForCycles(probeCat, visited, dependencyMap);
                    if (cycleExists) {
                        break;
                    }
                }
            }
            // done checking for cycles below
            visited.remove(root);
            return cycleExists;
        }

        /**
         * Build and return the {@link ProbeStitchingDependencyTracker} that represents the
         * stitching order dependencies for the {@link ProbeCategory}s or {@link SDKProbeType}s that
         * have dependencies after ensuring that no cycles exist in the stitching order.
         *
         * @return {@link ProbeStitchingDependencyTracker} encapsulating the stitching order, if it is
         * well defined.
         * @throws ProbeException if there is a cycle in the stitching order.
         */
        public ProbeStitchingDependencyTracker build() throws ProbeException {
            if (checkForCycles(probeCategoryDependencyMap)) {
                throw new ProbeException(
                    "Cycle found in generating stitching dependency graph for different probe " +
                        "categories.");
            }
            if (checkForCycles(probeTypeDependencyMap)) {
                throw new ProbeException(
                    "Cycle found in generating stitching dependency graph for different probe " +
                        "types");
            }
            return new ProbeStitchingDependencyTracker(probeCategoryDependencyMap,
                probeTypeDependencyMap);
        }
    }

    /**
     * Class for {@link ProbeCategory} or {@link SDKProbeType} to hold relationship between the
     * current probe and the probes which stitch before it.
     *
     * @param <CATEGORY_OR_TYPE> {@link ProbeCategory} or {@link SDKProbeType} that can define the
     *                          probe order
     */
    public static class StitchingEntry<CATEGORY_OR_TYPE> {

        private final Builder builder;

        private final Set<CATEGORY_OR_TYPE> stitchAfter = Sets.newHashSet();

        private StitchingEntry(Builder builder) {
            this.builder = builder;
        }

        /**
         * Add a stitching entry {@link CATEGORY_OR_TYPE} that will stitch before the
         * {@link CATEGORY_OR_TYPE} related to this entry.
         *
         * @param earlierEntry {@link CATEGORY_OR_TYPE} that needs to stitch before this one.
         * @return builder
         */
        public Builder stitchAfter(CATEGORY_OR_TYPE earlierEntry) {
            stitchAfter.add(earlierEntry);
            return builder;
        }

        /**
         * Get the set of {@link CATEGORY_OR_TYPE} objects representing probes that this entry needs
         * to stitch after.
         *
         * @return Set of {@link CATEGORY_OR_TYPE}
         */
        public Set<CATEGORY_OR_TYPE> getStitchAfter() {
            return stitchAfter;
        }
    }
}
