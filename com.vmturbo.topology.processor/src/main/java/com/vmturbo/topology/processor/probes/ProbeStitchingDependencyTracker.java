package com.vmturbo.topology.processor.probes;

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

/**
 * Tracks stitching order dependency among probes.  Can return a set of ProbeCategories that need
 * to stitch before a passed in ProbeCategory.
 */
public class ProbeStitchingDependencyTracker {
    private final static Logger logger = LogManager.getLogger();
    /**
     * Map that takes a ProbeCategory and returns a Set of ProbeCategory objects that need to stitch
     * before it.
     */
    private final Map<ProbeCategory, Set<ProbeCategory>> stitchBeforeMap = Maps.newHashMap();

    /**
     * Create a ProbeStitchingDependencyTracker from a dependency map that has no cycles.
     *
     * @param dependencyMap A map from ProbeCategory to a CategoryEntry object containing
     *                      all the probe categories that need to stitch before it.
     */
    private ProbeStitchingDependencyTracker(
            @Nonnull final Map<ProbeCategory, CategoryEntry> dependencyMap) {
        Objects.requireNonNull(dependencyMap);
        dependencyMap.entrySet().stream()
                .forEach(nextEntry -> stitchBeforeMap
                        .computeIfAbsent(nextEntry.getKey(), key -> Sets.newHashSet())
                        .addAll(nextEntry.getValue().getStitchAfter()));
        stitchBeforeMap.keySet().forEach(key -> stitchBeforeMap.put(key, getTransitiveClosure(key)));
    }

    /**
     * Compute the transitive closure of a ProbeCategory in stitchBeforeMap.  The transitive closure
     * of a vertex in a graph is the set of all vertices reachable from that vertex.  See
     * https://en.wikipedia.org/wiki/Transitive_closure#In_graph_theory. For example, if
     * Storage has a map entry of Hypervisor, Hyper_Converged and Hyper_Converged has an entry of
     * Hypervisor, Cloud_Management, then the transitive closure for Storage would be Hypervisor,
     * Hyper_Converged, Cloud_Management meaning Storage must stitch after all those categories are
     * stitched.  Note that we take advantage of the fact that stitchBeforeMap has no cycles in it.
     *
     * @param probeCategory The {@link ProbeCategory} to compute the transitive closure of.
     * @return {@link Set} giving the ProbeCategory objects in the transitive closure.
     */
    private Set<ProbeCategory> getTransitiveClosure(ProbeCategory probeCategory) {
        Set<ProbeCategory> retVal = Sets.newHashSet();
        Set<ProbeCategory> categoriesToAdd = stitchBeforeMap.get(probeCategory);
        logger.trace("ProbeCategory {} has categories {}", probeCategory, categoriesToAdd);
        if (categoriesToAdd != null) {
            categoriesToAdd.stream()
                    .forEach(probeCat -> retVal.addAll(getTransitiveClosure(probeCat)));
            retVal.addAll(categoriesToAdd);
        }
        logger.trace("getTransitiveClosure for {} returning {}", probeCategory, retVal);
        return retVal;

    }
    /**
     * Return the set of probeCategories that must stitch before this one.
     *
     * @param probeCategory {@link ProbeCategory} to get the dependencies of.
     * @return Set of probeCategories that must stitch before this one.
     */
    public Set<ProbeCategory> getProbeCategoriesThatStitchBefore(ProbeCategory probeCategory) {
        if (!stitchBeforeMap.keySet().contains(probeCategory)) {
            return Sets.newHashSet();
        }
        return stitchBeforeMap.get(probeCategory);
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
     * @return {@link ProbeStitchingDependencyTracker} that gives default probe category stitching
     * order.
     */
    @Nullable public static ProbeStitchingDependencyTracker getDefaultStitchingDependencyTracker() {
        try {
            return newBuilder().requireThat(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.HYPERVISOR)
                    .requireThat(ProbeCategory.FABRIC).stitchAfter(ProbeCategory.HYPERVISOR)
                    .requireThat(ProbeCategory.APPLICATION_SERVER).stitchAfter(ProbeCategory.HYPERVISOR)
                    .requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.HYPERVISOR)
                    .requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                    .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.HYPERVISOR)
                    .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                    .build();
        } catch (ProbeException e) {
            logger.error("Dependency cycle while building default ProbeStitchingDependencyTracker",
                    e);
            return null;
        }
    }

    public static class Builder {
        Map<ProbeCategory, CategoryEntry> probeCategoryDependencyMap = Maps.newHashMap();

        private Builder() {

        }

        /**
         * Create or get the CategoryEntry object that will enable us to add a ProbeCategory
         * stitching order dependency.
         *
         * @param probeCategory the {@link ProbeCategory} that we want to restrict to stitching
         *                      after some other ProbeCategory.
         * @return CategoryEntry for probeCategory that will encapsulate all the ProbeCategories
         * that need to stitch before it.
         */
        public CategoryEntry requireThat(@Nonnull final ProbeCategory probeCategory) {
            return probeCategoryDependencyMap.computeIfAbsent(probeCategory,
                    probeCat -> new CategoryEntry(this));
        }

        /**
         * Iterate over the probeCategories and see if we have any cycles in the
         * probeCategoryDependencyMap.
         *
         * @return true if there is a cycle, false if not.
         */
        private boolean checkForCycles() {
            boolean cycleExists = false;
            Set<ProbeCategory> allVisited = Sets.newHashSet();
            for (ProbeCategory probeCat : probeCategoryDependencyMap.keySet()) {
                // if we haven't already covered this probeCategory, check it for cycles by doing
                // a depth first search with this category as root
                if (allVisited.contains(probeCat)) {
                    continue;
                }
                Set<ProbeCategory> visited = Sets.newHashSet();
                cycleExists = checkForCycles(probeCat, visited);
                if (cycleExists) {
                    return true;
                }
                // keep track of all the ProbeCategories we've visited to optimize performance by
                // avoiding searching the same portion of the forest twice.
                allVisited.addAll(visited);
            }
            return cycleExists;
        }

        /**
         * Check if there is a cycle starting from a particular ProbeCategory in the dependency map.
         *
         * @param root The starting probe category.
         * @param visited The set of probe categories that have already been visited.
         * @return
         */
        private boolean checkForCycles(@Nonnull ProbeCategory root,
                                       @Nonnull Set<ProbeCategory> visited) {
            CategoryEntry categoryEntry = probeCategoryDependencyMap.get(root);
            boolean cycleExists = false;
            if (visited.contains(root)) {
                logger.error("Cycle detected while building ProbeStitchingDependencyTracker."
                        + "  Cycle begins at ProbeCategory {}", root);
                return true;
            }
            visited.add(root);
            if (categoryEntry!=null) {
                for (ProbeCategory probeCat : categoryEntry.getStitchAfter()) {
                    cycleExists = checkForCycles(probeCat, visited);
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
         * Build and return the {@link ProbeStitchingDependencyTracker} that represents the stitching order
         * dependencies for the ProbeCategories that have dependencies after ensuring that no cycles
         * exist in the stitching order.
         *
         * @return {@link ProbeStitchingDependencyTracker} encapsulating the stitching order, if it is
         * well defined.
         * @throws ProbeException if there is a cycle in the stitching order.
         */
        public ProbeStitchingDependencyTracker build() throws ProbeException {
            if (checkForCycles()) {
                throw new ProbeException(
                        "Cycle found in generating stitching dependency graph for probes.");
            }
            return new ProbeStitchingDependencyTracker(probeCategoryDependencyMap);
        }
    }

    public static class CategoryEntry {

        private final Builder builder;

        private final Set<ProbeCategory> stitchAfter = Sets.newHashSet();

        private CategoryEntry(Builder builder) {
            this.builder = builder;
        }

        /**
         * Add a {@link ProbeCategory} that will stitch before the ProbeCategory related to this
         * CategoryEntry.
         *
         * @param earlierCategory ProbeCategory that needs to stitch before this one.
         * @return
         */
        public Builder stitchAfter(ProbeCategory earlierCategory) {
            stitchAfter.add(earlierCategory);
            return builder;
        }

        /**
         * Get the set of {@link ProbeCategory} objects representing probes that this category
         * needs to stitch after.
         *
         * @return Set of {@link ProbeCategory}
         */
        public Set<ProbeCategory> getStitchAfter() {
            return stitchAfter;
        }
    }
}
