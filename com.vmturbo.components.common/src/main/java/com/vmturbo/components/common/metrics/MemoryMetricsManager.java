package com.vmturbo.components.common.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.memory.FastMemoryWalker;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.ClassHistogramSizeVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryGraphVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryPathVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;
import com.vmturbo.common.protobuf.memory.RelationshipMemoryWalker;

/**
 * Manager for collection and configuration of memory metrics for a component.
 * Manages the root set of objects available for memory walk traversal.
 * Also contains helpers for building memory walks.
 * <p/>
 * For ease of access to the manager from all parts of the code, we use a static singleton
 * so that you do not have to perform dependency injection into a part of the code where
 * you want access to memory metrics during development or debugging.
 */
@ThreadSafe
public class MemoryMetricsManager {
    private static final Logger logger = LogManager.getLogger();

    private final Map<Object, ManagedRoot> managedRoots = new IdentityHashMap<>();
    private static final MemoryMetricsManager instance = new MemoryMetricsManager();

    private MemoryMetricsManager() {
    }

    /**
     * Get the singleton manager instance.
     *
     * @return the singleton manager instance.
     */
    public static MemoryMetricsManager getInstance() {
        return instance;
    }

    /**
     * Add a collection of roots to the set of managed roots.
     * If an object is already added as a managed root, we overwrite existing data for it.
     *
     * @param roots The roots to add.
     */
    public static void addToManagedRootSet(@Nonnull final Collection<ManagedRoot> roots) {
        for (ManagedRoot root : roots) {
            Objects.requireNonNull(root.rootObj);
            if (root.name == null || root.name.isEmpty()) {
                logger.error("Unable to add {} ({}) as managed root because name is null or empty",
                    root.rootObj, root.rootObj.getClass());
                continue;
            }

            synchronized (instance) {
                instance.managedRoots.put(root.rootObj, root);
            }
        }
    }

    /**
     * Add a single ManagedRoot to the set of managed roots.
     * If an object is already added as a managed root, we overwrite existing data for it.
     *
     * @param name The name of the root.
     * @param rootObj The object to act as a managed root.
     * @param ownerComponentName The name of the component that owns the root.
     */
    public static void addToManagedRootSet(@Nonnull final String name,
                                           @Nonnull final Object rootObj,
                                           @Nonnull final String ownerComponentName) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(rootObj);
        Objects.requireNonNull(ownerComponentName);
        Preconditions.checkArgument(!name.isEmpty());

        synchronized (instance) {
            instance.managedRoots.put(rootObj, new ManagedRoot(name, rootObj, ownerComponentName));
        }
    }

    /**
     * Clear all managed roots.
     */
    public static void clearManagedRoots() {
        synchronized (instance) {
            instance.managedRoots.clear();
        }
    }

    /**
     * Clear managed roots for a single component.
     *
     * @param componentName The name of the component to clear.
     */
    public static void clearComponentRoots(@Nonnull final String componentName) {
        synchronized (instance) {
            instance.managedRoots.entrySet().removeIf(e ->
                e.getValue().ownerComponentName.equals(componentName));
        }
    }

    /**
     * Get the number of root objects under management.
     *
     * @return the number of root objects under management.
     */
    public static int managedRootCount() {
        return getInstance().managedRoots.size();
    }

    /**
     * Get a new {@link com.vmturbo.components.common.metrics.MemoryMetricsManager.WalkBuilder} to
     * perform a memory walk.
     *
     * @return a new {@link com.vmturbo.components.common.metrics.MemoryMetricsManager.WalkBuilder}.
     */
    public static WalkBuilder newWalk() {
        return new WalkBuilder();
    }

    /**
     * Get a copy of the map of root objects under management.
     *
     * @return a copy of the map of root objects under management.
     */
    public static synchronized Map<Object, ManagedRoot> getManagedRootsCopy() {
        return new IdentityHashMap<>(getInstance().managedRoots);
    }

    /**
     * Convenience wrapper to calculate sizes and counts on a single root object.
     * See {@link WalkBuilder#walkSizesAndCounts(Collection)} for more details.
     *
     * @param root The object whose sizes and counts should be calculated.
     * @return A {@link TotalSizesAndCountsVisitor} containing details about sizes and counts of
     *         the root.
     */
    public static TotalSizesAndCountsVisitor sizesAndCounts(@Nonnull final Object root) {
        return newWalk().walkSizesAndCounts(Collections.singleton(root));
    }

    /**
     * Convenience wrapper to calculate the object histogram on a single root object.
     * See {@link WalkBuilder#walkClassHistogram(Collection)} for more details.
     *
     * @param root The object whose object histogram should be calculated.
     * @return A {@link ClassHistogramSizeVisitor} containing details about the histogram of
     *         objects reachable from the root.
     */
    public static ClassHistogramSizeVisitor histogram(@Nonnull final Object root) {
        return newWalk().walkClassHistogram(Collections.singleton(root));
    }

    /**
     * Convenience wrapper to calculate the memory graph on a single root object.
     * See {@link WalkBuilder#walkMemoryGraph(int, boolean, Collection)} for more details.
     * <p/>
     * Logs with a minimum object size of 4 Kb.
     *
     * @param root The object whose memory graph should be calculated.
     * @return A {@link ClassHistogramSizeVisitor} containing details about the memory graph of
     *         objects reachable from the root.
     */
    public static String memoryGraph(@Nonnull final Object root) {
        return memoryGraph(root, 4096L);
    }

    /**
     * Convenience wrapper to calculate the memory graph on a single root object.
     * See {@link WalkBuilder#walkMemoryGraph(int, boolean, Collection)} for more details.
     *
     * @param root The object whose memory graph should be calculated.
     * @param minimumLogSizeBytes The minimum size in bytes of any entry in the memory graph table.
     *                            If a subgraph retained by the visitor is smaller than this minimum
     *                            size, it will not be included in the tabular results.
     * @return A {@link ClassHistogramSizeVisitor} containing details about the memory graph of
     *         objects reachable from the root.
     */
    public static String memoryGraph(@Nonnull final Object root,
                                     final long minimumLogSizeBytes) {
        return newWalk()
            .walkMemoryGraph(-1, false, Collections.singleton(new NamedObject("root", root)))
            .tabularResults(minimumLogSizeBytes);
    }

    /**
     * An object managed as a walkable root by the {@link MemoryMetricsManager}.
     * <p/>
     * {@code ManagedRoot} are usually objects near the base of the graph of walkable objects in
     * a component and are useful starting points in the root set of a memory walk.
     */
    public static class ManagedRoot {
        /**
         * The object under management.
         */
        public final Object rootObj;

        /**
         * The name of the object under management.
         */
        public final String name;

        /**
         * The name of the component that owns the object.
         */
        public final String ownerComponentName;

        /**
         * Construct a new {@link ManagedRoot}.
         *
         * @param name The name of the root object.
         * @param rootObj The object to install as a {@link ManagedRoot}.
         * @param ownerComponentName The name of the component that owns the root object.
         */
        public ManagedRoot(@Nonnull final String name,
                           @Nonnull final Object rootObj,
                           @Nonnull final String ownerComponentName) {
            this.rootObj = Objects.requireNonNull(rootObj);
            this.name = Objects.requireNonNull(name);
            this.ownerComponentName = Objects.requireNonNull(ownerComponentName);
            Preconditions.checkArgument(!name.isEmpty());
        }
    }

    /**
     * A builder for building and executing a memory walk.
     */
    public static class WalkBuilder {
        private Set<Object> exclusions;
        private int exclusionDepth;
        private int maxDepth;

        @Nonnull
        private WalkBuilder() {
            exclusions = Collections.emptySet();
            exclusionDepth = -1;
            maxDepth = -1;
        }

        /**
         * Set up exclusions for the memory walk.
         * Many objects in memory have references deep in their object graph to some form of
         * omniscient mega-object (ie a Spring Context) that then has references to many many
         * other objects with dubious relevance to the root object whose memory usage you are
         * attempting to measure.
         * <p/>
         * In order to generate a more meaningful and relevant estimate of the memory used
         * by the object you care about, when walking a given root, it is often helpful
         * to skip walking references to the other component root objects. Otherwise you
         * may wind up with an estimate of the entire spring context when you only intended
         * to generate memory metrics for a small portion of particular interest.
         * <p/>
         * To get a full measurement of every single object reachable from another, disable
         * this flag, but unless you are directly trying to measure the total size of all
         * spring-managed objects (which may be most easily done via the
         * WalkAllRootObjects rpc), it usually makes sense to use the component's
         * default exclusions.
         * <p/>
         * Note that exclusions are ignored in the {@code findPaths} traversal type.
         *
         * @param exclusions The objects to exclude from the walk.
         * @param exclusionDepth The depth at which to start excluding objects in the exclusion set.
         *                       This specifies the maximum depth before we start excluding objects
         *                       in the exclusion set.
         * @return A reference to {@link this} for method chaining.
         */
        @Nonnull
        public WalkBuilder withExclusions(@Nonnull final Set<Object> exclusions,
                                          final int exclusionDepth) {
            if (exclusions instanceof HashSet || exclusions instanceof TreeSet) {
                logger.warn("Consider providing a set that uses object identity rather than equality for exclusion. "
                    + "This will probably deliver better performance results that are more correct. "
                    + "You can use Collections.newSetFromMap(new IdentityHashMap<>()) to generate such a map.");
            }

            this.exclusions = exclusions;
            this.exclusionDepth = exclusionDepth;
            return this;
        }

        /**
         * See {@link #withExclusions(Set, int)} for a description of exclusions.
         * This is a convenience method that sets up a walk using the {@link MemoryMetricsManager}'s
         * managed root set as the exclusion set.
         * <p/>
         * Note that exclusions are ignored in the {@code findPaths} traversal type.
         *
         * @param exclusionDepth The depth at which to start excluding objects in the exclusion set.
         *                       This specifies the maximum depth before we start excluding objects
         *                       in the exclusion set.
         * @return A reference to {@link this} for method chaining.
         */
        @Nonnull
        public WalkBuilder withDefaultExclusions(final int exclusionDepth) {
            return withExclusions(MemoryMetricsManager.getManagedRootsCopy().keySet(),
                exclusionDepth);
        }

        /**
         * Set the maximum depth for the memory walk.
         * Memory walks are generated by traversing the references from the root-set in a
         * breadth-first fashion by using reflection. This is an expensive operation
         * when there are many objects to traverse, so it may not be practical to traverse
         * the entire reachable graph from the root set. This parameter limits the depth
         * of the walk. If not provided or set to a negative number, the walk will attempt
         * to traverse the entire graph.
         * <p/>
         * During the memory walk, we traverse TO the walk_depth, but not deeper.
         * ie specifying a depth of zero will result in only the roots being walked
         * but none of their descendants.
         *
         * @param maxDepth The maximum depth for the walk.
         * @return A reference to {@link this} for method chaining.
         */
        @Nonnull
        public WalkBuilder maxDepth(final int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        /**
         * Perform a memory walk to compute the total size and number of objects reached
         * during the memory walk of the given root set. The size computed is the "deep" size
         * of the root set.
         * <p/>
         * Performance: This is the lightest-weight operation with the least overhead
         *              (although computing the class histogram is not much more expensive).
         *
         * @param roots The roots to walk.
         * @return A {@link TotalSizesAndCountsVisitor} that has collected information about
         *         the deep size of the root set and the count of reachable objects from the root set.
         */
        @Nonnull
        public TotalSizesAndCountsVisitor walkSizesAndCounts(@Nonnull final Collection<Object> roots) {
            final TotalSizesAndCountsVisitor visitor = new TotalSizesAndCountsVisitor(
                exclusions, exclusionDepth, maxDepth);
            new FastMemoryWalker(visitor).traverse(roots.toArray());
            return visitor;
        }

        /**
         * Perform a memory walk to compute the class histogram of objects reachable from the root set.
         * This results in a histogram similar to the OpenJDK class histogram dump seen in Hotspot JVM
         * thread dumps. Sizes for the classes are computed by summing the "shallow" size of each object
         * of the given class.
         * <p/>
         * Performance: This is a slightly more expensive operation than computing just the
         *              size and count but is still much lighter-weight than generating the MemoryGraph.
         *
         * @param roots The roots to walk.
         * @return A {@link ClassHistogramSizeVisitor} that has collected information about
         *         the instances of various classes reachable from the root set.
         */
        @Nonnull
        public ClassHistogramSizeVisitor walkClassHistogram(@Nonnull final Collection<Object> roots) {
            final ClassHistogramSizeVisitor visitor = new ClassHistogramSizeVisitor(
                exclusions, exclusionDepth, maxDepth);
            new FastMemoryWalker(visitor).traverse(roots.toArray());
            return visitor;
        }

        /**
         * Perform a memory walk to compute a breakdown of the reference graph and the sizes of
         * objects and their ancestor references. This generates a table summarizing the graph
         * by the depth of references reachable from the root set.
         * <p/>
         * Performance: This operation has the highest overhead of any of the walk types.
         * <p/>
         * IMPORTANT: A note about computed sizes:
         * When performing heap analysis, we often use the terms "shallow size", "retained size",
         * and "deep size" of an object. See https://dzone.com/articles/shallow-retained-and-deep-size
         * for an explanation of all of these terms.
         * <p/>
         * The sizes generated by memory walks using the MemoryMetricsService are a mix of retained
         * and deep sizes which is important to keep in mind when interpreting the numbers. When
         * multiple parent objects refer to the same child, we assign the child's size to only
         * one of its parents when computing sizes. Consider the following object graph
         * <p/>
         * Foo --> Baz--> Quux
         *                /
         * Bar  ---------
         * <p/>
         * Here, if we perform a memory walk with a root set of [Foo, Bar], we see we can reach
         * Quux through both Foo and Bar. As a result, we will report the following sizes in the table
         * generated for the MemoryGraph:
         * <p/>
         * TOTAL                             CHILD_COUNT  TYPE                    PATH
         * shalllow(Foo) + shallow(Baz)      2            Foo                     r1
         * shallow(Bar) + shallow(Quux)      2            Bar                     r2
         * TOTAL                             CHILD_COUNT  TYPE                    PATH
         * shallow(Baz)                      1            Baz                     r1.baz
         * shallow(Quux)                     1            Quux                    r2.quux
         * <p/>
         * This amounts to reporting the deep size for Bar and the retained size for Foo. Note, however,
         * if we remove Bar from the root set, we will generate the following table for the MemoryGraph:
         * <p/>
         * TOTAL                                             CHILD_COUNT  TYPE                    PATH
         * shalllow(Foo) + shallow(Baz) + shallow(Quux)      3            Foo
         * TOTAL                                             CHILD_COUNT  TYPE                    PATH
         * shallow(Baz) + shallow(Quux)                      2            Baz                     .baz
         * TOTAL                                             CHILD_COUNT  TYPE                    PATH
         * shallow(Quux)                                     1            Quux                    .baz.quux
         * <p/>
         * Thus, if you want the true deep size of all objects in a root set, request repeated memory
         * walks for each member of the root set rather than one request with all members.
         * When a child object is referenced through multiple paths of the same length from the root
         * set, which descendant objects are credited with the size of the child object is not guaranteed,
         * but we will pick only a single path and not mix parents from different paths.
         * <p/>
         * We compute sizes in this somewhat convoluted manner because:
         * 1. It simplifies handling of cycles in the memory graph and is much easier to compute
         *    than the retained size of each reachable object from the root set (impossible without
         *    knowing if there are objects outside the root set that also refer to object inside
         *    the root set) and much less expensive to compute than the true deep size of all
         *    objects reachable from the root set.
         * 2. The computed size of the root set will actually be the size of all objects reachable from the root
         *    set. This is often the number you actually care about (although not always).
         *
         * @param logDepth The maximum depth to log.
         * @param retainDescendants Whether to retain descendants beyond the log depth. retained descendants can
         *                          be used to perform analysis on walked objects that are not logged. Set to
         *                          false to reduce the memory overhead of the walk if you only want to generate
         *                          the tabular breakdown of object sizes and do not need to do any further
         *                          in-code analysis.
         * @param roots The roots to walk.
         * @return A {@link MemoryGraphVisitor} that has collected information about the objects
         *         reachable from the root set.
         */
        @Nonnull
        public MemoryGraphVisitor walkMemoryGraph(final int logDepth,
                                                  final boolean retainDescendants,
                                                  @Nonnull final Collection<NamedObject> roots) {
            final MemoryGraphVisitor visitor = new MemoryGraphVisitor(
                exclusions, exclusionDepth, maxDepth, logDepth, retainDescendants);
            new RelationshipMemoryWalker(visitor).traverseNamed(roots);
            return visitor;
        }

        /**
         * Perform a memory walk to find the shortest paths from the root set to instances
         * of the given searchClasses.
         * <p/>
         * Performance: This operation has similar performance overhead to the
         * {@link MemoryGraphVisitor} type.
         *
         * @param maxInstances The maximum number of instances of any of the classes to find.
         *                     If we reach this maximum, we can stop the memory walk early.
         *                     Setting to a negative number will find all instances of the
         *                     searchClasses.
         * @param roots The roots to walk.
         * @param searchClasses The classes whose instances we should search for.
         * @return The paths found to the search class instances during the visit.
         */
        @Nonnull
        public MemoryPathVisitor findPaths(final int maxInstances,
                                           @Nonnull final Collection<NamedObject> roots,
                                           @Nonnull final Set<Class<?>> searchClasses) {
            return findPaths(maxInstances, 0, roots, searchClasses);
        }

        /**
         * Perform a memory walk to find the shortest paths from the root set to instances
         * of the given searchClasses.
         * <p/>
         * Performance: This operation has similar performance overhead to the
         * {@link MemoryGraphVisitor} type.
         *
         * @param maxInstances The maximum number of instances of any of the classes to find.
         *                     If we reach this maximum, we can stop the memory walk early.
         *                     Setting to a negative number will find all instances of the
         *                     searchClasses.
         * @param minInstanceDepth The minimum depth to reach before accepting found instances of the
         *                         search_classes. Usually a value of 0 is fine if any instances will do,
         *                         but if there are many instances at low depth and you only care to find
         *                         the ones at deeper depth, pass in a min_instance_depth to exclude the
         *                         shallower instances from the results.
         * @param roots The roots to walk.
         * @param searchClasses The classes whose instances we should search for.
         * @return The paths found to the search class instances during the visit.
         */
        @Nonnull
        public MemoryPathVisitor findPaths(final int maxInstances,
                                           final int minInstanceDepth,
                                           @Nonnull final Collection<NamedObject> roots,
                                           @Nonnull final Set<Class<?>> searchClasses) {
            if (exclusions != Collections.emptySet()) {
                logger.warn("Exclusions are ignored while finding paths");
            }

            final MemoryPathVisitor visitor = new MemoryPathVisitor(searchClasses, maxDepth,
                maxInstances, minInstanceDepth);
            new RelationshipMemoryWalker(visitor).traverseNamed(roots);
            return visitor;
        }
    }
}
