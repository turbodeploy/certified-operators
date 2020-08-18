package com.vmturbo.common.protobuf.memory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.github.jamm.MemoryLayoutSpecification;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.FoundPath;
import com.vmturbo.common.protobuf.memory.RelationshipMemoryWalker.MemoryReferenceNode;

/**
 * A {@link MemoryVisitor} works in conjunction with a {@link MemoryWalker} to collection information
 * about objects traversed during a memory walk.
 * <p/>
 * All {@link MemoryWalker}s walk the reference graph in breadth-first order to permit visitors to
 * do depth-based analysis of visited objects without having to do any sorting. They also promise
 * to only call {@link #visit(Object, int, Object)} once for any object during a walk no matter how
 * many paths there are to it. Because the walk is breadth-first the visit will always be a shortest
 * path.
 * <p/>
 * Subclasses of the {@link MemoryVisitor} implement custom functionality specific to the information
 * they collect, analyze, and report on.
 *
 * @param <T> The type of the data parameter passed to the {@link #visit(Object, int, Object)} method.
 */
public abstract class MemoryVisitor<T> {
    private final Set<Object> exclusions;
    final Map<Class<?>, Long> sizeCache;
    private final int exclusionDepth;
    private final int maxDepth;

    /**
     * Hard limit on the number of lines we log for some visitors to prevent locking up the
     * component for a long time while writing to the logs. If we hit this max, it generally
     * indicates that you may actually want a heap dump or impose stricter filters on
     * the information you are capturing.
     */
    static final long MAX_LOG_LINES = 100_000;

    /**
     * Create a new {@link MemoryVisitor}.
     *
     * @param exclusions The objects to exclude from the walk. Useful when an object has a reference to
     *                   another object you are not interested that may in turn have references to lots
     *                   of other things you don't care about, etc. Just insert the object(s) you don't
     *                   want to traverse in the exclusion list to prevent including them or their
     *                   descendants in the results.
     * @param exclusionDepth The depth at which to start excluding objects in the exclusion list.
     * @param maxDepth The maximum depth to traverse during a walk.
     */
    public MemoryVisitor(@Nonnull final Set<Object> exclusions,
                         final int exclusionDepth,
                         final int maxDepth) {
        this.exclusions = Objects.requireNonNull(exclusions);
        this.exclusionDepth = exclusionDepth < 0 ? Integer.MAX_VALUE : exclusionDepth;
        this.maxDepth = maxDepth < 0 ? Integer.MAX_VALUE : maxDepth;
        sizeCache = new HashMap<>();
    }

    /**
     * Protected method that custom implementations should implement. Internally called by the
     * {@link #visit(Object, int, Object)} method.
     *
     * @param klass The class of the object visited.
     * @param size The "shallow" size of the object visited.
     * @param depth The depth from the root set at which we reached this object.
     * @param data Custom data. The data passed depends on which {@link MemoryWalker} is being
     *             used to perform the memory walk.
     * @return Whether or not to continue the walk to this object's descendants.
     */
    protected abstract boolean handleVisit(@Nonnull Class<?> klass, long size,
                                        int depth, @Nullable T data);

    /**
     * Get the total count of objects visited by this visitor.
     *
     * @return the total count of objects visited by this visitor.
     */
    public abstract long totalCount();

    /**
     * Get the sum of the size of all the objects visited by this visitor. This is equivalent to the "deep size"
     * of the root set of objects. Note that if a maxDepth parameter limits the depth visited during
     * traversal or we exclude some objects via the exclusion set, the totalSize will be less than the
     * true deep size. We will only compute the true deep size when we traverse the entire graph of
     * objects reachable from the root set.
     *
     * @return The sum of the size of all the objects visited by this visitor.
     */
    public abstract long totalSize();

    /**
     * Visit an object during a memory walk.
     *
     * @param obj The object visited.
     * @param depth The depth from the root set at which we reached this object.
     * @param data Custom data. The data object depends on which {@link MemoryWalker} is performing the walk.
     *             {@link FastMemoryWalker} passes in null data.
     *             {@link RelationshipMemoryWalker} passes in {@link MemoryReferenceNode} data.
     * @return Whether or not to continue the walk to this object's descendants.
     */
    public boolean visit(@Nonnull Object obj, int depth,
                         @Nullable final T data) {
        final Class<?> klass = obj.getClass();
        final long size;
        if (klass.isArray()) {
            size = MemoryLayoutSpecification.sizeOfArray(obj, klass);
        } else {
            size = sizeCache.computeIfAbsent(obj.getClass(),
                MemoryLayoutSpecification::sizeOfInstance);
        }

        boolean shouldContinue = handleVisit(klass, size, depth, data);
        return shouldContinue
            && (depth < maxDepth)
            && (depth <= exclusionDepth || !exclusions.contains(obj));
    }

    /**
     * Get the set of objects to exclude from the walk.
     *
     * @return the set of objects to exclude from the walk.
     */
    public Set<Object> getExclusions() {
        return exclusions;
    }

    /**
     * Get the depth at which to start applying exclusions.
     *
     * @return the depth at which to start applying exclusions.
     */
    public int getExclusionDepth() {
        return exclusionDepth;
    }

    /**
     * Get the maximum depth to traverse to during the memory walk.
     *
     * @return the maximum depth to traverse to during the memory walk.
     */
    public int getMaxDepth() {
        return maxDepth;
    }

    /**
     * A named object is a simple pair of an object and its name.
     * <p/>
     * Useful for explicitly naming objects in the root set of a memory walk
     * which have no parents.
     */
    public static class NamedObject {
        /**
         * The object.
         */
        public final Object rootObj;

        /**
         * The name of the object.
         */
        public final String name;

        /**
         * Construct a new {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject}.
         *
         * @param name The name of the object.
         * @param rootObj The object.
         */
        public NamedObject(@Nonnull final String name,
                           @Nonnull final Object rootObj) {
            this.rootObj = Objects.requireNonNull(rootObj);
            this.name = Objects.requireNonNull(name);
            Preconditions.checkArgument(!name.isEmpty());
        }
    }

    /**
     * A {@link MemoryVisitor} for computing the total size and total count of objects
     * visited during a memory walk. Retains no information about individual objects
     * walked to reduce overhead. Best paired with {@link FastMemoryWalker}.
     */
    public static class TotalSizesAndCountsVisitor extends MemoryVisitor<Object> {
        private long totalSize = 0;
        private long totalCount = 0;

        /**
         * Construct a new {@link TotalSizesAndCountsVisitor}.
         *
         * @param exclusions The objects to exclude during the walk.
         * @param exclusionDepth The depth at which to begin applying exclusions.
         * @param maxDepth The maximum depth to traverse to.
         */
        public TotalSizesAndCountsVisitor(@Nonnull final Set<Object> exclusions,
                                          final int exclusionDepth,
                                          final int maxDepth) {
            super(exclusions, exclusionDepth, maxDepth);
        }

        @Override
        public boolean handleVisit(@Nonnull final Class<?> klass, final long size,
                                final int depth, @Nullable final Object data) {
            totalSize += size;
            totalCount++;
            return true;
        }

        @Override
        public long totalCount() {
            return totalCount;
        }

        @Override
        public long totalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return String.format("COUNT=%s, SIZE=%s",
                String.format("%,d", totalCount()),
                StringUtil.getHumanReadableSize(totalSize()));
        }
    }

    /**
     * A {@link MemoryVisitor} for computing a histogram of objects
     * visited during a memory walk. Retains no information about individual objects
     * walked to reduce overhead. Best paired with {@link FastMemoryWalker}.
     */
    public static class ClassHistogramSizeVisitor extends MemoryVisitor<Object> {
        private final Map<Class<?>, SizeAndCount> classCounts = new IdentityHashMap<>();

        /**
         * Construct a new {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.ClassHistogramSizeVisitor}.
         *
         * @param exclusions The objects to exclude during the walk.
         * @param exclusionDepth The depth at which to begin applying exclusions.
         * @param maxDepth The maximum depth to traverse to.
         */
        public ClassHistogramSizeVisitor(@Nonnull final Set<Object> exclusions,
                                         final int exclusionDepth,
                                         final int maxDepth) {
            super(exclusions, exclusionDepth, maxDepth);
        }

        @Override
        protected boolean handleVisit(@Nonnull final Class<?> klass, final long size,
                                   final int depth, @Nullable final Object data) {
            classCounts.compute(klass, (k, v) -> v == null
                ? new SizeAndCount(size)
                : v.increment(size));
            return true;
        }

        @Override
        public long totalCount() {
            return classCounts.values().stream()
                .mapToLong(i -> i.count)
                .sum();
        }

        @Override
        public long totalSize() {
            return classCounts.values().stream()
                .mapToLong(i -> i.totalSize)
                .sum();
        }

        /**
         * Example output excerpt:
         * 2020-06-23 12:46:22,955 INFO [grpc-default-executor-4] [MemoryMetricsRpcService] : Class Histogram:
         *            SIZE        COUNT TYPE
         *        198.2 MB    4,367,733 TOTAL
         * 1       47.7 MB      521,767 [C
         * 2       11.9 MB      521,375 java.lang.String
         * 3       11.6 MB      120,524 [Ljava.util.HashMap$Node;f
         * 4       10.0 MB      326,117 java.util.HashMap$Node
         * 5        9.5 MB      112,626 java.lang.reflect.Method
         * 6        8.0 MB      200,179 [Ljava.lang.Object;
         * 7        7.5 MB      196,423 java.util.LinkedHashMap$Entry
         * 8        6.0 MB       26,846 [B
         * 9        5.7 MB      248,037 java.util.ArrayList
         * 10       5.2 MB       97,825 java.util.LinkedHashMap
         * ...
         *
         * @return The String ClassHistogram.
         */
        @Override
        public String toString() {
            final int numberSpaces = Integer.toString(classCounts.size()).length();
            final String nsStr =  " %-" + numberSpaces + "d";
            final String header = String.format(" %" + numberSpaces + "s %10s %12s %s%n",
                "", "SIZE", "COUNT", "TYPE");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.printf("Class Histogram:\n");
            pw.printf(header);
            pw.printf(" %" + numberSpaces + "s %10s %12s %s%n",
                "",
                StringUtil.getHumanReadableSize(totalSize()),
                String.format("%,d", totalCount()), "TOTAL");

            final AtomicInteger curIndex = new AtomicInteger(1);
            classCounts.entrySet().stream()
                .sorted(Comparator.<Entry<Class<?>, SizeAndCount>>comparingLong(
                    e -> e.getValue().totalSize).reversed())
                .forEach(e ->
                    pw.printf(nsStr + " %10s %12s %s%n",
                        curIndex.getAndIncrement(),
                        StringUtil.getHumanReadableSize(e.getValue().totalSize),
                        String.format("%,d", e.getValue().count),
                        e.getKey().getName()));

            pw.close();
            return sw.toString();
        }
    }

    /**
     * A simple pairing of two long values to represent a size and count.
     */
    public static class SizeAndCount {
        private long totalSize;
        private long count;

        /**
         * Construct a new {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.SizeAndCount} with
         * a starting count of one and the given starting size.
         *
         * @param totalSize The starting size for the size and count.
         */
        public SizeAndCount(final long totalSize) {
            this.totalSize = totalSize;
            this.count = 1;
        }

        /**
         * Increment the size and count. Increments the size by the given value and the count by 1.
         *
         * @param size The size by which to increment the totalSize.
         * @return A reference to {@link this}.
         */
        public SizeAndCount increment(final long size) {
            this.totalSize += size;
            this.count++;
            return this;
        }

        /**
         * Reset the size and count values to zero.
         *
         * @return A reference to {@link this}.
         */
        public SizeAndCount reset() {
            this.totalSize = 0;
            this.count = 0;
            return this;
        }
    }

    /**
     * A {@link MemoryVisitor} for computing the size of objects and their descendants
     * visited during a memory walk. Retains information about individual objects
     * so has more significant overhead. Use with {@link RelationshipMemoryWalker}.
     */
    public static class MemoryGraphVisitor
        extends MemoryVisitor<MemoryReferenceNode> implements Iterable<MemoryReferenceNode> {
        private final int logDepth;

        private final List<MemorySubgraph> rootSubgraphs;
        private Map<MemoryReferenceNode, MemorySubgraph> parentSubgraphs;
        private Map<MemoryReferenceNode, MemorySubgraph> childSubgraphs;
        private int currentDepth;
        private boolean retainDescendents;

        long memoizedSize = -1L;
        long memoizedCount = -1L;

        /**
         * Construct a new {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryGraphVisitor}.
         *
         * @param exclusions The objects to exclude during the walk.
         * @param exclusionDepth The depth at which to begin applying exclusions.
         * @param maxDepth The maximum depth to traverse to.
         * @param logDepth The maximum depth at which we will log any objects visited.
         * @param retainDescendents Whether to retain descendants of objects deeper than the log
         *                          depth. Useful for running additional analysis on visited objects
         *                          beyond what is provided out of the backs through the
         *                          {@link #tabularResults()} methods.
         */
        public MemoryGraphVisitor(@Nonnull final Set<Object> exclusions,
                                  final int exclusionDepth,
                                  final int maxDepth,
                                  final int logDepth,
                                  boolean retainDescendents) {
            super(exclusions, exclusionDepth, maxDepth);
            this.logDepth = (logDepth < 0) ? Integer.MAX_VALUE : logDepth;

            rootSubgraphs = new ArrayList<>();
            parentSubgraphs = new IdentityHashMap<>();
            childSubgraphs = new IdentityHashMap<>();
            currentDepth = 0;
            this.retainDescendents = retainDescendents;
        }

        @Override
        protected boolean handleVisit(@Nonnull Class<?> klass, long size, int depth,
                                   @Nullable MemoryReferenceNode data) {
            if (depth != currentDepth) {
                // Depth should be monotonically increasing across handleVisit calls.
                assert (depth > currentDepth);
                currentDepth = depth;

                // Swap parent and child maps to descend to the next BFS depth.
                parentSubgraphs.clear();
                final Map<MemoryReferenceNode, MemorySubgraph> tmp = parentSubgraphs;
                parentSubgraphs = childSubgraphs;
                childSubgraphs = tmp;
            }

            // Build up subgraphs while at or above log depth
            if (depth <= logDepth) {
                if (depth == 0) {
                    final MemorySubgraph root = new MemorySubgraph(data, size);
                    rootSubgraphs.add(root);
                    childSubgraphs.put(data, root);
                } else {
                    final MemorySubgraph parentSubgraph = parentSubgraphs.get(data.getParent());
                    final MemorySubgraph childSubgraph = new MemorySubgraph(data, size);

                    parentSubgraph.subgraphChildren.add(childSubgraph);
                    childSubgraphs.put(data, childSubgraph);
                }
            } else {
                // Append to existing subgraphs
                final MemorySubgraph parentSubgraph = parentSubgraphs.get(data.getParent());
                parentSubgraph.addDescendant(data, size, retainDescendents);
                childSubgraphs.put(data, parentSubgraph);
            }

            return true;
        }

        @Override
        public long totalSize() {
            if (memoizedSize < 0) {
                memoizedSize = rootSubgraphs.stream()
                    .mapToLong(MemorySubgraph::totalSize)
                    .sum();
            }
            return memoizedSize;
        }

        @Override
        public long totalCount() {
            if (memoizedCount < 0) {
                memoizedCount = rootSubgraphs.stream()
                    .mapToLong(MemorySubgraph::totalCount)
                    .sum();
            }
            return memoizedCount;
        }

        /**
         * Get the log depth.
         *
         * @return the log depth.
         */
        public int getLogDepth() {
            return logDepth;
        }

        /**
         * Get whether or not to retain descendants.
         *
         * @return whether or not to retain descendants.
         */
        public boolean getRetainDescendants() {
            return retainDescendents;
        }

        /**
         * Compute tabular results with a minimumLogSizeBytes of 0.
         *
         * @return tabular results with a minimumLogSizeBytes of 0.
         */
        public String tabularResults() {
            return tabularResults(0);
        }

        /**
         * Compute tabular results.
         * <p/>
         * Example output excerpt:
         * 2020-06-23 12:59:13,225 INFO [grpc-default-executor-3] [MemoryMetricsRpcService] : Memory walk tabular results: [TOTAL=55.5 MB, CHILD_COUNT=1,306,580]
         *          TOTAL  CHILD_COUNT TYPE                                                                                   PATH
         * 0      55.5 MB    1,306,580 com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore underlyingStore
         *          TOTAL  CHILD_COUNT TYPE                                                                                   PATH
         * 1      43.2 MB    1,078,001 java.util.HashMap                                                                      underlyingStore.oid2Dto_
         * 1      11.5 MB      211,892 java.util.HashMap                                                                      underlyingStore.index_
         * 1     786.1 KB       16,682 com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore                  underlyingStore.identityDatabaseStore
         *          TOTAL  CHILD_COUNT TYPE                                                                                   PATH
         * 2      43.2 MB    1,078,000 [Ljava.util.HashMap$Node;                                                              underlyingStore.oid2Dto_.table
         * 2      11.5 MB      211,891 [Ljava.util.HashMap$Node;                                                              underlyingStore.index_.table
         * 2     785.9 KB       16,677 org.apache.logging.log4j.core.Logger                                                   underlyingStore.identityDatabaseStore.logger
         *
         * @param minimumLogSizeBytes The minimum size in bytes of any entry in the table. If a subgraph
         *                            retained by the visitor is smaller than this minimum size, it will
         *                            not be included in the tabular results.
         * @return A String table containing the results of the memory walk.
         */
        public String tabularResults(final long minimumLogSizeBytes) {
            final int typeLen = getMaxClassNameLength(minimumLogSizeBytes);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.printf("Memory walk tabular results: [TOTAL=%s, CHILD_COUNT=%s] %n",
                StringUtil.getHumanReadableSize(totalSize()), String.format("%,d", totalCount()));
            final String header = String.format(" %3s %10s %12s %-" + typeLen
                + "s %s%n", "", "TOTAL", "CHILD_COUNT", "TYPE", "PATH");

            final Map<Integer, List<MemorySubgraph>> depthGraphs =
                StreamSupport.stream(((Iterable<MemorySubgraph>)this::subgraphIterator).spliterator(), false)
                    .collect(Collectors.groupingBy(MemorySubgraph::getDepth));

            final AtomicLong remainingLines = new AtomicLong(MAX_LOG_LINES);
            final Iterable<Entry<Integer, List<MemorySubgraph>>> entries = depthGraphs.entrySet().stream()
                .sorted(Comparator.comparingInt(Entry::getKey))::iterator;
            for (Entry<Integer, List<MemorySubgraph>> entry : entries) {
                pw.printf(header);
                final int linesLogged = writeSubgraphs(entry.getValue(), pw, remainingLines,
                    minimumLogSizeBytes, typeLen);
                if (linesLogged == 0) {
                    // If there are no subgraphs large enough to be logged, none of the children will be large
                    // enough either so we can break
                    break;
                }
            }

            pw.close();
            return sw.toString();
        }

        private int writeSubgraphs(@Nonnull final List<MemorySubgraph> subgraphsAtDepth,
                                   @Nonnull final PrintWriter pw,
                                   @Nonnull final AtomicLong remainingLines,
                                   final long minimumLogSizeBytes, final int typeLen) {
            final Iterable<MemorySubgraph> subgraphs = subgraphsAtDepth.stream()
                .filter(sg -> sg.totalSize() >= minimumLogSizeBytes)
                .sorted(Comparator.comparingLong(MemorySubgraph::totalSize).reversed())
                .limit(remainingLines.get())::iterator;

            int linesLogged = 0;
            for (MemorySubgraph subgraph : subgraphs) {
                linesLogged++;
                remainingLines.getAndDecrement();
                pw.printf(" %-3d %10s %12s %-" + typeLen + "s %s%n",
                    subgraph.getDepth(),
                    StringUtil.getHumanReadableSize(subgraph.totalSize()),
                    String.format("%,d", subgraph.totalCount()),
                    subgraph.subgraphRoot.getKlass().getName(),
                    subgraph.subgraphRoot.pathDescriptor());
            }

            return linesLogged;
        }

        /**
         * Iterate the {@link MemoryReferenceNode}s retained from the memory walk.
         * If created with retainDescendents true, this will iterate all visited nodes.
         * If created with retainDescendents false, this will only iterate nodes retained
         * to the logDepth.
         *
         * @return An iterator for iterating retained {@link MemoryReferenceNode}s from
         *         the memory walk.
         */
        @Override
        public Iterator<MemoryReferenceNode> iterator() {
            return new MemoryNodeIterator(rootSubgraphs);
        }

        /**
         * Return an iterator for the subgraphs produced during the memory walk.
         * See {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.MemorySubgraph} for additional details
         * about memory subgraphs.
         *
         * @return An iterator for the subgraphs produced during the memory walk.
         */
        public MemorySubgraphIterator subgraphIterator() {
            return new MemorySubgraphIterator(rootSubgraphs);
        }

        /**
         * Helper method to compute the string length of the longest class name to be logged.
         *
         * @param minimumLogSizeBytes The minimum size in bytes of any subgraph to be logged.
         * @return the string length of the longest class name to be logged.
         */
        private int getMaxClassNameLength(final long minimumLogSizeBytes) {
            int maxClassLength = "TYPE".length();
            for (MemorySubgraph sg : (Iterable<MemorySubgraph>)this::subgraphIterator) {
                if (sg.totalSize() >= minimumLogSizeBytes) {
                    maxClassLength = Math.max(maxClassLength, sg.subgraphRoot.getKlass().getName().length());
                }
            }
            return maxClassLength;
        }
    }

    /**
     * A {@link MemoryVisitor} for finding the shortest path to instances of classes reachable from
     * a root set. Retains information about individual objects so has more significant overhead.
     * Use with {@link RelationshipMemoryWalker}.
     */
    public static class MemoryPathVisitor extends MemoryVisitor<MemoryReferenceNode> {
        private final Set<Class<?>> searchClasses;
        private final List<MemoryReferenceNode> matchingNodes;

        private final long numInstancesToFind;
        private final int minInstanceDepth;
        private long totalSize = 0;

        /**
         * Construct a new {@link com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryPathVisitor}.
         *
         * @param searchClasses The visitor finds shortest paths to instances of these searchClasses.
         * @param maxDepth The maximum depth to walk from the root set for instances of the searchClasses.
         * @param numInstancesToFind The number of instances of the searchClasses to find. Once we have found
         *                           this many instances, we can call off the search. Useful to speed up
         *                           searches when you don't need to find all reachable instances.
         * @param minInstanceDepth The minimum depth to reach before accepting found instances of the
         *                         search_classes. Usually a value of 0 is fine if any instances will do,
         *                         but if there are many instances at low depth and you only care to find
         *                         the ones at deeper depth, pass in a min_instance_depth to exclude the
         *                         shallower instances from the results.
         */
        public MemoryPathVisitor(@Nonnull final Set<Class<?>> searchClasses,
                                 final int maxDepth,
                                 final int numInstancesToFind,
                                 final int minInstanceDepth) {
            super(Collections.emptySet(), -1, maxDepth);
            this.searchClasses = Objects.requireNonNull(searchClasses);
            this.numInstancesToFind = numInstancesToFind < 0 ? Integer.MAX_VALUE : numInstancesToFind;
            this.minInstanceDepth = minInstanceDepth < 0 ? 0 : minInstanceDepth;
            matchingNodes = new ArrayList<>();
        }

        @Override
        protected boolean handleVisit(@Nonnull final Class<?> klass,
                                      final long size,
                                      final int depth,
                                      @Nullable final MemoryReferenceNode data) {
            if (totalCount() >= numInstancesToFind) {
                return false;
            }

            if (depth >= minInstanceDepth && searchClasses.contains(klass)) {
                matchingNodes.add(data);
                totalSize += size;
            }

            return totalCount() < numInstancesToFind;
        }

        @Override
        public long totalCount() {
            return matchingNodes.size();
        }

        @Override
        public long totalSize() {
            return totalSize;
        }

        /**
         * Get the number of instances to find with this visitor.
         *
         * @return the number of instances to find with this visitor.
         */
        public long getNumInstancesToFind() {
            return numInstancesToFind;
        }

        /**
         * Get the minimum depth at which to start accepting found instances of the search classes.
         *
         * @return the minimum depth at which to start accepting found instances of the search classes.
         */
        public int getMinInstanceDepth() {
            return minInstanceDepth;
        }

        @Nonnull
        public Set<Class<?>> getSearchClasses() {
            return searchClasses;
        }

        /**
         * Get a list of {@link FoundPath} objects representing the instances of the searchClasses
         * found by this visitor.
         *
         * @param includeToStringValues Whether to include the string representation of the objects found
         *                              in their {@link FoundPath}.
         * @return The list of {@link FoundPath}s.
         */
        public List<FoundPath> foundPaths(final boolean includeToStringValues) {
            return matchingNodes.stream()
                .map(path -> {
                    final FoundPath.Builder builder = FoundPath.newBuilder()
                        .setClassName(path.getKlass().getName())
                        .setPath(path.pathDescriptor());
                    if (includeToStringValues) {
                        builder.setToStringValue(path.getObj() == null ? "null" : path.getObj().toString());
                    }
                    return builder.build();
                }).collect(Collectors.toList());
        }

        /**
         * Compute tabular results.
         * <p/>
         * Example output excerpt:
         * 2020-06-23 12:45:00,291 INFO [grpc-default-executor-2] [MemoryMetricsRpcService] : Found paths:
         *     TYPE                                                     PATH
         * 0   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[0].target_
         * 1   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[1].target_
         * 2   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[2].target_
         * 3   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[3].target_
         * 4   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[4].target_
         * 5   com.vmturbo.platform.analysis.economy.TraderWithSettings marketRunner.realtimeReplayActions.deactivateActions_.array[5].target_
         *
         * @return A String table describing the paths to the searchClasses found during the memory walk.
         */
        public String tabularResults() {
            final int typeLen = getMaxClassNameLength();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            final String header = String.format(" %3s %-" + typeLen + "s %s%n", "", "TYPE", "PATH");

            pw.printf("Found paths:\n");
            pw.printf(header);
            int index = 0;
            for (MemoryReferenceNode node : matchingNodes) {
                pw.printf(" %-3d %-" + typeLen + "s %s%n",
                    index++,
                    node.getKlass().getName(),
                    node.pathDescriptor());

                if (index >= MAX_LOG_LINES) {
                    break;
                }
            }

            pw.close();
            return sw.toString();
        }

        /**
         * Helper method to compute the string length of the longest class name to be logged.
         *
         * @return the string length of the longest class name to be logged.
         */
        private int getMaxClassNameLength() {
            int maxClassLength = "TYPE".length();
            for (MemoryReferenceNode node : matchingNodes) {
                maxClassLength = Math.max(maxClassLength, node.getKlass().getName().length());
            }
            return maxClassLength;
        }
    }

    /**
     * A {@link MemorySubgraph} represents the subgraph of all objects visited as descendants
     * of the object in the subgraph root. Note that during a memory walk, no object is allowed in
     * more than one {@link MemorySubgraph} at a given depth because we retain only the single path
     * walked to the object during the memory walk. Thus, even if there are multiple paths to an
     * object and we walk all the objects in all those paths, we only add the object to a single
     * one of the subgraphs created at any given depth. This prevents counting the same object
     * more than once in the overall sizes and counts at a given depth. This, in turn, facilitates
     * a depth-based analysis of the memory graph as well as ensuring the sizes and counts
     * computed for the root set are the overall "deep size" of the root set.
     * <p/>
     * Subgraphs can be nested together. Parent subgraphs know their children, which is the inverse
     * of a {@link MemoryReferenceNode} where the child knows its parent. In both cases, a child
     * only ever have one parent but a parent may have multiple children.
     * <p/>
     * {@link MemorySubgraph}s are created by the {@link MemoryGraphVisitor} during a memory walk
     * for each object visited up to the log depth. After that, further descendants are only added
     * as descendants of the bottom layer of subgraphs. For example, with a log depth of 1:
     * <p/>
     *              A           B       <-- Construct subgraphs because depth <=1
     *              |          / \
     *              C         D  E      <-- Construct subgraphs because depth <=1
     *             / \        |
     *            F  G        H         <-- Added as descendants of subgraph at bottom depth of 1
     *                        |
     *                        I         <-- Added as descendant of subgraph at bottom depth of 1
     */
    public static class MemorySubgraph {
        private final MemoryReferenceNode subgraphRoot;
        private final List<MemorySubgraph> subgraphChildren;
        private final List<MemoryReferenceNode> descendants;
        private final SizeAndCount sizeAndCount;

        long memoizedSize = -1L;
        long memoizedCount = -1L;

        /**
         * Construct a new {@link MemorySubgraph}.
         *
         * @param subgraphRoot The object at the root of the subgraph.
         * @param subgraphRootShallowSize The shallow size of the object at the root of the subgraph.
         */
        public MemorySubgraph(@Nonnull final MemoryReferenceNode subgraphRoot,
                              final long subgraphRootShallowSize) {
            this.subgraphRoot = Objects.requireNonNull(subgraphRoot);
            subgraphChildren = new ArrayList<>();
            descendants = new ArrayList<>();
            sizeAndCount = new SizeAndCount(subgraphRootShallowSize);
        }

        /**
         * Add descendants to this subgraph. Only retain the actual {@link MemoryReferenceNode}
         * if retainDescendants is true.
         *
         * @param descendant The {@link MemoryReferenceNode} of the descendant object.
         * @param shallowSize The shallow size of the descendant object.
         * @param retainDescendants Whether or not to retain the full descendant object or just
         *                          track its size as part of this subgraph.
         */
        public void addDescendant(@Nonnull final MemoryReferenceNode descendant,
                                  final long shallowSize,
                                  final boolean retainDescendants) {
            sizeAndCount.increment(shallowSize);
            if (retainDescendants) {
                descendants.add(descendant);
            }
        }

        /**
         * Compute the total size in bytes of this subgraph.
         * This size is memoized so subsequent calls will not have to recompute.
         *
         * @return The total size in bytes of this subgraph.
         */
        public long totalSize() {
            if (memoizedSize < 0) {
                memoizedSize = sizeAndCount.totalSize + subgraphChildren.stream()
                    .mapToLong(MemorySubgraph::totalSize)
                    .sum();
            }
            return memoizedSize;
        }

        /**
         * Compute the total count of descendant objects of this subgraph. The subgraph
         * root counts as a member of the subgraph.  This count is memoized so subsequent
         * calls will not have to recompute.
         *
         * @return the total count of descendant objects of this subgraph
         */
        public long totalCount() {
            if (memoizedCount < 0) {
                memoizedCount = sizeAndCount.count + subgraphChildren.stream()
                    .mapToLong(MemorySubgraph::totalCount)
                    .sum();
            }
            return memoizedCount;
        }

        /**
         * Get the depth at which we visited the root object of this subgraph during
         * the memory walk.
         *
         * @return the depth at which we visited the root object of this subgraph during
         *         the memory walk.
         */
        public int getDepth() {
            return subgraphRoot.getDepth();
        }
    }

    /**
     * Iterator to iterate subgraphs in breadth-first order starting from the subgraphs at the
     * root of the memory walk.
     * <p/>
     * Removal is not permitted through this iterator.
     */
    public static class MemorySubgraphIterator implements Iterator<MemorySubgraph> {
        private final Deque<MemorySubgraph> subgraphs;

        private MemorySubgraphIterator(@Nonnull final List<MemorySubgraph> roots) {
            subgraphs = new ArrayDeque<>(roots);
        }

        @Override
        public boolean hasNext() {
            return !subgraphs.isEmpty();
        }

        @Override
        public MemorySubgraph next() {
            final MemorySubgraph sg = subgraphs.pop();
            subgraphs.addAll(sg.subgraphChildren);

            return sg;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove");
        }
    }

    /**
     * Iterate memory nodes visited during the memory walk in breadth-first order starting
     * from the subgraphs at the root of the walk.
     * <p/>
     * Removal is not permitted through this iterator.
     */
    public static class MemoryNodeIterator implements Iterator<MemoryReferenceNode> {
        private final Deque<MemorySubgraph> subgraphs;
        private final Deque<PeekingIterator<MemoryReferenceNode>> nodeIterators;
        private int currentDepth;

        private MemoryNodeIterator(@Nonnull final List<MemorySubgraph> roots) {
            subgraphs = new ArrayDeque<>(roots);
            nodeIterators = new ArrayDeque<>();
            currentDepth = 0;
        }

        @Override
        public boolean hasNext() {
            return !subgraphs.isEmpty() || !nodeIterators.isEmpty();
        }

        /**
         * Get the next {@link MemoryReferenceNode} in breadth-first order. In order to achieve this, we
         * iterate the nodes in all retained subgraphs in depth-first order until we have exhausted
         * all subgraphs. Then we iterate retained descendants of subgraphs in depth-first order
         * until we have exhausted those too.
         * <p/>
         * @return the next {@link MemoryReferenceNode} in breadth-first order.
         */
        @Override
        public MemoryReferenceNode next() {
            if (!subgraphs.isEmpty()) {
                final MemorySubgraph sg = subgraphs.pop();
                if (sg.subgraphChildren.isEmpty()) {
                    if (!sg.descendants.isEmpty()) {
                        nodeIterators.add(Iterators.peekingIterator(sg.descendants.iterator()));
                    }
                } else {
                    subgraphs.addAll(sg.subgraphChildren);
                }

                currentDepth = sg.getDepth();
                return sg.subgraphRoot;
            } else {
                final PeekingIterator<MemoryReferenceNode> curIt = nodeIterators.peekFirst();
                final MemoryReferenceNode nextNode = curIt.next();
                if (curIt.hasNext() && curIt.peek().getDepth() != currentDepth) {
                    assert (curIt.peek().getDepth() > currentDepth);
                    // Remaining descendants are at a deeper depth.
                    // Put this in the back of the deque so we can iterate the rest of the iterators
                    // with remaining descendents at the current depth.
                    nodeIterators.add(nodeIterators.pop());

                    // Adjust currentDepth for next call. We'll only get to the next depth after
                    // going through all at the current depth.
                    currentDepth = nodeIterators.peek().peek().getDepth();
                }

                // Pop off all empty iterators until we reach a non-empty iterator or we exhaust
                // all iterators
                while (!nodeIterators.isEmpty() && !nodeIterators.peekFirst().hasNext()) {
                    nodeIterators.pop();
                }

                return nextNode;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove");
        }
    }
}
