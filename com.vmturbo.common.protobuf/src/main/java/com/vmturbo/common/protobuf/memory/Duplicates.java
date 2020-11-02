package com.vmturbo.common.protobuf.memory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Streams;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryGraphVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemorySubgraph;

/**
 * A utility class useful for detecting duplicate objects in-memory.
 */
public class Duplicates {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Private because all methods are helpers.
     */
    private Duplicates() {

    }

    /**
     * Detect duplicate objects in memory and get some statistical information about them.
     * By detecting duplicates, it may be possible to eliminate those duplicates in order
     * to save memory. This process should be done off-line, it is too expensive to run
     * at run-time.
     * <p/>
     * It's important to note that this method expects all objects to have independent sub-graphs
     * and to not share memory for the deep memory sizes to be accurate. If objects
     * in the list share references to other objects, the deep sizes will be incorrect (although
     * the rest of the information, including shallow sizes, in the summary will still be correct).
     *
     * @param messages The messages whose duplicates should be detected.
     * @param klass The class of objects whose duplicates are being detected. Subclasses of this class will
     *              also be counted.
     * @param <T> The class of objects whose duplicates are being detected.
     * @return A summary describing the messages and their duplicates.
     */
    @SuppressWarnings("unchecked")
    public static <T> DuplicateSummary<T> detectDuplicates(
        @Nonnull final Collection<? extends T> messages,
        @Nonnull final Class<T> klass) {
        final long dupCheckStart = System.nanoTime();
        final MemoryGraphVisitor visitor = new MemoryGraphVisitor(Collections.emptySet(), -1, -1, -1, true);
        new RelationshipMemoryWalker(visitor).traverse(messages);
        final Map<T, MemUsageCount> counts = new HashMap<>();
        Streams.stream(visitor.subgraphIterator())
            .filter(subgraph -> klass.isInstance(subgraph.subgraphRoot.getObj()))
            .forEach(subgraph -> {
                T obj = (T)subgraph.subgraphRoot.getObj();
                counts.compute(obj, (k, v) -> v == null
                    ? new MemUsageCount(subgraph)
                    : v.addDuplicate(subgraph));
            });
        final long totalSize = visitor.totalSize();
        return new DuplicateSummary<>(counts, System.nanoTime() - dupCheckStart, totalSize);
    }

    /**
     * Detect duplicate protobuf messages. A specialization of the
     * {@link #detectDuplicates(Collection, Class)} method above specific for detecting
     * duplicate protobuf messages.
     *
     * @param protos The protos whose duplicates should be detected.
     * @return A summary of the duplicate protobufs detected.
     */
    public static DuplicateSummary<AbstractMessage> detectDuplicateProtobufs(
        @Nonnull final Collection<? extends AbstractMessage> protos) {
        return detectDuplicates(protos, AbstractMessage.class);
    }

    /**
     * A helper method to calculate the percentage of a quantity that is duplicated.
     *
     * @param duplicates The duplicate amount.
     * @param instances The total amount, including duplicates.
     * @return The percentage of duplicates.
     */
    public static double percentDup(long duplicates, long instances) {
        return ((double)duplicates / instances) * 100.0;
    }

    /**
     * A summary of duplicates.
     *
     * @param <T> The type of objects being summarized.
     */
    public static class DuplicateSummary<T> {
        /**
         * How long the duplicate scan took, in nanoseconds.
         */
        private final long scanDurationNanos;

        /**
         * The total size in bytes of the duplicates scanned.
         */
        private final long totalSizeBytes;

        /**
         * A summary of the objects scanned. Each unique instance of an object
         * is a key in the map. The summary may be aggregated in different ways.
         */
        private final Map<T, MemUsageCount> summary;

        /**
         * Create a new {@link DuplicateSummary}.
         *
         * @param summary A map containing the summary information.
         * @param scanDurationNanos how long the duplicate scan took, in nanoseconds.
         * @param totalSizeBytes total size in bytes of the duplicates scanned.
         */
        public DuplicateSummary(@Nonnull final Map<T, MemUsageCount> summary,
                                final long scanDurationNanos,
                                final long totalSizeBytes) {
            this.scanDurationNanos = scanDurationNanos;
            this.totalSizeBytes = totalSizeBytes;
            this.summary = Objects.requireNonNull(summary);
        }

        /**
         * Get the raw summary. Each key in the map is a unique instance scanned.
         *
         * @return the raw summary.
         */
        public final Map<T, MemUsageCount> getSummary() {
            return summary;
        }

        /**
         * Output is aggregated by class to summarize. The output is in the format:
         * <p/>
         * Duplicates Summary [PT20.843215655S]
         *         DEEP     SAVING   %DEEP    SHALLOW  %SHALW       COUNT  %COUNT TYPE
         * TOTAL                             334.3 MB  35.22%
         * 1   105.0 MB    64.3 MB  61.20%    34.2 MB  66.13%     373,217  66.13% CommodityBoughtDTO
         * 2    66.7 MB    55.3 MB  82.82%    24.2 MB  89.24%     633,923  89.24% CommodityType
         * 3    96.0 MB    54.1 MB  56.33%    39.8 MB  60.22%     260,706  60.22% CommoditySoldDTO
         * 4    42.9 MB    35.3 MB  82.19%    42.9 MB  82.19%     703,321  82.19% HistoricalValues
         * 5   116.7 MB    20.2 MB  17.30%     4.7 MB  31.07%      87,957  31.07% CommoditiesBoughtFromProvider
         * 6    36.8 MB    15.7 MB  42.75%     6.6 MB  50.28%     173,309  50.28% MapEntry
         * 7    10.6 MB     3.5 MB  33.52%     1.3 MB  81.60%      42,745  81.60% Tags
         * ...
         * <p/>
         * Each field means the following:
         * DEEP:    The deep size of all objects of that class. Includes child objects. Only the true deep
         *          size when scanned objects do not already share references.
         * SAVING:  If all duplicates of this object type were removed, how much memory could be saved by
         *          eliminating those duplicates and their children.
         * %DEEP:   The percentage of deep size that is duplicated for this class of object.
         * SHALLOW: The shallow size of each object of that class. Shallow size excludes child objects.
         * %SHALW:  The percentage of shallow size that is duplicated for htis class of object.
         * COUNT:   The number of instances of this type of object that were scanned.
         * %COUNT:  The percentage of instances of this type of object that are duplicated.
         * TYPE:    The class of the object.
         * <p/>
         * Lines in the report are sorted by their SAVING value in descending order because objects with
         * higher SAVING indicate a promising target for de-duplication.
         * <p/>
         * The TOTAL line summarizes the totals across all scanned objects. Note that the TOTAL number
         * in the SHALLOW column is approximately the sum of the shallow sizes (plus the sizes of other
         * objects in the memory graph that were not of the type T). The %SHALW number approximates
         * the percentage of memory that would be saved from the TOTAL if all duplicates were eliminated.
         *
         * @return A string representation of the summary.
         */
        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append(String.format("Duplicates Summary [%s]\n",
                Duration.ofNanos(scanDurationNanos)));

            final HashMap<Class<?>, MemUsageCount> byClass = new HashMap<>();
            summary.forEach((msg, usageCount) ->
                byClass.compute(msg.getClass(), (k, v) -> v == null
                    ? new MemUsageCount(usageCount)
                    : v.addMemUsageCount(usageCount)));

            final int numberSpaces = Integer.toString(byClass.size()).length();
            final String nsStr =  " %-" + numberSpaces + "d";
            final String header = String.format(" %" + numberSpaces + "s%10s %10s %7s %10s %7s %11s %7s %s%n",
                "", "DEEP", "SAVING", "%DEEP", "SHALLOW", "%SHALW", "COUNT", "%COUNT", "TYPE");
            builder.append(header);

            final long totalShallowDuplicateBytes = summary.values().stream()
                .mapToLong(MemUsageCount::getDuplicateShallowSizeBytes)
                .sum();
            builder.append(String.format("TOTAL %" + (24 + numberSpaces) + "s %10s %7s%n", "",
                StringUtil.getHumanReadableSize(totalSizeBytes),
                String.format("%.2f", percentDup(totalShallowDuplicateBytes, totalSizeBytes)) + "%"));

            final AtomicInteger curIndex = new AtomicInteger(1);
            byClass.entrySet().stream()
                .sorted(Comparator.comparingLong(e -> -e.getValue().getDuplicateDeepSizeBytes()))
                .forEach(entry -> builder.append(
                    format(entry.getValue(), entry.getKey(), curIndex.getAndIncrement(), nsStr)));

            return builder.toString();
        }

        /**
         * Get the {@link MemUsageCount}s for objects matching the predicate.
         *
         * @param predicate The predicate to test.
         * @return The {@link MemUsageCount}s for objects matching the predicate.
         */
        public Stream<MemUsageCount> usageBy(Predicate<T> predicate) {
            return summary.entrySet().stream()
                .filter(e -> predicate.test(e.getKey()))
                .map(Entry::getValue);
        }

        private String format(@Nonnull final MemUsageCount usageCount,
                              @Nonnull final Class<?> klass,
                              final int index, @Nonnull final String nsStr) {
            return String.format(nsStr + "%s %s%n", index, usageCount.toString(), klass.getSimpleName());
        }
    }

    /**
     * A small class summarizing various memory and count statistics.
     */
    public static class MemUsageCount {
        // The number of instances of a particular unique proto message
        private long instances;

        // The number of times the unique message is duplicated across a set of messages.
        // If a message only occurs once, it has 0 duplicates.
        private long duplicates;

        // The shallow size in bytes for the message and its duplicates.
        // This number DOES NOT include sub-messages of the given message.
        private long shallowBytes;

        // The shallow size in bytes for a unique instance of the message.
        // This number DOES NOT include sub-messages of the given message.
        private long uniqueShallowBytes;

        // The deep size in bytes for the message and its duplicates.
        // This number DOES include sub-messages of the given message.
        private long deepBytes;

        // The deep size for a unique instance of the message.
        // This number DOES include sub-messages of the given message.
        private long uniqueDeepBytes;

        /**
         * Create a new {@link MemUsageCount}.
         *
         * @param subgraph The subgraph whose memory will be tracked by this instance.
         */
        public MemUsageCount(@Nonnull final MemorySubgraph subgraph) {
            instances = 1;
            duplicates = 0;
            deepBytes = subgraph.totalSize();
            uniqueDeepBytes = deepBytes;
            shallowBytes = subgraph.shallowSize();
            uniqueShallowBytes = shallowBytes;
        }

        /**
         * Copy constructor for a {@link MemUsageCount}.
         *
         * @param other Another {@link MemUsageCount} to copy.
         */
        public MemUsageCount(@Nonnull final MemUsageCount other) {
            instances = other.instances;
            duplicates = other.duplicates;
            deepBytes = other.deepBytes;
            uniqueDeepBytes = other.uniqueDeepBytes;
            shallowBytes = other.shallowBytes;
            uniqueShallowBytes = other.uniqueShallowBytes;
        }

        /**
         * Add memory and count information for another subgraph associated with the one
         * used to create this {@link MemUsageCount}.
         *
         * @param subgraph The subgraph whose memory should be tracked.
         * @return A reference to {@link this} for method chaining.
         */
        public MemUsageCount addDuplicate(@Nonnull final MemorySubgraph subgraph) {
            instances++;
            duplicates++;
            deepBytes += subgraph.totalSize();
            shallowBytes += subgraph.shallowSize();

            return this;
        }

        /**
         * Combine the memory and usage statistics from another {@link MemUsageCount}
         * to this one.
         *
         * @param other The other {@link MemUsageCount} whose stats should be added to this one.
         * @return A reference to {@link this} for method chaining.
         */
        public MemUsageCount addMemUsageCount(@Nonnull final MemUsageCount other) {
            instances += other.instances;
            duplicates += other.duplicates;
            deepBytes += other.deepBytes;
            uniqueDeepBytes += other.uniqueDeepBytes;
            shallowBytes += other.shallowBytes;
            uniqueShallowBytes += other.uniqueShallowBytes;

            return this;
        }

        @Override
        public String toString() {
            return String.format("%10s %10s %7s %10s %7s %11s %7s",
                StringUtil.getHumanReadableSize(deepBytes),
                StringUtil.getHumanReadableSize(getDuplicateDeepSizeBytes()),
                String.format("%.2f", percentDup(getDuplicateDeepSizeBytes(), deepBytes)) + "%",
                StringUtil.getHumanReadableSize(shallowBytes),
                String.format("%.2f", percentDup(getDuplicateShallowSizeBytes(), shallowBytes)) + "%",
                String.format("%,d", instances),
                String.format("%.2f", percentDup(duplicates, instances)) + "%");
        }

        /**
         * Get the instance count.
         *
         * @return the instance count.
         */
        public long getInstanceCount() {
            return instances;
        }

        /**
         * Get the duplicate count.
         *
         * @return the duplicate count.
         */
        public long getDuplicateCount() {
            return duplicates;
        }

        /**
         * Get the deep size in bytes.
         *
         * @return the deep size in bytes.
         */
        public long getDeepSizeBytes() {
            return deepBytes;
        }

        /**
         * Get the shallow size in bytes.
         *
         * @return the shallow size in bytes.
         */
        public long getShallowSizeBytes() {
            return shallowBytes;
        }

        /**
         * Get the deep size for unique instances in bytes.
         *
         * @return the deep size for unique instances in bytes.
         */
        public long getUniqueDeepSizeBytes() {
            return uniqueDeepBytes;
        }

        /**
         * Get the shallow size for unique instances in bytes.
         *
         * @return the shallow size for unique instances in bytes.
         */
        public long getUniqueShallowSizeBytes() {
            return uniqueShallowBytes;
        }

        /**
         * Get the shallow size in bytes for duplicate instances.
         *
         * @return the shallow size in bytes for duplicate instances.
         */
        public long getDuplicateShallowSizeBytes() {
            return shallowBytes - uniqueShallowBytes;
        }

        /**
         * Get the deep size in bytes for duplicate instances.
         *
         * @return the deep size in bytes for duplicate instances.
         */
        public long getDuplicateDeepSizeBytes() {
            return deepBytes - uniqueDeepBytes;
        }
    }
}
