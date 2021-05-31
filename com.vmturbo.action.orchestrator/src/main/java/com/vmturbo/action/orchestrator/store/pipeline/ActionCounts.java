package com.vmturbo.action.orchestrator.store.pipeline;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.stringtemplate.v4.ST;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Counts of actions broken down by action type and target entity type.
 * Provides a method to compute the difference in actions between two {@link ActionCounts}.
 */
public class ActionCounts {
    /**
     * The template used to visualize the summary as a string.
     */
    private static final String LOGGING_TEMPLATE = "<header>\n"
        + "    <message>\n";

    private final Map<ActionTypeCase, EntityTypeCounts> counts;

    private boolean includeSizes = true;

    private String title;

    /**
     * Create a new {@link ActionCounts}.
     *
     * @param title The title of the counts. Rendered when the counts are rendered to string.
     * @param actions The actions to count. Note that {@link ActionCounts} constructed using this
     *                method (a collection of action model objects) do not include their sizes
     *                because we have no method to quickly compute the size of an action model object.
     */
    public ActionCounts(@Nonnull final String title,
                        @Nonnull final Collection<? extends ActionView> actions) {
        this(title, actions.stream().map(ActionView::getRecommendation));
        this.includeSizes = false;
    }

    /**
     * Create a new {@link ActionCounts}.
     *
     * @param title The title of the counts. Rendered when the counts are rendered to string.
     * @param actions The actions to count. Note that {@link ActionCounts} constructed using this
     *                method (a stream of protobuf action DTOs) do include their sizes
     *                because we have a method to quickly compute the size of proto objects.
     */
    public ActionCounts(@Nonnull final String title,
                        @Nonnull final Stream<ActionDTO.Action> actions) {
        this.title = Objects.requireNonNull(title);
        counts = actions.collect(Collectors.groupingBy(action -> action.getInfo().getActionTypeCase(),
            new EntityTypeCountsCollector()));
    }

    /**
     * Get the total number of actions counted by this {@link ActionCounts}.
     *
     * @return the total number of actions counted by this {@link ActionCounts}.
     */
    public long totalActionCount() {
        return counts.values().stream().mapToLong(counts -> counts.total.count).sum();
    }

    /**
     * The total size in bytes of all actions counted by this {@link ActionCounts}. Note that this
     * number may not be meaningful if created for action model objects.
     *
     * @return the total size in bytes of all actions counted by this {@link ActionCounts}.
     */
    public long totalActionSize() {
        return counts.values().stream().mapToLong(counts -> counts.total.sizeBytes).sum();
    }

    /**
     * Set the title.
     *
     * @param title The title.
     */
    public void setTitle(@Nonnull final String title) {
        this.title = Objects.requireNonNull(title);
    }

    /**
     * Render a string describing the difference in actions between this {@link ActionCounts}
     * and another. Example:
     * <p/>
     * ActionPlan Summary: (difference from previous action plan in parentheses)
     *     TOTAL: 303,041 [34.1 MB] (-42)
     *     RESIZE: 291,190 [32.9 MB] (0)
     *         VIRTUAL_MACHINE: 181,707 [11.8 MB] (0)
     *         CONTAINER: 97,023 [20.4 MB] (0)
     *         APPLICATION_COMPONENT: 12,460 [791.5 KB] (0)
     *     MOVE: 7,139 [649.3 KB] (-38)
     *         VIRTUAL_MACHINE: 7,004 [637.6 KB] (-38)
     *         CONTAINER_POD: 135 [11.6 KB] (0)
     *     PROVISION: 2,856 [194.9 KB] (-2)
     *         PHYSICAL_MACHINE: 1,855 [125.1 KB] (0)
     *         STORAGE: 1,001 [69.7 KB] (-2)
     *     DEACTIVATE: 1,812 [400.9 KB] (-2)
     *         PHYSICAL_MACHINE: 1,069 [324.7 KB] (-2)
     *         CONTAINER_POD: 540 [55.8 KB] (0)
     *         APPLICATION_COMPONENT: 101 [4.0 KB] (0)
     *         VIRTUAL_MACHINE: 101 [16.2 KB] (0)
     *         STORAGE: 1 [145 Bytes] (0)
     *     RECONFIGURE: 43 [5.0 KB] (0)
     *         VIRTUAL_MACHINE: 43 [5.0 KB] (0)
     *     ACTIVATE: 1 [126 Bytes] (0)
     *         VIRTUAL_MACHINE: 1 [126 Bytes] (0)
     *
     * @param other The other {@link ActionCounts} to diff.
     * @return A string describing the difference in actions between this {@link ActionCounts}
     *         and another.
     */
    public String difference(@Nonnull final ActionCounts other) {
        final ST template = new ST(LOGGING_TEMPLATE);
        final CountAndSize thisSize = new CountAndSize(totalActionCount(), totalActionSize());
        final CountAndSize otherSize = new CountAndSize(other.totalActionCount(), other.totalActionSize());

        final String totalLine = "TOTAL: " + thisSize.minus(otherSize, includeSizes) + "\n";
        final String thisContents = counts.entrySet().stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue().total.count, e1.getValue().total.count))
            .map(e -> e.getKey().name() + ": " + e.getValue().difference(
                other.counts.getOrDefault(e.getKey(), EntityTypeCounts.EMPTY), includeSizes))
            .collect(Collectors.joining(""));
        final String otherContents = Sets.difference(other.counts.keySet(), this.counts.keySet()).stream()
            .map(type -> type.name() + ": " + EntityTypeCounts.EMPTY
                .difference(other.counts.get(type), includeSizes))
            .collect(Collectors.joining(""));

        return template.add("header", title)
            .add("message", totalLine + thisContents + otherContents)
            .render();
    }

    @Override
    public String toString() {
        final ST template = new ST(LOGGING_TEMPLATE);
        final long count = totalActionCount();
        final long sizeBytes = totalActionSize();

        final String message = "TOTAL: " + new CountAndSize(count, sizeBytes).render(includeSizes) + "\n"
            + counts.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().total.count, e1.getValue().total.count))
                .map(e -> e.getKey().name() + ": " + e.getValue().render(includeSizes))
                .collect(Collectors.joining(""));

        return template.add("header", title)
            .add("message", message)
            .render();
    }

    /**
     * A helper class containing the counts of actions mapped by entity type.
     */
    private static class EntityTypeCounts {
        private final CountAndSize total;
        private final Map<Integer, CountAndSize> counts;

        /**
         * An empty {@link EntityTypeCounts}.
         */
        private static final EntityTypeCounts EMPTY = new EntityTypeCounts(Collections.emptyMap());

        /**
         * Construct a new {@link EntityTypeCounts}.
         *
         * @param counts A map of the counts.
         */
        private EntityTypeCounts(@Nonnull final Map<Integer, CountAndSize> counts) {
            this.counts = Objects.requireNonNull(counts);
            total = new CountAndSize();
            total.count = counts.values().stream().mapToLong(count -> count.count).sum();
            total.sizeBytes = counts.values().stream().mapToLong(count -> count.sizeBytes).sum();
        }

        /**
         * Compute a difference string of the entity types. The display is sorted
         * by action count per entity type, descending.
         *
         * @param other The other {@link EntityTypeCounts}.
         * @param includeSizes Whether to include the sizes in bytes of the actions counted.
         * @return A text rendering of the difference between this and another {@link EntityTypeCounts}.
         */
        private String difference(@Nonnull final EntityTypeCounts other,
                                 final boolean includeSizes) {
            final ST template = new ST(LOGGING_TEMPLATE);
            final String thisContents = counts.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().count, e1.getValue().count))
                .map(e -> EntityType.forNumber(e.getKey()).name() + ": " + e.getValue().minus(
                    other.counts.getOrDefault(e.getKey(), CountAndSize.EMPTY), includeSizes))
                .collect(Collectors.joining("\n"));
            final String otherContents = Sets.difference(other.counts.keySet(), this.counts.keySet()).stream()
                .map(type -> EntityType.forNumber(type).name() + ": "
                    + CountAndSize.EMPTY.minus(other.counts.get(type), includeSizes))
                .collect(Collectors.joining("\n"));
            final String separator = thisContents.isEmpty() || otherContents.isEmpty() ? "" : "\n";

            template.add("header", total.minus(other.total, includeSizes))
                .add("message", thisContents + separator + otherContents);
            return template.render();
        }

        /**
         * Render the action counts per entity-type to a string.
         *
         * @param includeSizes Whether to include the sizes in bytes of the actions counted.
         * @return the action counts per entity-type to a string.
         */
        private String render(final boolean includeSizes) {
            final ST template = new ST(LOGGING_TEMPLATE);
            return template.add("header", total.render(includeSizes))
                .add("message", counts.entrySet().stream()
                    .sorted((e1, e2) -> Long.compare(e2.getValue().count, e1.getValue().count))
                    .map(e -> EntityType.forNumber(e.getKey()).name() + ": " + e.getValue().render(includeSizes))
                    .collect(Collectors.joining("\n")))
                .render();
        }
    }

    /**
     * A collector to allow the collection of {@link EntityTypeCounts} via the stream
     * {@link Stream#collect(Collector)} interface.
     */
    private static class EntityTypeCountsCollector implements Collector<Action, Map<Integer, CountAndSize>, EntityTypeCounts> {

        @Override
        public Supplier<Map<Integer, CountAndSize>> supplier() {
            return HashMap::new;
        }

        @Override
        public BiConsumer<Map<Integer, CountAndSize>, Action> accumulator() {
            return (map, action) -> {
                final long size = action.getSerializedSize();
                try {
                    final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action, false);
                    map.compute(actionEntity.getType(), (type, count) -> count == null
                        ? new CountAndSize(1, size)
                        : count.increment(size));
                } catch (UnsupportedActionException e) {
                    map.compute(EntityType.UNKNOWN_VALUE, (type, count) -> count == null
                        ? new CountAndSize(1, size)
                        : count.increment(size));
                }
            };

        }

        @Override
        public BinaryOperator<Map<Integer, CountAndSize>> combiner() {
            return (left, right) -> {
                right.forEach((k, v) -> {
                    left.compute(k, (type, count) -> count == null ? v : count.add(v));
                });
                return left;
            };
        }

        @Override
        public Function<Map<Integer, CountAndSize>, EntityTypeCounts> finisher() {
            return EntityTypeCounts::new;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Sets.immutableEnumSet(Characteristics.UNORDERED);
        }
    }

    /**
     * A small helper class bundling a count number with the size in bytes.
     */
    private static class CountAndSize {
        private long count;
        private long sizeBytes;

        private static final CountAndSize EMPTY = new CountAndSize();

        /**
         * Construct a new {@link CountAndSize} with all values initialized to 0.
         */
        private CountAndSize() {
            count = 0;
            sizeBytes = 0;
        }

        /**
         * Construct a new {@link CountAndSize}.
         *
         * @param count The count.
         * @param sizeBytes The size in bytes.
         */
        private CountAndSize(final long count, final long sizeBytes) {
            this.count = count;
            this.sizeBytes = sizeBytes;
        }

        /**
         * Increment the count by one, and the sizeInBytes as specified.
         *
         * @param sizeBytes The amount to increment the size in bytes.
         * @return A reference to {@link this} for method chaining.
         */
        private CountAndSize increment(final long sizeBytes) {
            count++;
            this.sizeBytes += sizeBytes;
            return this;
        }

        /**
         * Add the counts and size in bytes from another {@link CountAndSize} to this one.
         * Note that this mutates {@link this} rather than creating a new {@link CountAndSize}.
         *
         * @param countAndSize The other {@link CountAndSize} to add to this one.
         * @return A reference to {@link this} for method chaining.
         */
        private CountAndSize add(@Nonnull final CountAndSize countAndSize) {
            count += countAndSize.count;
            sizeBytes += countAndSize.sizeBytes;

            return this;
        }

        /**
         * Render the string difference between another {@link CountAndSize} and this one. Example:
         * 303,041 [34.1 MB] (-42)
         *
         * @param other The other {@link CountAndSize} to difference from this one.
         * @param includeSizes Whether to include size in bytes in the description.
         * @return A String describing the difference between another {@link CountAndSize} and this one.
         */
        private String minus(@Nonnull final CountAndSize other,
                            final boolean includeSizes) {
            final long diff = count - other.count;
            String difference = "0";
            if (diff > 0) {
                difference = "+" + String.format("%,d", diff);
            } else if (diff < 0) {
                difference = String.format("%,d", diff);
            }
            return render(includeSizes) + " (" + difference + ")";
        }

        /**
         * Render the {@link CountAndSize} into a helpful description. Example:
         * 303,041 [34.1 MB]
         *
         * @param includeSizes Whether or not to include sizes.
         * @return A helpful string description of this {@link CountAndSize}.
         */
        private String render(final boolean includeSizes) {
            if (includeSizes) {
                return String.format("%,d", count) + " [" + StringUtil.getHumanReadableSize(sizeBytes) + "]";
            } else {
                return String.format("%,d", count);
            }
        }
    }
}
