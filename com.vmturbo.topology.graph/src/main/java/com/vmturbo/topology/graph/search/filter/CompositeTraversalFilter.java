package com.vmturbo.topology.graph.search.filter;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * {@link CompositeTraversalFilter} filter which combines results of individual {@link
 * TraversalFilter} instances.
 *
 * @param <E> The type of {@link TopologyGraphSearchableEntity} the filter works
 *                 with.
 */
public class CompositeTraversalFilter<E extends TopologyGraphSearchableEntity<E>>
                implements TopologyFilter<E> {
    private static final Map<LogicalOperator, ? extends BinaryOperator<Stream<?>>>
                    OPERATOR_TO_RESULT_COMBINER =
                    ImmutableMap.of(LogicalOperator.OR, Stream::concat, LogicalOperator.AND,
                                    (s1, s2) -> {
                                        final Set<?> elements = s2.collect(Collectors.toSet());
                                        return s1.filter(elements::contains);
                                    });
    private final Collection<TraversalFilter<E>> filters;
    private final BinaryOperator<Stream<?>> operator;

    /**
     * Creates {@link CompositeTraversalFilter} instance.
     *
     * @param filters individual {@link TraversalFilter} instances which results are
     *                 going to be combined.
     * @param operation points to the way how results of individual filters have to
     *                 be combined.
     */
    public CompositeTraversalFilter(@Nonnull Collection<TraversalFilter<E>> filters,
                    @Nonnull LogicalOperator operation) {
        this.filters = filters;
        this.operator = getOperator(operation);
    }

    private static BinaryOperator<Stream<?>> getOperator(LogicalOperator operation) {
        final BinaryOperator<Stream<?>> operator = OPERATOR_TO_RESULT_COMBINER.get(operation);
        if (operator == null) {
            throw new IllegalArgumentException(
                            String.format("There is no combining operator for '%s' operation",
                                            operation));
        }
        return operator;
    }

    @Nonnull
    @Override
    public Stream<E> apply(@Nonnull Stream<E> entities, @Nonnull TopologyGraph<E> graph) {
        final Collection<E> nodes = entities.collect(Collectors.toList());
        return filters.stream().map(filter -> filter.traverse(nodes.stream(), graph))
                        .reduce((s1, s2) -> {
                            @SuppressWarnings("unchecked")
                            final Stream<E> result = (Stream<E>)operator.apply(s1, s2);
                            return result;
                        }).orElse(Stream.empty()).distinct();
    }
}
