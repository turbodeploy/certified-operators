package com.vmturbo.topology.graph.search.filter;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

public class BulkPropertyFilter<E extends TopologyGraphSearchableEntity<E>> extends PropertyFilter<E> {

    private final Function<List<E>, Stream<E>> applyFn;

    public BulkPropertyFilter(Function<List<E>, Stream<E>> applyFn) {
        super(e -> false);
        this.applyFn = applyFn;
    }

    @NotNull
    @Override
    public Stream<E> apply(@NotNull Stream<E> entities, @NotNull TopologyGraph<E> graph) {
        final List<E> collectedInput = entities.collect(Collectors.toList());
        return applyFn.apply(collectedInput);
    }
}
