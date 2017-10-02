package com.vmturbo.repository.search;

import com.google.common.collect.Lists;
import javaslang.collection.Stream;

import java.util.List;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.search.Search.SearchParameters;

public class AQLReprFuser {

    /**
     * Fuse multiple {@link AQLRepr} together in order to reduce the number of queries.
     *
     * Each instance {@link AQLRepr} will entail a database query, by fusing them together we
     * can reduce the number of queries needed to send to the database.
     * Only property filters can be fused together; traversal filters cannot be fused together.
     *
     * Deal to the way {@link SearchParameters} is designed,
     * the first <em>overall</em> filter will never be a traversal filter.
     *
     * @param aqlReprs The list of {@link AQLRepr} to be fused.
     * @return A potentially fused list of {@link AQLRepr}.
     */
    public static List<AQLRepr> fuse(List<AQLRepr> aqlReprs) {
        final List<Filter<? extends AnyFilterType>> filters = aqlReprs.stream()
                .flatMap(repr -> repr.getFilters().toJavaStream())
                .collect(Collectors.toList());

        final List<List<Filter<? extends AnyFilterType>>> zero = Lists.newArrayList();
        zero.add(Lists.newArrayList());

        final List<List<Filter<? extends AnyFilterType>>> groupedFilters =
                Stream.ofAll(filters).foldLeft(zero, (grouped, filter) -> {
                    switch (filter.getType()) {
                        case PROPERTY_NUMERIC:
                        case PROPERTY_STRING:
                            final int idx = grouped.size() - 1;
                            final List<Filter<? extends AnyFilterType>> lastGroup = grouped.get(idx);
                            lastGroup.add(filter);
                            break;

                        case TRAVERSAL_COND:
                        case TRAVERSAL_HOP:
                            final List<Filter<? extends AnyFilterType>> newGroup = Lists.newArrayList(filter);
                            grouped.add(newGroup);
                            break;
                    }

                    return grouped;
                });

        final List<AQLRepr> fusedAQLReprs = groupedFilters.stream()
                .map(filterGroup -> new AQLRepr(javaslang.collection.List.ofAll(filterGroup)))
                .collect(Collectors.toList());

        return fusedAQLReprs;
    }
}
