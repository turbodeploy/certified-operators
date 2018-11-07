package com.vmturbo.repository.search;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import javaslang.collection.Stream;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

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
     * @param paginationParams if not set, it means search query is not paginated. Otherwise, it contains
     *                         all pagination parameters.
     * @return A potentially fused list of {@link AQLRepr}.
     */
    public static List<AQLRepr> fuse(@Nonnull final List<AQLRepr> aqlReprs,
                                     @Nonnull final Optional<PaginationParameters> paginationParams) {
        if (aqlReprs.isEmpty()) {
            return Collections.emptyList();
        }

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
                        case PROPERTY_MAP:
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

        final List<AQLRepr> fusedAQLReprs = paginationParams.isPresent()
                ? generateAQLWithPagination(groupedFilters, paginationParams.get())
                : (groupedFilters.stream()
                    .map(filterGroup -> new AQLRepr(javaslang.collection.List.ofAll(filterGroup)))
                    .collect(Collectors.toList()));

        return fusedAQLReprs;
    }

    /**
     * Generate a list of {@link AQLRepr} which contains AQL pagination query. And the AQL pagination
     * query will be added to last {@link AQLRepr}, because pagination
     *
     * @param groupedFilters a list of filters which have been fused.
     * @param paginationParams {@link PaginationParameters} contains the query pagination parameters.
     * @return a list of {@link AQLRepr} after add AQL pagination.
     */
    private static List<AQLRepr> generateAQLWithPagination(
            @Nonnull final List<List<Filter<? extends AnyFilterType>>> groupedFilters,
            @Nonnull final PaginationParameters paginationParams) {
        if (groupedFilters.isEmpty()) {
            return Collections.emptyList();
        }
        // fetch all AQLRepr except last one, because the last AQLRepr needs to add pagination.
        final List<AQLRepr> AQLReprs = javaslang.collection.List.ofAll(groupedFilters)
                .init()
                .map(group -> new AQLRepr(javaslang.collection.List.ofAll(group)))
                .toJavaList();

        final AQLPagination aqlPagination = createAqlPagination(paginationParams);
        // Add pagination into last AQLRepr.
        final AQLRepr AQLReprLastOne = new AQLRepr(javaslang.collection.List.ofAll(
                javaslang.collection.List.ofAll(groupedFilters).last()), aqlPagination);
        AQLReprs.add(AQLReprLastOne);
        return AQLReprs;
    }

    /**
     * Convert {@link PaginationParameters} into {@link AQLPagination} which will be converted to
     * AQL later.
     *
     * @param paginationParams {@link PaginationParameters} contains the query pagination parameters.
     * @return a {@link AQLPagination}.
     */
    private static AQLPagination createAqlPagination(@Nonnull final PaginationParameters paginationParams) {
        final long skipNum = paginationParams.hasCursor() ? Long.parseLong(paginationParams.getCursor()) : 0;
        final Optional<Long> limitNum = paginationParams.hasLimit()
                ? Optional.of(Long.valueOf(paginationParams.getLimit()))
                : Optional.empty();
        final boolean isAscending = paginationParams.getAscending();
        final SearchOrderBy sortFieldName = paginationParams.hasOrderBy()
                && paginationParams.getOrderBy().hasSearch()
                    ? paginationParams.getOrderBy().getSearch()
                    : OrderBy.SearchOrderBy.ENTITY_NAME;
        return new AQLPagination(sortFieldName, isAscending, skipNum, limitNum);
    }
}
