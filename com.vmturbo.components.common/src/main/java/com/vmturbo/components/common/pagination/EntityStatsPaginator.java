package com.vmturbo.components.common.pagination;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

/**
 * A utility class to do in-memory pagination of entities given a way to access their
 * sort commodity (via a {@link SortCommodityValueGetter}.
 */
public class EntityStatsPaginator {

    /**
     * A utility interface to get the sort commodity value for an entity given the OID.
     */
    @FunctionalInterface
    public interface SortCommodityValueGetter {

        /**
         * Get the value of the sort commodity for a given entity.
         *
         * @param entity The entity OID. {@link Long} to avoid autoboxing.
         * @return An optional containing the value of the sort commodity.
         */
        @Nonnull
        Optional<Float> getSortCommodityValue(@Nonnull final Long entity);

    }

    /**
     * Paginate a list of entity stats.
     *
     * @param entityIds The list of oids to paginate.
     * @param commodityFn Function to get the sort commodity value for an entity OID.
     * @param paginationParams The pagination parameters.
     * @return The {@link PaginatedStats} object that can be used to format a response to return
     *         to the client.
     */
    @Nonnull
    public PaginatedStats paginate(@Nonnull final Collection<Long> entityIds,
                                   @Nonnull final SortCommodityValueGetter commodityFn,
                                   @Nonnull final EntityStatsPaginationParams paginationParams) {
        final List<Long> entityIdsList = new ArrayList<>(entityIds);
        final Comparator<Long> ascendingComparator = (entity1, entity2) -> {
            final int comparisonResult;
            final Optional<Float> e1Commodity = commodityFn.getSortCommodityValue(entity1);
            final Optional<Float> e2Commodity = commodityFn.getSortCommodityValue(entity2);
            if (!e1Commodity.isPresent() && !e2Commodity.isPresent()) {
                comparisonResult = 0;
            } else if (!e1Commodity.isPresent()) {
                comparisonResult = -1;
            } else if (!e2Commodity.isPresent()) {
                comparisonResult = 1;
            } else {
                comparisonResult = Double.compare(e1Commodity.get(), e2Commodity.get());
            }

            // If the entities are equal, use OID as secondary sort field.
            return comparisonResult == 0 ?
                    Long.compare(entity1, entity2) : comparisonResult;
        };

        entityIdsList.sort(paginationParams.isAscending() ?
                ascendingComparator : ascendingComparator.reversed());
        final int skipCount = paginationParams.getNextCursor().map(Integer::parseInt).orElse(0);
        final int maxEndIdx = skipCount + paginationParams.getLimit();

        final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
        if (maxEndIdx < entityIds.size()) {
            paginationResponse.setNextCursor(Integer.toString(maxEndIdx));
        }
        paginationResponse.setTotalRecordCount(entityIds.size());

        final List<Long> nextStatsPage =
            entityIdsList.subList(skipCount, Math.min(maxEndIdx, entityIds.size()));
        return new PaginatedStats(nextStatsPage, paginationResponse.build());
    }

    /**
     * A wrapper around a page of oids and a {@link PaginationResponse} returned
     * by the {@link EntityStatsPaginator}.
     */
    public static class PaginatedStats {
        private final List<Long> nextPageIds;

        private final PaginationResponse paginationResponse;

        private PaginatedStats(@Nonnull final List<Long> nextPageIds,
                               @Nonnull final PaginationResponse paginationResponse) {
            this.nextPageIds = Objects.requireNonNull(nextPageIds);
            this.paginationResponse = Objects.requireNonNull(paginationResponse);
        }

        @Nonnull
        public List<Long> getNextPageIds() {
            return Collections.unmodifiableList(nextPageIds);
        }

        @Nonnull
        public PaginationResponse getPaginationResponse() {
            return paginationResponse;
        }
    }
}
