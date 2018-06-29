package com.vmturbo.components.common.pagination;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats.Builder;

/**
 * A utility class to do in-memory pagination of {@link EntityStats} protobuf objects.
 */
public class EntityStatsPaginator {

    /**
     * Paginate a list of entity stats.
     *
     * @param entityStats The list of {@link EntityStats} objects to paginate.
     * @param paginationParams The pagination parameters.
     * @return The {@link PaginatedStats} object that can be used to format a response to return
     *         to the client.
     */
    @Nonnull
    public PaginatedStats paginate(@Nonnull final List<EntityStats.Builder> entityStats,
                                   @Nonnull final EntityStatsPaginationParams paginationParams) {
        final Comparator<Builder> ascendingComparator = (entity1, entity2) -> {
            final String sortCommodity = paginationParams.getSortCommodity();
            final int comparisonResult;
            final Optional<Float> e1Commodity = getCommodity(entity1, sortCommodity);
            final Optional<Float> e2Commodity = getCommodity(entity2, sortCommodity);
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
                    Long.compare(entity1.getOid(), entity2.getOid()) : comparisonResult;
        };

        entityStats.sort(paginationParams.isAscending() ?
                ascendingComparator : ascendingComparator.reversed());
        final int skipCount = paginationParams.getNextCursor().map(Integer::parseInt).orElse(0);
        final int maxEndIdx = skipCount + paginationParams.getLimit();

        final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
        if (maxEndIdx < entityStats.size()) {
            paginationResponse.setNextCursor(Integer.toString(maxEndIdx));
        }

        final List<EntityStats> nextStatsPage =
            entityStats.subList(skipCount, Math.min(maxEndIdx, entityStats.size())).stream()
                .map(EntityStats.Builder::build)
                .collect(Collectors.toList());;
        return new PaginatedStats(nextStatsPage, paginationResponse.build());
    }

    private Optional<Float> getCommodity(EntityStats.Builder entityStats, String commodityName) {
        return entityStats.getStatSnapshotsList().stream()
                .findFirst()
                .flatMap(snapshot -> snapshot.getStatRecordsList().stream()
                        .filter(record -> record.getName().equals(commodityName))
                        .findFirst())
                .map(record -> record.getUsed().getAvg());
    }

    /**
     * A wrapper around a page of {@link EntityStats} and a {@link PaginationResponse} returned
     * by the {@link EntityStatsPaginator}.
     */
    public static class PaginatedStats {
        private final List<EntityStats> nextStatsPage;

        private final PaginationResponse paginationResponse;

        private PaginatedStats(@Nonnull final List<EntityStats> nextStatsPage,
                               @Nonnull final PaginationResponse paginationResponse) {
            this.nextStatsPage = Objects.requireNonNull(nextStatsPage);
            this.paginationResponse = Objects.requireNonNull(paginationResponse);
        }

        @Nonnull
        public List<EntityStats> getStatsPage() {
            return Collections.unmodifiableList(nextStatsPage);
        }

        @Nonnull
        public PaginationResponse getPaginationResponse() {
            return paginationResponse;
        }
    }
}
