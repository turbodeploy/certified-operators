package com.vmturbo.history.stats;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats.Builder;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;

/**
 * A utility class to do in-memory pagination of {@link EntityStats} protobuf objects.
 *
 * TODO (roman, June 21 2018): OM-35980 - This is a placeholder class -
 * we should be doing pagination at the database level.
 */
public class EntityStatsPaginator {

    private Optional<Float> getCommodity(EntityStats.Builder entityStats, String commodityName) {
        return entityStats.getStatSnapshotsList().stream()
                .findFirst()
                .flatMap(snapshot -> snapshot.getStatRecordsList().stream()
                        .filter(record -> record.getName().equals(commodityName))
                        .findFirst())
                .map(record -> record.getUsed().getAvg());
    }

    /**
     * Paginate a list of entity stats.
     *
     * @param entityStats The list of {@link EntityStats} objects to paginate.
     * @param paginationParams The pagination parameters.
     * @return The response to return to the client.
     */
    @Nonnull
    public GetEntityStatsResponse paginate(@Nonnull final List<EntityStats.Builder> entityStats,
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

        final GetEntityStatsResponse.Builder response = GetEntityStatsResponse.newBuilder()
                .setPaginationResponse(paginationResponse);
        entityStats.subList(skipCount, Math.min(maxEndIdx, entityStats.size()))
                .forEach(response::addEntityStats);
        return response.build();
    }
}
