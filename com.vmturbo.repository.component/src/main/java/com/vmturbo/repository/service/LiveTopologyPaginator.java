package com.vmturbo.repository.service;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.ImmutablePaginatedResults;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

public class LiveTopologyPaginator {
    private final Logger logger = LogManager.getLogger();

    private final int defaultPaginationLimit;

    private final int maxPaginationLimit;

    private static final Comparator<RepoGraphEntity> NAME_COMPARATOR = (e1, e2) -> {
        final int nameCompareResults =
            String.CASE_INSENSITIVE_ORDER.compare(e1.getDisplayName(), e2.getDisplayName());
        if (nameCompareResults == 0) {
            // Compare IDs to get a stable order.
            return Long.compare(e1.getOid(), e2.getOid());
        } else {
            return nameCompareResults;
        }
    };

    public LiveTopologyPaginator(final int defaultPaginationLimit,
                                 final int maxPaginationLimit) {
        this.defaultPaginationLimit = defaultPaginationLimit;
        this.maxPaginationLimit = maxPaginationLimit;
    }

    @Nonnull
    PaginatedResults<RepoGraphEntity> paginate(@Nonnull final Stream<RepoGraphEntity> fullResults,
                                               @Nonnull final PaginationParameters paginationParameters) {
        PaginationProtoUtil.validatePaginationParams(paginationParameters);
        // Right now we only need to paginate by name.
        //
        // Pagination by utilization (price index) is implemented by:
        //   1) Searching entity OIDs that match the query (repo)
        //   2) Getting the next page sorted by price indices (history)
        //   3) Retrieving full entities (repo)
        // TODO (roman, May 23 2019): We can get the price indices from the LiveTopologyStore and
        // get rid of the history-backed implementation! It will be faster.
        //
        // Pagination by severity is implemented by:
        //   1) Searching entity OIDs that match the query (repo)
        //   2) Getting the next page sorted by severity (action orchestrator)
        //   3) Retrieving full entities (repo)
        if (!paginationParameters.getOrderBy().hasSearch()) {
            logger.info("No pagination info provided. Paginating by name.");
        } else if (paginationParameters.getOrderBy().getSearch() != SearchOrderBy.ENTITY_NAME) {
            logger.warn("Repository doesn't paginate by {}. Paginating by name.",
                paginationParameters.getOrderBy().getSearch());
        }

        final long skipCount = StringUtils.isEmpty(paginationParameters.getCursor()) ?
            0 :
            Long.parseLong(paginationParameters.getCursor());

        final long limit;
        if (paginationParameters.hasLimit()) {
            if (paginationParameters.getLimit() > maxPaginationLimit) {
                logger.warn("Client-requested limit {} exceeds maximum!" +
                    " Lowering the limit to {}!", paginationParameters.getLimit(), maxPaginationLimit);
                limit = maxPaginationLimit;
            } else if (paginationParameters.getLimit() > 0) {
                limit = paginationParameters.getLimit();
            } else {
                throw new IllegalArgumentException("Illegal pagination limit: " +
                    paginationParameters.getLimit() + ". Must be be a positive integer");
            }
        } else {
            limit = defaultPaginationLimit;
        }

        // Set up a counter to get the size of the entire result set as we process the stream
        final AtomicInteger totalRecordCount = new AtomicInteger();

        // Perform the pagination
        final List<RepoGraphEntity> results = fullResults
            // Count every record, pre-pagination to get the total record count
            .peek(actionView -> totalRecordCount.incrementAndGet())
            // Sort according to sort parameter
            .sorted(paginationParameters.getAscending() ? NAME_COMPARATOR : NAME_COMPARATOR.reversed())
            .skip(skipCount)
            // Add 1 so we know if there are more results or not.
            .limit(limit + 1)
            .collect(Collectors.toList());

        PaginationResponse.Builder respBuilder = PaginationResponse.newBuilder();
        respBuilder.setTotalRecordCount(totalRecordCount.get());
        if (results.size() > limit) {
            final String nextCursor = Long.toString(skipCount + limit);
            // Remove the last element to conform to limit boundaries.
            results.remove(results.size() - 1);
            respBuilder.setNextCursor(nextCursor);
        }

        return ImmutablePaginatedResults.<RepoGraphEntity>builder()
            .nextPageEntities(results)
            .paginationResponse(respBuilder.build())
            .build();
    }
}
