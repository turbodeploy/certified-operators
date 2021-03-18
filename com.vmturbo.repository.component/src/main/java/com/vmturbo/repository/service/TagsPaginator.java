package com.vmturbo.repository.service;

import static java.util.Map.Entry.comparingByKey;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.ImmutablePaginatedResults;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

/**
 * A class to supply pagination for tags.
 */
public class TagsPaginator {
    private final Logger logger = LogManager.getLogger();

    private final int defaultPaginationLimit;

    private final int maxPaginationLimit;

    /**
     * Constructor for TagsPaginator.
     * @param defaultPaginationLimit defaultPaginationLimit
     * @param maxPaginationLimit maxPaginationLimit
     */
    public TagsPaginator(int defaultPaginationLimit, int maxPaginationLimit) {
        this.defaultPaginationLimit = defaultPaginationLimit;
        this.maxPaginationLimit = maxPaginationLimit;
    }

    @Nonnull
    PaginatedResults<Map.Entry<String, Set<String>>> paginate(@Nonnull final List<Map.Entry<String, Set<String>>> fullResults,
                                               @Nonnull final PaginationParameters paginationParameters) {
        PaginationProtoUtil.validatePaginationParams(paginationParameters);

        final long skipCount = StringUtils.isEmpty(paginationParameters.getCursor())
                ? 0 : Long.parseLong(paginationParameters.getCursor());

        final long limit;
        if (paginationParameters.hasLimit()) {
            if (paginationParameters.getLimit() > maxPaginationLimit) {
                logger.warn("Client-requested limit {} exceeds maximum!"
                        + " Lowering the limit to {}!", paginationParameters.getLimit(), maxPaginationLimit);
                limit = maxPaginationLimit;
            } else if (paginationParameters.getLimit() > 0) {
                limit = paginationParameters.getLimit();
            } else {
                throw new IllegalArgumentException("Illegal pagination limit: "
                        + paginationParameters.getLimit() + ". Must be be a positive integer");
            }
        } else {
            limit = defaultPaginationLimit;
        }

        // Currently only orderBy supported is by key. In case another ordering is introduced,
        // discriminate and apply between orderings at this point.
        Comparator<Map.Entry<String, Set<String>>> keyComparator = paginationParameters.getAscending()
                ? comparingByKey()
                : comparingByKey(Comparator.reverseOrder());
        // Set up a counter to get the size of the entire result set as we process the stream
        final AtomicInteger totalRecordCount = new AtomicInteger();

        // Perform the pagination
        final List<Map.Entry<String, Set<String>>> results = fullResults.stream()
                        .peek( e -> totalRecordCount.incrementAndGet())
                        .sorted(keyComparator)
                        .skip(skipCount)
                        .limit(limit + 1)
                        .collect(Collectors.toList());

        PaginationResponse.Builder respBuilder = PaginationResponse.newBuilder();
        respBuilder.setTotalRecordCount(totalRecordCount.get());

        // If following condition is true, it means that there are more results
        // to return and we must return a cursor
        if (results.size() > limit) {
            final String nextCursor = Long.toString( skipCount + limit);
            results.remove(results.size() - 1);
            respBuilder.setNextCursor(nextCursor);
        }

        return ImmutablePaginatedResults.<Map.Entry<String, Set<String>>>builder()
                .nextPageEntities(results)
                .paginationResponse(respBuilder.build())
                .build();

    }
}
