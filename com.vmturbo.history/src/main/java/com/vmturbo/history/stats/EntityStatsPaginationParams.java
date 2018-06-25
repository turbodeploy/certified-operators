package com.vmturbo.history.stats;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * The pagination parameters to use for an entity stats query.
 * <p>
 * This is a utility class initialized from the {@link PaginationParameters} received from the
 * client.
 */
@Immutable
public class EntityStatsPaginationParams {

    private static final Logger logger = LogManager.getLogger();

    private final boolean ascending;

    private final String sortCommodity;

    private final int limit;

    private final Optional<String> nextCursor;

    public EntityStatsPaginationParams(final int defaultLimit,
                                       final int maxLimit,
                                       @Nonnull final String defaultSortCommodity,
                                       @Nonnull final PaginationParameters paginationParameters) {
        this.ascending = paginationParameters.getAscending();
        this.nextCursor = paginationParameters.hasCursor() ?
                Optional.of(paginationParameters.getCursor()) : Optional.empty();
        if (!paginationParameters.getOrderBy().hasEntityStats()) {
            logger.info("No sort commodity provided. Using default: {}", defaultSortCommodity);
            sortCommodity = defaultSortCommodity;
        } else {
            final EntityStatsOrderBy orderBy = paginationParameters.getOrderBy().getEntityStats();
            sortCommodity = orderBy.hasStatName() ? orderBy.getStatName() : defaultSortCommodity;
        }

        if (!paginationParameters.hasLimit()) {
            logger.info("No client limit provided. Using default pagination limit {}", defaultLimit);
            this.limit = defaultLimit;
        } else if (paginationParameters.getLimit() > maxLimit) {
            logger.warn("Client limit {} exceeds max limit {}.", paginationParameters.getLimit(), maxLimit);
            this.limit = maxLimit;
        } else {
            this.limit = paginationParameters.getLimit();
        }
    }

    @Nonnull
    public Optional<String> getNextCursor() {
        return nextCursor;
    }

    public int getLimit() {
        return limit;
    }

    @Nonnull
    public String getSortCommodity() {
        return sortCommodity;
    }

    public boolean isAscending() {
        return ascending;
    }
}
