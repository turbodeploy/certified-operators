package com.vmturbo.components.common.pagination;

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

    /**
     * Create pagination parameters to use for an entity stats query.
     *
     * @param defaultLimit the default limit to use, if no limit is specified in the
     *                     paginationParameters
     * @param maxLimit the maximum limit to use, overriding any higher limit specified in the
     *                 paginationParameters
     * @param defaultSortCommodity the default commodity to sort on, if no statName is set on the
     *                             orderBy field of the paginationParameters
     * @param paginationParameters the parameters for pagination, as requested by the client
     */
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

        // Determine what limit will be used for the number of returned entities.
        if (!paginationParameters.getEnforceLimit()) {
            // We allow internal calls to disable the limit in very specific circumstances.
            logger.debug("Client requested unbounded (non-paged) response. "
                + "Limits will not be enforced on this request.");
            this.limit = Integer.MAX_VALUE;
        } else if (!paginationParameters.hasLimit()) {
            // If the API client does not specify a limit, we use a default limit instead
            logger.info("No client limit provided. Using default pagination limit {}", defaultLimit);
            this.limit = defaultLimit;
        } else if (paginationParameters.getLimit() > maxLimit) {
            // If the API client's specified limit exceeds a pre-determined maximum for this service,
            // we use the maximum instead.
            logger.warn("Client limit {} exceeds max limit {}.", paginationParameters.getLimit(), maxLimit);
            this.limit = maxLimit;
        } else if (paginationParameters.getLimit() <= 0) {
            // Limit must have a positive integer value
            throw new IllegalArgumentException("Invalid limit specified: "
                + paginationParameters.getLimit() + ". Limit must be a positve integer.");
        } else {
            // Use the limit as specified by the API client
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

    @Override
    public String toString() {
        return String.format("[limit=%d; sort by %s/%s]",
            limit, sortCommodity, ascending ? "asc" : "desc");
    }
}
