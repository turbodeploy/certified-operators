package com.vmturbo.common.protobuf;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

/**
 * Utilities for dealing with pagination-related protobuf messages.
 */
public class PaginationProtoUtil {

    // The default pagination parameters.
    public static final PaginationParameters DEFAULT_PAGINATION = PaginationParameters.newBuilder()
        .setAscending(false)
        .setCursor("0")
        .setLimit(Integer.MAX_VALUE)
        .build();

    /**
     * Get the next cursor (if any) contained in a {@link PaginationResponse}.
     *
     * @param paginationResponse The {@link PaginationResponse}.
     * @return An optional containing the next cursor, or an empty optional if there are no more
     *         results.
     */
    @Nonnull
    public static Optional<String> getNextCursor(@Nonnull final PaginationResponse paginationResponse) {
        return StringUtils.isEmpty(paginationResponse.getNextCursor()) ?
                Optional.empty() : Optional.of(paginationResponse.getNextCursor());
    }

    /**
     * Validate the given pagination parameters.
     *
     * @param paginationParameters the pagination parameters
     */
    public static void validatePaginationParams(@Nonnull final PaginationParameters paginationParameters) {
        if (!StringUtils.isEmpty(paginationParameters.getCursor())) {
            try {
                final long cursor = Long.parseLong(paginationParameters.getCursor());
                if (cursor < 0) {
                    throw new IllegalArgumentException("Illegal cursor: " +
                        cursor + ". Must be be a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cursor " + paginationParameters.getCursor() +
                    " is invalid. Should be a number.");
            }
        }

        if (paginationParameters.getLimit() < 0) {
            throw new IllegalArgumentException("Illegal pagination limit: " +
                paginationParameters.getLimit() + ". Must be be a positive integer");
        }
    }

    /**
     * The results after pagination is applied.
     *
     * @param <E> Type Parameter of the entity
     */
    @Value.Immutable
    public interface PaginatedResults<E> {

        // the entities after pagination is applied
        List<E> nextPageEntities();

        // pagination response
        PaginationResponse paginationResponse();
    }
}
