package com.vmturbo.common.protobuf;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

/**
 * Utilities for dealing with pagination-related protobuf messages.
 */
public class PaginationProtoUtil {

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
}
