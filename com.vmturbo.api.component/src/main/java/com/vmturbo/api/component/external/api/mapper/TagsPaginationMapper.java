package com.vmturbo.api.component.external.api.mapper;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.pagination.TagPaginationRequest;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.TagOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * Responsible for mapping API {@link TagPaginationRequest}s to {@link PaginationParameters}.
 */
public class TagsPaginationMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert a {@link TagPaginationRequest} to a {@link PaginationParameters} protobuf message.
     *
     * @param request The {@link TagPaginationRequest}.
     * @return A {@link PaginationParameters} proto representing the request that can be sent
     *          to the appropriate gRPC service.
     */
    public PaginationParameters toProtoParams(@Nonnull final TagPaginationRequest request) {
        final PaginationParameters.Builder paramsBuilder = PaginationParameters.newBuilder()
            .setLimit(request.getLimit())
            .setAscending(request.isAscending());
        request.getCursor().ifPresent(paramsBuilder::setCursor);
        extractOrderBy(request).ifPresent(paramsBuilder::setOrderBy);
        return paramsBuilder.build();
    }

    private static Optional<OrderBy> extractOrderBy(@Nonnull final TagPaginationRequest request) {
        final com.vmturbo.api.pagination.TagOrderBy tagOrderBy =
           request.getOrderBy();
        switch (tagOrderBy) {
            case KEY:
                return Optional.of(OrderBy.newBuilder().setTag(TagOrderBy.KEY).build());
            default:
                logger.error("Unhandled tag sort order: {}", tagOrderBy);
        }
        return Optional.empty();
    }
}
