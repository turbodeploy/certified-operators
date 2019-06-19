package com.vmturbo.api.component.external.api.mapper;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.PaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * Responsible for mapping API {@link PaginationRequest}s to {@link PaginationParameters}.
 */
public class PaginationMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert a {@link PaginationRequest} to a {@link PaginationParameters} protobuf message.
     *
     * @param request The {@link PaginationRequest}.
     * @param <T> The specific subtype of {@link PaginationRequest}.
     * @return A {@link PaginationParameters} proto representing the request that can be sent to
     *         the appropriate gRPC service.
     */
    @Nonnull
    public <T extends PaginationRequest<?, ?, ?>> PaginationParameters
            toProtoParams(@Nonnull final T request) {
        final PaginationParameters.Builder paramsBuilder = PaginationParameters.newBuilder()
                .setLimit(request.getLimit())
                .setAscending(request.isAscending());
        request.getCursor().ifPresent(paramsBuilder::setCursor);
        extractOrderBy(request).ifPresent(paramsBuilder::setOrderBy);
        return paramsBuilder.build();
    }

    private static <T extends PaginationRequest<?, ?, ?>> Optional<OrderBy>
            extractOrderBy(@Nonnull final T request) {
        if (request instanceof ActionPaginationRequest) {
            final com.vmturbo.api.pagination.ActionOrderBy apiOrderBy =
                    ((ActionPaginationRequest)request).getOrderBy();
            switch (apiOrderBy) {
                case NAME:
                    return Optional.of(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_NAME).build());
                case SEVERITY:
                    return Optional.of(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_SEVERITY).build());
                case RISK_CATEGORY:
                    return Optional.of(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_RISK_CATEGORY).build());
                case SAVINGS:
                    return Optional.of(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_SAVINGS).build());
                case CREATION_DATE:
                    return Optional.of(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_RECOMMENDATION_TIME).build());
                default:
                    logger.error("Unhandled action sort order: {}", apiOrderBy);
            }
        } else if (request instanceof SearchPaginationRequest) {
            final com.vmturbo.api.pagination.SearchOrderBy apiOrderBy =
                    ((SearchPaginationRequest)request).getOrderBy();
            switch (apiOrderBy) {
                case NAME:
                    return Optional.of(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_NAME).build());
                case UTILIZATION:
                    return Optional.of(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_UTILIZATION).build());
                case SEVERITY:
                    return Optional.of(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_SEVERITY).build());
                case COST:
                    return Optional.of(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_COST).build());
                default:
                    logger.error("Unhandled search sort order: {}", apiOrderBy);
            }
        } else if (request instanceof EntityStatsPaginationRequest) {
            final EntityStatsPaginationRequest statsPaginationReq =
                    (EntityStatsPaginationRequest)request;
            return statsPaginationReq.getOrderByStat().map(stat ->
                    OrderBy.newBuilder()
                        .setEntityStats(EntityStatsOrderBy.newBuilder()
                            .setStatName(stat))
                        .build());
        }
        return Optional.empty();
    }
}
