package com.vmturbo.api.component.external.api.mapper;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.GroupPaginationRequest;
import com.vmturbo.api.pagination.PaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.TargetOrderBy;
import com.vmturbo.api.pagination.TargetPaginationRequest;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.GroupOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * Responsible for mapping API {@link PaginationRequest}s to {@link PaginationParameters}.
 */
public class PaginationMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Maps the api orderBy between general search and group specific queries.
     */
    private static final BiMap<com.vmturbo.api.pagination.SearchOrderBy, GroupPaginationRequest.GroupOrderBy>
            SEARCH_TO_GROUP_ORDER_BY =
            ImmutableBiMap.<com.vmturbo.api.pagination.SearchOrderBy, GroupPaginationRequest.GroupOrderBy>builder()
                    .put(com.vmturbo.api.pagination.SearchOrderBy.NAME,
                            GroupPaginationRequest.GroupOrderBy.NAME)
                    .put(com.vmturbo.api.pagination.SearchOrderBy.SEVERITY,
                            GroupPaginationRequest.GroupOrderBy.SEVERITY)
                    .put(com.vmturbo.api.pagination.SearchOrderBy.COST,
                            GroupPaginationRequest.GroupOrderBy.COST)
            .build();

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
                case DISRUPTIVENESS:
                    return Optional.of(OrderBy.newBuilder()
                            .setAction(ActionOrderBy.ACTION_DISRUPTIVENESS).build());
                case REVERSIBILITY:
                    return Optional.of(OrderBy.newBuilder()
                            .setAction(ActionOrderBy.ACTION_REVERSIBILITY).build());
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
        } else if (request instanceof TargetPaginationRequest) {
            final TargetOrderBy orderBy =
                ((TargetPaginationRequest)request).getOrderBy();
            if (orderBy == null) {
                return Optional.empty();
            }

            switch (orderBy) {
                case DISPLAY_NAME:
                    return Optional.of(OrderBy.newBuilder()
                        .setTarget(OrderBy.TargetOrderBy.TARGET_DISPLAY_NAME).build());
                case VALIDATION_STATUS:
                    return Optional.of(OrderBy.newBuilder()
                        .setTarget(OrderBy.TargetOrderBy.TARGET_VALIDATION_STATUS).build());
                default:
                    logger.error("Cannot order targets by: {}", orderBy);
            }
        } else if (request instanceof GroupPaginationRequest) {
            final GroupPaginationRequest.GroupOrderBy apiOrderBy =
                    ((GroupPaginationRequest)request).getOrderBy();
            switch (apiOrderBy) {
                case NAME:
                    return Optional.of(OrderBy.newBuilder()
                            .setGroupSearch(GroupOrderBy.GROUP_NAME).build());
                case SEVERITY:
                    return Optional.of(OrderBy.newBuilder()
                            .setGroupSearch(GroupOrderBy.GROUP_SEVERITY).build());
                // Group component currently doesn't support orderBy COST, so it's not added here.
                default:
                    logger.error("Unhandled group sort order: {} for request with cursor: \"{}\""
                            + ", limit: {}{}, ascending: {}", apiOrderBy,
                            request.getCursor().orElse(""), request.getLimit(),
                            request.hasLimit() ? "" : " (default)", request.isAscending());
            }
        }
        return Optional.empty();
    }

    /**
     * Converts a {@link SearchPaginationRequest} to a {@link GroupPaginationRequest}.
     *
     * @param searchPaginationRequest the {@link SearchPaginationRequest} to convert.
     * @return the new {@link GroupPaginationRequest}.
     * @throws InvalidOperationException if initialization of {@link GroupPaginationRequest} fails.
     */
    public GroupPaginationRequest searchToGroupPaginationRequest(
            @Nonnull SearchPaginationRequest searchPaginationRequest) throws InvalidOperationException {
        GroupPaginationRequest.GroupOrderBy orderBy =
                SEARCH_TO_GROUP_ORDER_BY.get(searchPaginationRequest.getOrderBy());
        return new GroupPaginationRequest(searchPaginationRequest.getCursor().orElse(null),
                searchPaginationRequest.getLimit(),
                searchPaginationRequest.isAscending(),
                orderBy == null ? null : orderBy.name());
    }

    /**
     * Converts a {@link GroupPaginationRequest} to a {@link SearchPaginationRequest}.
     *
     * @param groupPaginationRequest the {@link GroupPaginationRequest} to convert.
     * @return the new {@link SearchPaginationRequest}.
     * @throws InvalidOperationException if initialization of {@link SearchPaginationRequest} fails.
     */
    public SearchPaginationRequest groupToSearchPaginationRequest(
            @Nonnull GroupPaginationRequest groupPaginationRequest) throws InvalidOperationException {
        com.vmturbo.api.pagination.SearchOrderBy orderBy =
                SEARCH_TO_GROUP_ORDER_BY.inverse().get(groupPaginationRequest.getOrderBy());
        return new SearchPaginationRequest(groupPaginationRequest.getCursor().orElse(null),
                groupPaginationRequest.getLimit(),
                groupPaginationRequest.isAscending(),
                orderBy == null ? null : orderBy.name());
    }
}
