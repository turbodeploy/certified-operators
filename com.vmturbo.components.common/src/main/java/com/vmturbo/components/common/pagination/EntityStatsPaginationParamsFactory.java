package com.vmturbo.components.common.pagination;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * A factory for {@link EntityStatsPaginationParams}, for unit tests and to help inject
 * configurable limits.
 */
public interface EntityStatsPaginationParamsFactory {

    @Nonnull
    EntityStatsPaginationParams newPaginationParams(@Nonnull final PaginationParameters params);

    /**
     * The default implementation of {@link EntityStatsPaginationParamsFactory}, for use in
     * production.
     */
    class DefaultEntityStatsPaginationParamsFactory implements EntityStatsPaginationParamsFactory {

        private final int defaultLimit;

        private final int maxLimit;

        private final String defaultSortCommodity;

        public DefaultEntityStatsPaginationParamsFactory(final int defaultLimit,
                                                  final int maxLimit,
                                                  @Nonnull final String defaultSortCommodity) {
            Preconditions.checkArgument(defaultLimit <= maxLimit);
            this.defaultLimit = defaultLimit;
            this.maxLimit = maxLimit;
            this.defaultSortCommodity = defaultSortCommodity;
        }

        @Nonnull
        @Override
        public EntityStatsPaginationParams newPaginationParams(@Nonnull final PaginationParameters params) {
            return new EntityStatsPaginationParams(defaultLimit, maxLimit, defaultSortCommodity, params);
        }
    }
}
