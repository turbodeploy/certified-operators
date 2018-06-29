package com.vmturbo.components.common.pagination;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;

public class EntityStatsPaginationParamsTest {

    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 100;
    private static final String DEFAULT_SORT_COMMODITY = "foo";

    private final EntityStatsPaginationParamsFactory factory =
        new DefaultEntityStatsPaginationParamsFactory(DEFAULT_LIMIT, MAX_LIMIT, DEFAULT_SORT_COMMODITY);

    @Test
    public void testFullyConfigured() {
        final PaginationParameters protoParams = PaginationParameters.newBuilder()
                .setAscending(false)
                .setOrderBy(OrderBy.newBuilder()
                        .setEntityStats(EntityStatsOrderBy.newBuilder()
                                .setStatName("foo")))
                .setCursor("boo!")
                .setLimit(20)
                .build();
        final EntityStatsPaginationParams params = factory.newPaginationParams(protoParams);
        assertThat(params.getLimit(), is(protoParams.getLimit()));
        assertThat(params.getNextCursor().get(), is(protoParams.getCursor()));
        assertFalse(params.isAscending());
        assertThat(params.getSortCommodity(), is(protoParams.getOrderBy().getEntityStats().getStatName()));
    }

    @Test
    public void testDefaultSortCommodityNoOrderBy() {
        final PaginationParameters protoParams = PaginationParameters.newBuilder()
                .build();
        final EntityStatsPaginationParams params = factory.newPaginationParams(protoParams);
        assertThat(params.getSortCommodity(), is(DEFAULT_SORT_COMMODITY));
    }

    @Test
    public void testDefaultSortCommodityNoEntityStatsOrderBy() {
        final PaginationParameters protoParams = PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                        .setAction(ActionOrderBy.ACTION_SEVERITY))
                .build();
        final EntityStatsPaginationParams params = factory.newPaginationParams(protoParams);
        assertThat(params.getSortCommodity(), is(DEFAULT_SORT_COMMODITY));
    }

    @Test
    public void testDefaultSortCommodityDefaultLimit() {
        final PaginationParameters protoParams = PaginationParameters.newBuilder()
                .build();
        final EntityStatsPaginationParams params = factory.newPaginationParams(protoParams);
        assertThat(params.getLimit(), is(DEFAULT_LIMIT));
    }

    @Test
    public void testDefaultSortCommodityMaxLimit() {
        final PaginationParameters protoParams = PaginationParameters.newBuilder()
                .setLimit(MAX_LIMIT + 100)
                .build();
        final EntityStatsPaginationParams params = factory.newPaginationParams(protoParams);
        assertThat(params.getLimit(), is(MAX_LIMIT));
    }
}
