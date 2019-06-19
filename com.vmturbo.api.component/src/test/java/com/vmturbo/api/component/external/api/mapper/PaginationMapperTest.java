package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.pagination.ActionOrderBy;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

public class PaginationMapperTest {

    private PaginationMapper paginationMapper = new PaginationMapper();

    @Test
    public void testCursor() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest("foo", null, true, null);
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getCursor(), is("foo"));
    }

    @Test
    public void testLimit() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, 10, true, null);
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getLimit(), is(10));
    }

    @Test
    public void testAscending() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, null, true, null);
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getAscending(), is(true));
    }

    @Test
    public void testActionOrderBySeverity() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, null, true, ActionOrderBy.SEVERITY.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getAction(), is(OrderBy.ActionOrderBy.ACTION_SEVERITY));
    }

    @Test
    public void testActionOrderByName() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, null, true, ActionOrderBy.NAME.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getAction(), is(OrderBy.ActionOrderBy.ACTION_NAME));
    }

    @Test
    public void testActionOrderByRiskCategory() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, null, true, ActionOrderBy.RISK_CATEGORY.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getAction(), is(OrderBy.ActionOrderBy.ACTION_RISK_CATEGORY));
    }

    @Test
    public void testActionOrderBySavings() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
                new ActionPaginationRequest(null, null, true, ActionOrderBy.SAVINGS.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getAction(), is(OrderBy.ActionOrderBy.ACTION_SAVINGS));
    }

    @Test
    public void testActionOrderByCreationDate() throws InvalidOperationException {
        final ActionPaginationRequest paginationRequest =
            new ActionPaginationRequest(null, null, true, ActionOrderBy.CREATION_DATE.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getAction(), is(OrderBy.ActionOrderBy.ACTION_RECOMMENDATION_TIME));
    }

    @Test
    public void testSearchOrderByName() throws InvalidOperationException {
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.NAME.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getSearch(), is(OrderBy.SearchOrderBy.ENTITY_NAME));
    }

    @Test
    public void testSearchOrderByUtilization() throws InvalidOperationException {
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.UTILIZATION.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getSearch(), is(OrderBy.SearchOrderBy.ENTITY_UTILIZATION));
    }

    @Test
    public void testSearchOrderBySeverity() throws InvalidOperationException {
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.SEVERITY.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getSearch(), is(OrderBy.SearchOrderBy.ENTITY_SEVERITY));
    }

    @Test
    public void testSearchOrderByCost() throws InvalidOperationException {
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getSearch(), is(OrderBy.SearchOrderBy.ENTITY_COST));
    }

    @Test
    public void testEntityStatOrderByStatName() throws InvalidOperationException {
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, null, true, "someStat");
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getEntityStats().getStatName(), is("someStat"));
    }
}
