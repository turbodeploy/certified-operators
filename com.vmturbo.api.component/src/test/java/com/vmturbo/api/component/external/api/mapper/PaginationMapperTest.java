package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.pagination.ActionOrderBy;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.GroupPaginationRequest;
import com.vmturbo.api.pagination.GroupPaginationRequest.GroupOrderBy;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.TargetOrderBy;
import com.vmturbo.api.pagination.TargetPaginationRequest;
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
    public void testEntityStatOrderByStatName() {
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, null, true, "someStat");
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getEntityStats().getStatName(), is("someStat"));
    }

    @Test
    public void testTargetOrderByDisplayName() {
        final TargetPaginationRequest paginationRequest =
                new TargetPaginationRequest(null, null, true, TargetOrderBy.DISPLAY_NAME);
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getTarget(), is(OrderBy.TargetOrderBy.TARGET_DISPLAY_NAME));
    }

    @Test
    public void testTargetOrderByValidationStatus() {
        final TargetPaginationRequest paginationRequest =
                new TargetPaginationRequest(null, null, true, TargetOrderBy.VALIDATION_STATUS);
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertThat(params.getOrderBy().getTarget(), is(OrderBy.TargetOrderBy.TARGET_VALIDATION_STATUS));
    }

    @Test
    public void testGroupOrderByName() throws InvalidOperationException {
        final GroupPaginationRequest paginationRequest =
                new GroupPaginationRequest(null, null, true, GroupOrderBy.NAME.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertTrue(params.getOrderBy().hasGroupSearch());
        assertThat(params.getOrderBy().getGroupSearch(), is(OrderBy.GroupOrderBy.GROUP_NAME));
    }

    @Test
    public void testGroupOrderBySeverity() throws InvalidOperationException {
        final GroupPaginationRequest paginationRequest =
                new GroupPaginationRequest(null, null, true, GroupOrderBy.SEVERITY.name());
        final PaginationParameters params = paginationMapper.toProtoParams(paginationRequest);
        assertTrue(params.getOrderBy().hasGroupSearch());
        assertThat(params.getOrderBy().getGroupSearch(), is(OrderBy.GroupOrderBy.GROUP_SEVERITY));
    }

    @Test
    public void testSearchToGroupPaginationRequestNoOrderBy() throws InvalidOperationException {
        final String cursor = "12";
        final int limit = 23;
        final boolean ascending = false;
        final SearchPaginationRequest searchRequest =
                new SearchPaginationRequest(cursor, limit, ascending, null);
        final GroupPaginationRequest groupRequest =
                paginationMapper.searchToGroupPaginationRequest(searchRequest);
        assertNotNull(groupRequest);
        assertTrue(groupRequest.getCursor().isPresent());
        assertEquals(cursor, groupRequest.getCursor().get());
        assertTrue(groupRequest.hasLimit());
        assertEquals(limit, groupRequest.getLimit());
        assertEquals(ascending, groupRequest.isAscending());
        assertEquals(GroupOrderBy.NAME, groupRequest.getOrderBy());
    }

    @Test
    public void testSearchToGroupPaginationRequestOrderByName() throws InvalidOperationException {
        final SearchPaginationRequest searchRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.NAME.name());
        final GroupPaginationRequest groupRequest =
                paginationMapper.searchToGroupPaginationRequest(searchRequest);
        assertNotNull(groupRequest);
        assertEquals(GroupOrderBy.NAME, groupRequest.getOrderBy());
    }

    @Test
    public void testSearchToGroupPaginationRequestOrderBySeverity() throws InvalidOperationException {
        final SearchPaginationRequest searchRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.SEVERITY.name());
        final GroupPaginationRequest groupRequest =
                paginationMapper.searchToGroupPaginationRequest(searchRequest);
        assertNotNull(groupRequest);
        assertEquals(GroupOrderBy.SEVERITY, groupRequest.getOrderBy());
    }

    @Test
    public void testSearchToGroupPaginationRequestOrderByCost() throws InvalidOperationException {
        final SearchPaginationRequest searchRequest =
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name());
        final GroupPaginationRequest groupRequest =
                paginationMapper.searchToGroupPaginationRequest(searchRequest);
        assertNotNull(groupRequest);
        assertEquals(GroupOrderBy.COST, groupRequest.getOrderBy());
    }

    @Test
    public void testGroupToSearchPaginationRequest() throws InvalidOperationException {
        final String cursor = "12";
        final int limit = 23;
        final boolean ascending = false;
        final GroupPaginationRequest groupRequest =
                new GroupPaginationRequest(cursor, limit, ascending, null);
        final SearchPaginationRequest searchRequest =
                paginationMapper.groupToSearchPaginationRequest(groupRequest);
        assertNotNull(searchRequest);
        assertTrue(searchRequest.getCursor().isPresent());
        assertEquals(cursor, searchRequest.getCursor().get());
        assertTrue(searchRequest.hasLimit());
        assertEquals(limit, searchRequest.getLimit());
        assertEquals(ascending, searchRequest.isAscending());
        assertEquals(SearchOrderBy.NAME, searchRequest.getOrderBy());
    }

    @Test
    public void testGroupToSearchPaginationRequestOrderByName() throws InvalidOperationException {
        final GroupPaginationRequest groupRequest =
                new GroupPaginationRequest(null, null, true, GroupOrderBy.NAME.name());
        final SearchPaginationRequest searchRequest =
                paginationMapper.groupToSearchPaginationRequest(groupRequest);
        assertNotNull(searchRequest);
        assertEquals(SearchOrderBy.NAME, searchRequest.getOrderBy());
    }

    @Test
    public void testGroupToSearchPaginationRequestOrderBySeverity() throws InvalidOperationException {
        final GroupPaginationRequest groupRequest =
                new GroupPaginationRequest(null, null, true, GroupOrderBy.SEVERITY.name());
        final SearchPaginationRequest searchRequest =
                paginationMapper.groupToSearchPaginationRequest(groupRequest);
        assertNotNull(searchRequest);
        assertEquals(SearchOrderBy.SEVERITY, searchRequest.getOrderBy());
    }

    @Test
    public void testGroupToSearchPaginationRequestOrderByCost() throws InvalidOperationException {
        final GroupPaginationRequest groupRequest =
                new GroupPaginationRequest(null, null, true, GroupOrderBy.COST.name());
        final SearchPaginationRequest searchRequest =
                paginationMapper.groupToSearchPaginationRequest(groupRequest);
        assertNotNull(searchRequest);
        assertEquals(SearchOrderBy.COST, searchRequest.getOrderBy());
    }
}
