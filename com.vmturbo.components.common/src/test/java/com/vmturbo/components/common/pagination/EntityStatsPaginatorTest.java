package com.vmturbo.components.common.pagination;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;

/**
 * Tests for the {@link EntityStatsPaginator}.
 */
public class EntityStatsPaginatorTest {

    private static final long E1_OID = 1;
    private static final long E2_OID = 2;
    private static final float E1_VAL = 1.0f;
    private static final float E2_VAL = 1.0f;
    private static final String COMMODITY = "love";

    private final EntityStatsPaginator paginator = new EntityStatsPaginator();

    private EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);

    private SortCommodityValueGetter sortCommodityValueGetter = (entityId) -> {
        if (entityId == E1_OID) {
            return Optional.of(E1_VAL);
        } else if (entityId == E2_OID) {
            return Optional.of(E2_VAL);
        } else {
            return Optional.empty();
        }
    };

    @Before
    public void setup() {
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.getNextCursor()).thenReturn(Optional.empty());
        when(params.getLimit()).thenReturn(10);
        when(params.isAscending()).thenReturn(false);
    }

    @Test
    public void testPaginateAscending() {
        when(params.isAscending()).thenReturn(true);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E2_OID, E1_OID), sortCommodityValueGetter, params);
        assertThat(response.getNextPageIds(), contains(E1_OID, E2_OID));
    }

    @Test
    public void testPaginateDescending() {
        when(params.isAscending()).thenReturn(false);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), sortCommodityValueGetter, params);
        assertThat(response.getNextPageIds(), contains(E2_OID, E1_OID));
    }

    @Test
    public void testPaginateLimitAndNextCursorSet() {
        when(params.getLimit()).thenReturn(1);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), sortCommodityValueGetter, params);
        assertThat(response.getNextPageIds(), contains(E2_OID));
        assertThat(response.getPaginationResponse().getNextCursor(), is("1"));
    }

    @Test
    public void testPaginateNextCursorInput() {
        when(params.getNextCursor()).thenReturn(Optional.of("1"));
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), sortCommodityValueGetter, params);
        assertThat(response.getNextPageIds(), contains(E1_OID));
    }

    @Test
    public void testPaginateEndOfResults() {
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), sortCommodityValueGetter, params);
        assertThat(response.getNextPageIds(), contains(E2_OID, E1_OID));
        assertThat(response.getPaginationResponse().getNextCursor(), is(""));
    }

    @Test
    public void testPaginateNoValue() {

        final SortCommodityValueGetter e1NoValueGetter = (id) ->
                id == E2_OID ? Optional.of(E2_VAL) : Optional.empty();
        final PaginatedStats response =
            paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), e1NoValueGetter, params);
        // Entity with no snapshot is considered smaller.
        assertThat(response.getNextPageIds(), contains(E2_OID, E1_OID));
    }

    @Test
    public void testPaginateStableSort() {
        final SortCommodityValueGetter allEmptyGetter = (id) -> Optional.empty();

        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1_OID, E2_OID), allEmptyGetter, params);
        // Entity with smaller ID is considered smaller.
        assertThat(response.getNextPageIds(), contains(E2_OID, E1_OID));
    }
}
