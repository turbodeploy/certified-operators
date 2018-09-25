package com.vmturbo.repository.search;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.vmturbo.repository.search.SearchTestUtil.makeStringFilter;
import static org.assertj.core.api.Assertions.assertThat;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

@SuppressWarnings("unchecked")
public class AQLReprFuserTest {
    @Test
    public void testFusing() {
        final Filter<PropertyFilterType> entityTypeFilter = Filter.stringPropertyFilter("entityType", makeStringFilter("DataCenter", true));
        final Filter<PropertyFilterType> stateFilter = Filter.stringPropertyFilter("state", makeStringFilter("RUNNING", true));
        final Filter<TraversalFilterType> traversalHopFilter = Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1);
        final Filter<PropertyFilterType> displayNameFilter = Filter.stringPropertyFilter("displayName", makeStringFilter("20", true));
        final Filter<PropertyFilterType> capacityFilter = Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 2L);

        final AQLRepr repr1 = AQLRepr.fromFilters(entityTypeFilter);
        final AQLRepr repr2 = AQLRepr.fromFilters(stateFilter);
        final AQLRepr repr3 = AQLRepr.fromFilters(traversalHopFilter);
        final AQLRepr repr4 = AQLRepr.fromFilters(displayNameFilter);
        final AQLRepr repr5 = AQLRepr.fromFilters(capacityFilter);

        final ArrayList<AQLRepr> aqlReprs = Lists.newArrayList(repr1, repr2, repr3, repr4, repr5);

        final List<AQLRepr> fused = AQLReprFuser.fuse(aqlReprs, Optional.empty());

        assertThat(fused).hasSize(2)
                .containsExactly(AQLRepr.fromFilters(entityTypeFilter, stateFilter),
                                 AQLRepr.fromFilters(traversalHopFilter, displayNameFilter, capacityFilter));
    }

    /**
     * Sometimes fusing is not possible.
     */
    @Test
    public void noFusing() {
        final Filter<PropertyFilterType> entityTypeFilter = Filter.stringPropertyFilter("entityType", makeStringFilter("DataCenter", true));
        final Filter<PropertyFilterType> stateFilter = Filter.stringPropertyFilter("state", makeStringFilter("RUNNING", true));
        final Filter<TraversalFilterType> traversalHopFilter = Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1);
        final Filter<TraversalFilterType> traversalCondFilter = Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, entityTypeFilter);

        final AQLRepr repr1 = AQLRepr.fromFilters(stateFilter);
        final AQLRepr repr2 = AQLRepr.fromFilters(traversalHopFilter);
        final AQLRepr repr3 = AQLRepr.fromFilters(traversalCondFilter);

        final ArrayList<AQLRepr> aqlReprs = Lists.newArrayList(repr1, repr2, repr3);

        final List<AQLRepr> fused = AQLReprFuser.fuse(aqlReprs, Optional.empty());

        assertThat(fused).hasSize(3)
                .containsExactly(
                        AQLRepr.fromFilters(stateFilter),
                        AQLRepr.fromFilters(traversalHopFilter),
                        AQLRepr.fromFilters(traversalCondFilter));
    }

    @Test
    public void testFusingWithPagination() {
        final Filter<PropertyFilterType> entityTypeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("DataCenter", true));
        final Filter<PropertyFilterType> stateFilter =
                Filter.stringPropertyFilter("state", makeStringFilter("RUNNING", true));
        final Optional<PaginationParameters> paginationParameters = Optional.of(PaginationParameters.newBuilder()
                .setCursor("20")
                .setLimit(20)
                .setAscending(true)
                .setOrderBy(OrderBy.newBuilder()
                    .setSearch(SearchOrderBy.ENTITY_NAME))
            .build());
        final AQLPagination pagination =
                new AQLPagination(SearchOrderBy.ENTITY_NAME, true, 20, Optional.of(20L));
        final AQLRepr repr1 = AQLRepr.fromFilters(entityTypeFilter);
        final AQLRepr repr2 = AQLRepr.fromFilters(stateFilter);
        final ArrayList<AQLRepr> aqlReprs = Lists.newArrayList(repr1, repr2);
        final List<AQLRepr> fused = AQLReprFuser.fuse(aqlReprs, paginationParameters);
        assertThat(fused).hasSize(1)
                .containsExactly(
                        new AQLRepr(javaslang.collection.List.of(entityTypeFilter, stateFilter), pagination));
    }
}