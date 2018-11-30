package com.vmturbo.repository.search;

import static com.vmturbo.repository.search.SearchTestUtil.makeStringFilter;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

@SuppressWarnings("unchecked")
public class AQLReprFuserTest {
    @Test
    public void testFusing() {
        final Filter<PropertyFilterType> entityTypeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("DataCenter", true))
                        .build());

        final Filter<PropertyFilterType> stateFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(makeStringFilter("RUNNING", true))
                        .build());

        final Filter<TraversalFilterType> traversalHopFilter = Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1);

        final Filter<PropertyFilterType> displayNameFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeStringFilter("20", true))
                        .build());

        final Filter<PropertyFilterType> capacityFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("capacity")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GTE)
                                .setValue(2L)
                                .build())
                        .build());

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
        final Filter<PropertyFilterType> entityTypeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("DataCenter", true))
                        .build());

        final Filter<PropertyFilterType> stateFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(makeStringFilter("RUNNING", true))
                        .build());
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
        final Filter<PropertyFilterType> entityTypeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("DataCenter", true))
                        .build());

        final Filter<PropertyFilterType> stateFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(makeStringFilter("RUNNING", true))
                        .build());
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