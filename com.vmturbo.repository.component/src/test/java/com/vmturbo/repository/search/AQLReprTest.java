package com.vmturbo.repository.search;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.AQLs;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

import javaslang.collection.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.registerCustomDateFormat;

import java.util.Optional;

public class AQLReprTest {
    @Test
    public void testMultiplePropertiesToAQL() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", Filter.StringOperator.REGEX, ".*foo");

        final AQLRepr repr = new AQLRepr(List.of(typeFilter, capacityFilter, nameFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("entityType =~ \"VirtualMachine\"",
                                 "capacity >= 3.01",
                                 "displayName =~ \".*foo\"");
    }

    @Test
    public void testHopTraversal() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 2);

        final AQLRepr repr = new AQLRepr(List.of(hopTraversal));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..2 OUTBOUND",
                                 "LENGTH(p.edges) == 2");
    }

    @Test
    public void testCondTraversal() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");
        final Filter<TraversalFilterType> condTraversal =
                Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, typeFilter);

        final AQLRepr repr = new AQLRepr(List.of(condTraversal));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..100 INBOUND",
                                 "entityType =~ \"VirtualMachine\"");
    }

    @Test
    public void testHopTraversalWithOtherFilters() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 3);
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);

        final AQLRepr repr = new AQLRepr(List.of(hopTraversal, typeFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..3 OUTBOUND",
                                 "LENGTH(p.edges) == 3",
                                 "entityType =~ \"VirtualMachine\"",
                                 "capacity >= 3.01");
    }

    @Test
    public void testCondTraversalWithOtherFilters() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");
        final Filter<TraversalFilterType> condTraversal =
                Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, typeFilter);
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", Filter.StringOperator.REGEX, ".*foo");

        final AQLRepr repr = new AQLRepr(List.of(condTraversal, nameFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..100 INBOUND",
                                 "entityType =~ \"VirtualMachine\"",
                                 "displayName =~ \".*foo\"",
                                 "capacity >= 3.01");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckFilters() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 3);
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");

        new AQLRepr(List.of(typeFilter, hopTraversal));
    }

    @Test
    public void testAQLPagination() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "VirtualMachine");
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", Filter.StringOperator.REGEX, ".*foo");
        final AQLPagination aqlPagination =
                new AQLPagination(SearchOrderBy.ENTITY_NAME, true, 20, Optional.of(20L));
        final AQLRepr aqlRepr = new AQLRepr(List.of(typeFilter, nameFilter), aqlPagination);
        final AQL aql = aqlRepr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                .contains("entityType =~ \"VirtualMachine\"",
                         "displayName =~ \".*foo\"",
                         "SORT service_entity.displayName ASC",
                         "LIMIT 20,20");
    }
}