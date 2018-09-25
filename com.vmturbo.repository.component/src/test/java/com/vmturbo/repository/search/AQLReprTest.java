package com.vmturbo.repository.search;

import static com.vmturbo.repository.search.SearchTestUtil.makeStringFilter;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import javaslang.collection.List;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.AQLs;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

public class AQLReprTest {
    @Test
    public void testMultiplePropertiesToAQL() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", makeStringFilter(".*foo", false));

        final AQLRepr repr = new AQLRepr(List.of(typeFilter, capacityFilter, nameFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "capacity >= 3.01",
                                 "REGEX_TEST(service_entity.displayName, \".*foo\", true)");
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
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));
        final Filter<TraversalFilterType> condTraversal =
                Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, typeFilter);

        final AQLRepr repr = new AQLRepr(List.of(condTraversal));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..100 INBOUND",
                                 "REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)");
    }

    @Test
    public void testHopTraversalWithOtherFilters() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 3);
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);

        final AQLRepr repr = new AQLRepr(List.of(hopTraversal, typeFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..3 OUTBOUND",
                                 "LENGTH(p.edges) == 3",
                                 "REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "capacity >= 3.01");
    }

    @Test
    public void testCondTraversalWithOtherFilters() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));
        final Filter<TraversalFilterType> condTraversal =
                Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, typeFilter);
        final Filter<PropertyFilterType> capacityFilter =
                Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 3.01f);
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", makeStringFilter(".*foo", false));

        final AQLRepr repr = new AQLRepr(List.of(condTraversal, nameFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..100 INBOUND",
                                 "REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "REGEX_TEST(service_entity.displayName, \".*foo\", true)",
                                 "capacity >= 3.01");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckFilters() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 3);
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));

        new AQLRepr(List.of(typeFilter, hopTraversal));
    }

    @Test
    public void testAQLPagination() {
        final Filter<PropertyFilterType> typeFilter =
                Filter.stringPropertyFilter("entityType", makeStringFilter("VirtualMachine", true));
        final Filter<PropertyFilterType> nameFilter =
                Filter.stringPropertyFilter("displayName", makeStringFilter(".*foo", false));
        final AQLPagination aqlPagination =
                new AQLPagination(SearchOrderBy.ENTITY_NAME, true, 20, Optional.of(20L));
        final AQLRepr aqlRepr = new AQLRepr(List.of(typeFilter, nameFilter), aqlPagination);
        final AQL aql = aqlRepr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                .contains("REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                         "REGEX_TEST(service_entity.displayName, \".*foo\", true)",
                         "SORT service_entity.displayName ASC",
                         "LIMIT 20,20");
    }
}