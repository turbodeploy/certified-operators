package com.vmturbo.repository.search;

import static com.vmturbo.repository.search.SearchTestUtil.makeStringFilter;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import javaslang.collection.List;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.AQLs;
import com.vmturbo.repository.search.AQLRepr.AQLPagination;

public class AQLReprTest {
    @Test
    public void testMultiplePropertiesToAQL() {
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());

        final Filter<PropertyFilterType> capacityFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("capacity")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GTE)
                                .setValue(3)
                                .build())
                        .build());

        final Filter<PropertyFilterType> nameFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeStringFilter(".*foo", false))
                        .build());

        final AQLRepr repr = new AQLRepr(List.of(typeFilter, capacityFilter, nameFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "capacity >= 3",
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
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());
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
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());
        final Filter<PropertyFilterType> capacityFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("capacity")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GTE)
                                .setValue(3)
                                .build())
                        .build());
        final AQLRepr repr = new AQLRepr(List.of(hopTraversal, typeFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..3 OUTBOUND",
                                 "LENGTH(p.edges) == 3",
                                 "REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "capacity >= 3");
    }

    @Test
    public void testCondTraversalWithOtherFilters() {
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());
        final Filter<TraversalFilterType> condTraversal =
                Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, typeFilter);
        final Filter<PropertyFilterType> capacityFilter =   Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("capacity")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GTE)
                                .setValue(3)
                                .build())
                        .build());
        final Filter<PropertyFilterType> nameFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeStringFilter(".*foo", false))
                        .build());

        final AQLRepr repr = new AQLRepr(List.of(condTraversal, nameFilter, capacityFilter));
        final AQL aql = repr.toAQL();

        assertThat(AQLs.getQuery(aql)).isNotEmpty()
                       .contains("1..100 INBOUND",
                                 "REGEX_TEST(service_entity.entityType, \"VirtualMachine\", false)",
                                 "REGEX_TEST(service_entity.displayName, \".*foo\", true)",
                                 "capacity >= 3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckFilters() {
        final Filter<TraversalFilterType> hopTraversal =
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 3);
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());

        new AQLRepr(List.of(typeFilter, hopTraversal));
    }

    @Test
    public void testAQLPagination() {
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeStringFilter("VirtualMachine", true))
                        .build());
        final Filter<PropertyFilterType> nameFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeStringFilter(".*foo", false))
                        .build());
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

    @Test
    public void testFilterByCommoditySoldVMemCapacity() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("commoditySoldList")
                .setListFilter(ListFilter.newBuilder()
                        .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("type")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("^VMem$")
                                                .setMatch(true)
                                                .build())
                                        .build())
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("capacity")
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.GT)
                                                .setValue(5000)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity, \"commoditySoldList\")\n" +
                "FILTER LENGTH(\n" +
                "FOR commoditySoldList IN service_entity.commoditySoldList\n" +
                "FILTER  REGEX_TEST(commoditySoldList.type, \"^VMem$\", true)\n" +
                "FILTER commoditySoldList.capacity > 5000\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }

    @Test
    public void testFilterByCommoditySoldVMemProviderOid() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("commoditySoldList")
                .setListFilter(ListFilter.newBuilder()
                        .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("type")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("^VMem$")
                                                .setMatch(true)
                                                .build())
                                        .build())
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("providerOid")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("1234")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity, \"commoditySoldList\")\n" +
                "FILTER LENGTH(\n" +
                "FOR commoditySoldList IN service_entity.commoditySoldList\n" +
                "FILTER  REGEX_TEST(commoditySoldList.type, \"^VMem$\", true)\n" +
                "FILTER  REGEX_TEST(commoditySoldList.providerOid, \"1234\", true)\n" +
                "RETURN 1\n" + ") > 0\n"
        );
    }

    @Test
    public void testFilterByVMGuestOsType() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("virtualMachineInfoRepoDTO")
                .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(PropertyFilter.newBuilder()
                                .setPropertyName("guestOsType")
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex("Linux")
                                        .setMatch(true)
                                        .build())
                                .build())
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER  REGEX_TEST(service_entity.virtualMachineInfoRepoDTO.guestOsType, \"Linux\", true)\n"
        );
    }

    @Test
    public void testFilterByDisplayName() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("abc")
                        .setMatch(true)
                        .setCaseSensitive(true)
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER  REGEX_TEST(service_entity.displayName, \"abc\", false)\n");
    }

    @Test
    public void testFilterByVMNumCpus() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("virtualMachineInfoRepoDTO")
                .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(PropertyFilter.newBuilder()
                                .setPropertyName("numCpus")
                                .setNumericFilter(NumericFilter.newBuilder()
                                        .setComparisonOperator(ComparisonOperator.GT)
                                        .setValue(2)
                                        .build())
                                .build())
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER service_entity.virtualMachineInfoRepoDTO.numCpus > 2\n"
        );
    }

    @Test
    public void testFilterByTargetId() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("targetIds")
                .setListFilter(ListFilter.newBuilder()
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.EQ)
                                .setValue(12345)
                                .build())
                        .build())
                .build();
        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER LENGTH(\n" +
                "FOR targetIds IN service_entity.targetIds\n" +
                "FILTER targetIds == 12345\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }

    @Test
    public void testFilterByProviderId() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("providers")
                .setListFilter(ListFilter.newBuilder()
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("23456")
                                .setMatch(true)
                                .setCaseSensitive(false)
                                .build())
                        .build())
                .build();
        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity, \"providers\")\n" +
                "FILTER LENGTH(\n" +
                "FOR providers IN service_entity.providers\n" +
                "FILTER  REGEX_TEST(providers, \"23456\", true)\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }

    @Test
    public void testFilterByTags() {
        final PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("Prop")
                .setMapFilter(MapFilter.newBuilder()
                        .setKey("AA")
                        .addValues("BB")
                        .addValues("CC")
                        .build()
                ).build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER service_entity.Prop[\"AA\"] ANY IN [\"BB\", \"CC\"]"
        );
    }

    @Test
    public void testFilterByNestedLists() {
        // find companies which has a ui team under development department and size is more than 5
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("departments")
                .setListFilter(ListFilter.newBuilder()
                        .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("departmentName")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("development")
                                                .setMatch(true)
                                                .build())
                                        .build())
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("teams")
                                        .setListFilter(ListFilter.newBuilder()
                                                .setObjectFilter(ObjectFilter.newBuilder()
                                                        .addFilters(PropertyFilter.newBuilder()
                                                                .setPropertyName("teamName")
                                                                .setStringFilter(StringFilter.newBuilder()
                                                                        .setStringPropertyRegex("ui")
                                                                        .setMatch(true)
                                                                        .build())
                                                                .build())
                                                        .addFilters(PropertyFilter.newBuilder()
                                                                .setPropertyName("size")
                                                                .setNumericFilter(NumericFilter.newBuilder()
                                                                        .setComparisonOperator(ComparisonOperator.GT)
                                                                        .setValue(5)
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity, \"departments\")\n" +
                "FILTER LENGTH(\n" +
                "FOR departments IN service_entity.departments\n" +
                "FILTER  REGEX_TEST(departments.departmentName, \"development\", true)\n" +
                "FILTER HAS(departments, \"teams\")\n" +
                "FILTER LENGTH(\n" +
                "FOR teams IN departments.teams\n" +
                "FILTER  REGEX_TEST(teams.teamName, \"ui\", true)\n" +
                "FILTER teams.size > 5\n" +
                "RETURN 1\n" +
                ") > 0\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }

    /**
     * Filter people by the criteria: his father has a friend whose name is Jack and Jack's
     * mother's age is > 40
     */
    @Test
    public void testFilterByComplexNestedListFilter() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("father")
                .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(PropertyFilter.newBuilder()
                                .setPropertyName("friends")
                                .setListFilter(ListFilter.newBuilder()
                                        .setObjectFilter(ObjectFilter.newBuilder()
                                                .addFilters(PropertyFilter.newBuilder()
                                                        .setPropertyName("name")
                                                        .setStringFilter(StringFilter.newBuilder()
                                                                .setStringPropertyRegex("Jack")
                                                                .setMatch(true)
                                                                .build())
                                                        .build())
                                                .addFilters(PropertyFilter.newBuilder()
                                                        .setPropertyName("mother")
                                                        .setObjectFilter(ObjectFilter.newBuilder()
                                                                .addFilters(PropertyFilter.newBuilder()
                                                                        .setPropertyName("age")
                                                                        .setNumericFilter(NumericFilter.newBuilder()
                                                                                .setComparisonOperator(ComparisonOperator.GT)
                                                                                .setValue(40)
                                                                                .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build()))
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity.father, \"friends\")\n" +
                "FILTER LENGTH(\n" +
                "FOR friends IN service_entity.father.friends\n" +
                "FILTER  REGEX_TEST(friends.name, \"Jack\", true)\n" +
                "FILTER friends.mother.age > 40\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }
}