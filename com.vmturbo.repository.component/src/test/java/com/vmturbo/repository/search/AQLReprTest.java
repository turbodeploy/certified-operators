package com.vmturbo.repository.search;

import static com.vmturbo.repository.search.SearchTestUtil.makeRegexStringFilter;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

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
    private static final String MAP_PROPERTY_NAME = "tags";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_REGEX = "^.*$";
    private static final String MAP_FULL_REFERENCE_TO_VALUES =
            "service_entity." + MAP_PROPERTY_NAME + "[\"" + MAP_KEY_NAME + "\"]";

    private static final String ENTITY_TYPE = "entityType";
    private static final String DISPLAY_NAME = "displayName";
    private static final String VIRTUAL_MACHINE = "VirtualMachine";

    @Test
    public void testMultiplePropertiesToAQL() {
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
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
                        .setPropertyName(DISPLAY_NAME)
                        .setStringFilter(makeRegexStringFilter(".*foo", false))
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
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
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
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
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
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
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
                        .setPropertyName(DISPLAY_NAME)
                        .setStringFilter(makeRegexStringFilter(".*foo", false))
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
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
                        .build());

        new AQLRepr(List.of(typeFilter, hopTraversal));
    }

    @Test
    public void testAQLPagination() {
        final Filter<PropertyFilterType> typeFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(makeRegexStringFilter(VIRTUAL_MACHINE, true))
                        .build());
        final Filter<PropertyFilterType> nameFilter = Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName(DISPLAY_NAME)
                        .setStringFilter(makeRegexStringFilter(".*foo", false))
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
                                        .setStringFilter(
                                            SearchTestUtil.makeRegexStringFilter("^VMem$", false))
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
                                        .setStringFilter(
                                               SearchTestUtil.makeRegexStringFilter("^VMem$", false))
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
                                .setStringFilter(
                                        SearchTestUtil.makeListStringFilter(
                                                Collections.singletonList("Linux"), true))
                                .build())
                        .build())
                .build();

        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER service_entity.virtualMachineInfoRepoDTO.guestOsType IN [\"Linux\"]\n"
        );
    }

    @Test
    public void testFilterByDisplayName() {
        PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName(DISPLAY_NAME)
                .setStringFilter(
                        SearchTestUtil.makeRegexStringFilter("abc", true))
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
                        .setStringFilter(
                                SearchTestUtil.makeListStringFilter(
                                        ImmutableList.of("23456", "78901"), false))
                        .build())
                .build();
        String actualAql = SearchDTOConverter.convertPropertyFilter(filter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER HAS(service_entity, \"providers\")\n" +
                "FILTER LENGTH(\n" +
                "FOR providers IN service_entity.providers\n" +
                "FILTER LOWER(providers) IN [LOWER(\"23456\"), LOWER(\"78901\")]\n" +
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
                                        .setStringFilter(
                                                SearchTestUtil.makeListStringFilter(
                                                        Collections.singletonList("development"), false))
                                        .build())
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("teams")
                                        .setListFilter(ListFilter.newBuilder()
                                                .setObjectFilter(ObjectFilter.newBuilder()
                                                        .addFilters(PropertyFilter.newBuilder()
                                                                .setPropertyName("teamName")
                                                                .setStringFilter(
                                                                        SearchTestUtil.makeListStringFilter(
                                                                            Collections.singletonList("ui"),
                                                                            false))
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
                "FILTER LOWER(departments.departmentName) IN [LOWER(\"development\")]\n" +
                "FILTER HAS(departments, \"teams\")\n" +
                "FILTER LENGTH(\n" +
                "FOR teams IN departments.teams\n" +
                "FILTER LOWER(teams.teamName) IN [LOWER(\"ui\")]\n" +
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
                                                        .setStringFilter(
                                                                SearchTestUtil.makeListStringFilter(
                                                                    Collections.singletonList("Jack"),
                                                                    false)))
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
                "FILTER LOWER(friends.name) IN [LOWER(\"Jack\")]\n" +
                "FILTER friends.mother.age > 40\n" +
                "RETURN 1\n" +
                ") > 0\n"
        );
    }

    /**
     * Translation of negative list string filter.
     */
    @Test
    public void testCaseSensitiveNegativeListStringFilter() {
        final PropertyFilter propertyFilter =
                PropertyFilter.newBuilder()
                    .setPropertyName("property")
                    .setStringFilter(
                            StringFilter.newBuilder()
                                .addOptions("xyz")
                                .addOptions("abc")
                                .setPositiveMatch(false)
                                .setCaseSensitive(true)
                                .build())
                    .build();
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo("FILTER service_entity.property NOT IN [\"xyz\", \"abc\"]\n");
    }

    /**
     * Translation of negative regex string filter.
     */
    @Test
    public void testNegativeRegexStringFilter() {
        final PropertyFilter propertyFilter =
                PropertyFilter.newBuilder()
                        .setPropertyName("property")
                        .setStringFilter(
                                StringFilter.newBuilder()
                                        .setStringPropertyRegex("^a.*$")
                                        .setPositiveMatch(false)
                                        .build())
                        .build();
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo("FILTER NOT REGEX_TEST(service_entity.property, \"^a.*$\", true)\n");
    }

    /**
     * Translation of empty map filter.
     */
    @Test
    public void testEmptyMapFilter() {
        final PropertyFilter propertyFilter =
            wrapMapFilterIntoPropertyFilter(mapFilterBuilderWithKey().build());
        final String actualAql =
            SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
            "FILTER \"" + MAP_KEY_NAME + "\" IN ATTRIBUTES(service_entity." + MAP_PROPERTY_NAME + ")");
    }

    /**
     * Translation of a map filter for normal maps that
     * pattern-matches against a regular expression
     * and negates the result.
     */
    @Test
    public void testRegexMapFilter() {
        final PropertyFilter propertyFilter =
                wrapMapFilterIntoPropertyFilter(
                        mapFilterBuilderWithKey()
                                .setRegex(MAP_REGEX)
                                .setIsMultimap(false)
                                .setPositiveMatch(false)
                                .build()
                );
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER !(" + MAP_FULL_REFERENCE_TO_VALUES + " =~ \"" + MAP_REGEX + "\")");
    }

    /**
     * Translation of a map filter for multimaps that
     * pattern-matches against a regular expression.
     */
    @Test
    public void testRegexMultiMapFilter() {
        final PropertyFilter propertyFilter =
                wrapMapFilterIntoPropertyFilter(
                        mapFilterBuilderWithKey().setRegex(MAP_REGEX).setIsMultimap(true).build());
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
            "FILTER LENGTH(FILTER " + MAP_FULL_REFERENCE_TO_VALUES + " != null FOR tagValue IN " +
            MAP_FULL_REFERENCE_TO_VALUES + " FILTER tagValue =~ \"" + MAP_REGEX + "\" RETURN tagValue)>0");
    }

    /**
     * Translation of a map filter for normal maps that
     * checks for an exact string match against a list of values
     */
    @Test
    public void testExactMapFilter() {
        final PropertyFilter propertyFilter =
                wrapMapFilterIntoPropertyFilter(
                        mapFilterBuilderWithKey()
                                .addValues("a")
                                .addValues("b")
                                .setIsMultimap(false)
                                .build());
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER " + MAP_FULL_REFERENCE_TO_VALUES + " IN [\"a\", \"b\"]");
    }

    /**
     * Translation of a map filter for multimaps that
     * checks for an exact string match against a list of values
     */
    @Test
    public void testExactMultiMapFilter() {
        final PropertyFilter propertyFilter =
                wrapMapFilterIntoPropertyFilter(
                        mapFilterBuilderWithKey()
                                .addValues("a")
                                .addValues("b")
                                .setIsMultimap(true)
                                .build());
        final String actualAql =
                SearchDTOConverter.convertPropertyFilter(propertyFilter).get().toAQLString();
        assertThat(actualAql).isEqualTo(
                "FILTER " + MAP_FULL_REFERENCE_TO_VALUES + " ANY IN [\"a\", \"b\"]");
    }

    private MapFilter.Builder mapFilterBuilderWithKey() {
        return MapFilter.newBuilder().setKey(MAP_KEY_NAME);
    }

    private PropertyFilter wrapMapFilterIntoPropertyFilter(MapFilter mapFilter) {
        return
            PropertyFilter.newBuilder().setPropertyName(MAP_PROPERTY_NAME).setMapFilter(mapFilter).build();
    }
}