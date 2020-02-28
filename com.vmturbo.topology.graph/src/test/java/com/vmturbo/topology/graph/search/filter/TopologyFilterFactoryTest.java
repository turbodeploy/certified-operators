package com.vmturbo.topology.graph.search.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToPropertyFilter;

public class TopologyFilterFactoryTest {
    private final TopologyFilterFactory<TestGraphEntity> filterFactory = new TopologyFilterFactory<>();
    private final TopologyGraph<TestGraphEntity> graph = mock(TopologyGraph.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSearchFilterForOid() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setNumericFilter(NumericFilter.newBuilder()
                    .setValue(1234L)
                    .setComparisonOperator(ComparisonOperator.EQ)
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForOidRegex() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions("1234")
                    .addOptions("4321")))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(4321L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
        assertFalse(propertyFilter.test(entity3));
    }

    @Test
    public void testSearchFilterForOidOption() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("1234")))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForEntityType() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setNumericFilter(NumericFilter.newBuilder()
                        .setValue(EntityType.VIRTUAL_MACHINE.getNumber())
                        .setComparisonOperator(ComparisonOperator.EQ)
                ))
            .build();

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.PHYSICAL_MACHINE).build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForDisplayNameEquality() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("foo")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForDisplayCaseSensitive() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("MyEntity")
                                .setCaseSensitive(true)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setName("MyEntity")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setName("myentity")
            .build();


        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForDisplayCaseInsensitive() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("myentity")
                                .setCaseSensitive(false)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setName("MyEntity")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setName("myentity")
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForDisplayNameRegex() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("f.*")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();


        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForDisplayNameRegexNegated() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setPositiveMatch(false)
                        .setStringPropertyRegex("f.*")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();


        assertFalse(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForEntityStateOptions() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("state")
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions(UIEntityState.ACTIVE.apiStr())
                    .addOptions(UIEntityState.IDLE.apiStr())
                    .setPositiveMatch(true)))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(4321L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.FAILOVER)
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
        assertFalse(propertyFilter.test(entity3));
    }

    @Test
    public void testSearchFilterForEntityStateMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("ACTIVE")
                                .setPositiveMatch(true)
                                .setStringPropertyRegex("ACTIVE")))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForEntityStateNoMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("ACTIVE")
                                // Match set to false, so "ACTIVE" entities shouldn't match.
                                .setPositiveMatch(false)
                                .setStringPropertyRegex("ACTIVE")))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(5678L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.FAILOVER)
            .build();

        assertFalse(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
        assertTrue(propertyFilter.test(entity3));
    }

    @Test
    public void testNumericSearchFilterForEntityState() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setValue(EntityState.POWERED_ON_VALUE)
                                .setComparisonOperator(ComparisonOperator.EQ)
                                .build())
                        .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
                .setState(EntityState.POWERED_ON)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
                .setState(EntityState.POWERED_OFF)
                .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    /**
     * Test environment type matching.
     */
    @Test
    public void testSearchFilterForEnvironmentTypeMatch() {
        testSearchFilterForEnvironmentTypeMatch("CLOUD", true, true, false, false);
        testSearchFilterForEnvironmentTypeMatch("ONPREM", true, false, true, false);
        testSearchFilterForEnvironmentTypeMatch("HYBRID", true, true, true, true);
        testSearchFilterForEnvironmentTypeMatch("UNKNOWN", false, false, false, true);
    }

    private void testSearchFilterForEnvironmentTypeMatch(
            @Nonnull String envType, boolean hybridShouldMatch, boolean cloudShouldMatch,
            boolean onpremShouldMatch, boolean unknownShouldMatch) {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("environmentType")
                        .setStringFilter(StringFilter.newBuilder()
                                .setPositiveMatch(true)
                                .setStringPropertyRegex(envType)))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, UIEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(3456L, UIEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.HYBRID)
                .build();
        final TestGraphEntity entity4 = TestGraphEntity.newBuilder(4567L, UIEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.UNKNOWN_ENV)
                .build();

        assertEquals(cloudShouldMatch, propertyFilter.test(entity1));
        assertEquals(onpremShouldMatch, propertyFilter.test(entity2));
        assertEquals(hybridShouldMatch, propertyFilter.test(entity3));
        assertEquals(unknownShouldMatch, propertyFilter.test(entity4));
    }

    @Test
    public void testSearchFilterForStringEntityTypeNumericMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setNumericFilter(NumericFilter.newBuilder()
                    .setValue(UIEntityType.VIRTUAL_MACHINE.typeNumber())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.PHYSICAL_MACHINE)
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForStringEntityTypeNumericNoMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setNumericFilter(NumericFilter.newBuilder()
                    .setComparisonOperator(ComparisonOperator.NE)
                    .setValue(UIEntityType.VIRTUAL_MACHINE.typeNumber())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.PHYSICAL_MACHINE)
            .build();

        assertFalse(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForStringEntityTypeRegexMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(UIEntityType.VIRTUAL_MACHINE.apiStr())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.PHYSICAL_MACHINE)
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForEntityTypeRegexNoMatch() {
        SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(UIEntityType.VIRTUAL_MACHINE.apiStr())
                    .setPositiveMatch(false)))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.PHYSICAL_MACHINE)
            .build();

        assertFalse(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
    }

    @Test
    public void testSearchFilterForEntityTypeOptions() {
        SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions(UIEntityType.VIRTUAL_MACHINE.apiStr())
                    .addOptions(UIEntityType.PHYSICAL_MACHINE.apiStr())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.PHYSICAL_MACHINE)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(1234L, UIEntityType.STORAGE)
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertTrue(propertyFilter.test(entity2));
        assertFalse(propertyFilter.test(entity3));
    }

    @Test
    public void testNumericDisplayNameIllegal() {
        expectedException.expect(IllegalArgumentException.class);

        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("displayName")
                    .setNumericFilter(NumericFilter.getDefaultInstance())
            ).build();
        filterFactory.filterFor(searchCriteria);
    }

    @Test
    public void testStringOid() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("oid")
                    .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("1234")
                    )
            ).build();

        final TestGraphEntity entity = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();;

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertThat(
            filter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testStringOidNegation() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("oid")
                    .setStringFilter(StringFilter.newBuilder()
                            .setPositiveMatch(false)
                            .setStringPropertyRegex("1234")
                    )
            ).build();

        final TestGraphEntity entity = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();;

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter.apply(Stream.of(entity), graph).collect(Collectors.toList()).isEmpty());
    }

    private class NumericFilterTest {
        private final ComparisonOperator operator;
        private final Collection<TestGraphEntity> expectedMatches;

        public NumericFilterTest(final ComparisonOperator operator,
                                 @Nonnull final Collection<TestGraphEntity> expectedMatches) {
            this.operator = operator;
            this.expectedMatches = expectedMatches;
        }

        public ComparisonOperator getOperator() {
            return operator;
        }

        public Collection<TestGraphEntity> getExpectedMatches() {
            return expectedMatches;
        }
    }

    @Test
    public void testIntNumericComparisons() {
        final TestGraphEntity entity1 = mock(TestGraphEntity.class);
        final TestGraphEntity entity2 = mock(TestGraphEntity.class);
        final TestGraphEntity entity3 = mock(TestGraphEntity.class);

        final List<TestGraphEntity> testCases = Arrays.asList(entity1, entity2, entity3);
        when(entity1.getEntityType()).thenReturn(1);
        when(entity2.getEntityType()).thenReturn(2);
        when(entity3.getEntityType()).thenReturn(3);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("entityType");

        // Execute the comparison on entities with entityType 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(entity2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(entity1, entity3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(entity3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(entity2, entity3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(entity1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(entity1, entity2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);

            assertThat(
                "Test for: " + testCase.getOperator().toString(),
                filter.apply(testCases.stream(), graph).collect(Collectors.toList()),
                containsInAnyOrder(testCase.getExpectedMatches().toArray())
            );
        });
    }

    @Test
    public void testLongNumericComparisons() {
        final TestGraphEntity entity1 = mock(TestGraphEntity.class);
        final TestGraphEntity entity2 = mock(TestGraphEntity.class);
        final TestGraphEntity entity3 = mock(TestGraphEntity.class);

        final List<TestGraphEntity> testCases = Arrays.asList(entity1, entity2, entity3);
        when(entity1.getOid()).thenReturn(1L);
        when(entity2.getOid()).thenReturn(2L);
        when(entity3.getOid()).thenReturn(3L);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2L);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("oid");

        // Execute the comparison on entities with oid 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(entity2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(entity1, entity3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(entity3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(entity2, entity3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(entity1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(entity1, entity2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);

            assertThat(
                "Test for: " + testCase.getOperator().toString(),
                filter.apply(testCases.stream(), graph).collect(Collectors.toList()),
                containsInAnyOrder(testCase.getExpectedMatches().toArray())
            );
        });
    }

    @Test
    public void testTraversalToDepthFilter() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setTraversalFilter(Search.TraversalFilter.newBuilder()
                .setTraversalDirection(TraversalDirection.CONSUMES)
                .setStoppingCondition(StoppingCondition.newBuilder().setNumberHops(3)))
            .build();

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof TraversalToDepthFilter);
        final TraversalToDepthFilter depthFilter = (TraversalToDepthFilter)filter;
        assertEquals(TraversalDirection.CONSUMES, depthFilter.getTraversalDirection());
    }

    @Test
    public void testTraversalToPropertyFilter() {
        final Search.PropertyFilter stoppingFilter = Search.PropertyFilter.newBuilder()
            .setPropertyName("displayName")
            .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("foo"))
            .build();

        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setTraversalFilter(Search.TraversalFilter.newBuilder()
                .setTraversalDirection(TraversalDirection.PRODUCES)
                .setStoppingCondition(StoppingCondition.newBuilder()
                    .setStoppingPropertyFilter(stoppingFilter)))
            .build();

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof TraversalToPropertyFilter);
        final TraversalToPropertyFilter propertyFilter = (TraversalToPropertyFilter)filter;
        assertEquals(TraversalDirection.PRODUCES, propertyFilter.getTraversalDirection());
    }

    @Test
    public void testRegexContainsMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("in-group")
                ).build());


        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testRegexAnchorMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("^entity")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testRegexWildcardMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex(".*group.+")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testStringStateActiveOption() {
        final PropertyFilter<TestGraphEntity> stringFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("state")
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("")
                    .addOptions("ACTIVE")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();

        assertThat(
            stringFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    /**
     * Checks various cases of the tags-related filter.
     */
    @Test
    public void testMapFilter() {
        final Search.PropertyFilter positiveSearchFilter = Search.PropertyFilter
            .newBuilder()
            .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
            .setMapFilter(
                MapFilter
                    .newBuilder()
                    .setKey("KEY")
                    .addValues("VALUE1")
                    .addValues("VALUE2")
            ).build();

        final Search.PropertyFilter negativeSearchFilter;
        {
            final Search.PropertyFilter.Builder negativeSearchFilterBldr = positiveSearchFilter.toBuilder();
            negativeSearchFilterBldr.getMapFilterBuilder().setPositiveMatch(false);
            negativeSearchFilter = negativeSearchFilterBldr.build();
        }

        final Search.PropertyFilter searchFilterWithRegex =
                Search.PropertyFilter.newBuilder()
                    .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
                    .setMapFilter(MapFilter.newBuilder()
                                           .setRegex("O.*=.*A.*"))
                    .build();

        final PropertyFilter<TestGraphEntity> positiveFilter = filterFactory.filterFor(positiveSearchFilter);
        final PropertyFilter<TestGraphEntity> negativeFilter = filterFactory.filterFor(negativeSearchFilter);
        final PropertyFilter<TestGraphEntity> filterWithRegex =
                filterFactory.filterFor(searchFilterWithRegex);

        // entity has no tags
        final TestGraphEntity noTagsEntity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .build();
        assertFalse(positiveFilter.test(noTagsEntity));
        assertTrue(negativeFilter.test(noTagsEntity));
        assertFalse(filterWithRegex.test(noTagsEntity));

        // entity does not have the key
        final TestGraphEntity noKeyEntity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Collections.singletonList("VALUE1"))
            .build();
        assertFalse(positiveFilter.test(noKeyEntity));
        assertTrue(negativeFilter.test(noKeyEntity));
        assertTrue(filterWithRegex.test(noKeyEntity));

        // entity has the key, but not one of the values
        final TestGraphEntity wrongValueEntity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Arrays.asList("VALUE1"))
            .addTag("KEY", Arrays.asList("VALUE3", "VALUE4"))
            .build();
        assertFalse(positiveFilter.test(wrongValueEntity));
        assertTrue(negativeFilter.test(wrongValueEntity));
        assertTrue(filterWithRegex.test(wrongValueEntity));

        // entity has the key, and one of the values
        final TestGraphEntity rightValueEntity = TestGraphEntity.newBuilder(123L, UIEntityType.VIRTUAL_MACHINE)
            .addTag("KEY", Arrays.asList("VALUE2", "VALUE4"))
            .build();
        assertTrue(positiveFilter.test(rightValueEntity));
        assertFalse(negativeFilter.test(rightValueEntity));
        assertFalse(filterWithRegex.test(rightValueEntity));
    }

    @Test
    public void testVmemCapacityFilter() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.COMMODITY_SOLD_LIST_PROPERTY_NAME)
                .setListFilter(ListFilter.newBuilder()
                    .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME)
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("^" + UICommodityType.VMEM.apiStr() + "$")
                                .build()))
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME)
                            .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GT)
                                .setValue(5))))))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.VMEM.typeNumber()))
                .setCapacity(10)
                .build())
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.VMEM.typeNumber()))
                .setCapacity(1)
                .build())
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testMemCapacityFilter() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.COMMODITY_SOLD_LIST_PROPERTY_NAME)
                .setListFilter(ListFilter.newBuilder()
                    .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME)
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("^" + UICommodityType.MEM.apiStr() + "$")
                                .build()))
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME)
                            .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GT)
                                .setValue(5))))))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))
                .setCapacity(10)
                .build())
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))
                .setCapacity(1)
                .build())
            .build();

        assertTrue(propertyFilter.test(entity1));
        assertFalse(propertyFilter.test(entity2));
    }

    @Test
    public void testDiscoveringTargetFilter() {
        final TestGraphEntity entity = TestGraphEntity.newBuilder(1L, UIEntityType.APPLICATION)
                                           .addTargetIdentity(1L, "")
                                           .build();
        final PropertyFilter<TestGraphEntity> filter1 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter2 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .setPositiveMatch(false)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter3 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .setCaseSensitive(true)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter4 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .addOptions("3")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter5 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .addOptions("1")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter6 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("1")
                                            .setPositiveMatch(false)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter7 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter8 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("1")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter9 =
            makeDiscoveringTargetFilter(NumericFilter.newBuilder()
                                            .setValue(2)
                                            .setComparisonOperator(ComparisonOperator.EQ)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter10 =
            makeDiscoveringTargetFilter(NumericFilter.newBuilder()
                                            .setValue(1)
                                            .setComparisonOperator(ComparisonOperator.EQ)
                                            .build());

        assertTrue(filter1.test(entity));
        assertFalse(filter2.test(entity));
        assertTrue(filter3.test(entity));
        assertFalse(filter4.test(entity));
        assertTrue(filter5.test(entity));
        assertFalse(filter6.test(entity));
        assertFalse(filter7.test(entity));
        assertTrue(filter8.test(entity));
        assertFalse(filter9.test(entity));
        assertTrue(filter10.test(entity));
    }

    @Test
    public void testSearchFilterForStorageLocalSupport() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DS_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName("local")
                        .setStringFilter(StringFilter.newBuilder()
                            .setStringPropertyRegex("true")))))
            .build();

        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1234L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2345L, UIEntityType.PHYSICAL_MACHINE).build();
        final TestGraphEntity storageEntityMatching =
            TestGraphEntity.newBuilder(3456L, UIEntityType.STORAGE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setStorage(StorageInfo.newBuilder().setIsLocal(true))
                    .build())
                .build();
        final TestGraphEntity storageEntityNotMatching =
            TestGraphEntity.newBuilder(4567L, UIEntityType.STORAGE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setStorage(StorageInfo.newBuilder().setIsLocal(false))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity));
        assertFalse(propertyFilter.test(pmEntity));
        assertTrue(propertyFilter.test(storageEntityMatching));
        assertFalse(propertyFilter.test(storageEntityNotMatching));
    }

    /**
     * Test that the search for finding a BusinessAccount by subscription ID works properly.
     */
    @Test
    public void testSearchFilterForBusinessAccountId() {
        final String subscriptionId = "subId22";
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_ACCOUNT_ID)
                        .setStringFilter(StringFilter.newBuilder()
                            .addOptions(subscriptionId)))))
            .build();

        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(11111L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(22222L, UIEntityType.PHYSICAL_MACHINE).build();
        final TestGraphEntity businessEntityMatching =
            TestGraphEntity.newBuilder(33333L, UIEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder()
                        .setAccountId(subscriptionId))
                    .build())
                .build();
        final TestGraphEntity businessEntityNotMatching =
            TestGraphEntity.newBuilder(44444L, UIEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder().setAccountId("Id22"))
                    .build())
                .build();

        final TestGraphEntity businessEntityNoAccountId =
            TestGraphEntity.newBuilder(44444L, UIEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder())
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity));
        assertFalse(propertyFilter.test(pmEntity));
        assertTrue(propertyFilter.test(businessEntityMatching));
        assertFalse(propertyFilter.test(businessEntityNotMatching));
        assertFalse(propertyFilter.test(businessEntityNoAccountId));
    }

    /**
     * Test the filter generated from a VMs-connected-to-Network filter
     */
    @Test
    public void testVMsConnectedtoNetworksFilter() {
        final String regex = "^a.*$";
        final String matchingString = "axx";
        final String nonMatchingString = "xaa";
        final SearchFilter searchFilter =
                SearchFilter.newBuilder()
                    .setPropertyFilter(
                            Search.PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.VM_CONNECTED_NETWORKS)
                                .setListFilter(
                                        ListFilter.newBuilder()
                                            .setStringFilter(
                                                    StringFilter.newBuilder()
                                                        .setStringPropertyRegex(regex))))
                .build();
        final PropertyFilter<TestGraphEntity> filter = (PropertyFilter)filterFactory.filterFor(searchFilter);

        final TestGraphEntity vm1 = makeVmWithConnectedNetworks(1);
        final TestGraphEntity vm2 = makeVmWithConnectedNetworks(2, matchingString);
        final TestGraphEntity vm3 = makeVmWithConnectedNetworks(3, nonMatchingString);
        final TestGraphEntity vm4 = makeVmWithConnectedNetworks(4, nonMatchingString, matchingString);

        assertFalse(filter.test(vm1));
        assertTrue(filter.test(vm2));
        assertFalse(filter.test(vm3));
        assertTrue(filter.test(vm4));
    }

    /**
     * Test the filter for a storage volume's attachment state.
     */
    @Test
    public void testSearchFilterVolumeAttachmentState() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VOLUME_REPO_DTO)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.VOLUME_ATTACHMENT_STATE)
                        .setStringFilter(StringFilter.newBuilder().addOptions(AttachmentState.UNATTACHED.name()).build())
                    )
                )
            ).build();
        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2L, UIEntityType.PHYSICAL_MACHINE).build();

        final TestGraphEntity volumeEntityMatching =
            TestGraphEntity.newBuilder(3L, UIEntityType.VIRTUAL_VOLUME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setAttachmentState(AttachmentState.UNATTACHED))
                    .build())
                .build();
        final TestGraphEntity volumeEntityNotMatching =
            TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_VOLUME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setAttachmentState(AttachmentState.ATTACHED))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity));
        assertFalse(propertyFilter.test(pmEntity));
        assertTrue(propertyFilter.test(volumeEntityMatching));
        assertFalse(propertyFilter.test(volumeEntityNotMatching));
    }

    /**
     * Test the filter for an entity's vendor ID.
     */
    @Test
    public void testSearchFilterVendorID() {
        final SearchFilter vendorIdFilter = SearchProtoUtil.searchFilterProperty(
            Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VENDOR_ID)
                .setListFilter(ListFilter.newBuilder()
                    .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("id-1.*"))
                ).build());

        final TestGraphEntity entityWithMatchingId =
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-123")
                .build();
        final TestGraphEntity entityNotMatching =
            TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-456")
                .build();
        final TestGraphEntity entityWithOneMatchingOneUnmatching =
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-789")
                .addTargetIdentity(444L, "id-100")
                .build();
        final TestGraphEntity entityNoVendorId =
            TestGraphEntity.newBuilder(3L, UIEntityType.VIRTUAL_VOLUME).build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(vendorIdFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(entityWithMatchingId));
        assertTrue(propertyFilter.test(entityWithOneMatchingOneUnmatching));
        assertFalse(propertyFilter.test(entityNotMatching));
        assertFalse(propertyFilter.test(entityNoVendorId));
    }

    /**
     * Test that this filter filters out business accounts which have no associated targets.
     */
    @Test
    public void testAssociatedTargetFilter() {
        final long account1Id = 1L;
        final long account2Id = 2L;
        final long account3Id = 3L;
        final long account4Id = 4L;
        final long target1Id = 11L;
        final long target2Id = 22L;

        final PropertyFilter<TestGraphEntity> associatedTargetFilter = filterFactory.filterFor(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ASSOCIATED_TARGET_ID)
                        .setNumericFilter(NumericFilter.getDefaultInstance())
                        .build());

        final TestGraphEntity account1 =
                TestGraphEntity.newBuilder(account1Id, UIEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                        .setAssociatedTargetId(target1Id)
                                        .build())
                                .build())
                        .build();
        final TestGraphEntity account2 =
                TestGraphEntity.newBuilder(account2Id, UIEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                        .setAssociatedTargetId(target2Id)
                                        .build())
                                .build())
                        .build();
        final TestGraphEntity account3 =
                TestGraphEntity.newBuilder(account3Id, UIEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.getDefaultInstance())
                                .build())
                        .build();
        final TestGraphEntity account4 =
                TestGraphEntity.newBuilder(account4Id, UIEntityType.BUSINESS_ACCOUNT).build();

        assertThat(associatedTargetFilter.apply(Stream.of(account1, account2, account3, account4),
                graph).collect(Collectors.toList()), contains(account1, account2));
    }

    /**
     * Test exception in case if we try to filter entity which has not this property.
     */
    @Test
    public void testAssociatedTargetFilterException() {
        expectedException.expect(IllegalArgumentException.class);
        final PropertyFilter<TestGraphEntity> associatedTargetFilter = filterFactory.filterFor(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ASSOCIATED_TARGET_ID)
                        .setNumericFilter(NumericFilter.getDefaultInstance())
                        .build());

        final TestGraphEntity notSupportedEntity =
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(VirtualMachineInfo.getDefaultInstance())
                                .build())
                        .build();

        associatedTargetFilter.apply(Stream.of(notSupportedEntity), graph)
                .collect(Collectors.toList());
    }

    private TestGraphEntity makeVmWithConnectedNetworks(long id, String... connectedNetworks) {
        final VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder()
                                              .addAllConnectedNetworks(Arrays.asList(connectedNetworks))
                                              .build();
        return TestGraphEntity.newBuilder(id, UIEntityType.VIRTUAL_MACHINE)
                   .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                            .setVirtualMachine(vmInfo)
                                            .build())
                   .build();
    }

    private PropertyFilter<TestGraphEntity> makeDiscoveringTargetFilter(StringFilter stringFilter) {
        return (PropertyFilter)filterFactory.filterFor(
                                 SearchFilter.newBuilder()
                                   .setPropertyFilter(
                                      Search.PropertyFilter.newBuilder()
                                          .setPropertyName(SearchableProperties.DISCOVERED_BY_TARGET)
                                          .setListFilter(ListFilter.newBuilder()
                                                            .setStringFilter(stringFilter)))
                                   .build());
    }

    private PropertyFilter<TestGraphEntity> makeDiscoveringTargetFilter(NumericFilter numericFilter) {
        return (PropertyFilter)filterFactory.filterFor(
                SearchFilter.newBuilder()
                        .setPropertyFilter(
                                Search.PropertyFilter.newBuilder()
                                        .setPropertyName(SearchableProperties.DISCOVERED_BY_TARGET)
                                        .setListFilter(ListFilter.newBuilder()
                                                .setNumericFilter(numericFilter)))
                        .build());
    }
}
