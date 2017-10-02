package com.vmturbo.topology.processor.group.filter;

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

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToPropertyFilter;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

public class TopologyFilterFactoryTest {
    final TopologyFilterFactory filterFactory = new TopologyFilterFactory();
    final Vertex vertex1 = mock(Vertex.class);
    final Vertex vertex2 = mock(Vertex.class);
    final TopologyGraph graph = mock(TopologyGraph.class);

    final TopologyEntityDTO.Builder fooEntity = TopologyEntityDTO.newBuilder()
        .setOid(1L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .setDisplayName("foo");
    final TopologyEntityDTO.Builder barEntity = TopologyEntityDTO.newBuilder()
        .setOid(2L)
        .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .setDisplayName("bar");

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

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter propertyFilter = (PropertyFilter)filter;

        when(vertex1.getOid()).thenReturn(1234L);
        assertTrue(propertyFilter.test(vertex1));

        when(vertex2.getOid()).thenReturn(2345L);
        assertFalse(propertyFilter.test(vertex2));
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

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter propertyFilter = (PropertyFilter)filter;

        when(vertex1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE.getNumber());
        assertTrue(propertyFilter.test(vertex1));

        when(vertex2.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE.getNumber());
        assertFalse(propertyFilter.test(vertex2));
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

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter propertyFilter = (PropertyFilter)filter;

        when(vertex1.getTopologyEntityDtoBuilder()).thenReturn(fooEntity);
        assertTrue(propertyFilter.test(vertex1));

        when(vertex2.getTopologyEntityDtoBuilder()).thenReturn(barEntity);
        assertFalse(propertyFilter.test(vertex2));
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

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter propertyFilter = (PropertyFilter)filter;

        when(vertex1.getTopologyEntityDtoBuilder()).thenReturn(fooEntity);
        assertTrue(propertyFilter.test(vertex1));

        when(vertex2.getTopologyEntityDtoBuilder()).thenReturn(barEntity);
        assertFalse(propertyFilter.test(vertex2));
    }

    @Test
    public void testSearchFilterForDisplayNameRegexNegated() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setMatch(false)
                        .setStringPropertyRegex("f.*")
                ))
            .build();

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter propertyFilter = (PropertyFilter)filter;

        when(vertex1.getTopologyEntityDtoBuilder()).thenReturn(fooEntity);
        assertFalse(propertyFilter.test(vertex1));

        when(vertex2.getTopologyEntityDtoBuilder()).thenReturn(barEntity);
        assertTrue(propertyFilter.test(vertex2));
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
    public void testStringEntityTypeIllegal() {
        expectedException.expect(IllegalArgumentException.class);

        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("entityType")
                    .setStringFilter(StringFilter.getDefaultInstance())
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

        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setOid(1234L);
        final Vertex vertex = new Vertex(builder);

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertThat(
            filter.apply(Stream.of(vertex), graph).collect(Collectors.toList()),
            contains(vertex));
    }

    @Test
    public void testStringOidNegation() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("oid")
                    .setStringFilter(StringFilter.newBuilder()
                            .setMatch(false)
                            .setStringPropertyRegex("1234")
                    )
            ).build();

        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setOid(1234L);
        final Vertex vertex = new Vertex(builder);

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter.apply(Stream.of(vertex), graph).collect(Collectors.toList()).isEmpty());
    }

    private class NumericFilterTest {
        private final ComparisonOperator operator;
        private final Collection<Vertex> expectedMatches;

        public NumericFilterTest(final ComparisonOperator operator,
                                 @Nonnull final Collection<Vertex> expectedMatches) {
            this.operator = operator;
            this.expectedMatches = expectedMatches;
        }

        public ComparisonOperator getOperator() {
            return operator;
        }

        public Collection<Vertex> getExpectedMatches() {
            return expectedMatches;
        }
    }

    @Test
    public void testIntNumericComparisons() {
        final Vertex vertex1 = mock(Vertex.class);
        final Vertex vertex2 = mock(Vertex.class);
        final Vertex vertex3 = mock(Vertex.class);

        final List<Vertex> testCases = Arrays.asList(vertex1, vertex2, vertex3);
        when(vertex1.getEntityType()).thenReturn(1);
        when(vertex2.getEntityType()).thenReturn(2);
        when(vertex3.getEntityType()).thenReturn(3);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("entityType");

        // Execute the comparison on entities with entityType 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(vertex2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(vertex1, vertex3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(vertex3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(vertex2, vertex3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(vertex1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(vertex1, vertex2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter filter = filterFactory.filterFor(searchCriteria);

            assertThat(
                "Test for: " + testCase.getOperator().toString(),
                filter.apply(testCases.stream(), graph).collect(Collectors.toList()),
                containsInAnyOrder(testCase.getExpectedMatches().toArray())
            );
        });
    }

    @Test
    public void testLongNumericComparisons() {
        final Vertex vertex1 = mock(Vertex.class);
        final Vertex vertex2 = mock(Vertex.class);
        final Vertex vertex3 = mock(Vertex.class);

        final List<Vertex> testCases = Arrays.asList(vertex1, vertex2, vertex3);
        when(vertex1.getOid()).thenReturn(1L);
        when(vertex2.getOid()).thenReturn(2L);
        when(vertex3.getOid()).thenReturn(3L);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2L);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("oid");

        // Execute the comparison on entities with oid 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(vertex2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(vertex1, vertex3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(vertex3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(vertex2, vertex3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(vertex1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(vertex1, vertex2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter filter = filterFactory.filterFor(searchCriteria);

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
            .setTraversalFilter(SearchFilter.TraversalFilter.newBuilder()
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
            .setTraversalFilter(SearchFilter.TraversalFilter.newBuilder()
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
        final PropertyFilter displayNameFilter = new TopologyFilterFactory()
            .filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("in-group")
                ).build());

        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setDisplayName("entity-in-group-1");

        final Vertex vertex = new Vertex(builder);
        assertThat(
            displayNameFilter.apply(Stream.of(vertex), graph).collect(Collectors.toList()),
            contains(vertex));
    }

    @Test
    public void testRegexAnchorMatch() {
        final PropertyFilter displayNameFilter = new TopologyFilterFactory()
            .filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("^entity")
                ).build());

        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setDisplayName("entity-in-group-1");

        final Vertex vertex = new Vertex(builder);
        assertThat(
            displayNameFilter.apply(Stream.of(vertex), graph).collect(Collectors.toList()),
            contains(vertex));
    }

    @Test
    public void testRegexWildcardMatch() {
        final PropertyFilter displayNameFilter = new TopologyFilterFactory()
            .filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex(".*group.+")
                ).build());

        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setDisplayName("entity-in-group-1");

        final Vertex vertex = new Vertex(builder);
        assertThat(
            displayNameFilter.apply(Stream.of(vertex), graph).collect(Collectors.toList()),
            contains(vertex));
    }
}