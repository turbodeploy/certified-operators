package com.vmturbo.topology.graph.search;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.filter.PropertyFilter;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;

public class SearchResolverTest {

    private final TopologyFilterFactory<TestGraphEntity> filterFactory = mock(TopologyFilterFactory.class);

    private final SearchResolver<TestGraphEntity> searchResolver = new SearchResolver<>(filterFactory);

    @Test
    public void testStartingFilterEntityTypeNumeric() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
                .setStartingFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName(SearchableProperties.ENTITY_TYPE)
                    .setNumericFilter(NumericFilter.newBuilder()
                        .setValue(UIEntityType.DISKARRAY.typeNumber())))
                .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testStartingFilterEntityTypeRegex() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(UIEntityType.DISKARRAY.apiStr())
                    .setPositiveMatch(true)))
            .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testStartingFilterEntityTypeOption() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions(UIEntityType.DISKARRAY.apiStr())
                    .setPositiveMatch(true)))
            .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testStartingFilterOidNumeric() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.OID)
                .setNumericFilter(NumericFilter.newBuilder()
                    .setValue(44L)))
            .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testStartingFilterOidRegex() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.OID)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("44")
                    .setPositiveMatch(true)))
            .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testStartingFilterOidOption() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.OID)
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions("44")
                    .setPositiveMatch(true)))
            .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testUnoptimizedStartingFilter() {
        Search.PropertyFilter startingFilter = Search.PropertyFilter.newBuilder()
            .setPropertyName(SearchableProperties.DISPLAY_NAME)
            .setStringFilter(StringFilter.newBuilder()
                .addOptions("44")
                .setPositiveMatch(true))
            .build();
        PropertyFilter<TestGraphEntity> propFilter = mock(PropertyFilter.class);
        when(filterFactory.filterFor(startingFilter)).thenReturn(propFilter);

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        when(propFilter.apply(any(), eq(graph))).thenReturn(Stream.of(graph.getEntity(44L).get()));

        final List<Long> results = searchResolver.search(SearchParameters.newBuilder()
                .setStartingFilter(startingFilter)
                .build(), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList());

        verify(filterFactory).filterFor(startingFilter);
        verify(propFilter).apply(any(), eq(graph));
        assertThat(results, containsInAnyOrder(44L));
    }

    @Test
    public void testSearchNoParameters() {

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(Collections.emptyList(), graph).count(), is(0L));
    }

    @Test
    public void testSearchSingleParameter() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
        );

        assertThat(searchResolver.search(Collections.singletonList(
            SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.idFilter(44L))
                .build()), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }

    @Test
    public void testSearchTwoParametersIntersection() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE),
            TestGraphEntity.newBuilder(11L, UIEntityType.VIRTUAL_MACHINE)
        );

        assertThat(searchResolver.search(Arrays.asList(
            SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(Arrays.asList(
                    UIEntityType.DISKARRAY.apiStr(), UIEntityType.STORAGE.apiStr())))
                .build(),
            SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(Arrays.asList(
                    UIEntityType.DISKARRAY.apiStr(), UIEntityType.VIRTUAL_MACHINE.apiStr())))
                .build()), graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(44L));
    }
}
