package com.vmturbo.common.protobuf.search;

import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit test for {@link SearchFilterResolver}.
 */
public class SearchFilterResolverTest {

    @Mock
    private Function<GroupFilter, Set<Long>> groupsGetter;
    private SearchFilterResolver filterResolver;

    /**
     * Set up the teset.
     */
    @Before
    public void initialize() {
        MockitoAnnotations.initMocks(this);
        filterResolver = new TestSearchFilterResolver(groupsGetter);
    }

    /**
     * Test substituting of filters for cluster membership.
     */
    @Test
    public void testClusterFilters() {
        // create a SearchParams for members of Cluster1
        final PropertyFilter clusterSpecifier = PropertyFilter.newBuilder()
                .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("Cluster1"))
                .setPropertyName("displayName")
                .build();
        final SearchParameters params = SearchParameters.newBuilder()
                .addSearchFilter(SearchFilter.newBuilder()
                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                .setClusterSpecifier(clusterSpecifier)
                                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)))
                .build();
        Mockito.when(groupsGetter.apply(Mockito.any())).thenReturn(Sets.newHashSet(1L, 2L));
        final SearchParameters resolvedParams = filterResolver.resolveGroupFilters(params);

        // we should get the members of cluster 1 in the static filter
        final StringFilter stringFilter =
                resolvedParams.getSearchFilter(0).getPropertyFilter().getStringFilter();
        Assert.assertEquals(ImmutableSet.of("1", "2"),
                stringFilter.getOptionsList().stream().collect(Collectors.toSet()));

        final ArgumentCaptor<GroupFilter> reqCaptor = ArgumentCaptor.forClass(GroupFilter.class);
        Mockito.verify(groupsGetter).apply(reqCaptor.capture());
        final GroupFilter req = reqCaptor.getValue();
        Assert.assertEquals(GroupType.COMPUTE_HOST_CLUSTER, req.getGroupType());
        Assert.assertTrue(req.hasGroupType());
        Assert.assertThat(req.getPropertyFilters(0).getStringFilter(),
                is(clusterSpecifier.getStringFilter()));
        Assert.assertEquals("displayName", req.getPropertyFilters(0).getPropertyName());
    }

    /**
     * Test substituting of filters for group membership by oid.
     */
    @Test
    public void testGroupFilterWithoutType() {
        // create a SearchParams for members of Cluster1
        final PropertyFilter clusterSpecifier = PropertyFilter.newBuilder()
                .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("Cluster1"))
                .setPropertyName("oid")
                .build();
        final SearchParameters params = SearchParameters.newBuilder()
                .addSearchFilter(SearchFilter.newBuilder()
                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                .setClusterSpecifier(clusterSpecifier)))
                .build();

        Mockito.when(groupsGetter.apply(Mockito.any())).thenReturn(Sets.newHashSet(1L, 2L));
        final SearchParameters resolvedParams = filterResolver.resolveGroupFilters(params);

        // we should get the members of cluster 1 in the static filter
        final StringFilter stringFilter =
                resolvedParams.getSearchFilter(0).getPropertyFilter().getStringFilter();
        Assert.assertEquals(ImmutableSet.of("1", "2"),
                stringFilter.getOptionsList().stream().collect(Collectors.toSet()));

        final ArgumentCaptor<GroupFilter> reqCaptor = ArgumentCaptor.forClass(GroupFilter.class);
        Mockito.verify(groupsGetter).apply(reqCaptor.capture());
        final GroupFilter req = reqCaptor.getValue();
        Assert.assertFalse(req.hasGroupType());
        Assert.assertThat(req.getPropertyFilters(0).getStringFilter(),
                is(clusterSpecifier.getStringFilter()));
    }

    /**
     * Test the case when there is not group membership filters in search parameters at all.
     */
    @Test
    public void testNoGroupMembershiptFilters() {
        final SearchParameters params =
                SearchParameters.newBuilder().addSearchFilter(createNonGroupFilter()).build();
        final SearchParameters resolvedParams = filterResolver.resolveGroupFilters(params);
        Assert.assertEquals(params, resolvedParams);
        Mockito.verify(groupsGetter, Mockito.never()).apply(Mockito.any());
    }

    /**
     * Tests the case when some of filters are group membership filters and have to be resolved,
     * but another are simple filters.
     */
    @Test
    public void testSomeFiltersAreGroupFilters() {
        final PropertyFilter clusterSpecifier = PropertyFilter.newBuilder()
                .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("Cluster1"))
                .setPropertyName("oid")
                .build();
        final SearchParameters params = SearchParameters.newBuilder()
                .addSearchFilter(SearchFilter.newBuilder()
                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                .setClusterSpecifier(clusterSpecifier)))
                .addSearchFilter(createNonGroupFilter())
                .build();
        Mockito.when(groupsGetter.apply(Mockito.any())).thenReturn(Sets.newHashSet(1L, 2L));
        final SearchParameters resolvedParams = filterResolver.resolveGroupFilters(params);
        Assert.assertEquals(params.getSearchFilter(1), resolvedParams.getSearchFilter(1));
        Assert.assertEquals(Sets.newHashSet(1L, 2L), resolvedParams.getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getOptionsList()
                .stream()
                .map(Long::parseLong)
                .collect(Collectors.toSet()));
    }

    @Nonnull
    private SearchFilter createNonGroupFilter() {
        return SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("field")
                        .setStringFilter(
                                StringFilter.newBuilder().setStringPropertyRegex("some-value")))
                .build();
    }

    /**
     * Test filter resolver with just a function inside.
     */
    private static class TestSearchFilterResolver extends SearchFilterResolver {

        private final Function<GroupFilter, Set<Long>> function;

        TestSearchFilterResolver(Function<GroupFilter, Set<Long>> function) {
            this.function = function;
        }

        @Nonnull
        @Override
        protected Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter) {
            return function.apply(groupFilter);
        }
    }
}
