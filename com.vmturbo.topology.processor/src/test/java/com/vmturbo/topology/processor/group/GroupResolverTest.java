package com.vmturbo.topology.processor.group;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityWithName;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityWithTags;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class GroupResolverTest {

    private TopologyGraph<TopologyEntity> topologyGraph;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * VM-10
     * |
     * VDC-9
     * | \
     * VDC-5 VDC-6  VDC-7 VM-8
     * \ /       |        /  \
     * PM-1      PM-2   PM-3  PM-4
     */
    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_DATACENTER, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_DATACENTER, 1));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_DATACENTER, 2));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 3, 4));
        topologyMap.put(9L, topologyEntity(9L, EntityType.VIRTUAL_DATACENTER, 5, 6));
        topologyMap.put(10L, topologyEntityWithName(10L, EntityType.VIRTUAL_MACHINE, "VM#10", 9));

        final Map<String, TagValuesDTO> tags11 =
                ImmutableMap.of(
                        "k1", TagValuesDTO.newBuilder().addValues("v1").addValues("v2").build(),
                        "k2", TagValuesDTO.newBuilder().addValues("v3").addValues("v2").build());
        final Map<String, TagValuesDTO> tags12 =
                ImmutableMap.of(
                        "k3", TagValuesDTO.newBuilder().addValues("v1").build());
        topologyMap.put(11L, topologyEntityWithTags(11L, EntityType.STORAGE, tags11));
        topologyMap.put(12L, topologyEntityWithTags(12L, EntityType.STORAGE, tags12));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
    }

    @Test
    public void testResolveStaticGroup() throws Exception {
        Grouping staticGroup = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                                .addAllMembers(Arrays.asList(1L, 2L)))))
                .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(Mockito.mock(SearchResolver.class),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(resolver.resolve(staticGroup, topologyGraph), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testResolveCluster() throws Exception {
        Grouping cluster = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(UIEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder()
                                                                        .setEntity(UIEntityType.PHYSICAL_MACHINE.typeNumber()))
                                                        .addAllMembers(Arrays.asList(1L, 2L)))))
                        .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(Mockito.mock(SearchResolver.class),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(resolver.resolve(cluster, topologyGraph), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testExceptionDuringResolution() throws Exception {
        final TopologyFilterFactory filterFactory = Mockito.mock(TopologyFilterFactory.class);
        when(filterFactory.filterFor(any(SearchFilter.class))).thenThrow(new RuntimeException("error!"));
        expectedException.expect(GroupResolutionException.class);


        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                                        .addEntityFilter(EntityFilter.newBuilder()
                                                        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                                                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(Search.PropertyFilter.getDefaultInstance()))))))
                        .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(Mockito.mock(SearchResolver.class),
                                    Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        resolver.resolve(dynamicGroup, topologyGraph);
    }

    @Test
    public void testResolveDynamicGroupStartingFilterOnly() throws Exception {
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                                .addSearchParameters(SearchParameters.newBuilder()
                                                                .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                                    .setPropertyName("entityType")
                                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                                        .setComparisonOperator(ComparisonOperator.EQ)
                                                                        .setValue(EntityType.PHYSICAL_MACHINE.getNumber()))))))))
                        .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(resolver.resolve(dynamicGroup, topologyGraph), containsInAnyOrder(1L, 2L, 3L, 4L));
    }

    @Test
    public void testResolveDynamicGroupStartingFilterNotEquals() throws Exception {
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                                .addSearchParameters(SearchParameters.newBuilder()
                                                                .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                                    .setPropertyName("entityType")
                                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                                        .setComparisonOperator(ComparisonOperator.NE)
                                                                        .setValue(EntityType.PHYSICAL_MACHINE.getNumber()))))))))
                        .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                                            Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(
                resolver.resolve(dynamicGroup, topologyGraph),
                containsInAnyOrder(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L));
    }

    @Test
    public void testTagFilters() throws Exception {
        testTagFilter(100L, "c", Collections.EMPTY_LIST, false, false);
        testTagFilter(101L, "k1", Collections.EMPTY_LIST, true, false);
        testTagFilter(102L, "k2", Collections.singletonList("v1"), false, false);
        testTagFilter(103L, "k2", Arrays.asList("v1", "v2"), true, false);
        testTagFilter(104L, "k3", Arrays.asList("v1", "v4"), false, true);
    }

    private void testTagFilter(
        long goid, String key, List<String> values,
        boolean entity11expected, boolean entity12expected
    ) throws Exception {
        final SearchParametersCollection searchParameters =
                SearchParametersCollection.newBuilder()
                    .addSearchParameters(
                        SearchParameters.newBuilder().setStartingFilter(
                            Search.PropertyFilter.newBuilder()
                                .setPropertyName("tags")
                                .setMapFilter(
                                    MapFilter.newBuilder()
                                        .setKey(key)
                                        .addAllValues(values)
                                        .build()
                                ).build()
                        )
                    ).build();

        final Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.STORAGE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                                        .addEntityFilter(EntityFilter.newBuilder()
                                                        .setEntityType(EntityType.STORAGE.getNumber())
                                                        .setSearchParametersCollection(searchParameters))))
                        .setId(goid).build();

        final Set<Long> groupMembers = new GroupResolver(new SearchResolver<TopologyEntity>(
            new TopologyFilterFactory<>()), Mockito.mock(GroupConfig.class).groupServiceBlockingStub())
                .resolve(dynamicGroup, topologyGraph);
        assertEquals(entity11expected, groupMembers.contains(11L));
        assertEquals(entity12expected, groupMembers.contains(12L));
    }

    @Test
    public void testResolveDynamicGroupWithMultipleFilters() throws Exception {
        // Find all virtual machines consuming from physical machines.
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                                .addEntityFilter(EntityFilter.newBuilder()
                                    .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                        .addSearchParameters(SearchParameters.newBuilder()
                                                        .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                            .setPropertyName("entityType")
                                                            .setNumericFilter(NumericFilter.newBuilder()
                                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                                .setValue(EntityType.PHYSICAL_MACHINE.getNumber()))
                                                        ).addSearchFilter(SearchFilter.newBuilder()
                                                            .setTraversalFilter(TraversalFilter.newBuilder()
                                                                .setTraversalDirection(TraversalDirection.PRODUCES)
                                                                .setStoppingCondition(StoppingCondition.newBuilder()
                                                                    .setStoppingPropertyFilter(Search.PropertyFilter.newBuilder()
                                                                        .setPropertyName("entityType")
                                                                        .setNumericFilter(NumericFilter.newBuilder()
                                                                            .setComparisonOperator(ComparisonOperator.EQ)
                                                                            .setValue(EntityType.VIRTUAL_MACHINE.getNumber()))
                                                                    ))
                                                                )))))))
                        .setId(1234L).build();



        final GroupResolver resolver = new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(resolver.resolve(dynamicGroup, topologyGraph), containsInAnyOrder(8L, 10L));
    }

    /**
     * Test resolve dynamic group with multiple search parameters, the final results are intersection
     * of each search parameters' results.
     * @throws Exception
     */
    @Test
    public void testResolveDynamicGroupWithMultipleSearchParameters() throws Exception {
        // Find all virtual machines consuming from physical machines and display name matches VM#10
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(SearchParameters.newBuilder()
                                                    .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                            .setPropertyName("entityType")
                                                            .setNumericFilter(NumericFilter.newBuilder()
                                                                    .setComparisonOperator(ComparisonOperator.EQ)
                                                                    .setValue(EntityType.PHYSICAL_MACHINE.getNumber()))
                                                    ).addSearchFilter(SearchFilter.newBuilder()
                                                            .setTraversalFilter(TraversalFilter.newBuilder()
                                                                    .setTraversalDirection(TraversalDirection.PRODUCES)
                                                                    .setStoppingCondition(StoppingCondition.newBuilder()
                                                                            .setStoppingPropertyFilter(Search.PropertyFilter.newBuilder()
                                                                                    .setPropertyName("entityType")
                                                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                                                            .setComparisonOperator(ComparisonOperator.EQ)
                                                                                            .setValue(EntityType.VIRTUAL_MACHINE.getNumber()))
                                                                            ))
                                                            )))
                                            .addSearchParameters(SearchParameters.newBuilder()
                                                    .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                            .setPropertyName("entityType")
                                                            .setNumericFilter(NumericFilter.newBuilder()
                                                                    .setComparisonOperator(ComparisonOperator.EQ)
                                                                    .setValue(EntityType.VIRTUAL_MACHINE.getNumber()))
                                                    ).addSearchFilter((SearchFilter.newBuilder()
                                                            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                                                                    .setPropertyName("displayName")
                                                                    .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                                                                            .setStringPropertyRegex("VM#10")))
                                                            )))
                                        ))))
                        .setId(1234L).build();

        final GroupResolver resolver = new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        assertThat(resolver.resolve(dynamicGroup, topologyGraph), contains(10L));
    }

    /**
     * Test group resolver cache for dynamic groups.
     *
     * @throws GroupResolutionException when a group cannot be resolved
     *
     */
    @Test
    public void testGroupResolverCacheDynamicGroups() throws GroupResolutionException {

        final long groupId = 9999L;
        // Group members of type PM or VM
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(SearchParameters.newBuilder()
                                        .setStartingFilter(Search.PropertyFilter.newBuilder()
                                            .setPropertyName("entityType")
                                            .setNumericFilter(NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                .setValue(EntityType.PHYSICAL_MACHINE_VALUE)
                                                )
                                            )
                                            .addSearchFilter(SearchFilter.newBuilder()
                                                .setTraversalFilter(TraversalFilter.newBuilder()
                                                    .setTraversalDirection(TraversalDirection.PRODUCES)
                                                    .setStoppingCondition(StoppingCondition.newBuilder()
                                                        .setStoppingPropertyFilter(Search.PropertyFilter.newBuilder()
                                                            .setPropertyName("entityType")
                                                            .setNumericFilter(NumericFilter.newBuilder()
                                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                                .setValue(EntityType.VIRTUAL_MACHINE_VALUE))
                                                        )
                                                    )
                                                )
                                            )
                                        )))))
                        .setId(groupId)
                        .setOrigin(GroupDTO.Origin.newBuilder().setUser(GroupDTO.Origin.User.newBuilder()))
                        .build();


        final GroupResolver resolver = spy(new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub()));
        resolver.resolve(dynamicGroup, topologyGraph);
        resolver.resolve(dynamicGroup, topologyGraph);
        resolver.resolve(dynamicGroup, topologyGraph);
        // resolveDynamicGroup should only be called once as the subsequent calls will
        // return from the cache
        verify(resolver, times(1)).resolveDynamicGroup(eq(groupId),
                anyInt(), any(), eq(topologyGraph));
    }

    /**
     * Test when the groupId is missing.
     *
     * @throws GroupResolutionException when a group cannot be resolved
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testResolveWithMissingGroupId() throws GroupResolutionException {

        final Grouping group = Grouping.newBuilder().build();
        final GroupResolver resolver = new GroupResolver(new SearchResolver<TopologyEntity>(new TopologyFilterFactory<>()),
                Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        resolver.resolve(group, topologyGraph);
    }

}
