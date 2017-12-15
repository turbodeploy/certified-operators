package com.vmturbo.topology.processor.group;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntityWithName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class GroupResolverTest {

    private TopologyGraph topologyGraph;

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

        topologyGraph = TopologyGraph.newGraph(topologyMap);
    }

    @Test
    public void testResolveStaticGroup() throws Exception {
        Group staticGroup = Group.newBuilder()
            .setId(1234L)
            .setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addAllStaticMemberOids(Arrays.asList(1L, 2L))))
            .build();

        final GroupResolver resolver = new GroupResolver(Mockito.mock(TopologyFilterFactory.class));
        assertThat(resolver.resolve(staticGroup, topologyGraph), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testResolveCluster() throws Exception {
        Group cluster = Group.newBuilder()
                .setId(1234L)
                .setType(Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addAllStaticMemberOids(Arrays.asList(1L, 2L))))
                .build();
        final GroupResolver resolver = new GroupResolver(Mockito.mock(TopologyFilterFactory.class));
        assertThat(resolver.resolve(cluster, topologyGraph), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testExceptionDuringResolution() throws Exception {
        final TopologyFilterFactory filterFactory = Mockito.mock(TopologyFilterFactory.class);
        when(filterFactory.filterFor(any(SearchFilter.class))).thenThrow(new RuntimeException("error!"));
        expectedException.expect(GroupResolutionException.class);

        final Group dynamicGroup = Group.newBuilder()
            .setId(1234L)
            .setGroup(GroupInfo.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(Search.PropertyFilter.getDefaultInstance()))))
            .build();

        final GroupResolver resolver = new GroupResolver(Mockito.mock(TopologyFilterFactory.class));
        resolver.resolve(dynamicGroup, topologyGraph);
    }

    @Test
    public void testResolveDynamicGroupStartingFilterOnly() throws Exception {
        final Group dynamicGroup = Group.newBuilder()
            .setId(1234L)
            .setGroup(GroupInfo.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(Search.PropertyFilter.newBuilder()
                                        .setPropertyName("entityType")
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                .setValue(EntityType.PHYSICAL_MACHINE.getNumber()))))))
            .build();

        final GroupResolver resolver = new GroupResolver(new TopologyFilterFactory());
        assertThat(resolver.resolve(dynamicGroup, topologyGraph), containsInAnyOrder(1L, 2L, 3L, 4L));
    }

    @Test
    public void testResolveDynamicGroupWithMultipleFilters() throws Exception {
        // Find all virtual machines consuming from physical machines.
        final Group dynamicGroup = Group.newBuilder()
                .setId(1234L)
                .setGroup(GroupInfo.newBuilder()
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
                                    )))))
                .build();

        final GroupResolver resolver = new GroupResolver(new TopologyFilterFactory());
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
        final Group dynamicGroup = Group.newBuilder()
                .setId(1234L)
                .setGroup(GroupInfo.newBuilder()
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
                        ))
                .build();

        final GroupResolver resolver = new GroupResolver(new TopologyFilterFactory());
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
        final Group dynamicGroup = Group.newBuilder()
                .setId(groupId)
                .setType(Type.GROUP)
                .setOrigin(Origin.USER)
                .setGroup(GroupInfo.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
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
                            )
                        )
                    )
                .build();

        final GroupResolver resolver = spy(new GroupResolver(new TopologyFilterFactory()));
        resolver.resolve(dynamicGroup, topologyGraph);
        resolver.resolve(dynamicGroup, topologyGraph);
        resolver.resolve(dynamicGroup, topologyGraph);
        // resolveDynamicGroup should only be called once as the subsequent calls will
        // return from the cache
        verify(resolver, times(1)).resolveDynamicGroup(eq(groupId),
                anyInt(), any(), eq(topologyGraph));
    }

    /**
     * Test group resolver cache for static groups.
     *
     * @throws GroupResolutionException when a group cannot be resolved
     *
     */
    @Test
    public void testGroupResolverCacheStaticGroups() throws GroupResolutionException {

        final long groupId = 9999L;
        final Group staticGroup =
            Group.newBuilder()
                .setId(groupId)
                .setType(Type.GROUP)
                .setOrigin(Origin.USER)
                .setGroup(GroupInfo.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addAllStaticMemberOids(Arrays.asList(1L, 2L))))
                .build();

        final GroupResolver resolver = spy(new GroupResolver(new TopologyFilterFactory()));
        resolver.resolve(staticGroup, topologyGraph);
        resolver.resolve(staticGroup, topologyGraph);
        resolver.resolve(staticGroup, topologyGraph);
        // resolveStaticGroup should be called only once as subsequent calls will
        // return from the cache
        verify(resolver, times(1)).resolveStaticGroup(staticGroup);
    }

    /**
     * Test when the groupId is missing.
     *
     * @throws GroupResolutionException when a group cannot be resolved
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testResolveWithMissingGroupId() throws GroupResolutionException {

        final Group group = Group.newBuilder().build();
        final GroupResolver resolver = new GroupResolver(Mockito.mock(TopologyFilterFactory.class));
        resolver.resolve(group, topologyGraph);
    }
}
