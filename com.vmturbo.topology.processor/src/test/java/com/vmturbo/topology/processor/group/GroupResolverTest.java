package com.vmturbo.topology.processor.group;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 * Unit tests for {@link GroupResolver}.
 */
public class GroupResolverTest {

    /**
     * Captures expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GroupServiceMole groupServiceBackend = spy(GroupServiceMole.class);

    /**
     * Test server to mock out the gRPC dependency.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceBackend);

    private SearchResolver<TopologyEntity> searchResolver = mock(SearchResolver.class);

    private GroupResolver groupResolver;

    private TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        groupResolver = new GroupResolver(searchResolver, GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    /**
     * Test resolving a static group of groups.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveNestedStaticGroup() throws Exception {
        final long childGroupId = 1L;
        final long vmId = 2L;
        Grouping parentGroup = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setGroup(GroupType.REGULAR))
                        .addMembers(childGroupId))))
            .setId(1234L).build();
        Grouping childGroup = Grouping.newBuilder()
            .setId(childGroupId)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addMembers(vmId))))
            .build();
        doReturn(Collections.singletonList(childGroup)).when(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addId(childGroupId))
            .build());

        assertThat(groupResolver.resolve(parentGroup, topologyGraph).getAllEntities(), containsInAnyOrder(vmId));
    }

    /**
     * Test resolving a dynamic group of groups.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveNestedDynamicGroup() throws Exception {
        final long childGroupId = 1L;
        final long vmId = 2L;
        final Grouping parentGroup = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder()
                        // Just something.
                        .setGroupType(GroupType.REGULAR))))
            .setId(1234L).build();
        final Grouping childGroup = Grouping.newBuilder()
            .setId(childGroupId)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addMembers(vmId))))
            .build();
        doReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                .setGroupId(parentGroup.getId())
                .addMemberId(childGroupId)
                .build()))
            .when(groupServiceBackend).getMembers(GetMembersRequest.newBuilder()
                .setExpectPresent(true)
                .addId(parentGroup.getId())
                .build());
        doReturn(Collections.singletonList(childGroup)).when(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addId(childGroupId))
            .build());

        assertThat(groupResolver.resolve(parentGroup, topologyGraph).getAllEntities(), containsInAnyOrder(vmId));
    }

    /**
     * Test that a not-found exception when resolving a dynamic group of groups is treated as empty.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveNestedDynamicGroupNotFound() throws Exception {
        final Grouping parentGroup = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder()
                        // Just something.
                        .setGroupType(GroupType.REGULAR))))
            .setId(1234L).build();
        doReturn(Optional.of(Status.NOT_FOUND.asException()))
            .when(groupServiceBackend).getMembersError(GetMembersRequest.newBuilder()
                .setExpectPresent(true)
                .addId(parentGroup.getId())
                .build());

        // If the parent group is not found, we treat it as empty.
        assertThat(groupResolver.resolve(parentGroup, topologyGraph).getAllEntities(), empty());
    }

    /**
     * Test that a gRPC exception when resolving a dynamic group of groups is rethrown as a
     * {@link GroupResolutionException}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveNestedDynamicGroupException() throws Exception {
        final Grouping parentGroup = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder()
                        // Just something.
                        .setGroupType(GroupType.REGULAR))))
            .setId(1234L).build();
        doReturn(Optional.of(Status.INTERNAL.asException()))
            .when(groupServiceBackend).getMembersError(GetMembersRequest.newBuilder()
            .setExpectPresent(true)
            .addId(parentGroup.getId())
            .build());

        // If the parent group is not found, we treat it as empty.
        expectedException.expect(GroupResolutionException.class);
        groupResolver.resolve(parentGroup, topologyGraph);
    }

    /**
     * Test a group of groups of groups. Three levels of nesting.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDoubleNestedGroup() throws Exception {
        final long vmId = 1111L;
        final long childGroupId = 1L;
        final long grandChildGroupId = 2L;
        final SearchParameters grandchildParams = SearchParameters.newBuilder()
            .setStartingFilter(PropertyFilter.newBuilder()
                .setPropertyName("foo"))
            .build();
        final Grouping parentGroup = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setGroup(GroupType.REGULAR))
                        .addMembers(childGroupId))))
            .setId(1234L).build();
        final Grouping childGroup = Grouping.newBuilder()
            .setId(childGroupId)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setGroup(GroupType.REGULAR))
                        .addMembers(grandChildGroupId))))
            .build();
        final Grouping grandChildGroup = Grouping.newBuilder()
            .setId(grandChildGroupId)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(grandchildParams)))))
            .build();
        doReturn(Collections.singletonList(childGroup)).when(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addId(childGroupId))
            .build());
        doReturn(Collections.singletonList(grandChildGroup)).when(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addId(grandChildGroupId))
            .build());

        when(searchResolver.search(SearchQuery.newBuilder()
            .setLogicalOperator(LogicalOperator.AND)
            .addSearchParameters(grandchildParams)
                .build(), topologyGraph))
            .thenReturn(Stream.of(topologyEntity(vmId, EntityType.VIRTUAL_MACHINE).build()));

        assertThat(groupResolver.resolve(parentGroup, topologyGraph).getAllEntities(), containsInAnyOrder(vmId));

        verify(searchResolver).search(SearchQuery.newBuilder()
            .setLogicalOperator(LogicalOperator.AND)
            .addSearchParameters(grandchildParams)
                .build(), topologyGraph);

        // Check that resolving the grandchild group directly does not trigger a search - it should
        // be cached.
        groupResolver.resolve(grandChildGroup, topologyGraph);
        verifyNoMoreInteractions(searchResolver);
    }

    /**
     * Test resolving a static group.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveStaticGroup() throws Exception {
        Grouping staticGroup = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addAllMembers(Arrays.asList(1L, 2L)))))
                .setId(1234L).build();

        assertThat(groupResolver.resolve(staticGroup, topologyGraph).getAllEntities(), containsInAnyOrder(1L, 2L));
    }

    /**
     * Test resolving a cluster.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveCluster() throws Exception {
        Grouping cluster = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder()
                                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                                                        .addAllMembers(Arrays.asList(1L, 2L)))))
                        .setId(1234L).build();

        assertThat(groupResolver.resolve(cluster, topologyGraph).getAllEntities(), containsInAnyOrder(1L, 2L));
    }

    /**
     * Test that exceptions thrown when resolving a dynamic groups get rethrown as
     * {@link GroupResolutionException}s.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testExceptionDuringResolution() throws Exception {

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

        when(searchResolver.search(any(SearchQuery.class), any())).thenThrow(new RuntimeException("error!"));

        expectedException.expect(GroupResolutionException.class);
        groupResolver.resolve(dynamicGroup, topologyGraph);
    }

    /**
     * Test resolving a dynamic group.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testResolveDynamicGroup() throws Exception {
        SearchParameters searchParams = SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setNumericFilter(NumericFilter.newBuilder()
                    .setComparisonOperator(ComparisonOperator.EQ)
                    .setValue(EntityType.PHYSICAL_MACHINE.getNumber())))
            .build();
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                                .addSearchParameters(searchParams)))))
                        .setId(1234L).build();

        when(searchResolver.search(SearchQuery.newBuilder()
                .setLogicalOperator(LogicalOperator.AND)
                .addSearchParameters(searchParams)
                .build(), topologyGraph))
            .thenReturn(Stream.of(topologyEntity(1L, EntityType.PHYSICAL_MACHINE).build(),
                    topologyEntity(2L, EntityType.PHYSICAL_MACHINE).build(),
                    topologyEntity(3L, EntityType.PHYSICAL_MACHINE).build()));

        assertThat(groupResolver.resolve(dynamicGroup, topologyGraph).getAllEntities(), containsInAnyOrder(1L, 2L, 3L));
    }

    /**
     * Test group resolver cache for dynamic groups.
     *
     * @throws GroupResolutionException when a group cannot be resolved
     */
    @Test
    public void testGroupResolverCacheDynamicGroups() throws GroupResolutionException {
        SearchParameters params1 = SearchParameters.newBuilder()
            .setStartingFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setNumericFilter(NumericFilter.newBuilder()
                    .setComparisonOperator(ComparisonOperator.EQ)
                    .setValue(EntityType.VIRTUAL_MACHINE_VALUE)))
            .build();

        final long groupId = 9999L;
        Grouping dynamicGroup = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(params1)))))
                        .setId(groupId)
                        .build();

        when(searchResolver.search(SearchQuery.newBuilder()
            .setLogicalOperator(LogicalOperator.AND)
            .addSearchParameters(params1)
                .build(), topologyGraph))
            .thenReturn(Stream.of(topologyEntity(1L, EntityType.VIRTUAL_MACHINE).build()));

        groupResolver.resolve(dynamicGroup, topologyGraph);
        groupResolver.resolve(dynamicGroup, topologyGraph);
        groupResolver.resolve(dynamicGroup, topologyGraph);
        // The search resolver should only be called once as the subsequent calls will return
        // from the cache.
        verify(searchResolver, times(1)).search(any(SearchQuery.class), any());
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
        groupResolver.resolve(group, topologyGraph);
    }

}
