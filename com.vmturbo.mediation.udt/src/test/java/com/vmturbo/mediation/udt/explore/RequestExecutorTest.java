package com.vmturbo.mediation.udt.explore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionMoles.TopologyDataDefinitionServiceMole;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesResponse;
import com.vmturbo.common.protobuf.search.Search.TaggedEntities;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Test class for {@link RequestExecutor}.
 */
public class RequestExecutorTest {

    private static final long PROBE_ID = 1234567L;
    private static final String TAG1 = "tag1";
    private static final String TAG2 = "tag2";

    private Connection connection;
    private GrpcTestServer groupServer;
    private GrpcTestServer repositoryServer;
    private GrpcTestServer tpServer;
    private TopologyDataDefinitionServiceMole tddMole;
    private RepositoryServiceMole rsMole;
    private GroupServiceMole groupMole;
    private SearchServiceMole searchMole;
    private TopologyProcessor topologyProcessorMock;

    /**
     * Initialize request executor.
     *
     * @throws IOException on exception starting gRPC in-memory servers
     */
    @Before
    public void setup() throws IOException {
        tddMole = Mockito.spy(new TopologyDataDefinitionServiceMole());
        rsMole = new RepositoryServiceMole();
        groupMole = Mockito.spy(new GroupServiceMole());
        searchMole = Mockito.spy(new SearchServiceMole());
        topologyProcessorMock = Mockito.mock(TopologyProcessor.class);
        groupServer = GrpcTestServer.newServer(tddMole, groupMole);
        groupServer.start();
        repositoryServer = GrpcTestServer.newServer(rsMole, searchMole);
        repositoryServer.start();
        tpServer = GrpcTestServer.newServer();
        tpServer.start();

        connection = Mockito.spy(
                new Connection(groupServer.getChannel(), repositoryServer.getChannel(),
                        tpServer.getChannel(), topologyProcessorMock));
    }

    /**
     * Cleans up tests.
     */
    @After
    public void cleanup() {
        groupServer.close();
        repositoryServer.close();
        tpServer.close();
    }

    /**
     * The method tests that 'RequestExecutor' correctly calls Connection`s channels.
     */
    @Test
    public void testCreateServices() {
        new RequestExecutor(connection);
        Mockito.verify(connection, Mockito.times(2)).getGroupChannel();
        Mockito.verify(connection, Mockito.times(2)).getRepositoryChannel();
    }

    /**
     * The method tests that services uses correct gRPC channels.
     */
    @Test
    public void testChannelsUsage() {
        RequestExecutor requestExecutor = new RequestExecutor(connection);
        Assert.assertEquals(groupServer.getChannel(), requestExecutor.getGroupService().getChannel());
        Assert.assertEquals(groupServer.getChannel(), requestExecutor.getTopologyDataDefService().getChannel());
        Assert.assertEquals(repositoryServer.getChannel(), requestExecutor.getSearchService().getChannel());
        Assert.assertEquals(repositoryServer.getChannel(), requestExecutor.getRepositoryService().getChannel());
    }

    /**
     * Tests for {@link RequestExecutor#getGroupIds} method.
     */
    @Test
    public void testGetGroupIds() {
        Set<Long> expectedGroupIds = new LinkedHashSet<>();
        expectedGroupIds.add(1L);
        expectedGroupIds.add(2L);

        List<Grouping> groups = expectedGroupIds.stream()
                .map(id -> Grouping.newBuilder().setId(id).build())
                .collect(Collectors.toList());

        GroupDTO.GetGroupsRequest request = GroupDTO.GetGroupsRequest.newBuilder()
                .build();

        Mockito.when(groupMole.getGroups(request)).thenReturn(groups);

        RequestExecutor requestExecutor = new RequestExecutor(connection);

        Set<Long> groupIds = requestExecutor.getGroupIds(request);

        Mockito.verify(groupMole).getGroups(request);

        Assert.assertEquals(expectedGroupIds, groupIds);
    }

    /**
     * Test for {@link RequestExecutor#getGroupMembers}.
     */
    @Test
    public void testGetGroupMembers() {
        GetMembersRequest request = GetMembersRequest.newBuilder().build();
        List<GetMembersResponse> responses = Collections.singletonList(
                GetMembersResponse.newBuilder().setGroupId(1L).build());

        Mockito.when(groupMole.getMembers(request)).thenReturn(responses);

        RequestExecutor requestExecutor = new RequestExecutor(connection);
        Iterator<GetMembersResponse> groupMembers = requestExecutor.getGroupMembers(request);

        Assert.assertTrue(groupMembers.hasNext());
        Assert.assertEquals(1L, groupMembers.next().getGroupId());
        Assert.assertFalse(groupMembers.hasNext());

        Mockito.verify(groupMole).getMembers(request);
    }

    /**
     * Test for {@link RequestExecutor#getAllTopologyDataDefinitions}.
     */
    @Test
    public void testGetAllTopologyDefinitions() {
        GetTopologyDataDefinitionsRequest request = GetTopologyDataDefinitionsRequest.newBuilder().build();
        List<GetTopologyDataDefinitionResponse> responses = Collections.singletonList(
                GetTopologyDataDefinitionResponse.newBuilder().build());

        Mockito.when(tddMole.getAllTopologyDataDefinitions(request)).thenReturn(responses);

        RequestExecutor requestExecutor = new RequestExecutor(connection);
        Iterator<GetTopologyDataDefinitionResponse> topologyDefinitions =
                requestExecutor.getAllTopologyDataDefinitions(request);

        Assert.assertTrue(topologyDefinitions.hasNext());
        Assert.assertNotNull(topologyDefinitions.next());
        Assert.assertFalse(topologyDefinitions.hasNext());

        Mockito.verify(tddMole).getAllTopologyDataDefinitions(request);
    }

    /**
     * Test for {@link RequestExecutor#getOwnersOfGroups}.
     */
    @Test
    public void testGetOwnerOfGroups() {
        GetOwnersRequest request = GetOwnersRequest.newBuilder().build();

        Mockito.when(groupMole.getOwnersOfGroups(request)).thenReturn(
                GetOwnersResponse.newBuilder().addOwnerId(1L).build());

        RequestExecutor requestExecutor = new RequestExecutor(connection);

        Set<Long> ownerIds = requestExecutor.getOwnersOfGroups(request);

        Mockito.verify(groupMole).getOwnersOfGroups(request);

        Assert.assertNotNull(ownerIds);
        Assert.assertEquals(1, ownerIds.size());
        Assert.assertEquals(Long.valueOf(1L), ownerIds.iterator().next());
    }

    /**
     * Test {@link RequestExecutor#getTagValues}.
     */
    @Test
    public void testGetTagValues() {
        SearchTagValuesRequest request = SearchTagValuesRequest.newBuilder().build();
        SearchTagValuesResponse response = SearchTagValuesResponse.newBuilder()
                .putEntitiesByTagValue(TAG1, TaggedEntities.newBuilder().addOid(1L).build())
                .putEntitiesByTagValue(TAG2, TaggedEntities.newBuilder().addOid(2L).build())
                .build();

        Mockito.when(searchMole.searchTagValues(request)).thenReturn(response);

        RequestExecutor requestExecutor = new RequestExecutor(connection);

        Map<String, TaggedEntities> tagValues = requestExecutor.getTagValues(request);

        Assert.assertNotNull(tagValues);
        Assert.assertEquals(2, tagValues.size());
        Assert.assertNotNull(tagValues.get(TAG1));
        Assert.assertEquals(1, tagValues.get(TAG1).getOidCount());
        Assert.assertEquals(1L, tagValues.get(TAG1).getOid(0));
        Assert.assertNotNull(tagValues.get(TAG2));
        Assert.assertEquals(1, tagValues.get(TAG2).getOidCount());
        Assert.assertEquals(2L, tagValues.get(TAG2).getOid(0));
    }

    /**
     * Test for {@link RequestExecutor#findTarget}.
     *
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testFindTarget() throws CommunicationException {
        Set<TargetInfo> allTargets = new HashSet<>();
        allTargets.addAll(TestUtils.createMockedTargets(PROBE_ID, 10L, 11L, 12L));
        allTargets.addAll(TestUtils.createMockedTargets(9983L, 20L, 21L, 22L, 23L));

        Mockito.when(topologyProcessorMock.getAllTargets()).thenReturn(allTargets);

        RequestExecutor requestExecutor = new RequestExecutor(connection);
        List<TargetInfo> actualTargets = requestExecutor.findTarget(PROBE_ID);

        Set<Long> expectedTargetIds = Sets.newHashSet(10L, 11L, 12L);
        Assert.assertNotNull(actualTargets);
        Assert.assertEquals(expectedTargetIds.size(), actualTargets.size());
        Assert.assertEquals(expectedTargetIds, actualTargets.stream()
                .map(TargetInfo::getId).collect(Collectors.toSet()));

        Mockito.verify(topologyProcessorMock).getAllTargets();
    }

    /**
     * Test for {@link RequestExecutor#findProbe}.
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testFindProbe() throws CommunicationException {
        Set<ProbeInfo> allProbes = Sets.newHashSet(TestUtils.mockProbeInfo(1L, SDKProbeType.AWS),
                TestUtils.mockProbeInfo(2L, SDKProbeType.UDT),
                TestUtils.mockProbeInfo(3L, SDKProbeType.NEWRELIC));

        Mockito.when(topologyProcessorMock.getAllProbes()).thenReturn(allProbes);

        RequestExecutor requestExecutor = new RequestExecutor(connection);
        ProbeInfo probe = requestExecutor.findProbe(SDKProbeType.UDT);

        Assert.assertNotNull(probe);
        Assert.assertEquals(SDKProbeType.UDT.getProbeType(), probe.getType());
        Assert.assertEquals(2L, probe.getId());

        Mockito.verify(topologyProcessorMock).getAllProbes();
    }

    /**
     * Test pagination when getting the entities.
     */
    @Test
    public void testSearchEntities() {
        final String nextCursor = "3";

        // first request - no pagination parameter
        SearchEntitiesRequest request1 = SearchEntitiesRequest.newBuilder()
                .setSearch(Search.SearchQuery.newBuilder()
                    .addAllSearchParameters(Collections.singletonList(SearchParameters.newBuilder().build()))
                    .build())
                .build();
        // first response - has pagination cursor
        SearchEntitiesResponse response1 = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.newBuilder()
                        .setNextCursor(nextCursor)
                        .build())
                .addAllEntities(Arrays.asList(
                        createPartialEntity(1L),
                        createPartialEntity(2L),
                        createPartialEntity(3L)))
                .build();
        // second request - send cursor in pagination parameter
        SearchEntitiesRequest request2 = SearchEntitiesRequest.newBuilder()
                .mergeFrom(request1)
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor(nextCursor)
                        .build())
                .build();
        // second response - pagination finished
        SearchEntitiesResponse response2 = SearchEntitiesResponse.newBuilder()
                .addAllEntities(Arrays.asList(
                        createPartialEntity(4L),
                        createPartialEntity(5L)))
                .build();

        Mockito.when(searchMole.searchEntities(request1)).thenReturn(response1);
        Mockito.when(searchMole.searchEntities(request2)).thenReturn(response2);
        final RequestExecutor requestExecutor = new RequestExecutor(connection);

        SearchEntitiesResponse response = requestExecutor.searchEntities(request1);

        Mockito.verify(searchMole, Mockito.times(2))
                .searchEntities(Mockito.any());
        Assert.assertEquals(5, response.getEntitiesCount());
    }

    /**
     * Create a partial minimal entity.
     *
     * @param oid the id of the entity.
     * @return a partial entity with the given ID.
     */
    private PartialEntity createPartialEntity(long oid) {
        return PartialEntity.newBuilder()
                .setMinimal(MinimalEntity.newBuilder()
                        .setOid(oid)
                        .build())
                .build();
    }
}
