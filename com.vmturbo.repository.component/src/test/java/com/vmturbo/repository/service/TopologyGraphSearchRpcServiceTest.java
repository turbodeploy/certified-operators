package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 *
 */
public class TopologyGraphSearchRpcServiceTest {

    private static final SearchParameters SEARCH_PARAMS = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(UIEntityType.VIRTUAL_MACHINE)).build();

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);
    private SearchResolver<RepoGraphEntity> searchResolver = mock(SearchResolver.class);
    private LiveTopologyPaginator liveTopologyPaginator = mock(LiveTopologyPaginator.class);

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);


    private TopologyGraphSearchRpcService topologyGraphSearchRpcService = new TopologyGraphSearchRpcService(liveTopologyStore,
        searchResolver, liveTopologyPaginator, partialEntityConverter, userSessionContext, 1);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(topologyGraphSearchRpcService);

    @Captor
    public ArgumentCaptor<List<SearchParameters>> paramsCaptor;

    private SearchServiceBlockingStub client;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        client = SearchServiceGrpc.newBlockingStub(server.getChannel());

        SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));
        when(liveTopologyPaginator.paginate(any(), any()))
            .thenAnswer(invocation -> {
                Stream<RepoGraphEntity> e = invocation.getArgumentAt(0, Stream.class);
                ImmutablePaginatedResults.Builder bldr = ImmutablePaginatedResults.builder()
                    .paginationResponse(PaginationResponse.getDefaultInstance());
                e.forEach(bldr::addNextPageEntities);
                return bldr.build();
            });
    }

    @Test
    public void testSearchWithScopedUser() {
        RepoGraphEntity e1 = createEntity(1);
        RepoGraphEntity e2 = createEntity(2);
        when(searchResolver.search(any(List.class), any())).thenAnswer(invocation -> Stream.of(e1, e2));
        SearchEntitiesRequest req = SearchEntitiesRequest.newBuilder()
            .addSearchParameters(SEARCH_PARAMS)
            .setReturnType(Type.MINIMAL)
            .build();

        final List<Long> resultOids = RepositoryDTOUtil.topologyEntityStream(
            client.searchEntitiesStream(req))
                .map(PartialEntity::getMinimal)
                .map(MinimalEntity::getOid)
                .collect(Collectors.toList());

        // verify the unscoped user can see all entities.
        assertThat(resultOids, containsInAnyOrder(1L,2L));

        // Check same result as in the streaming call.
        assertThat(client.searchEntities(req).getEntitiesList().stream()
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList()), is(resultOids));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final List<Long> scopedResultOids = RepositoryDTOUtil.topologyEntityStream(
            client.searchEntitiesStream(req))
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList());
        // verify the scoped user can only see entity 2
        Assert.assertThat(scopedResultOids, containsInAnyOrder(2L));
        Assert.assertThat(scopedResultOids, not(containsInAnyOrder(1L)));
        // Check same result as in the streaming call.
        assertThat(client.searchEntities(req).getEntitiesList().stream()
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList()), is(scopedResultOids));
    }

    @Test
    public void testSearchTagsWithScopedUser() {
        SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));

        RepoGraphEntity e1 = createEntity(1);
        RepoGraphEntity e2 = createEntity(2);
        RepoGraphEntity e3 = createEntity(3);

        TopologyGraph<RepoGraphEntity> graph = mock(TopologyGraph.class);
        when(graph.getEntity(1L)).thenReturn(Optional.of(e1));
        when(graph.getEntity(2L)).thenReturn(Optional.of(e2));
        when(graph.getEntity(3L)).thenReturn(Optional.of(e3));
        when(topology.entityGraph()).thenReturn(graph);

        SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .addAllEntities(Arrays.asList(1L, 2L, 3L))
                .build();

        SearchTagsResponse response = client.searchTags(request);

        // verify that the non-scoped user can see all tags.
        Map<String, TagValuesDTO> tags = response.getTags().getTagsMap();
        Assert.assertTrue(tags.containsKey("1"));
        Assert.assertTrue(tags.containsKey("2"));
        Assert.assertTrue(tags.containsKey("3"));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        // verify that the non-scoped user can only see tag #2.
        tags = client.searchTags(request).getTags().getTagsMap();
        Assert.assertFalse(tags.containsKey("1"));
        Assert.assertTrue(tags.containsKey("2"));
        Assert.assertFalse(tags.containsKey("3"));
    }

    @Test
    public void testSearchEntitiesStream() {
        List<Long> entityOids = Arrays.asList(1L, 2L);

        RepoGraphEntity e1 = createEntity(1);
        when(searchResolver.search(any(List.class), any())).thenReturn(Stream.of(e1));

        List<MinimalEntity> entities = RepositoryDTOUtil.topologyEntityStream(client.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                .addAllEntityOid(entityOids)
                .setReturnType(Type.MINIMAL)
                .addSearchParameters(SEARCH_PARAMS)
                .build()))
            .map(PartialEntity::getMinimal)
            .collect(Collectors.toList());

        assertThat(entities.size(), is(1));

        verify(searchResolver).search(paramsCaptor.capture(), any());
        List<SearchParameters> params = paramsCaptor.getValue();
        assertThat(params, containsInAnyOrder(
            SearchProtoUtil.makeSearchParameters(SEARCH_PARAMS.getStartingFilter())
            .addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.idFilter(entityOids))).build()));
    }

    private RepoGraphEntity createEntity(long oid) {
        final RepoGraphEntity e1 = mock(RepoGraphEntity.class);
        when(e1.getOid()).thenReturn(oid);
        when(e1.getEntityType()).thenReturn(UIEntityType.VIRTUAL_MACHINE.typeNumber());
        when(e1.getDisplayName()).thenReturn(Long.toString(oid));
        when(e1.getEnvironmentType()).thenReturn(EnvironmentType.ON_PREM);
        when(e1.getTags()).thenReturn(ImmutableMap.of(String.valueOf(oid), Collections.singletonList(String.valueOf(oid))));
        when(e1.getTopologyEntity()).thenReturn(createTestEntity(oid));
        when(e1.getDiscoveringTargetIds()).thenAnswer(invocation -> Stream.empty());
        return e1;
    }

    private TopologyEntityDTO createTestEntity(long oid) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(1)
                .setTags(Tags.newBuilder()
                        .putTags(String.valueOf(oid), TagValuesDTO.newBuilder()
                                .addValues(String.valueOf(oid))
                                .build()))
                .build();
    }


}
