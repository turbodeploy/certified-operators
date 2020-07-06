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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.ImmutablePaginatedResults;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.TagIndex;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 * Unit tests for the search RPC service.
 */
public class TopologyGraphSearchRpcServiceTest {

    private static final SearchParameters SEARCH_PARAMS = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_MACHINE)).build();

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);
    private SearchResolver<RepoGraphEntity> mockSearchResolver = mock(SearchResolver.class);
    private LiveTopologyPaginator liveTopologyPaginator = mock(LiveTopologyPaginator.class);

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private TopologyGraphSearchRpcService topologyGraphSearchRpcService1 = new TopologyGraphSearchRpcService(liveTopologyStore,
            mockSearchResolver, liveTopologyPaginator, partialEntityConverter, userSessionContext, 1);

    /**
     * gRPC test server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(topologyGraphSearchRpcService1);

    /**
     * Argument captor.
     */
    @Captor
    public ArgumentCaptor<SearchQuery> paramsCaptor;

    private SearchServiceBlockingStub serviceClient;

    /**
     * Setup before each test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        serviceClient = SearchServiceGrpc.newBlockingStub(testServer.getChannel());

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

    /**
     * Search with scoped user should limit results to the scope.
     */
    @Test
    public void testSearchWithScopedUser() {
        RepoGraphEntity e1 = createEntity(1);
        RepoGraphEntity e2 = createEntity(2);
        when(mockSearchResolver.search(any(SearchQuery.class), any())).thenAnswer(invocation -> Stream.of(e1, e2));
        SearchEntitiesRequest req = SearchEntitiesRequest.newBuilder()
            .setSearch(SearchQuery.newBuilder()
                .addSearchParameters(SEARCH_PARAMS))
            .setReturnType(Type.MINIMAL)
            .build();

        final List<Long> resultOids = RepositoryDTOUtil.topologyEntityStream(
            serviceClient.searchEntitiesStream(req))
                .map(PartialEntity::getMinimal)
                .map(MinimalEntity::getOid)
                .collect(Collectors.toList());

        // verify the unscoped user can see all entities.
        assertThat(resultOids, containsInAnyOrder(1L, 2L));

        // Check same result as in the streaming call.
        assertThat(serviceClient.searchEntities(req).getEntitiesList().stream()
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList()), is(resultOids));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final List<Long> scopedResultOids = RepositoryDTOUtil.topologyEntityStream(
            serviceClient.searchEntitiesStream(req))
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList());
        // verify the scoped user can only see entity 2
        Assert.assertThat(scopedResultOids, containsInAnyOrder(2L));
        Assert.assertThat(scopedResultOids, not(containsInAnyOrder(1L)));
        // Check same result as in the streaming call.
        assertThat(serviceClient.searchEntities(req).getEntitiesList().stream()
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList()), is(scopedResultOids));
    }

    /**
     * Test entity search.
     */
    @Test
    public void testSearchEntitiesStream() {
        List<Long> entityOids = Arrays.asList(1L, 2L);

        RepoGraphEntity e1 = createEntity(1);
        when(mockSearchResolver.search(any(SearchQuery.class), any())).thenReturn(Stream.of(e1));

        List<MinimalEntity> entities = RepositoryDTOUtil.topologyEntityStream(serviceClient.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                .addAllEntityOid(entityOids)
                .setReturnType(Type.MINIMAL)
                .setSearch(SearchQuery.newBuilder()
                    .addSearchParameters(SEARCH_PARAMS))
                .build()))
            .map(PartialEntity::getMinimal)
            .collect(Collectors.toList());

        assertThat(entities.size(), is(1));

        verify(mockSearchResolver).search(paramsCaptor.capture(), any());
        SearchQuery params = paramsCaptor.getValue();
        assertThat(params.getSearchParametersList(), containsInAnyOrder(
            SearchProtoUtil.makeSearchParameters(SEARCH_PARAMS.getStartingFilter())
            .addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.idFilter(entityOids))).build()));
    }


    private void testTags(SearchTagsRequest request, LongSet expectedMatchedEntities, RepoGraphEntity... entities) {
        SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));

        DefaultTagIndex mockTags = mock(DefaultTagIndex.class);
        Map<String, Set<String>> expectedResponse = Collections.singletonMap("foo", Collections.singleton("bar"));
        when(mockTags.getTagsForEntities(any())).thenReturn(expectedResponse);
        when(topology.globalTags()).thenReturn(mockTags);

        TopologyGraph<RepoGraphEntity> graph = mock(TopologyGraph.class);
        when(graph.entities()).thenReturn(Stream.of(entities));
        for (RepoGraphEntity e : entities) {
            when(graph.getEntity(e.getOid())).thenReturn(Optional.of(e));
        }
        when(topology.entityGraph()).thenReturn(graph);

        final SearchTagsResponse response = serviceClient.searchTags(request);

        assertThat(response.getTags().getTagsMap().keySet(), is(expectedResponse.keySet()));
        assertThat(response.getTags().getTagsMap().get("foo").getValuesList(),
                containsInAnyOrder(expectedResponse.get("foo").toArray()));
        verify(mockTags).getTagsForEntities(expectedMatchedEntities);
    }

    /**
     * Test getting tags from entities with a specific environment type.
     */
    @Test
    public void testTagsEnvTypeFilter() {
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        RepoGraphEntity e1 = createEntity(1, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        RepoGraphEntity e2 = createEntity(2, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.ON_PREM);
        LongSet expectedMatched = new LongOpenHashSet();
        expectedMatched.add(e1.getOid());
        testTags(request, expectedMatched, e1, e2);
    }

    /**
     * Test getting tags from all entities.
     */
    @Test
    public void testTagsAll() {
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .build();
        RepoGraphEntity e1 = createEntity(1, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        RepoGraphEntity e2 = createEntity(2, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);


        LongSet expectedMatched = new LongOpenHashSet();
        expectedMatched.add(e1.getOid());
        expectedMatched.add(e2.getOid());
        testTags(request, expectedMatched, e1, e2);
    }

    /**
     * Test getting tags from "all" entities of a scoped user.
     */
    @Test
    public void testTagsScopedUser() {
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .build();
        final RepoGraphEntity e1 = createEntity(1, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        final RepoGraphEntity e2 = createEntity(2, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        // now set up a scoped user and try again
        final List<Long> accessibleEntities = Arrays.asList(e2.getOid());
        final EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        LongSet expectedMatched = new LongOpenHashSet();
        expectedMatched.add(e2.getOid());
        testTags(request, expectedMatched, e1, e2);
    }

    /**
     * Test getting tags from specific entities.
     */
    @Test
    public void testTagsEntityIdFilter() {

        RepoGraphEntity e1 = createEntity(1, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        RepoGraphEntity e2 = createEntity(2, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.CLOUD);
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .addEntities(e2.getOid())
                .build();
        LongSet expectedMatched = new LongOpenHashSet();
        expectedMatched.add(e2.getOid());
        testTags(request, expectedMatched, e1, e2);
    }

    /**
     * Test getting tags from entities of a specific type.
     */
    @Test
    public void testTagsEntityTypeFilter() {
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                .build();
        RepoGraphEntity e1 = createEntity(1, ApiEntityType.PHYSICAL_MACHINE, EnvironmentType.ON_PREM);
        RepoGraphEntity e2 = createEntity(2, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.ON_PREM);
        LongSet expectedMatched = new LongOpenHashSet();
        expectedMatched.add(e1.getOid());
        testTags(request, expectedMatched, e1, e2);
    }



    private RepoGraphEntity createEntity(long oid, ApiEntityType type, EnvironmentType environmentType) {
        final RepoGraphEntity e1 = mock(RepoGraphEntity.class);
        when(e1.getOid()).thenReturn(oid);
        when(e1.getEntityType()).thenReturn(type.typeNumber());
        when(e1.getDisplayName()).thenReturn(Long.toString(oid));
        when(e1.getEnvironmentType()).thenReturn(environmentType);
        TagIndex tags = DefaultTagIndex.singleEntity(oid, Tags.newBuilder()
            .putTags(String.valueOf(oid), TagValuesDTO.newBuilder()
                    .addValues(String.valueOf(oid))
                    .build())
                .build());
        SearchableProps mockProps = mock(SearchableProps.class);
        when(mockProps.getTagIndex()).thenReturn(tags);
        when(e1.getSearchableProps()).thenReturn(mockProps);
        when(e1.getTopologyEntity()).thenReturn(createTestEntity(oid, environmentType));
        when(e1.getDiscoveringTargetIds()).thenAnswer(invocation -> Stream.empty());
        when(e1.getEntityState()).thenReturn(EntityState.POWERED_ON);
        return e1;
    }

    private RepoGraphEntity createEntity(long oid) {
        return createEntity(oid, ApiEntityType.VIRTUAL_MACHINE, EnvironmentType.UNKNOWN_ENV);
    }

    private TopologyEntityDTO createTestEntity(long oid, EnvironmentType environmentType) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(1)
                .setEnvironmentType(environmentType)
                .setTags(Tags.newBuilder()
                        .putTags(String.valueOf(oid), TagValuesDTO.newBuilder()
                                .addValues(String.valueOf(oid))
                                .build()))
                .build();
    }
}
