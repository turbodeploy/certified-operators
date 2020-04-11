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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;

/**
 *
 */
public class TopologyGraphSearchRpcServiceTest {

    private static final SearchParameters SEARCH_PARAMS = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_MACHINE)).build();

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);
    private SearchResolver<RepoGraphEntity> mockSearchResolver = mock(SearchResolver.class);
    private SearchResolver<RepoGraphEntity> realSearchResolver =
            new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
    private LiveTopologyPaginator liveTopologyPaginator = mock(LiveTopologyPaginator.class);

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private TopologyGraphSearchRpcService topologyGraphSearchRpcService1 = new TopologyGraphSearchRpcService(liveTopologyStore,
            mockSearchResolver, liveTopologyPaginator, partialEntityConverter, userSessionContext, 1);
    private TopologyGraphSearchRpcService topologyGraphSearchRpcService2 = new TopologyGraphSearchRpcService(liveTopologyStore,
            realSearchResolver, liveTopologyPaginator, partialEntityConverter, userSessionContext, 1);

    @Rule
    public GrpcTestServer server1 = GrpcTestServer.newServer(topologyGraphSearchRpcService1);

    @Rule
    public GrpcTestServer server2 = GrpcTestServer.newServer(topologyGraphSearchRpcService2);

    @Captor
    public ArgumentCaptor<SearchQuery> paramsCaptor;

    private SearchServiceBlockingStub client1;
    private SearchServiceBlockingStub client2;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        client1 = SearchServiceGrpc.newBlockingStub(server1.getChannel());
        client2 = SearchServiceGrpc.newBlockingStub(server2.getChannel());

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
        when(mockSearchResolver.search(any(SearchQuery.class), any())).thenAnswer(invocation -> Stream.of(e1, e2));
        SearchEntitiesRequest req = SearchEntitiesRequest.newBuilder()
            .setSearch(SearchQuery.newBuilder()
                .addSearchParameters(SEARCH_PARAMS))
            .setReturnType(Type.MINIMAL)
            .build();

        final List<Long> resultOids = RepositoryDTOUtil.topologyEntityStream(
            client1.searchEntitiesStream(req))
                .map(PartialEntity::getMinimal)
                .map(MinimalEntity::getOid)
                .collect(Collectors.toList());

        // verify the unscoped user can see all entities.
        assertThat(resultOids, containsInAnyOrder(1L,2L));

        // Check same result as in the streaming call.
        assertThat(client1.searchEntities(req).getEntitiesList().stream()
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList()), is(resultOids));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final List<Long> scopedResultOids = RepositoryDTOUtil.topologyEntityStream(
            client1.searchEntitiesStream(req))
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toList());
        // verify the scoped user can only see entity 2
        Assert.assertThat(scopedResultOids, containsInAnyOrder(2L));
        Assert.assertThat(scopedResultOids, not(containsInAnyOrder(1L)));
        // Check same result as in the streaming call.
        assertThat(client1.searchEntities(req).getEntitiesList().stream()
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

        SearchTagsResponse response = client1.searchTags(request);

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
        tags = client1.searchTags(request).getTags().getTagsMap();
        Assert.assertFalse(tags.containsKey("1"));
        Assert.assertTrue(tags.containsKey("2"));
        Assert.assertFalse(tags.containsKey("3"));
    }

    @Test
    public void testSearchEntitiesStream() {
        List<Long> entityOids = Arrays.asList(1L, 2L);

        RepoGraphEntity e1 = createEntity(1);
        when(mockSearchResolver.search(any(SearchQuery.class), any())).thenReturn(Stream.of(e1));

        List<MinimalEntity> entities = RepositoryDTOUtil.topologyEntityStream(client1.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
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

    /**
     * Test getting tags with environment type filter HYBRID.
     */
    @Test
    public void testTagsWithHybridEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.HYBRID, this::requestTagsFromEnvironment);
    }

    /**
     * Test getting tags with environment type filter CLOUD.
     */
    @Test
    public void testTagsWithCloudEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.CLOUD, this::requestTagsFromEnvironment);
    }

    /**
     * Test getting tags with environment type filter ON_PREM.
     */
    @Test
    public void testTagsWithOnPremEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.ON_PREM, this::requestTagsFromEnvironment);
    }

    /**
     * Test getting tags with unknown environment type filter.
     */
    @Test
    public void testTagsWithUnknownEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.UNKNOWN_ENV, this::requestTagsFromEnvironment);
    }

    /**
     * Test getting entities with environment type filter HYBRID.
     */
    @Test
    public void testEntitiesWithHybridEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.HYBRID, this::requestEntitiesFromEnvironment);
    }

    /**
     * Test getting entities with environment type filter CLOUD.
     */
    @Test
    public void testEntitiesWithCloudEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.CLOUD, this::requestEntitiesFromEnvironment);
    }

    /**
     * Test getting entities with environment type filter ON_PREM.
     */
    @Test
    public void testEntitiesWithOnPremEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.ON_PREM, this::requestEntitiesFromEnvironment);
    }

    /**
     * Test getting entities with unknown environment type filter.
     */
    @Test
    public void testEntitiesWithUnknownEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.UNKNOWN_ENV, this::requestEntitiesFromEnvironment);
    }



    /**
     * Test getting entity oids with environment type filter HYBRID.
     */
    @Test
    public void testEntityOidsWithHybridEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.HYBRID, this::requestEntityOidsFromEnvironment);
    }

    /**
     * Test getting entitiy oids with environment type filter CLOUD.
     */
    @Test
    public void testEntityOidsWithCloudEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.CLOUD, this::requestEntityOidsFromEnvironment);
    }

    /**
     * Test getting entitiy oids with environment type filter ON_PREM.
     */
    @Test
    public void testEntityOidsWithOnPremEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.ON_PREM, this::requestEntityOidsFromEnvironment);
    }

    /**
     * Test getting entity oids with unknown environment type filter.
     */
    @Test
    public void testEntityOidsWithUnknownEnvironmentFilter() {
        testFilteringByEnvironmentType(EnvironmentType.UNKNOWN_ENV, this::requestEntityOidsFromEnvironment);
    }

    private RepoGraphEntity createEntity(long oid, EnvironmentType environmentType) {
        final RepoGraphEntity e1 = mock(RepoGraphEntity.class);
        when(e1.getOid()).thenReturn(oid);
        when(e1.getEntityType()).thenReturn(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
        when(e1.getDisplayName()).thenReturn(Long.toString(oid));
        when(e1.getEnvironmentType()).thenReturn(environmentType);
        when(e1.getTags()).thenReturn(ImmutableMap.of(String.valueOf(oid), Collections.singletonList(String.valueOf(oid))));
        when(e1.getTopologyEntity()).thenReturn(createTestEntity(oid, environmentType));
        when(e1.getDiscoveringTargetIds()).thenAnswer(invocation -> Stream.empty());
        return e1;
    }

    private RepoGraphEntity createEntity(long oid) {
        return createEntity(oid, EnvironmentType.UNKNOWN_ENV);
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

    private Set<Long> requestEntitiesFromEnvironment(@Nonnull EnvironmentType environmentType) {
        final SearchParameters searchParameters =
            SearchParameters.newBuilder()
                .setStartingFilter(
                    PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ENTITY_TYPE)
                        .setNumericFilter(NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                .setValue(ApiEntityType.VIRTUAL_MACHINE.typeNumber())))
                .addSearchFilter(
                    SearchFilter.newBuilder()
                        .setPropertyFilter(
                            PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.ENVIRONMENT_TYPE)
                                .setStringFilter(
                                    StringFilter.newBuilder()
                                        .addOptions(EnvironmentTypeUtil.toApiString(environmentType)))))
                .build();
        final SearchEntitiesRequest request = SearchEntitiesRequest.newBuilder()
            .setReturnType(Type.FULL)
            .setSearch(SearchQuery.newBuilder()
                .addSearchParameters(searchParameters))
            .build();
        final SearchEntitiesResponse response = client2.searchEntities(request);

        return response.getEntitiesList().stream()
                    .map(PartialEntity::getFullEntity)
                    .map(TopologyEntityDTO::getOid)
                    .collect(Collectors.toSet());
    }

    private Set<Long> requestEntityOidsFromEnvironment(@Nonnull EnvironmentType environmentType) {
        final SearchParameters searchParameters =
            SearchParameters.newBuilder()
                .setStartingFilter(
                    PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ENTITY_TYPE)
                        .setNumericFilter(NumericFilter.newBuilder()
                                            .setComparisonOperator(ComparisonOperator.EQ)
                                            .setValue(ApiEntityType.VIRTUAL_MACHINE.typeNumber())))
                .addSearchFilter(
                    SearchFilter.newBuilder()
                        .setPropertyFilter(
                            PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.ENVIRONMENT_TYPE)
                                .setStringFilter(
                                    StringFilter.newBuilder()
                                        .addOptions(EnvironmentTypeUtil.toApiString(environmentType)))))
                .build();
        final SearchEntityOidsRequest request = SearchEntityOidsRequest.newBuilder()
            .setSearch(SearchQuery.newBuilder()
                .addSearchParameters(searchParameters))
            .build();
        final SearchEntityOidsResponse response = client2.searchEntityOids(request);

        return response.getEntitiesList().stream()
                .collect(Collectors.toSet());
    }

    private Set<Long> requestTagsFromEnvironment(@Nonnull EnvironmentType environmentType) {
        final SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .setEnvironmentType(environmentType)
                .build();
        final SearchTagsResponse response = client2.searchTags(request);

        return response.getTags().getTagsMap().keySet().stream()
                .map(Long::valueOf)
                .collect(Collectors.toSet());
    }

    private void testFilteringByEnvironmentType(@Nonnull EnvironmentType environmentType,
                                                @Nonnull Function<EnvironmentType, Set<Long>> request) {
        final Map<EnvironmentType, Set<Long>> expectedAnswers =
                ImmutableMap.of(EnvironmentType.HYBRID, ImmutableSet.of(1L, 2L, 3L, 4L),
                                EnvironmentType.CLOUD, ImmutableSet.of(2L, 4L),
                                EnvironmentType.ON_PREM, ImmutableSet.of(3L, 4L),
                                EnvironmentType.UNKNOWN_ENV, Collections.singleton(1L));

        SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));

        RepoGraphEntity e1 = createEntity(1);
        RepoGraphEntity e2 = createEntity(2, EnvironmentType.CLOUD);
        RepoGraphEntity e3 = createEntity(3, EnvironmentType.ON_PREM);
        RepoGraphEntity e4 = createEntity(4, EnvironmentType.HYBRID);

        TopologyGraph<RepoGraphEntity> graph = mock(TopologyGraph.class);
        when(graph.getEntity(1L)).thenReturn(Optional.of(e1));
        when(graph.getEntity(2L)).thenReturn(Optional.of(e2));
        when(graph.getEntity(3L)).thenReturn(Optional.of(e3));
        when(graph.getEntity(4L)).thenReturn(Optional.of(e4));
        when(graph.entities()).thenReturn(Stream.of(e1, e2, e3, e4));
        when(topology.entityGraph()).thenReturn(graph);
        when(graph.entitiesOfType(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                .thenReturn(Stream.of(e1, e2, e3, e4));

        Assert.assertEquals(expectedAnswers.get(environmentType),
                            request.apply(environmentType));
    }
}
