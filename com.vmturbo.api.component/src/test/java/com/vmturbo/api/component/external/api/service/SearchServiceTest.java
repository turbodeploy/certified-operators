package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.ACCOUNT_OID;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.CONNECTED_COMPUTE_TIER_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.CONNECTED_STORAGE_TIER_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.STATE;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.USER_DEFINED_ENTITY;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.VOLUME_ATTACHMENT_STATE_FILTER_PATH;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getNonPaginatedSearchResults;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getPaginatedSearchResults;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedSearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.Origin;
import com.vmturbo.api.enums.QueryType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.pagination.TagPaginationRequest;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.search.UIBooleanFilter;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit test for {@link SearchService}.
 */
public class SearchServiceTest {

    private static final String DISCOVEREDBY = "discoveredBy:";
    private static final String CLOUD_PROVIDER_OPTION =
                    DISCOVEREDBY + SearchableProperties.CLOUD_PROVIDER;
    private static final String TARGET_TYPE_OPTION =
                    DISCOVEREDBY + SearchableProperties.PROBE_TYPE;

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();
    private SearchService searchService;
    private final ThinTargetCache thinTargetCacheMock = mock(ThinTargetCache.class);
    private MarketsService marketsService = mock(MarketsService.class);
    private GroupsService groupsService = mock(GroupsService.class);
    private TargetsService targetsService = mock(TargetsService.class);
    private RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private final GroupUseCaseParser groupUseCaseParser =
            new GroupUseCaseParser("groupBuilderUsecases.json");
    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);
    private final UuidMapper uuidMapper = mock(UuidMapper.class);
    private final GroupExpander groupExpander = mock(GroupExpander.class);
    private final PaginationMapper paginationMapperSpy = spy(new PaginationMapper());
    private final TagsService tagsService = mock(TagsService.class);
    private final BusinessAccountRetriever businessAccountRetriever = mock(BusinessAccountRetriever.class);
    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private final EntityFilterMapper entityFilterMapper =
            new EntityFilterMapper(groupUseCaseParser, thinTargetCacheMock);
    private final EntityAspectMapper entityAspectMapper = mock(EntityAspectMapper.class);
    private final PriceIndexPopulator priceIndexPopulator = mock(PriceIndexPopulator.class);

    private SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
    private EntitySeverityServiceMole entitySeverityServiceSpy = Mockito.spy(new EntitySeverityServiceMole());
    private StatsHistoryServiceMole historyServiceSpy = Mockito.spy(new StatsHistoryServiceMole());
    private ActionsServiceMole actionOrchestratorRpcService = new ActionsServiceMole();
    private GroupServiceMole groupRpcService = new GroupServiceMole();
    private CostMoles.CostServiceMole costRpcService = spy(new CostMoles.CostServiceMole());

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(
            searchServiceSpy, entitySeverityServiceSpy, historyServiceSpy,
            actionOrchestratorRpcService, groupRpcService, costRpcService);

    private static final long ENTITY_ID_1 = 1L;
    private static final long ENTITY_ID_2 = 2L;
    private static final long ENTITY_ID_3 = 3L;
    private static final long ENTITY_ID_4 = 4L;

    private final long targetId1 = 111L;
    private final long targetId2 = 112L;
    private final String probeType1 = SDKProbeType.AWS.getProbeType();
    private final String probeType2 = SDKProbeType.AZURE.getProbeType();

    // Map for testing regex handling by query type
    private ImmutableMap<String, ImmutableMap<QueryType, String>> nameQueryMap =
            ImmutableMap.<String, ImmutableMap<QueryType, String>>builder()
                    .put(".*win.*", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*win.*$")
                            .put(QueryType.EXACT, "^\\.\\*win\\.\\*$")
                            .put(QueryType.REGEX, "^.*win.*$")
                            .build())

                    .put(".*win (.*", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*win \\(.*$")
                            .put(QueryType.EXACT, "^\\.\\*win \\(\\.\\*$")
                            .put(QueryType.REGEX, "^.*win (.*$")
                            .build())
                    .put("win (", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*win \\(.*$")
                            .put(QueryType.EXACT, "^win \\($")
                            .put(QueryType.REGEX, "^win ($")
                            .build())
                    .put("win (=| .*", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*win \\(=\\| .*$")
                            .put(QueryType.EXACT, "^win \\(=\\| \\.\\*$")
                            .put(QueryType.REGEX, "^win (=| .*$")
                            .build())
                    .build();

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);
    private ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    @Before
    public void setUp() throws Exception {
        final long realTimeContextId = 777777;
        final SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final StatsHistoryServiceBlockingStub statsHistoryServiceStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final EntitySeverityServiceBlockingStub severityGrpcStub =
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final GroupServiceBlockingStub groupServiceBlockingStub =
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final SearchFilterResolver
                searchFilterResolver = Mockito.mock(SearchFilterResolver.class);
        Mockito.when(searchFilterResolver.resolveExternalFilters(Mockito.any()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);
        when(userSessionContext.isUserScoped()).thenReturn(false);
        searchService = spy(new SearchService(
                repositoryApi,
                marketsService,
                groupsService,
                targetsService,
                searchGrpcStub,
                severityGrpcStub,
                severityPopulator,
                statsHistoryServiceStub,
                groupExpander,
                paginationMapperSpy,
                groupUseCaseParser,
                tagsService,
                businessAccountRetriever,
                realTimeContextId,
                userSessionContext,
                groupServiceBlockingStub,
                serviceEntityMapper,
                entityFilterMapper,
                entityAspectMapper,
                searchFilterResolver,
                priceIndexPopulator,
                thinTargetCacheMock,
                DEFAULT_LIMIT,
                MAX_LIMIT));
    }

    /**
     * Test the method {@link SearchService#getObjectByUuid}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetObjectByUuid() throws Exception {
        final String entityUuid = "203892293934";
        final long targetUuid = 32;
        final String targetType = "AppDynamics";
        final ServiceEntityApiDTO desiredResponse = new ServiceEntityApiDTO();
        final TargetApiDTO target = new TargetApiDTO();
        target.setUuid(Long.toString(targetUuid));
        target.setType(targetType);
        desiredResponse.setDiscoveredBy(target);

        // Prepare the mocks
        doThrow(UnknownObjectException.class)
            .when(groupsService).getGroupByUuid(anyString(),anyBoolean());

        doThrow(UnknownObjectException.class)
            .when(businessAccountRetriever).getBusinessAccount(entityUuid);

        Mockito.when(repositoryApi.getByIds(Collections.singleton(Long.valueOf(entityUuid)),
                Collections.emptySet(), false))
                .thenReturn(new RepositoryRequestResult(Collections.emptySet(),
                        Collections.singleton(desiredResponse)));

        // Test the search service
        BaseApiDTO response = searchService.getObjectByUuid(entityUuid);

        // Verify the results
        assertEquals(desiredResponse, response);
    }

    @Test
    public void testGetSearchResultsPaginationLimitForEntities() throws Exception {

        List<String> types = Lists.newArrayList("VirtualMachine");
        Pair<SearchQuery, SearchPaginationRequest> resultPair;

        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
            null,
            null);

        SearchPaginationRequest paginationRequest =  resultPair.getSecond();
        assertEquals(paginationRequest.getLimit(), DEFAULT_LIMIT);


        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            new SearchPaginationRequest(null, 600, true, null),
            null,
            types,
            null,
            null,
            null,
            null,
            null,
            null);

        assertEquals(resultPair.getSecond().getLimit(), MAX_LIMIT);
    }

    @Test
    public void testGetSearchResultsPaginationLimitForGroups() throws Exception {

        Pair<SearchQuery, SearchPaginationRequest> resultPair;
        List<String> types = Lists.newArrayList("Group");
        final SearchPaginationResponse paginationResponse =
            Mockito.mock(SearchPaginationResponse.class);
        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null))).thenReturn(paginationResponse);
        searchService.getSearchResults(
            "myGroup",
            Lists.newArrayList("Group"),
            null,
            null,
            null,
            EnvironmentType.ONPREM,
            null,
            null,
            null,
            null,
            null,
            null);

        ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(groupsService, Mockito.atMost(4)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
        SearchPaginationRequest searchPaginationRequestCaptured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(DEFAULT_LIMIT, searchPaginationRequestCaptured.getLimit());


        SearchPaginationRequest searchPaginationRequest = getSearchPaginationRequest(600);
        searchService.getSearchResults(
            "myGroup",
            Lists.newArrayList("Group"),
            null,
            null,
            null,
            EnvironmentType.ONPREM,
            null,
            searchPaginationRequest,
            null,
            null,
            null,
            null);

        // Searching for Groups with GroupTypes is a different code path
        searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(groupsService, Mockito.atMost(4)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
        searchPaginationRequestCaptured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(MAX_LIMIT, searchPaginationRequestCaptured.getLimit());

        searchService.getSearchResults(
            "myGroup",
            Lists.newArrayList("Group"),
            null,
            null,
            Arrays.asList("VirtualMachine"),
            EnvironmentType.ONPREM,
            null,
            null,
            null,
            null,
            null,
            null);

        searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(groupsService, Mockito.atMost(4)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
        searchPaginationRequestCaptured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(DEFAULT_LIMIT, searchPaginationRequestCaptured.getLimit());


        searchPaginationRequest = getSearchPaginationRequest(600);
        searchService.getSearchResults(
            "myGroup",
            Lists.newArrayList("Group"),
            null,
            null,
            Arrays.asList("VirtualMachine"),
            EnvironmentType.ONPREM,
            null,
            searchPaginationRequest,
            null,
            null,
            null,
            null);

        searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(groupsService, Mockito.atMost(4)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
        searchPaginationRequestCaptured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(MAX_LIMIT, searchPaginationRequestCaptured.getLimit());

    }

    @Test
    public void testGetSearchResultsPaginationLimitForBusinessAccounts() throws Exception {

        final List<String> types = ImmutableList.of(ApiEntityType.BUSINESS_ACCOUNT.apiStr());

        searchService.getSearchResults(null, types, null, null, null, null,
            null, null, null, null, null,
            null);
        ArgumentCaptor<SearchPaginationRequest> paginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(businessAccountRetriever, Mockito.atMost(2)).getBusinessAccountsInScope(any(), any(), paginationRequestCaptor.capture());
        assertEquals(DEFAULT_LIMIT, paginationRequestCaptor.getValue().getLimit());

        final SearchPaginationRequest paginationReq = spy(new SearchPaginationRequest(null, 600,
            true, null));
        searchService.getSearchResults(null, types, null, null, null, null,
            null, paginationReq, null, null, null,
            null);
        paginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(businessAccountRetriever, Mockito.atMost(2)).getBusinessAccountsInScope(any(), any(), paginationRequestCaptor.capture());
        assertEquals(MAX_LIMIT, paginationRequestCaptor.getValue().getLimit());

    }


    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResultsServiceEntityPaginated() throws Exception {
        final SearchPaginationResponse paginationResponse = Mockito.mock(SearchPaginationResponse.class);

        final PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        when(mockPaginatedSearchRequest.getResponse()).thenReturn(mock(SearchPaginationResponse.class));
        // With current implementation, passing a limit of null in getSearchResults will result in paginated output for ServiceEntities like VirtualMachine
        final SearchPaginationRequest paginatedSearchRequest = getSearchPaginationRequest(null);
        searchService.getSearchResults(null,
                         Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()), null, null,
                         null, EnvironmentType.CLOUD, null, paginatedSearchRequest, null, null, null, null);
        PropertyFilter vmEntityTypeFilter = SearchProtoUtil.entityTypeFilter(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        SearchQuery searchQuery = SearchQuery.newBuilder()
                        .addSearchParameters(
                                        SearchProtoUtil
                                                        .makeSearchParameters(vmEntityTypeFilter)
                                                        .addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.environmentTypeFilter(EnvironmentTypeEnum.EnvironmentType.CLOUD))))
                        .build();

        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);

        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), any(), searchPaginationRequestCaptor.capture());
        assertEquals(searchPaginationRequestCaptor.getValue().getLimit(), DEFAULT_LIMIT);
        ApiEntityType vm = ApiEntityType.fromString("VirtualMachine");
        assertEquals(searchQueryCaptor.getValue().getSearchParametersList().get(0).getStartingFilter(), vmEntityTypeFilter );

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), isA(
                Origin.class))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
            null,
            Lists.newArrayList("Group", "Cluster"),
            null,
            null,
            null,
            EnvironmentType.ONPREM,
            null,
            null,
            null,
            null,
            Origin.USER,  null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), isA(Origin.class));

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
                null,
                Lists.newArrayList("Group", "Cluster"),
                null,
                null,
                null,
                EnvironmentType.ONPREM,
                null,
                null,
                null,
                null,
                null,
                null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(),
                eq(GroupType.COMPUTE_HOST_CLUSTER), any(), eq(EnvironmentType.ONPREM), any(), any(),
                eq(false), eq(null)))
                .thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
            null,
            Lists.newArrayList("Cluster"),
            null,
            null,
            null,
            EnvironmentType.ONPREM,
            null,
            null,
            null,
            null,
            null, null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(),
                eq(GroupType.COMPUTE_HOST_CLUSTER), any(), eq(EnvironmentType.ONPREM), any(), any(),
                eq(false), eq(null));
    }

    @Test
    public void testGetSearchResultsServiceEntityNonPaginated() throws Exception {
        final SearchPaginationResponse paginationResponse = Mockito.mock(SearchPaginationResponse.class);
        final SearchPaginationRequest paginationRequest = Mockito.mock(SearchPaginationRequest.class);

        getNonPaginatedSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getNonPaginatedSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(targetsService).getTargets();

        final SearchRequest mockSearchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(mockSearchRequest);
        when(mockSearchRequest.getSEList()).thenReturn(Collections.emptyList());


        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), isA(
                        Origin.class))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
                        null,
                        Lists.newArrayList("Group", "Cluster"),
                        null,
                        null,
                        null,
                        EnvironmentType.ONPREM,
                        null,
                        null,
                        null,
                        null,
                        Origin.USER,  null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), isA(Origin.class));

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
                        null,
                        Lists.newArrayList("Group", "Cluster"),
                        null,
                        null,
                        null,
                        EnvironmentType.ONPREM,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
    }
    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * <p>Search on cluster should apply query to request</p>
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResultsClusterWithQuery() throws Exception {
        final SearchPaginationResponse paginationResponse =
                Mockito.mock(SearchPaginationResponse.class);

        String query = "query";
        when(groupsService.getPaginatedGroupApiDTOs(any(), any(),
                eq(GroupType.COMPUTE_HOST_CLUSTER), any(), eq(EnvironmentType.ONPREM), any(), any(),
                eq(false), eq(null)))
                .thenReturn(paginationResponse);

        ArgumentCaptor<String> queryArgCap = ArgumentCaptor.forClass(String.class);

        //WHEN
        SearchPaginationResponse response = searchService.getSearchResults(
                query,
                Lists.newArrayList("Cluster"),
                Lists.newArrayList("Market"),
                null,
                null,
                EnvironmentType.ONPREM,
                null,
                null,
                null,
                null,
                null, null);

        //THEN
        assertEquals(response, paginationResponse);
        ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(
                SearchPaginationRequest.class);
        verify(groupsService).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(),
                eq(GroupType.COMPUTE_HOST_CLUSTER), any(), eq(EnvironmentType.ONPREM), any(), any(),
                eq(false), eq(null));
        SearchPaginationRequest captured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(DEFAULT_LIMIT, captured.getLimit());
        verify(searchService).addNameMatcher(queryArgCap.capture(), any(), any(), any());
        assertEquals(query, queryArgCap.getValue());
    }



    /**
     * This tests if query is handled accordingly in case it contains
     * a special char.
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResultsWithSpecialCharNameQueryServiceEntityPaginated() throws Exception {

        final SearchPaginationRequest searchPaginationRequest = getSearchPaginationRequest(10);
        List<ServiceEntityApiDTO> searchResultDTOs = Lists.newArrayList(
                supplyChainTestUtils.createServiceEntityApiDTO(999, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1));

        PaginatedSearchRequest paginatedSearchRequest = ApiTestUtils.mockPaginatedSearchRequest(searchPaginationRequest, searchResultDTOs);
        doReturn(paginatedSearchRequest).when(repositoryApi).newPaginatedSearch(any(), any(), any());

        for (Entry<String, ImmutableMap<QueryType, String>> entry : nameQueryMap.entrySet()) {
            for (QueryType queryType : QueryType.values()) {
                searchService.getSearchResults(entry.getKey(), Lists.newArrayList("VirtualMachine"),
                        Lists.newArrayList("Market"), null, null, EnvironmentType.ONPREM, null,
                                               searchPaginationRequest, null, null, null, queryType);

                final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
                final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
                verify(repositoryApi, Mockito.atMost(nameQueryMap.size() * QueryType.values().length)).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), any());
                assertThat(searchQueryCaptor.getValue()
                        .getSearchParameters(0)
                        .getSearchFilter(0)
                        .getPropertyFilter()
                        .getStringFilter()
                        .getStringPropertyRegex(), is(entry.getValue().get(queryType)));
            }
        }
    }


    /**
     * This tests whether an exception is thrown if the query does not specify either a type or group type.
     * @throws Exception this is expected from this test because the arguments are invalid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidGetSearchResults() throws Exception {
        // Type, entity type and group type are all empty. This should throw an exception.
        searchService.getSearchResults(".*a.*", null, Lists.newArrayList("string1"), "ACTIVE", null, EnvironmentType.CLOUD,
                null, null, null, Lists.newArrayList("probeType1"), null, null);
    }


    /**
     * This tests whether an exception is thrown if the query specifies an invalid scope.
     * @throws Exception this is expected from this test because the arguments are invalid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidScopeGetSearchResultsServiceEntityPaginated() throws Exception {
        // Type, entity type and group type are all empty. This should throw an exception.
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(Collections.emptySet());
        searchService.getSearchResults(".*a.*", Lists.newArrayList("VirtualMachine"), Lists.newArrayList("string1"), "ACTIVE", null, EnvironmentType.CLOUD,
                null, getSearchPaginationRequest(10), null, Lists.newArrayList("probeType1"), null, null);
    }

    /**
     * This tests whether an exception is thrown if the query specifies an invalid scope.
     * @throws Exception this is expected from this test because the arguments are invalid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidScopeGetSearchResultsServiceEntityNonePaginated() throws Exception {
        // Type, entity type and group type are all empty. This should throw an exception.
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(Collections.emptySet());
        searchService.getSearchResults(".*a.*", Lists.newArrayList("VirtualMachine"), Lists.newArrayList("string1"), "ACTIVE", null, EnvironmentType.CLOUD,
                                       null, getSearchPaginationRequest(null), null, Lists.newArrayList("probeType1"), null, null);
    }

    @Test
    public void testGetSearchEntitiesByProbeTypesPaginated() throws Exception {

        // Given
        final ThinTargetInfo thinTargetInfoAWS = ImmutableThinTargetInfo
            .builder()
            .oid(targetId1)
            .displayName("mockedThinTargetInfo")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                .oid(1)
                .type(probeType1)
                .category("Cloud")
                .uiCategory("Cloud")
                .build())
            .build();

        final ThinTargetInfo thinTargetInfoAzure = ImmutableThinTargetInfo
            .builder()
            .oid(targetId2)
            .displayName("mockedThinTargetInfo")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                .oid(1)
                .type(probeType2)
                .category("Cloud")
                .uiCategory("Cloud")
                .build())
            .build();

        ServiceEntityApiDTO se1 = supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1);
        se1.getDiscoveredBy().setType(probeType1);
        se1.setEnvironmentType(EnvironmentType.CLOUD);
        ServiceEntityApiDTO se2 = supplyChainTestUtils.createServiceEntityApiDTO(2L, targetId1);
        se2.getDiscoveredBy().setType(probeType1);
        se2.setEnvironmentType(EnvironmentType.CLOUD);
        ServiceEntityApiDTO se3 = supplyChainTestUtils.createServiceEntityApiDTO(3L, targetId2);
        se3.getDiscoveredBy().setType(probeType2);
        se3.setEnvironmentType(EnvironmentType.CLOUD);
        List<ServiceEntityApiDTO> regions = Lists.newArrayList(se1, se2, se3);
        List<String> types = Lists.newArrayList("Region");

        doReturn(Arrays.asList(thinTargetInfoAWS, thinTargetInfoAzure)).when(thinTargetCacheMock).getAllTargets();
        SearchQuery searchQuery;
        Pair<SearchQuery, SearchPaginationRequest> resultPair;


        // filter by AWS
        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
             Lists.newArrayList(probeType1),
            null);

        searchQuery = resultPair.getFirst();
        assertEqualsSearchFilterByTargetIds(Collections.singletonList(targetId1), searchQuery);

        // filter by Azure
        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
            Lists.newArrayList(probeType2),
            null);

        searchQuery = resultPair.getFirst();
        assertEqualsSearchFilterByTargetIds(Collections.singletonList(targetId2), searchQuery);

        // filter by both AWS and Azure
        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
            Lists.newArrayList(probeType1, probeType2),
            null);

        searchQuery = resultPair.getFirst();
        assertEqualsSearchFilterByTargetIds(Lists.newArrayList(targetId1, targetId2), searchQuery);

        // filter by a vc probe type
        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
            Lists.newArrayList(SDKProbeType.VCENTER.getProbeType()),
            null);

        searchQuery = resultPair.getFirst();
        assertEqualsSearchFilterByTargetIds(Collections.emptyList(), searchQuery);

        // filter by null probeTypes
        resultPair = getPaginatedSearchResults(searchService,
            repositoryApi,
            null,
            null,
            types,
            null,
            null,
            null,
            null,
            null,
            null);

        searchQuery = resultPair.getFirst();
        assertEquals(searchQuery.getSearchParameters(0).getSearchFilterCount(), 0);

    }

    private void assertEqualsSearchFilterByTargetIds(List<Long> expectedTargetIds, SearchQuery actualSearchQuery) {
        SearchFilter searchFilter = actualSearchQuery.getSearchParameters(0).getSearchFilter(0);

        List<Long>
            targetIds = searchFilter.getPropertyFilter().getListFilter().getStringFilter().getOptionsList()
            .stream().map( id -> Long.valueOf(id)).collect(Collectors.toList());
        assertTrue(targetIds.size() == expectedTargetIds.size() && targetIds.containsAll(expectedTargetIds) && expectedTargetIds.containsAll(targetIds));
    }


    @Test
    public void testGetSearchGroup() throws Exception {
        final SearchPaginationResponse paginationResponse =
            Mockito.mock(SearchPaginationResponse.class);
        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
                "myGroup",
                Lists.newArrayList("Group"),
                null,
                null,
                Arrays.asList("VirtualMachine"),
                EnvironmentType.ONPREM,
                null,
                null,
                null,
                null,
                null,
                null));
        verify(targetsService, Mockito.never()).getTargets();
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), any(), eq(EnvironmentType.ONPREM), any(), any(), eq(true), eq(null));
        verify(searchService).addNameMatcher(any(), any(), any(), any());

    }

    /**
     * Test scoped to a cluster;  Expanded entities passed as scope arguments with pagination applied.
     * https://ml-xl-dev-2/vmturbo/rest/search?disable_hateoas=true&q=&scopes=283218897841408&types=PhysicalMachine
     * @throws Exception Not expected to be thrown
     */
    @Test
    public void testSearchWithClusterInScopesServiceEntityPaginated() throws Exception {
        // Arrange
        final String clusterOid = "283218897841408";
        GroupApiDTO clusterGroup = new GroupApiDTO();
        clusterGroup.setMemberUuidList(Lists.newArrayList("1", "2", "3", "4"));
        List<ServiceEntityApiDTO> searchResultDTOs = Lists.newArrayList(
                supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1));

        List<String> scopes = Lists.newArrayList(clusterOid);
        List<String> types = Lists.newArrayList("PhysicalMachine");

        SearchPaginationRequest searchPaginationRequest = getSearchPaginationRequest(10);
        PaginatedSearchRequest req = ApiTestUtils.mockPaginatedSearchRequest(searchPaginationRequest, searchResultDTOs);
        when(repositoryApi.newPaginatedSearch(any(SearchQuery.class), any(), any())).thenReturn(req);

        when(groupsService.expandUuids(eq(Sets.newHashSet(scopes)), eq(types), eq(null)))
                        .thenReturn(ImmutableSet.of(1L, 2L, 3L, 4L));

        ApiTestUtils.mockRealtimeId(UuidMapper.UI_REAL_TIME_MARKET_STR, 777777, uuidMapper);

        when(groupExpander.getGroup(any())).thenReturn(Optional.of(Grouping.newBuilder()
                                                                                   .setId(5L)
                                                                                   .setDefinition(GroupDefinition.getDefaultInstance())
                                                                                   .build()
        ));

        // Act
        searchService.getSearchResults(
                        null, types, scopes, null, null, null,
                        null, searchPaginationRequest, null, null, null, null);
        //Assert

        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), any());

        assertEquals(scopeIds.getValue().size(), 4);
        assertThat(scopeIds.getValue(), containsInAnyOrder(1L, 2L, 3L, 4L));
    }


    /**
     * Test scoped to a PM;  search result returns the Cluster the PM belongs to .
     * https://IP/vmturbo/rest/search?disable_hateoas=true&q=&scopes=283218897841408&types=Cluster
     * @throws Exception Not expected to be thrown
     */
    @Test
    public void testSearchWithPMInScopes() throws Exception {

        // Arrange
        final String PM_OID = "283218897841408";

        List<String> scopes = Lists.newArrayList(PM_OID);
        List<String> types = Lists.newArrayList("Cluster");

        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(null, null, true, null);

        // Act
       searchService.getSearchResults(null, types, scopes, null, null, null, null,
               paginationRequest, null,  null, null, null);

        // Assert
        ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(groupsService).getPaginatedGroupApiDTOs(eq(Collections.EMPTY_LIST),
                searchPaginationRequestArgumentCaptor.capture(), eq(GroupType.COMPUTE_HOST_CLUSTER), eq(null), eq(null),
                eq(null), eq(scopes), eq(false), eq(null));

        SearchPaginationRequest searchPaginationRequestCaptured = searchPaginationRequestArgumentCaptor.getValue();
        assertEquals(true, searchPaginationRequestCaptured.isAscending());
    }

    /**
     * Tests getSearchResults paginated with {@link EntityDetailType}.aspects.
     *
     * <p>Expect {@link EntityAspectMapper} to not be added to search request</p>
     * @throws Exception Not expected to be thrown
     */
    @Test
    public void testSearchWithClusterEntityDetailTypeAspectsServiceEntityPaginated() throws Exception {
        //GIVEN
        PaginatedSearchRequest req = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(SearchQuery.class), any(), any())).thenReturn(req);
        when(req.getResponse()).thenReturn(null);

        List<String> scopes = Lists.newArrayList("283218897841408");
        List<String> types = Lists.newArrayList("VirtualMachine");
        EntityDetailType entityDetailType = EntityDetailType.aspects;

        when(groupExpander.getGroup(any())).thenReturn(Optional.of(Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.getDefaultInstance())
                .build()
        ));
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(2L, 4L,
                5L));

        final SearchPaginationRequest paginationRequest = getSearchPaginationRequest(10);

        //WHEN
        searchService.getSearchResults(
                        null,
                        types,
                        scopes,
                        null,
                        null,
                        null,
                        entityDetailType,
                        paginationRequest,
                        null,
                        null, null, null);

        //THEN
        verify(req).requestAspects(entityAspectMapper, null);
    }


    /**
     * Tests getSearchResults on type VirtualMachine, list of scopes, with entityDetailType null.
     *
     * <p>Expect {@link EntityAspectMapper} to not be added to search request</p>
     * @throws Exception Not expected to be thrown
     */
    @Test
    public void testSearchWithClusterEntityDetailTypeNoneAspectsServiceEntityPaginated() throws Exception {
        //GIVEN
        PaginatedSearchRequest req = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(SearchQuery.class), any(), any())).thenReturn(req);
        when(req.getResponse()).thenReturn(null);

        final SearchPaginationRequest paginationRequest = getSearchPaginationRequest(10);

        List<String> scopes = Lists.newArrayList("283218897841408");
        List<String> types = Lists.newArrayList("VirtualMachine");
        EntityDetailType entityDetailType = null;

        when(groupExpander.getGroup(any())).thenReturn(Optional.of(Grouping.newBuilder()
            .setId(1L)
            .setDefinition(GroupDefinition.getDefaultInstance())
            .build()
        ));
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(2L, 4L,
            5L));

        //WHEN
        searchService.getSearchResults(
            null,
            types,
            scopes,
            null,
            null,
            null,
            entityDetailType,
            paginationRequest,
            null,
            null, null, null);

        //THEN
        verify(req, Mockito.never()).requestAspects(entityAspectMapper, null);
    }

    /**
     * Tests getSearchResults on type VirtualMachine, list of scopes, with entityDetailType null.
     *
     * <p>Expect {@link EntityAspectMapper} to not be added to search request</p>
     * @throws Exception Not expected to be thrown
     */
    @Test
    public void testSearchWithClusterEntityDetailTypeNoneAspects() throws Exception {
        //GIVEN
        PaginatedSearchRequest req = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(SearchQuery.class), any(), any())).thenReturn(req);
        when(req.getResponse()).thenReturn(null);

        List<String> scopes = Lists.newArrayList("283218897841408");
        List<String> types = Lists.newArrayList("VirtualMachine");
        EntityDetailType entityDetailType = null;

        when(groupExpander.getGroup(any())).thenReturn(Optional.of(Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.getDefaultInstance())
                .build()
        ));
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(2L, 4L,
                5L));

        //WHEN
        searchService.getSearchResults(
                        null,
                        types,
                        scopes,
                        null,
                        null,
                        null,
                        entityDetailType,
                        getSearchPaginationRequest(10),
                        null,
                        null, null, null);

        //THEN
        verify(req, times(0)).requestAspects(entityAspectMapper, null);
    }

    /**
     * Test that SearchService.getSearchResults calls {@link BusinessAccountRetriever} with the
     * right args when BusinessAccount is the type and scope is sent.
     *
     * @throws Exception if getSearchResults throws an Exception.
     */
    @Test
    public void testBusinesUnitsWithScope() throws Exception {
        BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setDisplayName("Test Business Account");
        final List<String> scopes = ImmutableList.of("target1", "target2");
        final List<String> types = ImmutableList.of(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        final SearchPaginationRequest paginationReq = spy(new SearchPaginationRequest(null, null,
                true, null));
        searchService.getSearchResults(null, types, scopes, null, null, null,
                null, paginationReq, null, null, null,
                null);
        final ArgumentCaptor<List<String>> scopesCaptor = ArgumentCaptor.forClass( (Class<List<String>>) (Class)(List.class));
        final ArgumentCaptor<SearchPaginationRequest> paginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        final ArgumentCaptor<List<FilterApiDTO>> filterListCaptor =
            ArgumentCaptor.forClass((Class<List<FilterApiDTO>>)(Class)List.class);
        verify(businessAccountRetriever).getBusinessAccountsInScope(scopesCaptor.capture(), filterListCaptor.capture(), paginationRequestCaptor.capture());
        assertEquals(filterListCaptor.getValue(), Collections.emptyList());
        assertEquals(scopesCaptor.getValue(), scopes);
        assertEquals(DEFAULT_LIMIT, paginationRequestCaptor.getValue().getLimit());
    }

    /**
     * Test that a business account search request with probe types specified will create and use
     * the appropriate filter.
     *
     * @throws Exception never
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBusinessAccountFilterByProbeType() throws Exception {
        final String probeType1 = "asdfasdf";
        final String probeType2 = "jkljkl";
        final SearchPaginationRequest paginationReq = spy(new SearchPaginationRequest(null, null, true, null));
        searchService.getSearchResults(null, Collections.singletonList(ApiEntityType.BUSINESS_ACCOUNT.apiStr()),
                null, null, null, null, null, paginationReq,
                null, Arrays.asList(probeType1, probeType2), null, null);

        final ArgumentCaptor<List<FilterApiDTO>> filterListCaptor =
            ArgumentCaptor.forClass((Class<List<FilterApiDTO>>)(Class)List.class);
        final ArgumentCaptor<SearchPaginationRequest> paginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(businessAccountRetriever).getBusinessAccountsInScope(
            isNull((Class<List<String>>)(Class)List.class), filterListCaptor.capture(), paginationRequestCaptor.capture());
        final List<FilterApiDTO> actualFilterList = filterListCaptor.getValue();
        assertEquals(1, actualFilterList.size());
        final FilterApiDTO filterDTO = actualFilterList.iterator().next();
        assertFalse(filterDTO.getCaseSensitive());
        assertEquals(EntityFilterMapper.ACCOUNT_PROBE_TYPE_FILTER, filterDTO.getFilterType());
        assertEquals(EntityFilterMapper.EQUAL, filterDTO.getExpType());
        assertEquals(probeType1 + GroupFilterMapper.OR_DELIMITER + probeType2, filterDTO.getExpVal());
        assertEquals(DEFAULT_LIMIT, paginationRequestCaptor.getValue().getLimit() );
    }

    @Test
    public void testGetMembersBasedOnFilterSeverity() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setCriteriaList(Collections.emptyList());
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(paginationRequest, serviceEntities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest,
                null, null);
        List<BaseApiDTO> results = response.getRawResults();

        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
    }

    /**
     * Test that GetMembersBasedOnFilter works without raising exception
     * for every possible filter type contained in COMPARISON_STRING_TO_COMPARISON_OPERATOR map
     * In the test below we try to order by severity in our paginated request but this is not
     * the point of the test. The point is to make sure that for any filter type no exception is
     * raised.
     *
     * @throws Exception if getSearchResults throws an Exception.
     */
    @Test
    public void testGetMembersBasedOnFilterForVariousFilterTypes() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        FilterApiDTO filter = new FilterApiDTO();
        filter.setExpVal("1000");
        filter.setFilterType("vmsByMem");
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(paginationRequest, serviceEntities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);

        for( String key : EntityFilterMapper.getComparisonOperators()) {
            ArrayList<FilterApiDTO> criteriaList = new ArrayList<>(1);
            filter.setExpType(key);
            criteriaList.add(filter);
            request.setCriteriaList(criteriaList);
            SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                    paginationRequest, null, null);
            List<BaseApiDTO> results = response.getRawResults();

            assertEquals(1, results.size());
            assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
            assertEquals("1", results.get(0).getUuid());
        }
    }

    /**
     * Test that GetMembersBasedOnFilter raises an exception if there is a filter type
     * that is not contained in COMPARISON_STRING_TO_COMPARISON_OPERATOR map, for example
     * filter type "GE" should yield an exception.
     *
     * @throws IllegalArgumentException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetMembersBasedOnFilterForNotExitingFilterTypes() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        FilterApiDTO filter = new FilterApiDTO();
        filter.setExpVal("1000");
        filter.setFilterType("vmsByMem");
        filter.setExpType("GE");
        ArrayList<FilterApiDTO> criteriaList = new ArrayList<>(1);
        criteriaList.add(filter);
        request.setCriteriaList(criteriaList);
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(paginationRequest, serviceEntities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                paginationRequest, null, null);
        List<BaseApiDTO> results = response.getRawResults();

        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
    }

    /**
     * Test that GetMembersBasedOnFilter raises an exception if there is a logical operator type
     * that is not contained in LogicalOperator class.
     *
     * @throws IllegalArgumentException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetMembersBasedOnFilterForNotExitingOperatorTypes() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setLogicalOperator("INVALID");
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(paginationRequest, serviceEntities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                paginationRequest, null, null);
    }

    /**
     * Test that GetMembersBasedOnFilter is not raising an exception if there is a filter type based on REGEX.
     */
    @Test
    public void testGetMembersBasedOnRegexFilterNoException() throws Exception {
        GroupApiDTO request1 = new GroupApiDTO();
        FilterApiDTO filter1 = new FilterApiDTO();
        filter1.setExpVal(".*PT.*");
        filter1.setFilterType("vmsByName");
        filter1.setExpType(EntityFilterMapper.REGEX_MATCH);
        ArrayList<FilterApiDTO> criteriaList1 = new ArrayList<>(1);
        criteriaList1.add(filter1);
        request1.setCriteriaList(criteriaList1);
        GroupApiDTO request2 = new GroupApiDTO();
        FilterApiDTO filter2 = new FilterApiDTO();
        filter2.setExpVal(".*PT.*");
        filter2.setFilterType("vmsByName");
        filter2.setExpType(EntityFilterMapper.REGEX_NO_MATCH);
        ArrayList<FilterApiDTO> criteriaList2 = new ArrayList<>(1);
        criteriaList2.add(filter2);
        request2.setCriteriaList(criteriaList2);
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(paginationRequest, serviceEntities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);

        // test REGEX_MATCH
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request1,
                paginationRequest, null, null);
        List<BaseApiDTO> results = response.getRawResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());

        // test REGEX_NO_MATCH
        response = searchService.getMembersBasedOnFilter("", request2,
                paginationRequest, null, null);
        results = response.getRawResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
    }

    /**
     * Sending a request that should not return any results, this should return an empty response
     * and not throw an exception.
     *
     * @throws Exception should not get thrown
     */
    @Test
    public void testGetMembersBasedOnFilterEmptyScope() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setScope(Lists.newArrayList(String.valueOf(ENTITY_ID_1)));
        request.setCriteriaList(Collections.emptyList());
        final SearchPaginationRequest paginationRequest =
            new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        when(groupExpander.getGroup(eq(String.valueOf(ENTITY_ID_1)))).thenReturn(Optional.of(Grouping.newBuilder()
            .setId(ENTITY_ID_1)
            .setDefinition(GroupDefinition.getDefaultInstance())
            .build()
        ));
        MultiEntityRequest mockReq = mock(MultiEntityRequest.class);
        final Stream<ApiPartialEntity> stream = ImmutableList.of(ApiPartialEntity.newBuilder()
            .setOid(ENTITY_ID_2).setEntityType(0).build()).stream();
        when(mockReq.getEntities()).thenReturn(stream);
        when(repositoryApi.entitiesRequest(any())).thenReturn(mockReq);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest,
            null, null);
        List<BaseApiDTO> results = response.getRawResults();
        assertEquals(0, results.size());
    }

    /**
     * Sending a request that should not return any results, but throw an exception as the given
     * uuid does is fake.
     *
     * @throws IllegalArgumentException should be thrown
     * @throws Exception any other exception should not get thrown
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetMembersBasedOnFilterInvalidScope() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setScope(Lists.newArrayList(String.valueOf(ENTITY_ID_1)));
        request.setCriteriaList(Collections.emptyList());
        final SearchPaginationRequest paginationRequest =
            new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        when(groupExpander.getGroup(eq(String.valueOf(ENTITY_ID_1)))).thenReturn(Optional.of(Grouping.newBuilder()
            .setId(ENTITY_ID_1)
            .setDefinition(GroupDefinition.getDefaultInstance())
            .build()
        ));
        MultiEntityRequest mockReq = mock(MultiEntityRequest.class);
        when(mockReq.getEntities()).thenReturn(new ArrayList<ApiPartialEntity>().stream());
        when(repositoryApi.entitiesRequest(any())).thenReturn(mockReq);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest,
            null, null);
    }

    @Test
    public void testGetMembersBasedOnFilterUtilization() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName("PhysicalMachine");
        request.setCriteriaList(Collections.emptyList());
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
            supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId2));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.UTILIZATION.name());
        when(historyServiceSpy.getEntityStats(any())).thenReturn(
                GetEntityStatsResponse.newBuilder()
                    .addEntityStats(EntityStats.newBuilder()
                        .setOid(1L))
                    .build());

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(serviceEntities);
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest,
                null, null);
        List<BaseApiDTO> results = response.getRawResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
    }

    /**
     * Test getMembersBasedOnFilterQuery where a name query string is passed
     * Verify that the search service rpc call is invoked.
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersBasedOnFilterQuery() throws Exception {
        //Given
        GroupApiDTO groupApiDTO = new GroupApiDTO();

        final SearchPaginationRequest paginationRequest = getSearchPaginationRequest(null);

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();


        //WHEN
        searchService.getMembersBasedOnFilter("foo", groupApiDTO, paginationRequest, null, null);

        //THEN
        verify(mockPaginatedSearchRequest, times(1)).getResponse();
    }

    /**
     * Test testGetMembersBasedEntitiesOnPaginationLimit where different pagination
     * parameters are pasSed and make sure that DEFAULT and PAGINATION_LIMIT is enforced.
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersBasedEntitiesOnPaginationLimit() throws Exception {
        //Given
        GroupApiDTO groupApiDTO = new GroupApiDTO();

        SearchPaginationRequest paginationRequest = getSearchPaginationRequest(null);

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();


        //WHEN
        searchService.getMembersBasedOnFilter("foo", groupApiDTO, paginationRequest, null, null);

        //THEN
        verify(mockPaginatedSearchRequest, times(1)).getResponse();
        ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(any(), any(), searchPaginationRequestArgumentCaptor.capture());
        assertEquals(searchPaginationRequestArgumentCaptor.getValue().getLimit(), DEFAULT_LIMIT );

        paginationRequest = getSearchPaginationRequest(600);
        searchService.getMembersBasedOnFilter("foo", groupApiDTO, paginationRequest, null, null);
        verify(repositoryApi, Mockito.times(2)).newPaginatedSearch(any(), any(), searchPaginationRequestArgumentCaptor.capture());
        assertEquals(searchPaginationRequestArgumentCaptor.getValue().getLimit(), MAX_LIMIT );
    }

    /**
     * Test get members when there is a special character in the query
     * and we pass in the querytype parameter.
     * @throws Exception if there is an error processing the query
     */
    @Test
    public void testGetMembersBasedOnFilterQueryWSpecialCharsWithQueryType() throws Exception {
        //GIVEN
        PaginatedSearchRequest paginatedSearchRequestMock = mock(PaginatedSearchRequest.class);
        doReturn(paginatedSearchRequestMock).when(repositoryApi).newPaginatedSearch(any(), any(), any());

        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest(null, null, true, null);

        // Test a search with a special character
        final GroupApiDTO request = new GroupApiDTO();
        request.setClassName("VirtualMachine");

        for (Entry<String, ImmutableMap<QueryType, String>> entry : nameQueryMap.entrySet()) {
            for (QueryType queryType : QueryType.values()) {
                //WHEN
                SearchPaginationResponse response = searchService.getMembersBasedOnFilter(entry.getKey(), request, paginationRequest, null, queryType);

                //THEN
                final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
                final ArgumentCaptor<Set<Long>> scopeIdsCaptor = ArgumentCaptor.forClass((Class)Set.class);
                final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
                verify(repositoryApi, Mockito.atMost(nameQueryMap.size() * QueryType.values().length)).newPaginatedSearch(searchQueryCaptor.capture(), scopeIdsCaptor.capture(), searchPaginationRequestCaptor.capture());

                assertEquals(searchQueryCaptor.getValue().getSearchParameters(0)
                        .getSearchFilter(0)
                        .getPropertyFilter()
                        .getStringFilter()
                        .getStringPropertyRegex(), entry.getValue().get(queryType));
            }
        }
    }

    /**
     * Test get members when there is a special character in the query.
     * @throws Exception if there is an error processing the query
     */
    @Test
    public void testGetMembersBasedOnFilterQueryWSpecialChars() throws Exception {
        //Given
        GroupApiDTO request = new GroupApiDTO();

        // Test a search with a special character
        request.setClassName("VirtualMachine");
        PaginatedSearchRequest paginatedSearchRequestMock = mock(PaginatedSearchRequest.class);
        doReturn(paginatedSearchRequestMock).when(repositoryApi).newPaginatedSearch(any(), any(), any());

        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest(null, null, true, null);

        //WHEN
        searchService.getMembersBasedOnFilter("[b", request, paginationRequest, null, null);

        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), searchPaginationRequestCaptor.capture());

        final SearchQuery searchQuery = searchQueryCaptor.getValue();
        assertEquals(1, searchQuery.getSearchParametersCount());
        SearchParameters searchParameters = searchQuery.getSearchParameters(0);
        assertEquals(1, searchParameters.getSearchFilterCount());
        SearchFilter nameFilter = searchParameters.getSearchFilter(0);
        String value = nameFilter.getPropertyFilter().getStringFilter().getStringPropertyRegex();
        assertEquals("^.*\\[b.*$", value);

        verify(paginatedSearchRequestMock, times(1)).getResponse();
        assertTrue(scopeIds.getValue().isEmpty());
    }

    /**
     * Test testGetMembersBasedOfGroupsOnPaginationLimit where Group class is used
     * and pagination limits are tested for DEFAULT and MAX limit.*
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersBasedOfGroupsOnPaginationLimit() throws Exception {
        final GroupApiDTO requestForVirtualMachineGroups = new GroupApiDTO();
        requestForVirtualMachineGroups.setGroupType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        requestForVirtualMachineGroups.setClassName(StringConstants.GROUP);
        final GroupApiDTO requestForAllGroups = new GroupApiDTO();
        requestForAllGroups.setClassName(StringConstants.GROUP);
        final SearchPaginationResponse response = mock(SearchPaginationResponse.class);
        Mockito.when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), any(), any(), any(), eq(false), eq(null)))
            .thenReturn(response);
        final ArgumentCaptor<Set<String>> resultCaptor = ArgumentCaptor.forClass((Class)HashSet.class);
        SearchPaginationRequest paginationRequest = getSearchPaginationRequest(null);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);

        searchService.getMembersBasedOnFilter("foo",
            requestForVirtualMachineGroups,
            paginationRequest, null, null);

        verify(groupsService, times(1)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), any(), any(), any(), eq(false), eq(null));
        // verify that first call to groupsService.getPaginatedGroupApiDTOs passed in VirtualMachine
        // as entityType argument
        assertEquals(searchPaginationRequestArgumentCaptor.getValue().getLimit(), DEFAULT_LIMIT);

        paginationRequest = getSearchPaginationRequest(600);

        searchService.getMembersBasedOnFilter("foo",
            requestForVirtualMachineGroups,
            paginationRequest, null, null);

        verify(groupsService, times(2)).getPaginatedGroupApiDTOs(any(), searchPaginationRequestArgumentCaptor.capture(), any(), any(), any(), any(), any(), eq(false), eq(null));
        assertEquals(searchPaginationRequestArgumentCaptor.getValue().getLimit(), MAX_LIMIT);
    }
    /**
     * Test getMembersBasedOnFilter where Group class is used with and without groupType set.
     * Verify that the groupsService rpc call is invoked with the correct type parameter.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersBasedOnFilterWithGroupTypeQuery() throws Exception {
        final GroupApiDTO requestForVirtualMachineGroups = new GroupApiDTO();
        requestForVirtualMachineGroups.setGroupType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        requestForVirtualMachineGroups.setClassName(StringConstants.GROUP);
        final GroupApiDTO requestForAllGroups = new GroupApiDTO();
        requestForAllGroups.setClassName(StringConstants.GROUP);
        final SearchPaginationResponse response = mock(SearchPaginationResponse.class);
        Mockito.when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), any(), any(), any(), eq(false), eq(null)))
            .thenReturn(response);
        final ArgumentCaptor<Set<String>> resultCaptor = ArgumentCaptor.forClass((Class)HashSet.class);
        final SearchPaginationRequest paginationRequest = getSearchPaginationRequest(null);

        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForVirtualMachineGroups,
            paginationRequest, null, null));
        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForAllGroups,
            paginationRequest, null, null));
        verify(groupsService, times(2)).getPaginatedGroupApiDTOs(any(), any(), any(), resultCaptor.capture(), any(), any(), any(), eq(false), eq(null));
        // verify that first call to groupsService.getPaginatedGroupApiDTOs passed in VirtualMachine
        // as entityType argument
        assertEquals(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE.apiStr()),
            resultCaptor.getAllValues().get(0));
        // verify that second call to groupsService.getPaginatedGroupApiDTOs had null as
        // entityType argument
        assertNull(resultCaptor.getAllValues().get(1));
    }

    /**
     * Test to get the Workload members (VirtualMachine, Database or DatabaseServer entities).
     *
     * @throws Exception if there is an error when processing the search query.
     */
    @Test
    public void testGetWorkloadMembersBasedOnFilterQuery() throws Exception {
        final long oid = 1234;
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setCriteriaList(Collections.emptyList());
        request.setScope(Arrays.asList(String.valueOf(oid)));

        List<ServiceEntityApiDTO> entities = setupEntitiesForWorkloadsQuery();

        ConnectedEntity connectedEntity1 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(ENTITY_ID_1)
                .build();
        ConnectedEntity connectedEntity2 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(ENTITY_ID_2)
                .build();
        ConnectedEntity connectedEntity3 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(ENTITY_ID_3)
                .build();
        ConnectedEntity connectedEntity4 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(ENTITY_ID_4)
                .build();

        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("Business Account 1")
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(0, connectedEntity1)
                .addConnectedEntityList(1, connectedEntity2)
                .addConnectedEntityList(2, connectedEntity3)
                .addConnectedEntityList(3, connectedEntity4)
                .build();

        Map<Long, ServiceEntityApiDTO> entityMap = new HashMap<>();
        entityMap.put(Long.valueOf(ENTITY_ID_1), entities.get(0));
        entityMap.put(Long.valueOf(ENTITY_ID_2), entities.get(1));
        entityMap.put(Long.valueOf(ENTITY_ID_3), entities.get(2));
        entityMap.put(Long.valueOf(ENTITY_ID_4), entities.get(3));

        SingleEntityRequest singleEntityRequest = ApiTestUtils.mockSingleEntityRequest(businessAccount);
        when(repositoryApi.entityRequest(oid)).thenReturn(singleEntityRequest);

        when(singleEntityRequest.getFullEntity()).thenReturn(Optional.of(businessAccount));

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiSEReq(entities);
        when(repositoryApi
                .entitiesRequest(ImmutableSet.of(
                        ENTITY_ID_1, ENTITY_ID_2, ENTITY_ID_3, ENTITY_ID_4)))
                .thenReturn(multiEntityRequest);

        when(multiEntityRequest.getSEMap()).then(invocation -> entityMap);
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(1L, 2L));
        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest("0", 10, true,
                SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(
                paginationRequest, entities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                paginationRequest, null, null);

        final List<Long> resultIds = response.getRawResults().stream().map(BaseApiDTO::getUuid).map(
                uuid -> Long.parseLong(uuid)).collect(Collectors.toList());

        final Map<Long, String> resultById = response.getRawResults().stream().collect(
                Collectors.toMap(se -> Long.valueOf(se.getUuid()), se -> se.getClassName()));

        assertThat(resultIds.size(), is(4));
        assertThat(resultById.keySet(), containsInAnyOrder(
                ENTITY_ID_1, ENTITY_ID_2, ENTITY_ID_3, ENTITY_ID_4));
        assertThat(resultById.values(), containsInAnyOrder(
                StringConstants.DATABASE,
                StringConstants.DATABASE_SERVER,
                StringConstants.VIRTUAL_MACHINE,
                StringConstants.VIRTUAL_MACHINE
        ));
    }

    /**
     * Test to get the Workload members (VirtualMachine, Database or DatabaseServer entities)
     * based on Name filter.
     *
     * @throws Exception if there is an error when processing the search query.
     */
    @Test
    public void testGetWorkloadMembersBasedOnNameFilterQuery() throws Exception {
        final long oid = 1234;
        List<FilterApiDTO> criteriaList = new ArrayList<>(1);
        FilterApiDTO filterApiDTO = new FilterApiDTO();
        filterApiDTO.setExpVal(".*data.*");
        filterApiDTO.setExpType("RXEQ");
        filterApiDTO.setFilterType("workloadByName");
        criteriaList.add(filterApiDTO);
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setLogicalOperator("AND");
        request.setCriteriaList(criteriaList);
        request.setScope(Arrays.asList(String.valueOf(oid)));
        request.setEnvironmentType(EnvironmentType.CLOUD);

        // create entities and entity list
        final ServiceEntityApiDTO entity1 = supplyChainTestUtils.createServiceEntityApiDTO(
                ENTITY_ID_1, targetId1);
        entity1.setDisplayName("Database 1");
        entity1.setClassName(StringConstants.DATABASE);

        final ServiceEntityApiDTO entity2 = supplyChainTestUtils.createServiceEntityApiDTO(
                ENTITY_ID_2, targetId1);
        entity2.setDisplayName("Database Server 1");
        entity2.setClassName(StringConstants.DATABASE_SERVER);
        List<ServiceEntityApiDTO> entities = Arrays.asList(entity1, entity2);

        ConnectedEntity connectedEntity1 = ConnectedEntity.newBuilder().setConnectedEntityId(
                ENTITY_ID_1).build();
        ConnectedEntity connectedEntity2 = ConnectedEntity.newBuilder().setConnectedEntityId(
                ENTITY_ID_2).build();

        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("Business Account 1")
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(0, connectedEntity1)
                .addConnectedEntityList(1, connectedEntity2)
                .build();

        Map<Long, ServiceEntityApiDTO> entityMap = new HashMap<>();
        entityMap.put(Long.valueOf(ENTITY_ID_1), entities.get(0));
        entityMap.put(Long.valueOf(ENTITY_ID_2), entities.get(1));

        SingleEntityRequest singleEntityRequest = ApiTestUtils.mockSingleEntityRequest(
                businessAccount);
        when(repositoryApi.entityRequest(oid)).thenReturn(singleEntityRequest);
        when(singleEntityRequest.getFullEntity()).thenReturn(Optional.of(businessAccount));
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());

        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest("0", 10, true,
                SearchOrderBy.SEVERITY.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(
                paginationRequest, entities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(1L, 2L));

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                paginationRequest, null, null);
        assertNotNull(response);
        assertEquals(2, response.getRawResults().size());
        assertTrue(response.getRawResults().get(0).getDisplayName().contains("Data"));
    }

    /**
     * Test to get the Workload members (VirtualMachine, Database or DatabaseServer entities)
     * in sorted order when no filter is specified.
     *
     * @throws Exception if there is an error when processing the search query.
     */
    @Test
    public void testGetWorkloadMembersSortedWithoutAnyFilter() throws Exception {
        final long oid = 1234;
        List<FilterApiDTO> criteriaList = new ArrayList<>(1);
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setCriteriaList(criteriaList);
        request.setScope(Arrays.asList(String.valueOf(oid)));
        request.setEnvironmentType(EnvironmentType.CLOUD);

        // create entities and entity list
        final ServiceEntityApiDTO entity1 = supplyChainTestUtils.createServiceEntityApiDTO(
                ENTITY_ID_1, targetId1);
        entity1.setDisplayName("Database 1");
        entity1.setClassName(StringConstants.DATABASE);

        final ServiceEntityApiDTO entity2 = supplyChainTestUtils.createServiceEntityApiDTO(
                ENTITY_ID_2, targetId1);
        entity2.setDisplayName("database Server 1");
        entity2.setClassName(StringConstants.DATABASE_SERVER);
        List<ServiceEntityApiDTO> entities = Arrays.asList(entity1, entity2);

        ConnectedEntity connectedEntity1 = ConnectedEntity.newBuilder().setConnectedEntityId(
                ENTITY_ID_1).build();
        ConnectedEntity connectedEntity2 = ConnectedEntity.newBuilder().setConnectedEntityId(
                ENTITY_ID_2).build();

        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("Business Account 1")
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(0, connectedEntity1)
                .addConnectedEntityList(1, connectedEntity2)
                .build();

        Map<Long, ServiceEntityApiDTO> entityMap = new HashMap<>();
        entityMap.put(Long.valueOf(ENTITY_ID_1), entities.get(0));
        entityMap.put(Long.valueOf(ENTITY_ID_2), entities.get(1));

        SingleEntityRequest singleEntityRequest = ApiTestUtils.mockSingleEntityRequest(
                businessAccount);
        when(repositoryApi.entityRequest(oid)).thenReturn(singleEntityRequest);
        when(singleEntityRequest.getFullEntity()).thenReturn(Optional.of(businessAccount));
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());

        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest("0", 10, true,
                SearchOrderBy.NAME.name());
        RepositoryApi.PaginatedSearchRequest searchReq = ApiTestUtils.mockPaginatedSearchRequest(
                paginationRequest, entities);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(searchReq);
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(1L, 2L));

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request,
                paginationRequest, null, null);
        assertNotNull(response);
        assertEquals(2, response.getRawResults().size());
        assertTrue(response.getRawResults().get(0).getDisplayName().equals("Database 1"));
        assertTrue(response.getRawResults().get(1).getDisplayName().equals("database Server 1"));
    }

    /**
     * Test getMembersBasedOnFilterQuery when we are searching for business accounts.
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfBusinessAccountOnQueryName() throws Exception {
        GroupApiDTO inputDto = new GroupApiDTO();
        inputDto.setClassName(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        String nameQueryString = "test";
        final SearchPaginationRequest paginationReq = getSearchPaginationRequest(null);
        searchService.getMembersBasedOnFilter(nameQueryString, inputDto, paginationReq, null, null);
        FilterApiDTO filterApiDTO = new FilterApiDTO();
        filterApiDTO.setExpType(EntityFilterMapper.REGEX_MATCH);
        filterApiDTO.setExpVal(".*test.*");
        filterApiDTO.setFilterType("businessAccountByName");
        final ArgumentCaptor<List<FilterApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        verify(businessAccountRetriever).getBusinessAccountsInScope(eq(null), resultCaptor.capture(), any());
        FilterApiDTO filterApiDTOCaptured = resultCaptor.getValue().get(0);
        assertEquals(filterApiDTOCaptured.getExpType(), EntityFilterMapper.REGEX_MATCH);
        assertEquals(filterApiDTOCaptured.getExpVal(), ".*" + "test" + ".*" );
        assertEquals(filterApiDTOCaptured.getFilterType(), "businessAccountByName");
    }

    /**
        * Test getMembersBasedOfBusinessAccountOnPaginationLimits when we are searching for business accounts
        * making sure that Paginatin limits MAX and DEFAULT are enforced.
        * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfBusinessAccounOnPaginationLimits() throws Exception {
        GroupApiDTO inputDto = new GroupApiDTO();
        inputDto.setClassName(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        SearchPaginationRequest paginationReq = getSearchPaginationRequest(null);
        searchService.getMembersBasedOnFilter("", inputDto, paginationReq, null, null);

        ArgumentCaptor<SearchPaginationRequest> searchPaginatinoRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(businessAccountRetriever).getBusinessAccountsInScope(any(), any(), searchPaginatinoRequestArgumentCaptor.capture());

        assertEquals(searchPaginatinoRequestArgumentCaptor.getValue().getLimit(), DEFAULT_LIMIT);

        paginationReq = getSearchPaginationRequest(600);
        searchService.getMembersBasedOnFilter("", inputDto, paginationReq, null, null);
        verify(businessAccountRetriever, Mockito.times(2)).getBusinessAccountsInScope(any(), any(), searchPaginatinoRequestArgumentCaptor.capture());
        assertEquals(searchPaginatinoRequestArgumentCaptor.getValue().getLimit(), MAX_LIMIT);
    }

    /**
     * Test getMembersBasedOnFilterQuery when we are searching for workloads
     * in scope of Resource Group.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfResourceGroupBasedOnFilterQuery() throws Exception {
        //GIVEN
        final long oid = 1234;
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setCriteriaList(Collections.emptyList());
        request.setScope(Arrays.asList(String.valueOf(oid)));

        SingleEntityRequest singleEntityRequest = mock(SingleEntityRequest.class);
        when(repositoryApi.entityRequest(oid)).thenReturn(singleEntityRequest);

        when(singleEntityRequest.getFullEntity()).thenReturn(Optional.empty());

        when(groupExpander.getGroup(eq(String.valueOf(oid)))).thenReturn(Optional.of(Grouping.newBuilder()
            .setId(oid)
            .setDefinition(GroupDefinition.getDefaultInstance())
            .build()
        ));

        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(1L, 4L,
            5L));

        PaginatedSearchRequest paginatedSearchRequestMock = mock(PaginatedSearchRequest.class);
        doReturn(paginatedSearchRequestMock).when(repositoryApi).newPaginatedSearch(any(), any(), any());
        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest(null, null, true, null);

        //WHEN
        searchService.getMembersBasedOnFilter("foo", request, paginationRequest, null, null);

        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIdsCaptor = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIdsCaptor.capture(), searchPaginationRequestCaptor.capture());

        //Filter Checks
        SearchQuery searchQuery = searchQueryCaptor.getValue();
        assertEquals(searchQuery.getSearchParametersCount(), 1);

        //Scope check
        Set<Long> scopeIds = scopeIdsCaptor.getValue();
        assertTrue(scopeIds.size() == 3);
        assertTrue(scopeIds.contains(1L));
        assertTrue(scopeIds.contains(4L));
        assertTrue(scopeIds.contains(5L));

        //EntityTypeCheck
        PropertyFilter startingFilter = searchQuery.getSearchParameters(0).getStartingFilter();
        assertEquals(startingFilter.getPropertyName(), "entityType");
        //Workload gets expanded to 4 entityTypes
        assertEquals(4, startingFilter.getStringFilter().getOptionsCount());

        Collection<SearchFilter> searchFilters = searchQuery.getSearchParameters(0).getSearchFilterList();

        SearchFilter queryFilter = SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.nameFilterRegex("^.*foo.*$"));

        assertTrue(searchFilters.contains(queryFilter));

    }

    /**
     * Test testGetMembersOfResourceGroupBasedOnPaginationLimits when we are searching for workloads
     * and test that pagination limits DEFAULT and MAX are enforced.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfResourceGroupBasedOnPaginationLimits() throws Exception {
        //GIVEN
        final long oid = 1234;
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setCriteriaList(Collections.emptyList());
        request.setScope(Arrays.asList(String.valueOf(oid)));


        SingleEntityRequest singleEntityRequest = mock(SingleEntityRequest.class);
        when(repositoryApi.entityRequest(oid)).thenReturn(singleEntityRequest);

        when(singleEntityRequest.getFullEntity()).thenReturn(Optional.empty());

        when(groupExpander.getGroup(eq(String.valueOf(oid)))).thenReturn(Optional.of(Grouping.newBuilder()
            .setId(oid)
            .setDefinition(GroupDefinition.getDefaultInstance())
            .build()
        ));

        when(groupsService.expandUuids(any(), any(), any())).thenReturn(ImmutableSet.of(1L, 4L,
            5L));

        PaginatedSearchRequest paginatedSearchRequestMock = mock(PaginatedSearchRequest.class);
        doReturn(paginatedSearchRequestMock).when(repositoryApi).newPaginatedSearch(any(), any(), any());
        SearchPaginationRequest paginationRequest = getSearchPaginationRequest(null);

        //WHEN
        searchService.getMembersBasedOnFilter("foo", request, paginationRequest, null, null);

        //THEN
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(any(), any(), searchPaginationRequestCaptor.capture());

        assertEquals(searchPaginationRequestCaptor.getValue().getLimit(), DEFAULT_LIMIT);

        paginationRequest = getSearchPaginationRequest(600);
        searchService.getMembersBasedOnFilter("foo", request, paginationRequest, null, null);
        verify(repositoryApi, Mockito.times(2)).newPaginatedSearch(any(), any(), searchPaginationRequestCaptor.capture());

        assertEquals(searchPaginationRequestCaptor.getValue().getLimit(), MAX_LIMIT);
    }

    /**
     * The options for the tags fields should come from the tags that are in the live topology.
     * These can be fetched by calling the tag service.
     *
     * For every existing tag key k there should be an option k for the first field.
     * For every existing value v under k, there should be an option v for the second field,
     * when the first field is k.
     * No other options should exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testOptionsForTags() throws Exception {
        // mock tag service response
        final TagApiDTO[] tagsFromTagService = new TagApiDTO[3];
        tagsFromTagService[0] = new TagApiDTO();
        tagsFromTagService[0].setKey("0");
        tagsFromTagService[0].setValues(Arrays.asList("Value1", "Value2", "Value3"));
        tagsFromTagService[1] = new TagApiDTO();
        tagsFromTagService[1].setKey("1");
        tagsFromTagService[1].setValues(Arrays.asList("Value4", "Value5"));
        tagsFromTagService[2] = new TagApiDTO();
        tagsFromTagService[2].setKey("2");
        tagsFromTagService[2].setValues(Collections.singletonList("Value6"));
        final TagPaginationRequest tagPaginationRequest = new TagPaginationRequest(null, null, true, null);
        Mockito.when(tagsService.getTags(any(), any(), any(), isNull(TagPaginationRequest.class))).thenReturn(
                tagPaginationRequest.allResultsResponse(Arrays.asList(tagsFromTagService)) );

        // retrieve search service response
        final List<CriteriaOptionApiDTO> result =
                searchService.getCriteriaOptions(StringConstants.TAGS_ATTR, null, null, null);

        // compare response with expected response
        int resultCount = 0;
        for (CriteriaOptionApiDTO option : result) {
            int index = Integer.valueOf(option.getValue());
            assertTrue(0 <= index);
            assertTrue(index < 3);
            assertEquals(
                tagsFromTagService[index].getValues().stream().collect(Collectors.toSet()),
                option.getSubValues().stream().collect(Collectors.toSet())
            );
            resultCount++;
        }
        assertEquals(3, resultCount);
    }

    /**
     * All State options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testStateOptions() throws Exception {
        final List<CriteriaOptionApiDTO> result =
            searchService.getCriteriaOptions(STATE, null, null, null);
        final List<String> expected = Arrays.stream(UIEntityState.values())
            .map(UIEntityState::apiStr)
            .collect(Collectors.toList());
        final List<String> actual = result.stream().map(CriteriaOptionApiDTO::getValue)
            .collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    /**
     * All "Boolean" filter options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testBooleanFilterOptions() throws Exception {
        final List<CriteriaOptionApiDTO> result =
                searchService.getCriteriaOptions(USER_DEFINED_ENTITY, null, null, null);
        final List<String> expected = Arrays.stream(UIBooleanFilter.values())
                .map(UIBooleanFilter::apiStr)
                .collect(Collectors.toList());
        final List<String> actual = result.stream().map(CriteriaOptionApiDTO::getValue)
                .collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    /**
     * All business account options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testAccountOptions() throws Exception {


        final List<MinimalEntity> accountEntities = ImmutableList.of(
            MinimalEntity.newBuilder().setOid(1L).setDisplayName("account-entity-1").build(),
            MinimalEntity.newBuilder().setOid(2L).setDisplayName("account-entity-2").build(),
            MinimalEntity.newBuilder().setOid(3L).setDisplayName("account-entity-3").build()
        );
        final SearchRequest mockRequest = ApiTestUtils.mockSearchMinReq(accountEntities);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(mockRequest);
        final List<CriteriaOptionApiDTO> result =
            searchService.getCriteriaOptions(ACCOUNT_OID, null, null, null);
        assertEquals(accountEntities.size(), result.size());
        accountEntities.forEach(entity ->
            assertTrue(result.stream().anyMatch(dto ->
                entity.getDisplayName().equals(dto.getDisplayName()) &&
                    String.valueOf(entity.getOid()).equals(dto.getValue()))
            )
        );
    }

    /**
     * All storage volume attachment state options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testAttachmentStateOptions() throws Exception {

        final List<CriteriaOptionApiDTO> result =
            searchService.getCriteriaOptions(VOLUME_ATTACHMENT_STATE_FILTER_PATH, null, null, null);
        final List<String> expected = Arrays.stream(AttachmentState.values())
            .map(AttachmentState::name)
            .collect(Collectors.toList());
        final List<String> actual = result.stream().map(CriteriaOptionApiDTO::getValue)
            .collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    /**
     * All storage tier options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testStorageTierOptions() throws Exception {

        final List<MinimalEntity> storageTierEntities = ImmutableList.of(
            MinimalEntity.newBuilder().setOid(1L).setDisplayName("storage-tier-entity-1").build(),
            MinimalEntity.newBuilder().setOid(2L).setDisplayName("storage-tier-entity-2").build(),
            MinimalEntity.newBuilder().setOid(3L).setDisplayName("storage-tier-entity-3").build()
        );
        final SearchRequest mockRequest = ApiTestUtils.mockSearchMinReq(storageTierEntities);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(mockRequest);
        final List<CriteriaOptionApiDTO> result =
            searchService.getCriteriaOptions(CONNECTED_STORAGE_TIER_FILTER_PATH, null, null, null);
        assertEquals(storageTierEntities.size(), result.size());
        storageTierEntities.forEach(entity ->
            assertTrue(result.stream().anyMatch(dto ->
                entity.getDisplayName().equals(dto.getDisplayName()) &&
                    String.valueOf(entity.getOid()).equals(dto.getValue()))
            )
        );
    }

    /**
     * All compute tier options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testComputeTierOptions() throws Exception {

        final List<MinimalEntity> computeTierEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(1L).setDisplayName("compute-tier-entity-1").build(),
                MinimalEntity.newBuilder().setOid(2L).setDisplayName("compute-tier-entity-2").build(),
                MinimalEntity.newBuilder().setOid(3L).setDisplayName("compute-tier-entity-3").build()
        );
        final SearchRequest mockRequest = ApiTestUtils.mockSearchMinReq(computeTierEntities);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(mockRequest);
        final List<CriteriaOptionApiDTO> result =
                searchService.getCriteriaOptions(CONNECTED_COMPUTE_TIER_FILTER_PATH, null, ApiEntityType.VIRTUAL_MACHINE.apiStr(), null);
        assertEquals(computeTierEntities.size(), result.size());
        computeTierEntities.forEach(entity ->
                assertTrue(result.stream().anyMatch(dto ->
                        entity.getDisplayName().equals(dto.getDisplayName()) &&
                                String.valueOf(entity.getOid()).equals(dto.getValue()))
                )
        );
    }

    /**
     * All cloud provider options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testCloudProviderOptions() throws Exception {

        final TargetApiDTO probe1 = new TargetApiDTO();
        probe1.setType(probeType1);

        when(targetsService.getProbes()).thenReturn(Collections.singletonList(probe1));
        final List<CriteriaOptionApiDTO> result1 =
            searchService.getCriteriaOptions(CLOUD_PROVIDER_OPTION, null, null, null);

        assertEquals(1, result1.size());
        assertEquals(CloudType.AWS.name(), result1.get(0).getValue());

        final TargetApiDTO probe2 = new TargetApiDTO();
        probe2.setType(probeType2);

        when(targetsService.getProbes()).thenReturn(Collections.singletonList(probe2));
        final List<CriteriaOptionApiDTO> result2 =
            searchService.getCriteriaOptions(CLOUD_PROVIDER_OPTION, null, null, null);

        assertEquals(1, result2.size());
        assertEquals(CloudType.AZURE.name(), result2.get(0).getValue());

        final TargetApiDTO probe3 = new TargetApiDTO();
        probe3.setType("something");

        when(targetsService.getProbes()).thenReturn(Collections.singletonList(probe3));
        final List<CriteriaOptionApiDTO> result3 =
            searchService.getCriteriaOptions(CLOUD_PROVIDER_OPTION, null, null, null);

        Assert.assertEquals(Collections.emptyList(), result3);
    }

    /**
     * All cloud provider options should be present in criteria.
     *
     * @throws Exception if something is catastrophically wrong.
     */
    @Test
    public void testResourceGroupOptions() throws Exception {
        final GroupApiDTO resourceGroup1 = new GroupApiDTO();
        resourceGroup1.setDisplayName("Ravenclaw");
        resourceGroup1.setUuid("111");
        final GroupApiDTO resourceGroup2 = new GroupApiDTO();
        resourceGroup2.setDisplayName("Gryffindor");
        resourceGroup2.setUuid("222");

        Mockito.when(
                groupsService.getGroupsByType(GroupType.RESOURCE, null, Collections.emptyList()))
                .thenReturn(Arrays.asList(resourceGroup1, resourceGroup2));
        final List<CriteriaOptionApiDTO> result1 =
                searchService.getCriteriaOptions(EntityFilterMapper.MEMBER_OF_RESOURCE_GROUP_OID, null, null,
                        null);
        Assert.assertEquals(2, result1.size());
        Assert.assertEquals("Ravenclaw", result1.get(0).getDisplayName());
        Assert.assertEquals("111", result1.get(0).getValue());
        Assert.assertEquals("Gryffindor", result1.get(1).getDisplayName());
        Assert.assertEquals("222", result1.get(1).getValue());
    }

    /**
     * Test that all billing families exist in criteria options list.
     *
     * @throws Exception if something wrong.
     */
    @Test
    public void testBillingFamiliesOptions() throws Exception {
        final String bf1Name = "BillingFamily1";
        final String bf1Uuid = "1";
        final String bf2Name = "BillingFamily2";
        final String bf2Uuid = "2";

        final BillingFamilyApiDTO billingFamily1 = new BillingFamilyApiDTO();
        billingFamily1.setDisplayName(bf1Name);
        billingFamily1.setUuid(bf1Uuid);
        final BillingFamilyApiDTO billingFamily2 = new BillingFamilyApiDTO();
        billingFamily2.setDisplayName(bf2Name);
        billingFamily2.setUuid(bf2Uuid);

        Mockito.when(groupsService.getGroupsByType(GroupType.BILLING_FAMILY, null,
                Collections.emptyList())).thenReturn(Arrays.asList(billingFamily1, billingFamily2));
        final List<CriteriaOptionApiDTO> result =
                searchService.getCriteriaOptions(EntityFilterMapper.MEMBER_OF_BILLING_FAMILY_OID,
                        null, null, null);
        final Set<String> optionsValues =
                result.stream().map(CriteriaOptionApiDTO::getValue).collect(Collectors.toSet());
        final Set<String> optionsNames = result.stream()
                .map(CriteriaOptionApiDTO::getDisplayName)
                .collect(Collectors.toSet());
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(Sets.newHashSet(bf1Uuid, bf2Uuid), optionsValues);
        Assert.assertEquals(Sets.newHashSet(bf1Name, bf2Name), optionsNames);
    }

    /**
     * Tests to validate that logic to auto-create a display name matching filter on group searches
     * is working as expected.
     */
    @Test
    public void testAddGroupNameMatcher() {
        List<FilterApiDTO> originalFilters = new ArrayList<>();
        originalFilters.add(new FilterApiDTO());

        // verify that an empty or null string doesn't alter the contents of the list.
        Assert.assertEquals(1, searchService.addNameMatcher("", originalFilters, "Type", null).size());
        Assert.assertEquals(1, searchService.addNameMatcher(null, originalFilters, "Type", null).size());

        // a valid search string should increase the number of filters by 1
        Assert.assertEquals(2, searchService.addNameMatcher("match me bro", originalFilters, "Type", null).size());

        // verify that a null filter list but valid search string will give you a singleton list
        Assert.assertEquals(1, searchService.addNameMatcher("match me bro", null, "Type", null).size());
    }

    /**
     * This test ensures that options loading is implemented by the {@link SearchService} for all
     * the search criteria declaring that options have to be loaded from the server.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testCriteriaLoadOptions() throws Exception {
        for (Entry<String, GroupUseCase> useCaseEntry : groupUseCaseParser.getUseCases()
                .entrySet()) {
            final String entityType = useCaseEntry.getKey();
            for (GroupUseCaseCriteria criteria: useCaseEntry.getValue().getCriteria()) {
                if (criteria.isLoadOptions()) {
                    testCriteriaLoadOptions(entityType, criteria.getElements());
                }
            }
        }
    }

    private void testCriteriaLoadOptions(@Nonnull String entityType, @Nonnull String criteriaKey) {
        try {
            searchService.getCriteriaOptions(criteriaKey, null, entityType, null);
        } catch (UnknownObjectException e) {
            Assert.fail("Loading options for entity type " + entityType +
                    " is not supported for criteria " + criteriaKey);
        } catch (Exception e) {
            // It is Ok to have it here.
        }
    }

    private List<ApiPartialEntity> setupEntitiesForMemberQuery() {
        final ApiPartialEntity e1 = ApiPartialEntity.newBuilder()
                .setOid(1)
                .setDisplayName("afoobar")
                .setEntityType(0)
                .putDiscoveredTargetData(targetId1, PerTargetEntityInformation.getDefaultInstance())
                .build();
        final ApiPartialEntity e2 = ApiPartialEntity.newBuilder()
                .setOid(4)
                .setDisplayName("Foo")
                .setEntityType(0)
                .putDiscoveredTargetData(targetId2, PerTargetEntityInformation.getDefaultInstance())
                .build();
        final ApiPartialEntity e3 = ApiPartialEntity.newBuilder()
                .setOid(5)
                .setDisplayName("Foo [bar]")
                .setEntityType(0)
                .putDiscoveredTargetData(targetId2, PerTargetEntityInformation.getDefaultInstance())
                .build();
        final ServiceEntityApiDTO mappedE1 = new ServiceEntityApiDTO();
        mappedE1.setUuid("1");
        final ServiceEntityApiDTO mappedE2 = new ServiceEntityApiDTO();
        mappedE2.setUuid("4");
        final ServiceEntityApiDTO mappedE3 = new ServiceEntityApiDTO();
        mappedE3.setUuid("5");
        when(serviceEntityMapper.toServiceEntityApiDTO(e1)).thenReturn(mappedE1);
        when(serviceEntityMapper.toServiceEntityApiDTO(e2)).thenReturn(mappedE2);
        when(serviceEntityMapper.toServiceEntityApiDTO(e3)).thenReturn(mappedE3);

        Map<Long, ServiceEntityApiDTO> seMap = new HashMap<>();
        seMap.put(Long.parseLong(mappedE1.getUuid()), mappedE1);
        seMap.put(Long.parseLong(mappedE2.getUuid()), mappedE2);
        seMap.put(Long.parseLong(mappedE3.getUuid()), mappedE3);
        when(serviceEntityMapper.toServiceEntityApiDTOMap(any())).thenReturn(seMap);

        final List<ApiPartialEntity> entities = Arrays.asList(e1, e2, e3);
        return entities;
    }

    private List<ServiceEntityApiDTO> setupEntitiesForWorkloadsQuery() {
        final ServiceEntityApiDTO entity1 = supplyChainTestUtils
                .createServiceEntityApiDTO(ENTITY_ID_1, targetId1);
        entity1.setDisplayName("Virtual Machine 1");
        entity1.setClassName(StringConstants.VIRTUAL_MACHINE);

        final ServiceEntityApiDTO entity2 = supplyChainTestUtils
                .createServiceEntityApiDTO(ENTITY_ID_2, targetId1);
        entity2.setDisplayName("Database 1");
        entity2.setClassName(StringConstants.DATABASE);

        final ServiceEntityApiDTO entity3 = supplyChainTestUtils
                .createServiceEntityApiDTO(ENTITY_ID_3, targetId1);
        entity3.setDisplayName("Database Server 1");
        entity3.setClassName(StringConstants.DATABASE_SERVER);

        final ServiceEntityApiDTO entity4 = supplyChainTestUtils
                .createServiceEntityApiDTO(ENTITY_ID_4, targetId1);
        entity4.setDisplayName("Virtual Machine 2");
        entity4.setClassName(StringConstants.VIRTUAL_MACHINE);

        return Arrays.asList(entity1, entity2, entity3, entity4);
    }


    /**
     * Tests search service entities with all nullable options unset.
     * @throws Exception pagination request is invalid
     */
    @Test
    public void getSearchResultsonServiceEntitiesPaginated() throws Exception {
        //GIVEN
        final List<String> types = Collections.singletonList("VirtualMachine");

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();

        //WHEN
        searchService.getSearchResults(null, types, null, null, null, null, null,
                                       getSearchPaginationRequest(10), null, null, null, null);
        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), searchPaginationRequestCaptor.capture());

        SearchQuery searchQuery = searchQueryCaptor.getValue();
        assertEquals(searchQuery.getSearchParametersCount(), 1);
        assertEquals(searchQuery.getSearchParameters(0).getStartingFilter().getNumericFilter().getValue(), 10L);
        assertTrue(scopeIds.getValue().isEmpty());
    }

    /**
     * Tests search service entities, types is null.
     * @throws Exception something went wrong
     */
    @Test(expected = IllegalArgumentException.class)
    public void getSearchResultsOnServiceEntitiesWithTypesNull() throws Exception {
        //WHEN
        searchService.queryServiceEntitiesPaginated(null, null, null, null, null, null, null,
                                       null, null);
    }

    /**
     * Tests search service entities, types is empty collection.
     * @throws Exception something went wrong
     */
    @Test(expected = IllegalArgumentException.class)
    public void getSearchResultsonServiceEntitiesWithTypesEmptyCollection() throws Exception {
        searchService.queryServiceEntitiesPaginated(null, Collections.EMPTY_LIST, null, null, null, null, null,
                                                    null, null);
    }

    /**
     * Tests search service entities.
     *
     * <p>All options non null</p>
     * @throws Exception something went wrong
     */
    @Test
    public void getSearchResultsonServiceEntitiesWithAllParamsNonNull() throws Exception {
        //GIVEN
        final String nameRegex = "nameRegex";
        final List<String> types = Collections.singletonList("VirtualMachine");
        final String state = EntityState.ACTIVE.name();
        final EnvironmentType envirType = EnvironmentType.CLOUD;
        final EntityDetailType entityDetailType = EntityDetailType.aspects;
        final SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest("99", 99, true, SearchOrderBy.DEFAULT);
        final List<String> scopes = Collections.EMPTY_LIST;
        final List<String> probeTypes = Collections.singletonList("AWS");
        long targetID = 12345;
        final ThinTargetInfo thinTargetInfoAWS = ImmutableThinTargetInfo
                        .builder()
                        .oid(targetID)
                        .displayName("mockedThinTargetInfo")
                        .isHidden(false)
                        .probeInfo(ImmutableThinProbeInfo.builder()
                                                   .oid(1)
                                                   .type(probeTypes.get(0))
                                                   .category("Cloud")
                                                   .uiCategory("Cloud")
                                                   .build())
                        .build();

        final ThinTargetInfo thinTargetInfoRandom = ImmutableThinTargetInfo
                        .builder()
                        .oid(1222222)
                        .displayName("mockedThinTargetInfo")
                        .isHidden(false)
                        .probeInfo(ImmutableThinProbeInfo.builder()
                                                   .oid(444)
                                                   .type("SOME TYPE")
                                                   .category("Cloud")
                                                   .uiCategory("Cloud")
                                                   .build())
                        .build();

        doReturn(Arrays.asList(thinTargetInfoRandom, thinTargetInfoAWS)).when(thinTargetCacheMock).getAllTargets();

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();

        //WHEN
        searchService.getSearchResults(nameRegex, types, scopes, state, null, envirType, entityDetailType,
                                       searchPaginationRequest, null, probeTypes, null, null);
        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), searchPaginationRequestCaptor.capture());

        //Filter Checks
        SearchQuery searchQuery = searchQueryCaptor.getValue();
        assertEquals(searchQuery.getSearchParametersCount(), 1);
        assertEquals(searchQuery.getSearchParameters(0).getStartingFilter().getNumericFilter().getValue(), 10L);

        Collection<SearchFilter> searchFilters = searchQuery.getSearchParameters(0).getSearchFilterList();
        assertEquals(searchFilters.size(), 4);

        SearchFilter envFilter = SearchProtoUtil.searchFilterProperty(SearchProtoUtil.environmentTypeFilter(
                        EnvironmentTypeMapper.fromApiToXL(envirType)));
        assertTrue(searchFilters.contains(envFilter));

        SearchFilter queryFilter = SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.nameFilterRegex(nameRegex));
        assertTrue(searchFilters.contains(queryFilter));

        SearchFilter stateFilter = SearchProtoUtil.searchFilterProperty(SearchProtoUtil.stateFilter(state));
        assertTrue(searchFilters.contains(stateFilter));

        SearchFilter probeFilters = SearchProtoUtil.searchFilterProperty(SearchProtoUtil.discoveredBy(Collections.singletonList(targetID)));
        assertTrue(searchFilters.contains(probeFilters));

        //Scope check
        assertTrue(scopeIds.getValue().isEmpty());
        //assertEquals(searchPaginationRequestCaptor.getValue(), searchPaginationRequest);
        SearchPaginationRequest searchPaginationRequestCaptured = searchPaginationRequestCaptor.getValue();
        assertEquals(searchPaginationRequestCaptured.getLimit(), searchPaginationRequest.getLimit());
        assertEquals(searchPaginationRequestCaptured.getCursor(), searchPaginationRequest.getCursor());
        assertEquals(searchPaginationRequestCaptured.getOrderBy(), searchPaginationRequest.getOrderBy());
        assertEquals(searchPaginationRequestCaptured.isAscending(), searchPaginationRequest.isAscending());

    }

    /**
     * Tests search service entities.
     *
     * <p>Tests scope added to search parameters</p>
     * @throws Exception something went wrong
     */
    @Test
    public void getSearchResultsEntityWithScope() throws Exception {
        //GIVEN
        final String scopeID = "324234";
        List<String> scopes = Collections.singletonList(scopeID);

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();

        //WHEN
            searchService.queryServiceEntitiesPaginated(null, Collections.singletonList("VirtualMachine"),
                                                        null, null, null, getSearchPaginationRequest(10),
                                                        scopes, null, null);
        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIdsCaptor = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIdsCaptor.capture(), searchPaginationRequestCaptor.capture());

        assertEquals(1, scopeIdsCaptor.getValue().size());
        assertTrue(scopeIdsCaptor.getValue().contains(Long.valueOf(scopeID)));
    }

    /**
     * Test target type options for filter.
     *
     * @throws Exception if something is wrong.
     */
    @Test
    public void testTargetTypeOptions() throws Exception {
        final TargetApiDTO probe1 = new TargetApiDTO();
        probe1.setType(probeType1);

        when(targetsService.getProbesForTargetTypeFilter()).thenReturn(Collections.singletonList(probe1));
        final List<CriteriaOptionApiDTO> result1 =
                        searchService.getCriteriaOptions(TARGET_TYPE_OPTION, null, null, null);

        assertEquals(1, result1.size());
        assertEquals(SDKProbeType.AWS.getProbeType(), result1.get(0).getValue());

        final TargetApiDTO probe2 = new TargetApiDTO();
        probe2.setType(probeType2);

        when(targetsService.getProbesForTargetTypeFilter()).thenReturn(Collections.singletonList(probe2));
        final List<CriteriaOptionApiDTO> result2 =
                        searchService.getCriteriaOptions(TARGET_TYPE_OPTION, null, null, null);

        assertEquals(1, result2.size());
        assertEquals(SDKProbeType.AZURE.getProbeType(), result2.get(0).getValue());

        final TargetApiDTO probe3 = new TargetApiDTO();
        probe3.setType("Azure Service Principal");

        when(targetsService.getProbesForTargetTypeFilter()).thenReturn(Collections.singletonList(probe3));
        final List<CriteriaOptionApiDTO> result3 =
                        searchService.getCriteriaOptions(TARGET_TYPE_OPTION, null, null, null);

        Assert.assertNotEquals(Collections.emptyList(), result3);
    }


    /**
     * Create SearchPaginationRequest object.
     * @param limit used to configure pagination object
     * @return SearchPaginationRequest
     * @throws InvalidOperationException invalid pagination parameters
     */
    private SearchPaginationRequest getSearchPaginationRequest(Integer limit)
                    throws InvalidOperationException {
        return new SearchPaginationRequest(null, limit, true, null);
    }
}
