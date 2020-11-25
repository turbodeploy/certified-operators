package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.ACCOUNT_OID;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.CONNECTED_STORAGE_TIER_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.STATE;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.USER_DEFINED_ENTITY;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.VOLUME_ATTACHMENT_STATE_FILTER_PATH;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getMembersBasedOnFilter;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getSearchResults;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
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
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
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
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit test for {@link SearchService}.
 */
public class SearchServiceTest {

    private static final String CLOUD_PROVIDER_OPTION =
            "discoveredBy:" + SearchableProperties.CLOUD_PROVIDER;

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
                            .put(QueryType.CONTAINS, "^.*\\Qwin\\E.*$")
                            .put(QueryType.EXACT, "^\\Q.*win.*\\E$")
                            .put(QueryType.REGEX, "^.*win.*$")
                            .build())

                    .put(".*win (.*", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*\\Qwin (\\E.*$")
                            .put(QueryType.EXACT, "^\\Q.*win (.*\\E$")
                            .put(QueryType.REGEX, "^.*win (.*$")
                            .build())
                    .put("win (", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*\\Qwin (\\E.*$")
                            .put(QueryType.EXACT, "^\\Qwin (\\E$")
                            .put(QueryType.REGEX, "^win ($")
                            .build())
                    .put("win (=| .*", ImmutableMap.<QueryType, String>builder()
                            .put(QueryType.CONTAINS, "^.*\\Qwin (=| \\E.*$")
                            .put(QueryType.EXACT, "^\\Qwin (=| .*\\E$")
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
                thinTargetCacheMock));
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

    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResults() throws Exception {
        final SearchPaginationResponse paginationResponse = Mockito.mock(SearchPaginationResponse.class);
        final SearchPaginationRequest paginationRequest = Mockito.mock(SearchPaginationRequest.class);

        getSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(targetsService).getTargets(null);

        getSearchResults(searchService, null, Lists.newArrayList("BusinessAccount"), null, null, null, EnvironmentType.CLOUD, null, null);
        verify(businessAccountRetriever).getBusinessAccountsInScope(null, Collections.emptyList());

        //TODO: MOVE TO OWN TEST
        final PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        when(mockPaginatedSearchRequest.getResponse()).thenReturn(mock(SearchPaginationResponse.class));
        final SearchPaginationRequest paginatedSearchRequest = new SearchPaginationRequest(null, null, true, null);
        searchService.getSearchResults(null,
                         Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()), null, null,
                         null, EnvironmentType.CLOUD, null, paginatedSearchRequest, null, null, true, null, null);
        SearchQuery searchQuery = SearchQuery.newBuilder()
                        .addSearchParameters(
                                        SearchProtoUtil
                                                        .makeSearchParameters(SearchProtoUtil.entityTypeFilter(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr())))
                                                        .addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.environmentTypeFilter(EnvironmentTypeEnum.EnvironmentType.CLOUD))))
                        .build();

        verify(repositoryApi).newPaginatedSearch(searchQuery, Collections.EMPTY_SET, paginatedSearchRequest);

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), isA(
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
            true, Origin.USER,  null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), isA(Origin.class));

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), eq(null))).thenReturn(paginationResponse);
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
                true, null,
                null));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), eq(null));

        when(groupsService.getGroupsByType(any(), any(), any(), eq(EnvironmentType.ONPREM))).thenReturn(Collections.emptyList());
        when(paginationRequest.allResultsResponse(any())).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
            null,
            Lists.newArrayList("Cluster"),
            null,
            null,
            null,
            EnvironmentType.ONPREM,
            null,
            paginationRequest,
            null,
            null,
            true, null, null));
        verify(groupsService).getGroupsByType(eq(GroupType.COMPUTE_HOST_CLUSTER), any(), any(), eq(EnvironmentType.ONPREM));
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
        final SearchPaginationRequest paginationRequest =
                Mockito.mock(SearchPaginationRequest.class);
        String query = "query";
        when(paginationRequest.allResultsResponse(any())).thenReturn(paginationResponse);
        when(groupsService.getGroupsByType(any(), any(), any(), eq(EnvironmentType.ONPREM))).thenReturn(Lists.newArrayList());

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
                paginationRequest,
                null,
                null,
                true,
                null, null);

        //THEN
        assertEquals(response, paginationResponse);
        verify(groupsService).getGroupsByType(eq(GroupType.COMPUTE_HOST_CLUSTER), any(), any(), eq(EnvironmentType.ONPREM));
        verify(searchService).addNameMatcher(queryArgCap.capture(), any(), any(), any());
        assertEquals(query, queryArgCap.getValue());
    }



    /**
     * This tests if query is handled accordingly in case it contains
     * a special char.
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResultsWithSpecialCharNameQuery() throws Exception {

        final SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(null, null, true, null);
        List<ServiceEntityApiDTO> searchResultDTOs = Lists.newArrayList(
                supplyChainTestUtils.createServiceEntityApiDTO(999, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1));

        PaginatedSearchRequest paginatedSearchRequest = ApiTestUtils.mockPaginatedSearchRequest(searchPaginationRequest, searchResultDTOs);
        doReturn(paginatedSearchRequest).when(repositoryApi).newPaginatedSearch(any(), any(), eq(searchPaginationRequest));

        for (Entry<String, ImmutableMap<QueryType, String>> entry : nameQueryMap.entrySet()) {
            for (QueryType queryType : QueryType.values()) {
                searchService.getSearchResults(entry.getKey(), Lists.newArrayList("VirtualMachine"),
                        Lists.newArrayList("Market"), null, null, EnvironmentType.ONPREM, null,
                                               searchPaginationRequest, null, null, true, null, queryType);

                final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
                final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
                verify(repositoryApi, Mockito.atMost(nameQueryMap.size() * QueryType.values().length)).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), eq(searchPaginationRequest));
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
                null, null, null, Lists.newArrayList("probeType1"), true, null, null);
    }


    /**
     * This tests whether an exception is thrown if the query specifies an invalid scope.
     * @throws Exception this is expected from this test because the arguments are invalid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidScopeGetSearchResults() throws Exception {
        // Type, entity type and group type are all empty. This should throw an exception.
        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());
        when(groupsService.expandUuids(any(), any(), any())).thenReturn(Collections.emptySet());
        searchService.getSearchResults(".*a.*", Lists.newArrayList("VirtualMachine"), Lists.newArrayList("string1"), "ACTIVE", null, EnvironmentType.CLOUD,
                null, null, null, Lists.newArrayList("probeType1"), true, null, null);
    }

    @Test
    public void testGetSearchGroup() throws Exception {
        final SearchPaginationResponse paginationResponse =
            Mockito.mock(SearchPaginationResponse.class);
        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), eq(null))).thenReturn(paginationResponse);
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
                true,
                null,
                null));
        verify(targetsService, Mockito.never()).getTargets(null);
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true), eq(null));
        verify(searchService).addNameMatcher(any(), any(), any(), any());

    }
    /**
     * Test scoped to a cluster;  Expanded entities passed as scope arguments
     * https://ml-xl-dev-2/vmturbo/rest/search?disable_hateoas=true&q=&scopes=283218897841408&types=PhysicalMachine
     */
    @Test
    public void testSearchWithClusterInScopes() throws Exception {
        // Arrange
        final String CLUSTER_OID = "283218897841408";
        GroupApiDTO clusterGroup = new GroupApiDTO();
        clusterGroup.setMemberUuidList(Lists.newArrayList("1", "2", "3", "4"));
        List<ServiceEntityApiDTO> searchResultDTOs = Lists.newArrayList(
                supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1));

        List<String> scopes = Lists.newArrayList(CLUSTER_OID);
        List<String> types = Lists.newArrayList("PhysicalMachine");

        SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(null, null, true, null);
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
                        null, searchPaginationRequest, null, null, true, null, null);
        //Assert

        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIds = ArgumentCaptor.forClass((Class)Set.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIds.capture(), eq(searchPaginationRequest));

        assertEquals(scopeIds.getValue().size(), 4);
        assertThat(scopeIds.getValue(), containsInAnyOrder(1L, 2L, 3L, 4L));
    }

    /**
     * Test scoped to a PM;  search result returns the Cluster the PM belongs to .
     * https://IP/vmturbo/rest/search?disable_hateoas=true&q=&scopes=283218897841408&types=Cluster
     */
    @Test
    public void testSearchWithPMInScopes() throws Exception {

        // Arrange
        final String PM_OID = "283218897841408";
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setDisplayName("display name");
        groupApiDTO.setUuid("00000");
        groupApiDTO.setClassName(ConstraintType.CLUSTER.name());

        List<String> scopes = Lists.newArrayList(PM_OID);
        List<String> types = Lists.newArrayList("Cluster");

        when(groupsService.getGroupsByType(GroupType.COMPUTE_HOST_CLUSTER,
                        Collections.singletonList(PM_OID), Collections.emptyList(), null))
            .thenReturn(Collections.singletonList(groupApiDTO));

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes,
                null, null, null, null, null);

        // Assert
        assertThat(results, hasItems(groupApiDTO));
    }

    /**
     * Tests getSearchResults with {@link EntityDetailType}.aspects.
     *
     * <p>Expect {@link EntityAspectMapper} to not be added to search request</p>
     * @throws Exception No expected to be thrown
     */
    @Test
    public void testSearchWithClusterEntityDetailTypeAspects() throws Exception {
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

        final SearchPaginationRequest paginationRequest = Mockito.mock(SearchPaginationRequest.class);

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
                        null, true, null, null);

        //THEN
        verify(req).requestAspects(entityAspectMapper, null);
    }

    /**
     * Tests getSearchResults on type VirtualMachine, list of scopes, with entityDetailType null.
     *
     * <p>Expect {@link EntityAspectMapper} to not be added to search request</p>
     * @throws Exception No expected to be thrown
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
                        Mockito.mock(SearchPaginationRequest.class),
                        null,
                        null, true, null, null);

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
        getSearchResults(searchService, null, types, scopes,
            null, null, null, null, null);
        verify(businessAccountRetriever).getBusinessAccountsInScope(scopes, Collections.emptyList());
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

        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();


        //WHEN
        searchService.getMembersBasedOnFilter("foo", groupApiDTO, paginationRequest, null, null);

        //THEN
        verify(mockPaginatedSearchRequest, times(1)).getResponse();
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
        assertEquals("^.*\\Q[b\\E.*$", value);

        verify(paginatedSearchRequestMock, times(1)).getResponse();
        assertTrue(scopeIds.getValue().isEmpty());
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
        Mockito.when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), any(), eq(false), eq(null)))
            .thenReturn(response);
        final ArgumentCaptor<Set<String>> resultCaptor = ArgumentCaptor.forClass((Class)HashSet.class);
        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);

        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForVirtualMachineGroups,
            paginationRequest, null, null));
        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForAllGroups,
            paginationRequest, null, null));
        verify(groupsService, times(2)).getPaginatedGroupApiDTOs(any(), any(), resultCaptor.capture(), any(), any(), eq(false), eq(null));
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

        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.getCursor()).thenReturn(Optional.empty());
        Mockito.when(paginationRequest.allResultsResponse(any()))
                .thenReturn(mock(SearchPaginationResponse.class));
        Mockito.when(paginationRequest.getOrderBy())
                .thenReturn(SearchOrderBy.NAME);

        searchService.getMembersBasedOnFilter("", request, paginationRequest, null, null);
        verify(paginationRequest).allResultsResponse(resultCaptor.capture());

        final List<Long> resultIds = resultCaptor.getValue()
                .stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toList());

        final Map<Long, String> resultById = resultCaptor.getValue().stream()
            .collect(Collectors.toMap(se -> Long.valueOf(se.getUuid()), se -> se.getClassName()));

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
     * Test getMembersBasedOnFilterQuery when we are searching for business accounts.
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfBusinessAccountOnQueryName() throws Exception {
        GroupApiDTO inputDto = new GroupApiDTO();
        inputDto.setClassName(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        String nameQueryString = "test";
        getMembersBasedOnFilter(searchService, nameQueryString, inputDto);
        FilterApiDTO filterApiDTO = new FilterApiDTO();
        filterApiDTO.setExpType(EntityFilterMapper.REGEX_MATCH);
        filterApiDTO.setExpVal(".*test.*");
        filterApiDTO.setFilterType("businessAccountByName");
        final ArgumentCaptor<List<FilterApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        verify(businessAccountRetriever).getBusinessAccountsInScope(eq(null), resultCaptor.capture());
        FilterApiDTO filterApiDTOCaptured = resultCaptor.getValue().get(0);
        assertEquals(filterApiDTOCaptured.getExpType(), EntityFilterMapper.REGEX_MATCH);
        assertEquals(filterApiDTOCaptured.getExpVal(), ".*test.*" );
        assertEquals(filterApiDTOCaptured.getFilterType(), "businessAccountByName");
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
        //Workload gets expanded to 3 entityTypes
        assertEquals(startingFilter.getStringFilter().getOptionsCount(), 3);

        Collection<SearchFilter> searchFilters = searchQuery.getSearchParameters(0).getSearchFilterList();

        SearchFilter queryFilter = SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.nameFilterRegex("^.*\\Qfoo\\E.*$"));

        assertTrue(searchFilters.contains(queryFilter));

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
        Mockito.when(tagsService.getTags(any(), any(), any())).thenReturn(Arrays.asList(tagsFromTagService));

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
    public void getSearchResultsonServiceEntities() throws Exception {
        //GIVEN
        final List<String> types = Collections.singletonList("VirtualMachine");

        PaginatedSearchRequest mockPaginatedSearchRequest = mock(PaginatedSearchRequest.class);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();

        //WHEN
        searchService.getSearchResults(null, types, null, null, null, null, null,
                                       null, null, null, true, null, null);
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
                                       searchPaginationRequest, null, probeTypes, true, null, null);
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
        assertEquals(searchPaginationRequestCaptor.getValue(), searchPaginationRequest);
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
                                                        null, null, null, null,
                                                        scopes, null, null);
        //THEN
        final ArgumentCaptor<SearchQuery> searchQueryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<Set<Long>> scopeIdsCaptor = ArgumentCaptor.forClass((Class)Set.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryCaptor.capture(), scopeIdsCaptor.capture(), searchPaginationRequestCaptor.capture());

        assertEquals(1, scopeIdsCaptor.getValue().size());
        assertTrue(scopeIdsCaptor.getValue().contains(Long.valueOf(scopeID)));
    }
}
