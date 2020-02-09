package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.ACCOUNT_OID;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.CONNECTED_STORAGE_TIER_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.STATE;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.VOLUME_ATTACHMENT_STATE_FILTER_PATH;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getSearchResults;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;

import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Unit test for {@link SearchService}.
 */
public class SearchServiceTest {

    private static final String CLOUD_PROVIDER_OPTION =
            "discoveredBy:" + SearchableProperties.CLOUD_PROVIDER;

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();
    private SearchService searchService;
    private MarketsService marketsService = mock(MarketsService.class);
    private GroupsService groupsService = mock(GroupsService.class);
    private TargetsService targetsService = mock(TargetsService.class);
    private RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private GroupMapper groupMapper;
    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final GroupUseCaseParser groupUseCaseParser =
            new GroupUseCaseParser("groupBuilderUsecases.json");
    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);
    private final UuidMapper uuidMapper = mock(UuidMapper.class);
    private final GroupExpander groupExpander = mock(GroupExpander.class);
    private final PaginationMapper paginationMapperSpy = spy(new PaginationMapper());
    private final TagsService tagsService = mock(TagsService.class);
    private final BusinessAccountRetriever businessAccountRetriever = mock(BusinessAccountRetriever.class);
    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private final EntityFilterMapper entityFilterMapper = new EntityFilterMapper(groupUseCaseParser);
    private final GroupFilterMapper groupFilterMapper = new GroupFilterMapper();
    private final EntityAspectMapper entityAspectMapper = mock(EntityAspectMapper.class);

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

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);
    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);
    private final CloudTypeMapper cloudTypeMapper = mock(CloudTypeMapper.class);
    private ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);
    private ExecutorService threadPool;

    @Before
    public void setUp() throws Exception {
        threadPool = Executors.newCachedThreadPool();
        final long realTimeContextId = 777777;
        final SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final StatsHistoryServiceBlockingStub statsHistoryServiceStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final EntitySeverityServiceBlockingStub severityGrpcStub =
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final GroupServiceBlockingStub groupServiceBlockingStub =
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final CostServiceBlockingStub costServiceBlockingStub =
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final SearchFilterResolver
                searchFilterResolver = Mockito.mock(SearchFilterResolver.class);
        Mockito.when(searchFilterResolver.resolveExternalFilters(Mockito.any()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);
        when(userSessionContext.isUserScoped()).thenReturn(false);
        groupMapper = new GroupMapper(supplyChainFetcherFactory, groupExpander,
                repositoryApi, entityFilterMapper, groupFilterMapper, severityPopulator,
                businessAccountRetriever, costServiceBlockingStub, realTimeContextId, targetCache,
                cloudTypeMapper, threadPool);

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
                searchFilterResolver));
    }

    /**
     * Cleans up resources after the test.
     */
    @After
    public void shutdown() {
        threadPool.shutdownNow();
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
        final SearchPaginationResponse paginationResponse =
            Mockito.mock(SearchPaginationResponse.class);
        final SearchPaginationRequest paginationRequest =
            Mockito.mock(SearchPaginationRequest.class);

        getSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM, null, null);
        verify(targetsService).getTargets(null);

        getSearchResults(searchService, null, Lists.newArrayList("BusinessAccount"), null, null, null, EnvironmentType.CLOUD, null, null);
        verify(businessAccountRetriever).getBusinessAccountsInScope(null, null);

        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true))).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
            null,
            Lists.newArrayList("Group", "Cluster"),
            null,
            null,
            Collections.singletonList(null),
            EnvironmentType.ONPREM,
            null,
            null,
            null,
            null,
            true));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(true));
        when(groupsService.getGroupsByType(any(), any(), any(), eq(EnvironmentType.ONPREM))).thenReturn(Collections.emptyList());
        when(paginationRequest.allResultsResponse(any())).thenReturn(paginationResponse);
        assertEquals(paginationResponse, searchService.getSearchResults(
            null,
            Lists.newArrayList("Cluster"),
            null,
            null,
            Collections.singletonList(null),
            EnvironmentType.ONPREM,
            null,
            paginationRequest,
            null,
            null,
            true));
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
                Collections.singletonList(null),
                EnvironmentType.ONPREM,
                null,
                paginationRequest,
                null,
                null,
                true);

        //THEN
        assertEquals(response, paginationResponse);
        verify(groupsService).getGroupsByType(eq(GroupType.COMPUTE_HOST_CLUSTER), any(), any(), eq(EnvironmentType.ONPREM));
        verify(searchService).addNameMatcher(queryArgCap.capture(), any(), any());
        assertEquals(query, queryArgCap.getValue());
    }

    /**
     * This tests whether an exception is thrown if the query does not specify either a type or group type
     * @throws Exception this is expected from this test because the arguments are invalid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidGetSearchResults() throws Exception {
        // Type, entity type and group type are all empty. This should throw an exception.
        searchService.getSearchResults(".*a.*", null, Lists.newArrayList("string1"), "ACTIVE", null, EnvironmentType.CLOUD,
                null, null, null, Lists.newArrayList("probeType1"), true);
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
                null, null, null, Lists.newArrayList("probeType1"), true);
    }

    @Test
    public void testGetSearchEntitiesByProbeTypes() throws Exception {
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
        SearchRequest req = ApiTestUtils.mockSearchSEReq(regions);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);

        // filter by AWS
        Collection<BaseApiDTO> regions_aws = getSearchResults(searchService, null,
            types, null, null, null, null,
            Lists.newArrayList(probeType1), null);
        assertThat(regions_aws.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L));

        // filter by Azure
        Collection<BaseApiDTO> regions_azure = getSearchResults(searchService, null,
            types, null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(probeType2), null);
        assertThat(regions_azure.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(3L));

        // filter by both AWS and Azure
        Collection<BaseApiDTO> regions_all = getSearchResults(searchService, null,
            types, null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(probeType1, probeType2),  null);
        assertThat(regions_all.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));

        // filter by a vc probe type
        Collection<BaseApiDTO> regions_vc = getSearchResults(searchService, null,
            types, null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(SDKProbeType.VCENTER.getProbeType()), null);
        assertThat(regions_vc, empty());

        // filter by null probeTypes
        Collection<BaseApiDTO> regions_null_probeType = getSearchResults(searchService, null,
            types, null, null, null, EnvironmentType.CLOUD, null, null);
        assertThat(regions_null_probeType.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));
    }

    @Test
    public void testGetSearchGroup() throws Exception {
        final SearchPaginationResponse paginationResponse =
            Mockito.mock(SearchPaginationResponse.class);
        when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(false))).thenReturn(paginationResponse);
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
            true));
        verify(targetsService, Mockito.never()).getTargets(null);
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));
        verify(groupsService).getPaginatedGroupApiDTOs(any(), any(), any(), eq(EnvironmentType.ONPREM), any(), eq(false));
        verify(searchService).addNameMatcher(any(), any(), any());

    }
    /**
     * Test scoped to a cluster;  search result returns 5 SE's, one of which isn't in cluster.
     * https://ml-xl-dev-2/vmturbo/rest/search?disable_hateoas=true&q=&scopes=283218897841408&types=PhysicalMachine
     */
    @Test
    public void testSearchWithClusterInScopes() throws Exception {

        // Arrange
        final String CLUSTER_OID = "283218897841408";
        GroupApiDTO clusterGroup = new GroupApiDTO();
        clusterGroup.setMemberUuidList(Lists.newArrayList("1", "2", "3", "4"));
        List<ServiceEntityApiDTO> searchResultDTOs = Lists.newArrayList(
                supplyChainTestUtils.createServiceEntityApiDTO(999, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1));

        List<String> scopes = Lists.newArrayList(CLUSTER_OID);
        List<String> types = Lists.newArrayList("PhysicalMachine");
        SearchRequest req = ApiTestUtils.mockSearchSEReq(searchResultDTOs);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);

        SupplychainEntryDTO pmSupplyChainEntryDTO = new SupplychainEntryDTO();
        pmSupplyChainEntryDTO.setInstances(ImmutableMap.of(
                "1", supplyChainTestUtils.createServiceEntityApiDTO(1, targetId1),
                "2", supplyChainTestUtils.createServiceEntityApiDTO(2, targetId1),
                "3", supplyChainTestUtils.createServiceEntityApiDTO(3, targetId1),
                "4", supplyChainTestUtils.createServiceEntityApiDTO(4, targetId1)));

        SupplychainApiDTO mockSupplychainApiDto = supplyChainTestUtils.createSupplychainApiDTO();
        mockSupplychainApiDto.getSeMap().put("PhysicalMachine", pmSupplyChainEntryDTO);
        Map<String, SupplychainEntryDTO> seMap = ImmutableMap.of("PhysicalMachine", pmSupplyChainEntryDTO);
        mockSupplychainApiDto.setSeMap(seMap);
        /*
                    SupplychainApiDTO supplychain = supplyChainFetcher.fetch(
                    uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR).oid(),
                    scopeEntities, types, environmentType, null, false, 3, TimeUnit.MINUTES);

         */
        SupplychainApiDTOFetcherBuilder mockOperationBuilder =
            ApiTestUtils.mockApiDTOFetcherBuilder(mockSupplychainApiDto);
        when(supplyChainFetcherFactory.newApiDtoFetcher()).thenReturn(mockOperationBuilder);

        when(groupsService.expandUuids(eq(Sets.newHashSet(scopes)), eq(types), eq(null)))
            .thenReturn(ImmutableSet.of(1L, 2L, 3L, 4L));

        ApiTestUtils.mockRealtimeId(UuidMapper.UI_REAL_TIME_MARKET_STR, 777777, uuidMapper);

        when(groupExpander.getGroup(any())).thenReturn(Optional.of(Grouping.newBuilder()
                .setId(5L)
                .setDefinition(GroupDefinition.getDefaultInstance())
                .build()
        ));

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types,
                scopes, null, null, null, null,
                null);

        // Assert
        assertThat(results.size(), is(4));
        assertThat(results.stream().map(BaseApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1", "2", "3", "4"));

        verify(groupsService, Mockito.never()).getGroups();
        verify(targetsService, Mockito.never()).getTargets(null);
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));

        ArgumentCaptor<SearchParameters> paramsCaptor = ArgumentCaptor.forClass(SearchParameters.class);
        verify(repositoryApi).newSearchRequest(paramsCaptor.capture());
        assertThat(paramsCaptor.getValue().getStartingFilter(), is(SearchProtoUtil.entityTypeFilter("PhysicalMachine")));
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
        SearchRequest req = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);
        when(req.getSEList()).thenReturn(Collections.EMPTY_LIST);

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

        //WHEN
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes, null, null, null, null, entityDetailType);

        //THEN
        verify(req).useAspectMapper(entityAspectMapper);
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
        SearchRequest req = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);
        when(req.getSEList()).thenReturn(Collections.EMPTY_LIST);

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
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes, null, null, null, null, entityDetailType);

        //THEN
        verify(req, times(0)).useAspectMapper(entityAspectMapper);
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
        final List<String> types = ImmutableList.of(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes,
            null, null, null, null, null);
        verify(businessAccountRetriever).getBusinessAccountsInScope(scopes, null);
    }

    @Test
    public void testGetMembersBasedOnFilterSeverity() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setCriteriaList(Collections.emptyList());
        List<ServiceEntityApiDTO> serviceEntities = Collections.singletonList(
                supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        when(searchServiceSpy.searchEntityOids(any())).thenReturn(SearchEntityOidsResponse.newBuilder()
                .addEntities(1L)
                .addEntities(2L)
                .addEntities(3L)
                .addEntities(4L)
                .build());
        when(entitySeverityServiceSpy.getEntitySeverities(any())).thenReturn(
            Arrays.asList(
                EntitySeveritiesResponse.newBuilder()
                    .setEntitySeverity(EntitySeveritiesChunk.newBuilder()
                        .addEntitySeverity(EntitySeverity.newBuilder()
                            .setEntityId(1L)
                            .setSeverity(Severity.CRITICAL))).build(),
                EntitySeveritiesResponse.newBuilder()
                    .setPaginationResponse(PaginationResponse.newBuilder()).build()));

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(serviceEntities);
        when(repositoryApi.entitiesRequest(any()))
            .thenReturn(req);

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest);
        List<BaseApiDTO> results = response.getRawResults();

        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
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

        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest);
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
        GroupApiDTO request = new GroupApiDTO();
        List<ApiPartialEntity> entities = setupEntitiesForMemberQuery();
        when(searchServiceSpy.searchEntities(any())).thenReturn(SearchEntitiesResponse.newBuilder()
            .addAllEntities(entities.stream()
                .map(e -> PartialEntity.newBuilder().setApi(e).build())
                .collect(Collectors.toList()))
            .setPaginationResponse(PaginationResponse.newBuilder())
            .build());

        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final ArgumentCaptor<Integer> totalRecordCount =
                ArgumentCaptor.forClass((Class)Integer.class);
        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.getCursor()).thenReturn(Optional.empty());
        Mockito.when(paginationRequest.allResultsResponse(any()))
                .thenReturn(mock(SearchPaginationResponse.class));
        Mockito.when(paginationRequest.getOrderBy())
                .thenReturn(SearchOrderBy.NAME);

        searchService.getMembersBasedOnFilter("foo", request, paginationRequest);
        verify(paginationRequest).finalPageResponse(resultCaptor.capture(), totalRecordCount.capture());

        final List<Long> resultIds = resultCaptor.getValue()
                .stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toList());

        final Map<Long, BaseApiDTO> resultById = resultCaptor.getValue().stream()
            .collect(Collectors.toMap(se -> Long.valueOf(se.getUuid()), Function.identity()));

        assertThat(resultIds.size(), is(3));
        assertThat(resultById.keySet(), containsInAnyOrder(1L, 4L, 5L));
    }


    /**
     * Test get members when there is a special character in the query.
     * @throws Exception if there is an error processing the query
     */
    @Test
    public void testGetMembersBasedOnFilterQueryWSpecialChars() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        List<ApiPartialEntity> entities = setupEntitiesForMemberQuery();
        when(searchServiceSpy.searchEntities(any())).thenReturn(SearchEntitiesResponse.newBuilder()
                .addAllEntities(entities.stream()
                        .map(e -> PartialEntity.newBuilder().setApi(e).build())
                        .collect(Collectors.toList()))
                .setPaginationResponse(PaginationResponse.newBuilder())
                .build());

        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.getCursor()).thenReturn(Optional.empty());
        Mockito.when(paginationRequest.allResultsResponse(any()))
                .thenReturn(mock(SearchPaginationResponse.class));
        Mockito.when(paginationRequest.getOrderBy())
                .thenReturn(SearchOrderBy.NAME);

        // Test a search with a special character
        request.setClassName("VirtualMachine");
        searchService.getMembersBasedOnFilter("[b", request, paginationRequest);

        final ArgumentCaptor<SearchEntitiesRequest> captor = ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchServiceSpy).searchEntities(captor.capture());

        final SearchEntitiesRequest params = captor.getValue();
        assertEquals(1, params.getSearchParametersCount());
        SearchParameters searchParameters = params.getSearchParameters(0);
        assertEquals(1, searchParameters.getSearchFilterCount());
        SearchFilter nameFilter = searchParameters.getSearchFilter(0);
        String value = nameFilter.getPropertyFilter().getStringFilter().getStringPropertyRegex();
        assertEquals("^.*\\Q[b\\E.*$", value);
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
        requestForVirtualMachineGroups.setGroupType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        requestForVirtualMachineGroups.setClassName(StringConstants.GROUP);
        final GroupApiDTO requestForAllGroups = new GroupApiDTO();
        requestForAllGroups.setClassName(StringConstants.GROUP);
        final SearchPaginationResponse response = mock(SearchPaginationResponse.class);
        Mockito.when(groupsService.getPaginatedGroupApiDTOs(any(), any(), any(), any(), any(), eq(false)))
            .thenReturn(response);
        final ArgumentCaptor<String> resultCaptor =
            ArgumentCaptor.forClass((Class)String.class);
        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);

        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForVirtualMachineGroups,
            paginationRequest));
        assertTrue(response == searchService.getMembersBasedOnFilter("foo",
            requestForAllGroups,
            paginationRequest));
        verify(groupsService, times(2)).getPaginatedGroupApiDTOs(any(), any(), resultCaptor.capture(), any(), any(), eq(false));
        // verify that first call to groupsService.getPaginatedGroupApiDTOs passed in VirtualMachine
        // as entityType argument
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(),
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

        searchService.getMembersBasedOnFilter("", request, paginationRequest);
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
     * Test getMembersBasedOnFilterQuery when we are searching for workloads
     * in scope of Resource Group.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testGetMembersOfResourceGroupBasedOnFilterQuery() throws Exception {
        final long oid = 1234;
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName(StringConstants.WORKLOAD);
        request.setCriteriaList(Collections.emptyList());
        request.setScope(Arrays.asList(String.valueOf(oid)));

        List<ApiPartialEntity> entities = setupEntitiesForMemberQuery();
        when(searchServiceSpy.searchEntities(any())).thenReturn(SearchEntitiesResponse.newBuilder()
            .addAllEntities(entities.stream()
                .map(e -> PartialEntity.newBuilder().setApi(e).build())
                .collect(Collectors.toList()))
            .setPaginationResponse(PaginationResponse.newBuilder())
            .build());

        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor =
            ArgumentCaptor.forClass((Class)List.class);
        final SearchPaginationRequest paginationRequest = mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.getCursor()).thenReturn(Optional.empty());
        Mockito.when(paginationRequest.allResultsResponse(any()))
            .thenReturn(mock(SearchPaginationResponse.class));
        Mockito.when(paginationRequest.getOrderBy())
            .thenReturn(SearchOrderBy.NAME);

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

        searchService.getMembersBasedOnFilter("foo", request, paginationRequest);

        final ArgumentCaptor<Integer> totalRecordCount = ArgumentCaptor.forClass((Class)Integer.class);
        verify(paginationRequest).finalPageResponse(resultCaptor.capture(), totalRecordCount.capture());

        final List<Long> resultIds = resultCaptor.getValue()
            .stream()
            .map(BaseApiDTO::getUuid)
            .map(Long::parseLong)
            .collect(Collectors.toList());

        final Map<Long, BaseApiDTO> resultById = resultCaptor.getValue().stream()
            .collect(Collectors.toMap(se -> Long.valueOf(se.getUuid()), Function.identity()));

        assertThat(resultIds.size(), is(3));
        assertThat(resultById.keySet(), containsInAnyOrder(1L, 4L, 5L));
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
     * Tests to validate that logic to auto-create a display name matching filter on group searches
     * is working as expected.
     */
    @Test
    public void testAddGroupNameMatcher() {
        List<FilterApiDTO> originalFilters = new ArrayList<>();
        originalFilters.add(new FilterApiDTO());

        // verify that an empty or null string doesn't alter the contents of the list.
        Assert.assertEquals(1, searchService.addNameMatcher("", originalFilters, "Type").size());
        Assert.assertEquals(1, searchService.addNameMatcher(null, originalFilters, "Type").size());

        // a valid search string should increase the number of filters by 1
        Assert.assertEquals(2, searchService.addNameMatcher("match me bro", originalFilters, "Type").size());

        // verify that a null filter list but valid search string will give you a singleton list
        Assert.assertEquals(1, searchService.addNameMatcher("match me bro", null, "Type").size());
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

    /**
     * Expected pagination response X-Total-Record-Count list size returned from getCandidateEntitiesForSearch().
     *
     * @throws OperationFailedException If any part of the operation failed.
     */
    @Test
    public void testGetServiceEntityPaginatedWithSeverity() throws InvalidOperationException {
        //GIVEN
        Set<Long> uuids = ImmutableSet.of(ENTITY_ID_1, ENTITY_ID_2, ENTITY_ID_3, ENTITY_ID_4);
        doReturn(uuids).when(searchService).getCandidateEntitiesForSearch(any(), any(), any(), any());
        SearchPaginationRequest searchPaginationRequestnew = new SearchPaginationRequest("", 20, true, null);

        doReturn(ImmutableList.builder().build()).when(entitySeverityServiceSpy).getEntitySeverities(any());
        doReturn((mock(MultiEntityRequest.class))).when(repositoryApi).entitiesRequest(any());

        //WHEN
        SearchPaginationResponse response = searchService.getServiceEntityPaginatedWithSeverity(new GroupApiDTO(),
                null,
                searchPaginationRequestnew,
                uuids, SearchEntityOidsRequest.newBuilder().build());

        //THEN
        assertNotNull(response);
        assertTrue(response.getRestResponse().getHeaders().containsKey("X-Total-Record-Count"));
        assertTrue(response.getRestResponse().getHeaders().get("X-Total-Record-Count")
                .get(0).equals(String.valueOf(uuids.size())));
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

}
