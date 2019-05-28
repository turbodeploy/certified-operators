package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getSearchResults;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.ImmutableGroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.component.external.api.util.action.SearchUtil;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Unit test for {@link SearchService}.
 */
public class SearchServiceTest {

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();
    private SearchService searchService;
    private MarketsService marketsService = mock(MarketsService.class);
    private GroupsService groupsService = mock(GroupsService.class);
    private TargetsService targetsService = mock(TargetsService.class);
    private RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private final GroupMapper groupMapper = mock(GroupMapper.class);
    private final GroupUseCaseParser groupUseCaseParser = mock(GroupUseCaseParser.class);
    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);
    private final TopologyProcessorClient topologyProcessor = mock(TopologyProcessorClient.class);
    private final UuidMapper uuidMapper = mock(UuidMapper.class);
    private final GroupExpander groupExpander = mock(GroupExpander.class);
    private final PaginationMapper paginationMapperSpy = spy(new PaginationMapper());
    private final TagsService tagsService = mock(TagsService.class);
    private final RepositoryClient repositoryClient = mock(RepositoryClient.class);
    private final BusinessUnitMapper businessUnitMapper = mock(BusinessUnitMapper.class);
    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
    private EntitySeverityServiceMole entitySeverityServiceSpy = Mockito.spy(new EntitySeverityServiceMole());
    private StatsHistoryServiceMole historyServiceSpy = Mockito.spy(new StatsHistoryServiceMole());
    private ActionsServiceMole actionOrchestratorRpcService = new ActionsServiceMole();
    private GroupServiceMole groupRpcService = new GroupServiceMole();

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(
            searchServiceSpy, entitySeverityServiceSpy, historyServiceSpy,
            actionOrchestratorRpcService, groupRpcService);

    private final long targetId1 = 111L;
    private final long targetId2 = 112L;
    private final String probeType1 = SDKProbeType.AWS.getProbeType();
    private final String probeType2 = SDKProbeType.AZURE.getProbeType();

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);

    @Before
    public void setUp() throws Exception {
        final long realTimeContextId = 777777;
        final SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final StatsHistoryServiceBlockingStub statsHistoryServiceStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final EntitySeverityServiceBlockingStub severityGrpcStub =
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final ActionsServiceBlockingStub actionsServiceBlockingStub =
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final GroupServiceBlockingStub groupServiceBlockingStub =
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(userSessionContext.isUserScoped()).thenReturn(false);
        final SearchUtil searchUtil =
            new SearchUtil(
                searchGrpcStub, topologyProcessor, actionsServiceBlockingStub,
                mock(ActionSpecMapper.class), mock(PaginationMapper.class),
                supplyChainFetcherFactory, realTimeContextId);
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
                supplyChainFetcherFactory,
                topologyProcessor,
                groupMapper,
                paginationMapperSpy,
                groupUseCaseParser,
                uuidMapper,
                tagsService,
                repositoryClient,
                businessUnitMapper,
                realTimeContextId,
                userSessionContext,
                searchUtil,
                groupServiceBlockingStub));

        doReturn(ImmutableMap.of(
            targetId1, probeType1,
            targetId2, probeType2
        )).when(searchService).fetchTargetIdToProbeTypeMap();
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
        // Note that we are not setting the type on the target, as Repository does not do this
        // It must be handled by the API layer--this is what we're testing!
        desiredResponse.setDiscoveredBy(target);

        // Prepare Topology Processor responses
        final long probeId = 2;
        final TargetSpec targetSpec = new TargetSpec(probeId, Collections.emptyList());
        final TargetInfo targetInfo =
            new TargetRESTApi.TargetInfo(targetUuid, null, targetSpec, null, null, null);
        final ProbeInfo probeInfo = new ProbeDescription(probeId, targetType, "fakeCategory",
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        // Prepare the mocks
        doThrow(UnknownObjectException.class)
            .when(groupsService).getGroupByUuid(anyString(),anyBoolean());
        doReturn(desiredResponse)
            .when(repositoryApi).getServiceEntityForUuid(eq(Long.valueOf(entityUuid)));
        doReturn(targetInfo).when(topologyProcessor).getTarget(targetUuid);
        doReturn(probeInfo).when(topologyProcessor).getProbe(probeId);

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

        getSearchResults(searchService, null, Lists.newArrayList("Group"), null, null, null, EnvironmentType.ONPREM, null);
        verify(groupsService, Mockito.times(1)).getGroups();

        getSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM, null);
        verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM, null);
        verify(targetsService).getTargets(null);

        getSearchResults(searchService, null, Lists.newArrayList("BusinessAccount"), null, null, null, EnvironmentType.CLOUD, null);
        verify(businessUnitMapper).getAndConvertDiscoveredBusinessUnits(searchService, targetsService, repositoryClient);
    }

    @Test
    public void testGetSearchEntitiesByProbeTypes() throws Exception {
        List<ServiceEntityApiDTO> regions = Lists.newArrayList(
            supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1),
            supplyChainTestUtils.createServiceEntityApiDTO(2L, targetId1),
            supplyChainTestUtils.createServiceEntityApiDTO(3L, targetId2)
        );
        List<String> types = Lists.newArrayList("Region");
        when(repositoryApi.getSearchResults(null, types, null)).thenReturn(regions);

        // filter by AWS
        Collection<BaseApiDTO> regions_aws = getSearchResults(searchService, null,
            Lists.newArrayList("Region"), null, null, null, null,
            Lists.newArrayList(probeType1));
        assertThat(regions_aws.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L));

        // filter by Azure
        Collection<BaseApiDTO> regions_azure = getSearchResults(searchService, null,
            Lists.newArrayList("Region"), null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(probeType2));
        assertThat(regions_azure.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(3L));

        // filter by both AWS and Azure
        Collection<BaseApiDTO> regions_all = getSearchResults(searchService, null,
            Lists.newArrayList("Region"), null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(probeType1, probeType2));
        assertThat(regions_all.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));

        // filter by a vc probe type
        Collection<BaseApiDTO> regions_vc = getSearchResults(searchService, null,
            Lists.newArrayList("Region"), null, null, null, EnvironmentType.CLOUD,
            Lists.newArrayList(SDKProbeType.VCENTER.getProbeType()));
        assertThat(regions_vc, empty());

        // filter by null probeTypes
        Collection<BaseApiDTO> regions_null_probeType = getSearchResults(searchService, null,
            Lists.newArrayList("Region"), null, null, null, EnvironmentType.CLOUD, null);
        assertThat(regions_null_probeType.stream()
            .map(dto -> Long.valueOf(dto.getUuid()))
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));
    }

    @Test
    public void testGetSearchGroup() throws Exception {
        getSearchResults(searchService, null, null, null, null, "SomeGroupType", EnvironmentType.ONPREM, null);
        verify(groupsService).getGroups();
        verify(targetsService, Mockito.never()).getTargets(null);
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));
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
        Set<String> scopesSet = Sets.newHashSet(scopes);
        List<String> types = Lists.newArrayList("PhysicalMachine");
        when(repositoryApi.getSearchResults(null, types, null))
                .thenReturn(searchResultDTOs);

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

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes, null, null, null, null);

        // Assert
        verify(groupsService, Mockito.never()).getGroups();
        verify(targetsService, Mockito.never()).getTargets(null);
        verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));

        assertThat(results.size(), is(4));
        assertThat(results.stream().map(BaseApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1", "2", "3", "4"));
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

        when(groupsService.getClusters(Type.COMPUTE, Collections.singletonList(PM_OID), Collections.emptyList()))
            .thenReturn(Collections.singletonList(groupApiDTO));

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes,
                null, null, null, null);

        // Assert
        assertThat(results, hasItems(groupApiDTO));
    }

    @Test
    public void testClusterFilters() throws Exception {
        // create a SearchParams for members of Cluster1
        final PropertyFilter clusterSpecifier = PropertyFilter.newBuilder()
            .setStringFilter(StringFilter.newBuilder()
                .setStringPropertyRegex("Cluster1"))
            .setPropertyName("displayName")
            .build();
        final SearchParameters params = SearchParameters.newBuilder()
            .addSearchFilter(SearchFilter.newBuilder()
                .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                    .setClusterSpecifier(clusterSpecifier)))
            .build();

        final GroupAndMembers clusterAndMembers = ImmutableGroupAndMembers.builder()
            .group(Group.newBuilder()
                .setType(Group.Type.CLUSTER)
                .setId(1L)
                .setCluster(ClusterInfo.newBuilder()
                    .setName("Cluster1"))
                .build())
            .members(ImmutableSet.of(1L, 2L))
            // Not needed for clusters
            .entities(Collections.emptyList())
            .build();

        when(groupExpander.getGroupsWithMembers(any())).thenReturn(Stream.of(clusterAndMembers));

        SearchParameters resolvedParams = searchService.resolveClusterFilters(params);

        // we should get the members of cluster 1 in the static filter
        StringFilter stringFilter = resolvedParams.getSearchFilter(0).getPropertyFilter().getStringFilter();
        assertEquals(
                ImmutableSet.of("1", "2"),
                stringFilter.getOptionsList().stream().collect(Collectors.toSet()));

        final ArgumentCaptor<GetGroupsRequest> reqCaptor = ArgumentCaptor.forClass(GetGroupsRequest.class);
        verify(groupExpander).getGroupsWithMembers(reqCaptor.capture());
        GetGroupsRequest req = reqCaptor.getValue();
        assertThat(req.getTypeFilterList(), contains(Group.Type.CLUSTER));
        assertThat(
                req.getPropertyFilters().getPropertyFilters(0).getStringFilter(),
                is(clusterSpecifier.getStringFilter()));
    }

    @Test
    public void testGetMembersBasedOnFilterSeverity() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setCriteriaList(Collections.emptyList());
        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(1L,
                Optional.of(supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1)));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        when(searchServiceSpy.searchEntityOids(any())).thenReturn(SearchEntityOidsResponse.newBuilder()
                .addEntities(1L)
                .addEntities(2L)
                .addEntities(3L)
                .addEntities(4L)
                .build());
        when(entitySeverityServiceSpy.getEntitySeverities(any())).thenReturn(
                EntitySeveritiesResponse.newBuilder().addEntitySeverity(EntitySeverity.newBuilder()
                        .setEntityId(1L)
                        .setSeverity(Severity.CRITICAL))
                    .setPaginationResponse(PaginationResponse.newBuilder()).build());

        when(repositoryApi.getServiceEntitiesById(any())).thenReturn(serviceEntityMap);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest);
        List<BaseApiDTO> results = response.getRawResults();

        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
        assertEquals("Critical", ((ServiceEntityApiDTO) results.get(0)).getSeverity());
        verifyDiscoveredBy(results.get(0), targetId1, probeType1);
    }

    @Test
    public void testGetMembersBasedOnFilterUtilization() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        request.setClassName("PhysicalMachine");
        request.setCriteriaList(Collections.emptyList());
        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(1L,
                Optional.of(supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId2)));
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.UTILIZATION.name());
        when(historyServiceSpy.getEntityStats(any())).thenReturn(
                GetEntityStatsResponse.newBuilder()
                    .addEntityStats(EntityStats.newBuilder()
                        .setOid(1L))
                    .build());
        when(repositoryApi.getServiceEntitiesById(any())).thenReturn(serviceEntityMap);
        SearchPaginationResponse response = searchService.getMembersBasedOnFilter("", request, paginationRequest);
        List<BaseApiDTO> results = response.getRawResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
        verifyDiscoveredBy(results.get(0), targetId2, probeType2);
    }

    @Test
    public void testGetMembersBasedOnFilterQuery() throws Exception {
        GroupApiDTO request = new GroupApiDTO();
        final List<Entity> entities = Arrays.asList(
                Entity.newBuilder().setOid(1).setDisplayName("afoobar").setType(0).addTargetIds(targetId1).build(),
                Entity.newBuilder().setOid(4).setDisplayName("Foo").setType(0).addTargetIds(targetId2).build()
        );
        when(searchServiceSpy.searchEntities(any())).thenReturn(SearchEntitiesResponse.newBuilder()
            .addAllEntities(entities)
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
        searchService.getMembersBasedOnFilter("foo", request, paginationRequest);
        verify(paginationRequest).finalPageResponse(resultCaptor.capture());

        final List<Long> resultIds = resultCaptor.getValue()
                .stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toList());

        final Map<Long, BaseApiDTO> resultById = resultCaptor.getValue().stream()
            .collect(Collectors.toMap(se -> Long.valueOf(se.getUuid()), Function.identity()));

        assertThat(resultIds.size(), is(2));
        assertThat(resultById.keySet(), containsInAnyOrder(1L, 4L));
        verifyDiscoveredBy(resultById.get(1L), targetId1, probeType1);
        verifyDiscoveredBy(resultById.get(4L), targetId2, probeType2);
    }

    /**
     * Verify that the given ServiceEntityApiDTO's discoveredBy field is set correctly and matching
     * the given targetId and probeType.
     */
    private void verifyDiscoveredBy(BaseApiDTO se, Long targetId, String probeType) {
        TargetApiDTO targetApiDTO = ((ServiceEntityApiDTO)se).getDiscoveredBy();
        assertThat(targetApiDTO.getUuid(), is(String.valueOf(targetId)));
        assertThat(targetApiDTO.getType(), is(probeType));
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
}
