package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getSearchResults;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
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
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.repository.api.RepositoryClient;
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

    private SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
    private EntitySeverityServiceMole entitySeverityServiceSpy = Mockito.spy(new EntitySeverityServiceMole());
    private StatsHistoryServiceMole historyServiceSpy = Mockito.spy(new StatsHistoryServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceSpy,
        entitySeverityServiceSpy,
        historyServiceSpy);

    private final long targetId1 = 111L;
    private final long targetId2 = 112L;
    private final String probeType1 = SDKProbeType.AWS.getProbeType();
    private final String probeType2 = SDKProbeType.AZURE.getProbeType();

    @Before
    public void setUp() throws Exception {
        SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        StatsHistoryServiceBlockingStub statsHistoryServiceStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        EntitySeverityServiceBlockingStub severityGrpcStub =
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        searchService = spy(new SearchService(
                repositoryApi,
                marketsService,
                groupsService,
                targetsService,
                searchGrpcStub,
                severityGrpcStub,
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
                777777
        ));

        doReturn(ImmutableMap.of(
            targetId1, probeType1,
            targetId2, probeType2
        )).when(searchService).fetchTargetIdToProbeTypeMap();
    }

    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResults() throws Exception {

        getSearchResults(searchService, null, Lists.newArrayList("Group"), null, null, null, EnvironmentType.ONPREM, null);
        Mockito.verify(groupsService, Mockito.times(1)).getGroups();

        getSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM, null);
        Mockito.verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM, null);
        Mockito.verify(targetsService).getTargets(null);

        getSearchResults(searchService, null, Lists.newArrayList("BusinessAccount"), null, null, null, EnvironmentType.CLOUD, null);
        Mockito.verify(businessUnitMapper).getAndConvertDiscoveredBusinessUnits(searchService, targetsService, repositoryClient);
    }

    @Test
    public void testGetSearchEntitiesByProbeTypes() throws Exception {
        List<ServiceEntityApiDTO> regions = Lists.newArrayList(
            supplyChainTestUtils.createServiceEntityApiDTO(1L, targetId1),
            supplyChainTestUtils.createServiceEntityApiDTO(2L, targetId1),
            supplyChainTestUtils.createServiceEntityApiDTO(3L, targetId2)
        );
        List<String> types = Lists.newArrayList("Region");
        when(repositoryApi.getSearchResults(null, types, UuidMapper.UI_REAL_TIME_MARKET_STR, null,
            null)).thenReturn(regions);

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
        Mockito.verify(groupsService).getGroups();
        Mockito.verify(targetsService, Mockito.never()).getTargets(null);
        Mockito.verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));
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
        when(repositoryApi.getSearchResults(null, types, UuidMapper.UI_REAL_TIME_MARKET_STR, null,
                null))
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

        when(groupExpander.expandUuids(eq(scopesSet))).thenReturn(ImmutableSet.of(1L, 2L, 3L, 4L));

        ApiTestUtils.mockRealtimeId(UuidMapper.UI_REAL_TIME_MARKET_STR, 777777, uuidMapper);

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes, null, null, null, null);

        // Assert
        Mockito.verify(groupsService, Mockito.never()).getGroups();
        Mockito.verify(targetsService, Mockito.never()).getTargets(null);
        Mockito.verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));

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

        when(groupsService.getComputeCluster(Long.valueOf(PM_OID))).thenReturn(Optional.of(groupApiDTO));

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes,
                null, null, null, null);

        // Assert
        assertThat(results, hasItems(groupApiDTO));
    }

    @Test
    public void testClusterFilters() throws Exception {
        GroupApiDTO cluster1 = new GroupApiDTO();
        cluster1.setDisplayName("Cluster1");
        cluster1.setGroupType("PhysicalMachine");
        cluster1.setIsStatic(true);
        cluster1.setLogicalOperator("AND");
        cluster1.setMemberUuidList(Arrays.asList("1","2"));

        // create a SearchParams for members of Cluster1
        SearchParameters params = SearchParameters.newBuilder()
                .addSearchFilter(SearchFilter.newBuilder()
                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                .setClusterSpecifier(PropertyFilter.newBuilder()
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("Cluster1"))
                                        .setPropertyName("displayName"))))
                .build();
        when(groupsService.getGroupApiDTOS(anyObject())).thenReturn(Arrays.asList(cluster1));
        SearchParameters resolvedParams = searchService.resolveClusterFilters(params);

        // we should get the members of cluster 1 in the static regex
        StringFilter stringFilter = resolvedParams.getSearchFilter(0).getPropertyFilter().getStringFilter();
        assertEquals("^1$|^2$", stringFilter.getStringPropertyRegex());
    }

    /**
     * For search by cluster filter, make sure resolveClusterFilters method is invoked.
     *
     * @throws Exception
     */
    @Test
    public void testSearchFilterByClusters() throws Exception {
        GroupApiDTO cluster1 = new GroupApiDTO();
        cluster1.setCriteriaList(Collections.emptyList());
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 10, true, SearchOrderBy.SEVERITY.name());
        SearchParameters params = SearchParameters.newBuilder()
                .addSearchFilter(SearchFilter.newBuilder()
                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                .setClusterSpecifier(PropertyFilter.newBuilder()
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("Cluster1"))
                                        .setPropertyName("displayName"))))
                .build();
        when(groupMapper.convertToSearchParameters(any(), anyString(), anyString()))
                .thenReturn(Lists.newArrayList(params));
        searchService.getMembersBasedOnFilter("", cluster1, paginationRequest);
        SearchParameters resolvedParams = searchService.resolveClusterFilters(params);
        final SearchEntityOidsRequest request = SearchEntityOidsRequest.newBuilder()
                .addSearchParameters(resolvedParams)
                .build();
        Mockito.verify(searchServiceSpy).searchEntityOids(request);
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
        Mockito.verify(paginationRequest).finalPageResponse(resultCaptor.capture());

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
                searchService.getCriteriaOptions(GroupMapper.TAGS, null, null, null);

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
