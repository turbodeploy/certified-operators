package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getMembersBasedOnFilter;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getSearchResults;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.assertj.core.util.Sets;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit test for {@link SearchService}.
 */
public class SearchServiceTest {

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();
    private SearchService searchService;
    private MarketsService marketsService = Mockito.mock(MarketsService.class);
    private GroupsService groupsService = Mockito.mock(GroupsService.class);
    private TargetsService targetsService = Mockito.mock(TargetsService.class);
    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
    private final GroupMapper groupMapper = Mockito.mock(GroupMapper.class);
    private final GroupUseCaseParser groupUseCaseParser = Mockito.mock(GroupUseCaseParser.class);
    private final SupplyChainFetcherFactory supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
    private final UuidMapper uuidMapper = new UuidMapper(7777777L);
    private final GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
    private EntitySeverityServiceMole entitySeverityServiceSpy = Mockito.spy(new EntitySeverityServiceMole());

    @Before
    public void setUp() throws Exception {

        GrpcTestServer grpcTestServer = GrpcTestServer.newServer(searchServiceSpy, entitySeverityServiceSpy);
        grpcTestServer.start();

        SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        EntitySeverityServiceBlockingStub severityGrpcStub =
                EntitySeverityServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        searchService = new SearchService(
                repositoryApi,
                marketsService,
                groupsService,
                targetsService,
                searchGrpcStub,
                severityGrpcStub,
                groupExpander,
                supplyChainFetcherFactory,
                groupMapper,
                groupUseCaseParser,
                uuidMapper,
                777777
        );
    }

    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResults() throws Exception {

        getSearchResults(searchService, null, Lists.newArrayList("Group"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(groupsService, Mockito.times(1)).getGroups();

        getSearchResults(searchService, null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        getSearchResults(searchService, null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(targetsService).getTargets(null);
    }

    @Test
    public void testGetSearchGroup() throws Exception {
        getSearchResults(searchService, null, null, null, null, "SomeGroupType", EnvironmentType.ONPREM);
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
                supplyChainTestUtils.createServiceEntityApiDTO(999),
                supplyChainTestUtils.createServiceEntityApiDTO(1),
                supplyChainTestUtils.createServiceEntityApiDTO(2),
                supplyChainTestUtils.createServiceEntityApiDTO(3),
                supplyChainTestUtils.createServiceEntityApiDTO(4));

        List<String> scopes = Lists.newArrayList(CLUSTER_OID);
        Set<String> scopesSet = Sets.newHashSet(scopes);
        List<String> types = Lists.newArrayList("PhysicalMachine");
        when(repositoryApi.getSearchResults(null, types, UuidMapper.UI_REAL_TIME_MARKET_STR, null,
                null))
                .thenReturn(searchResultDTOs);

        SupplychainEntryDTO pmSupplyChainEntryDTO = new SupplychainEntryDTO();
        pmSupplyChainEntryDTO.setInstances(ImmutableMap.of(
                "1", supplyChainTestUtils.createServiceEntityApiDTO(1),
                "2", supplyChainTestUtils.createServiceEntityApiDTO(2),
                "3", supplyChainTestUtils.createServiceEntityApiDTO(3),
                "4", supplyChainTestUtils.createServiceEntityApiDTO(4)));

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
                Mockito.mock(SupplychainApiDTOFetcherBuilder.class);
        when(supplyChainFetcherFactory.newApiDtoFetcher()).thenReturn(mockOperationBuilder);

        // we need to set up these mocks to support the builder pattern
        when(mockOperationBuilder.topologyContextId(anyLong())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.addSeedUuids(anyObject())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.entityTypes(anyObject())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.environmentType(anyObject())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.includeHealthSummary(anyBoolean())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.entityDetailType(anyObject())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.fetch()).thenReturn(mockSupplychainApiDto);
        when(groupExpander.expandUuids(eq(scopesSet))).thenReturn(ImmutableSet.of(1L, 2L, 3L, 4L));

        // Act
        Collection<BaseApiDTO> results = getSearchResults(searchService, null, types, scopes, null, null, null);

        // Assert
        Mockito.verify(groupsService, Mockito.never()).getGroups();
        Mockito.verify(targetsService, Mockito.never()).getTargets(null);
        Mockito.verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));

        assertThat(results.size(), is(4));
        assertThat(results.stream().map(BaseApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1", "2", "3", "4"));
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

    @Test
    public void testGetMembersBasedOnFilterSeverity() throws Exception {
        GroupApiDTO request = new GroupApiDTO();

        when(searchServiceSpy.searchEntities(any())).thenReturn(Arrays.asList(
                Entity.newBuilder().setOid(1).setType(0).build(),
                Entity.newBuilder().setOid(2).setType(0).build()
                ));

        when(entitySeverityServiceSpy.getEntitySeverities(any())).thenReturn(
                Arrays.asList(EntitySeverity.newBuilder().setEntityId(3).setSeverity(Severity.MAJOR).build(),
                EntitySeverity.newBuilder().setEntityId(1).setSeverity(Severity.MINOR).build(),
                EntitySeverity.newBuilder().setEntityId(2).setSeverity(Severity.CRITICAL).build()));
        List<BaseApiDTO> results = getMembersBasedOnFilter(searchService, "", request);
        assertEquals(2, results.size());
        assertTrue(results.get(0) instanceof ServiceEntityApiDTO);
        assertEquals("1", results.get(0).getUuid());
        assertEquals("Minor", ((ServiceEntityApiDTO) results.get(0)).getSeverity());
        assertEquals("Critical", ((ServiceEntityApiDTO) results.get(1)).getSeverity());
    }

    @Test
    public void testGetMembersBasedOnFilterQuery() throws Exception {
        GroupApiDTO request = new GroupApiDTO();

        when(searchServiceSpy.searchEntities(any())).thenReturn(Arrays.asList(
                Entity.newBuilder().setOid(1).setDisplayName("afoobar").setType(0).build(),
                Entity.newBuilder().setOid(2).setDisplayName("bar").setType(0).build(),
                Entity.newBuilder().setOid(3).setType(0).build(),
                Entity.newBuilder().setOid(4).setDisplayName("Foo").setType(0).build()
        ));

        final List<Long> resultIds = getMembersBasedOnFilter(searchService, "foo", request)
                .stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toList());

        assertThat(resultIds.size(), is(2));
        assertThat(resultIds, containsInAnyOrder(1L, 4L));
    }
}
