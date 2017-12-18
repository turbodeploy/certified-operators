package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Channel;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

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

    @Before
    public void setUp() {
        SearchServiceBlockingStub searchGrpcStub =
                SearchServiceGrpc.newBlockingStub(Mockito.mock(Channel.class));
        searchService = new SearchService(
                repositoryApi,
                marketsService,
                groupsService,
                targetsService,
                searchGrpcStub,
                groupExpander,
                supplyChainFetcherFactory,
                groupMapper,
                groupUseCaseParser,
                uuidMapper
        );
    }

    /**
     * Test the method {@link SearchService#getSearchResults}.
     *
     * @throws Exception when something goes wrong (is not expected here)
     */
    @Test
    public void testGetSearchResults() throws Exception {

        searchService.getSearchResults(null, Lists.newArrayList("Group"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(groupsService, Mockito.times(1)).getGroups();

        searchService.getSearchResults(null, Lists.newArrayList("Market"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(marketsService).getMarkets(Mockito.anyListOf(String.class));

        searchService.getSearchResults(null, Lists.newArrayList("Target"), null, null, null, EnvironmentType.ONPREM);
        Mockito.verify(targetsService).getTargets();
    }

    @Test
    public void testGetSearchGroup() throws Exception {
        searchService.getSearchResults(null, null, null, null, "SomeGroupType", EnvironmentType.ONPREM);
        Mockito.verify(groupsService).getGroups();
        Mockito.verify(targetsService, Mockito.never()).getTargets();
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
        when(mockOperationBuilder.supplyChainDetailType(anyObject())).thenReturn(mockOperationBuilder);
        when(mockOperationBuilder.fetch()).thenReturn(mockSupplychainApiDto);
        when(groupExpander.expandUuids(eq(scopesSet))).thenReturn(ImmutableSet.of(1L, 2L, 3L, 4L));

        // Act
        Collection<BaseApiDTO> results = searchService.getSearchResults(null, types, scopes, null, null, null);

        // Assert
        Mockito.verify(groupsService, Mockito.never()).getGroups();
        Mockito.verify(targetsService, Mockito.never()).getTargets();
        Mockito.verify(marketsService, Mockito.never()).getMarkets(Mockito.anyListOf(String.class));

        assertThat(results.size(), is(4));
        assertThat(results.stream().map(BaseApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1", "2", "3", "4"));
    }

}
