package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.DATACENTER;
import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.PHYSICAL_MACHINE;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.rmi.activation.UnknownObjectException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reports.db.StringConstants;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

    public static final String PHYSICAL_MACHINE_TYPE = "PhysicalMachine";

    private StatsService statsService;

    private UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private PlanServiceMole planServiceSpy = spy(new PlanServiceMole());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    private SupplyChainFetcherFactory supplyChainFetcherFactory =
            Mockito.mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private StatsMapper statsMapper = Mockito.mock(StatsMapper.class);

    private TargetsService targetsService = Mockito.mock(TargetsService.class);

    private Clock mockClock = Mockito.mock(Clock.class);

    private final String oid1 = "1";
    private final ApiId apiId1 = mock(ApiId.class);
    private final String oid2 = "2";
    private final ApiId apiId2 = mock(ApiId.class);

    private final ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO se2 = new ServiceEntityApiDTO();

    private static final StatSnapshot STAT_SNAPSHOT = StatSnapshot.newBuilder()
            .setSnapshotDate("foo")
            .build();

    private static final EntityStats ENTITY_STATS = EntityStats.newBuilder()
            .setOid(1L)
            .addStatSnapshots(STAT_SNAPSHOT)
            .build();

    private static final ServiceEntityApiDTO ENTITY_DESCRIPTOR = new ServiceEntityApiDTO();

    static {
        ENTITY_DESCRIPTOR.setUuid("1");
        ENTITY_DESCRIPTOR.setDisplayName("hello japan");
        ENTITY_DESCRIPTOR.setClassName(PHYSICAL_MACHINE_TYPE);
    }

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy,
            groupServiceSpy, planServiceSpy, repositoryServiceSpy);

    @Before
    public void setUp() throws IOException {
        final StatsHistoryServiceBlockingStub statsServiceRpc =
                StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        final PlanServiceGrpc.PlanServiceBlockingStub planRpcService =
                PlanServiceGrpc.newBlockingStub(testServer.getChannel());
        final RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService =
                RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());

        groupExpander = Mockito.mock(GroupExpander.class);
        GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());

        statsService = new StatsService(statsServiceRpc, planRpcService, repositoryApi,
                repositoryRpcService, supplyChainFetcherFactory, statsMapper, groupExpander, mockClock,
                targetsService, groupService);

        when(uuidMapper.fromUuid(oid1)).thenReturn(apiId1);
        when(uuidMapper.fromUuid(oid2)).thenReturn(apiId2);
        when(apiId1.uuid()).thenReturn(oid1);
        when(apiId1.oid()).thenReturn(Long.parseLong(oid1));
        when(apiId2.uuid()).thenReturn(oid2);
        when(apiId2.oid()).thenReturn(Long.parseLong(oid2));

        se1.setUuid(apiId1.uuid());
        se1.setClassName("ClassName-1");
        se2.setUuid(apiId2.uuid());
        se2.setClassName("ClassName-2");
    }

    @Test
    public void testGetStatsByEntityQueryWithFiltering() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty()))
                .thenReturn(request);

        when(statsHistoryServiceSpy.getAveragedEntityStats(request))
            .thenReturn(Collections.singletonList(STAT_SNAPSHOT));

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(apiDto);

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid1, inputDto);

        verify(statsMapper).toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        assertThat(resp, containsInAnyOrder(apiDto));
    }

    /**
     * Test that the uid "Market" is accepted without error.
     *
     * @throws Exception not expected
     */
    @Test
    public void testGetStatsForFullMarket() throws Exception {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.empty());

        GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(Collections.emptySet(), inputDto, Optional.empty()))
                .thenReturn(request);

        when(statsHistoryServiceSpy.getAveragedEntityStats(request))
                .thenReturn(Collections.singletonList(STAT_SNAPSHOT));

        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        dto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(dto);

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(
                UuidMapper.UI_REAL_TIME_MARKET_STR, inputDto);

        verify(groupExpander).getGroup(UuidMapper.UI_REAL_TIME_MARKET_STR);
        verify(statsMapper).toAveragedEntityStatsRequest(Collections.emptySet(), inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        assertEquals(1, resp.size());
        assertThat(resp, containsInAnyOrder(dto));
    }

    @Test
    public void testGetStatsByEntityQueryWithAllFiltered() throws Exception {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid2, inputDto);

        // The returned stats will be all filtered out.
        assertEquals(0, resp.size());
    }

    @Test
    public void testGetGroupStats() throws Exception {
        // arrange
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        final Set<Long> listOfOidsInGroup = Sets.newHashSet(7L, 8L);
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);

        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(listOfOidsInGroup, inputDto, Optional.empty()))
                .thenReturn(request);
        when(statsHistoryServiceSpy.getAveragedEntityStats(any()))
            .thenReturn(Collections.singletonList(STAT_SNAPSHOT));

        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        dto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(dto);

        // act
        final List<StatSnapshotApiDTO> retDtos = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(listOfOidsInGroup, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);

        assertThat(retDtos, containsInAnyOrder(dto));
    }

    /**
     * Test fetching averaged stats from a group of DataCenters. In this case, the
     * PMs for each data center should be substituted for the original DataCenters
     * when constructing the stats query.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testDatacenterStats() throws Exception {
        // arrange
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));

        // set up DataCenter id 7
        when(groupExpander.expandUuid(anyObject())).thenReturn(Sets.newHashSet(7L));
        ServiceEntityApiDTO se7 = new ServiceEntityApiDTO();
        se7.setUuid("7");
        se7.setClassName(DATACENTER.getValue());
        when(repositoryApi.getSearchResults(any(), any(), eq(UuidMapper.UI_REAL_TIME_MARKET_STR),
                any(), any()))
                .thenReturn(Lists.newArrayList(se7));

        final Set<Long> oids = Sets.newHashSet(101L, 102L);

        // set up the supplychainfetcherfactory for DC 7
        SupplyChainNodeFetcherBuilder fetcherBuilder = Mockito.mock(SupplyChainNodeFetcherBuilder.class);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(anyList())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.addSeedUuid(anyString())).thenReturn(fetcherBuilder);
        // set up supply chain result for DC 7
        Map<String, SupplyChain.SupplyChainNode> supplyChainNodeMap1 = ImmutableMap.of(
                PHYSICAL_MACHINE.getValue(), SupplyChain.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addAllMemberOids(oids)
                            .build())
                        .build());
        when(fetcherBuilder.fetch()).thenReturn(supplyChainNodeMap1);

        final GetAveragedEntityStatsRequest request =
                GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(oids, inputDto, Optional.empty()))
                .thenReturn(request);

        // We don't really care about what happens after the RPC call, because the thing
        // we're testing is that the datacenter gets expanded into the right IDs before
        // the call to statsHistoryService.

        // act
        statsService.getStatsByEntityQuery("7", inputDto);

        verify(statsMapper).toAveragedEntityStatsRequest(oids, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
    }

    /**
     * Test fetching averaged stats from a group of DataCenters. In this case, the
     * PMs for each data center should be substituted for the original DataCenters
     * when constructing the stats query.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGroupOfDatacenterStats() throws Exception {
        // arrange
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        // set up a group with two datacenters, OID 7 & 8
        final Set<Long> listOfOidsInGroup = Sets.newHashSet(7L, 8L);
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);
        ServiceEntityApiDTO se7 = new ServiceEntityApiDTO();
        se7.setUuid("7");
        se7.setClassName(DATACENTER.getValue());
        ServiceEntityApiDTO se8 = new ServiceEntityApiDTO();
        se8.setUuid("8");
        se8.setClassName(DATACENTER.getValue());
        when(repositoryApi.getSearchResults(any(), any(),  eq(UuidMapper.UI_REAL_TIME_MARKET_STR),
                any(), any()))
                .thenReturn(Lists.newArrayList(se7, se8));

        // set up the supplychainfetcherfactory
        SupplyChainNodeFetcherBuilder fetcherBuilder = Mockito.mock(SupplyChainNodeFetcherBuilder.class);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(anyList())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.addSeedUuid(anyString())).thenReturn(fetcherBuilder);
        // first req, for DC 7, return PMs 101 and 102 for supply chain
        Map<String, SupplyChain.SupplyChainNode> supplyChainNodeMap1 = ImmutableMap.of(
                PHYSICAL_MACHINE.getValue(), SupplyChain.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                                .addMemberOids(101L)
                                .addMemberOids(102L)
                                .build())
                        .build()
        );
        // second call, for DC8, return PMs 103 and 104.
        Map<String, SupplyChain.SupplyChainNode> supplyChainNodeMap2 = ImmutableMap.of(
                PHYSICAL_MACHINE.getValue(), SupplyChain.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                                .addMemberOids(103L)
                                .addMemberOids(104L)
                                .build())
                        .build()
        );
        when(fetcherBuilder.fetch()).thenReturn(supplyChainNodeMap1)
                .thenReturn(supplyChainNodeMap2);

        final Set<Long> expandedOids = Sets.newHashSet(101L, 102L, 103L, 104L);
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(expandedOids, inputDto, Optional.empty()))
                .thenReturn(request);

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(expandedOids, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
    }

    @Test
    public void testGetHistoricalStatsByEntityQuery() throws Exception {
        // arrange
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "1000",
                "1500", "a");

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.empty());
        // just a simple SE, not group or cluster; expanded list is just the input OID
        final Set<Long> expandedOid = Collections.singleton(1L);
        when(groupExpander.expandUuid(oid1)).thenReturn(expandedOid);
        when(repositoryApi.getServiceEntitiesById(Mockito.any()))
                .thenReturn(ImmutableMap.of(1L, Optional.of(se1)));

        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(expandedOid, inputDto, Optional.empty()))
                .thenReturn(request);

        when(statsHistoryServiceSpy.getAveragedEntityStats(request))
            .thenReturn(Collections.singletonList(STAT_SNAPSHOT));

        final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
        retDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(retDto);

        // act
        final List<StatSnapshotApiDTO> results = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(expandedOid, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        assertThat(results, containsInAnyOrder(retDto));

        verify(statsHistoryServiceSpy, times(0)).getProjectedStats(anyObject(), anyObject());
    }

    public StatPeriodApiInputDTO buildStatPeriodApiInputDTO(long currentDate, String startDate, String endDate, String statName) {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        inputDto.setStartDate(startDate);
        inputDto.setEndDate(endDate);
        when(mockClock.millis()).thenReturn(currentDate);
        List<StatApiInputDTO> statisticsRequested = new ArrayList<>();
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(statName);
        statisticsRequested.add(statApiInputDTO);
        inputDto.setStatistics(statisticsRequested);
        return inputDto;
    }

    @Test
    public void testGetProjectedStatsByEntityQuery() throws Exception {
        // arrange
        // request is in the future
        final StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.empty());
        // just a simple SE, not group or cluster; expanded list is just the input OID
        final Set<Long> expandedUuids = Collections.singleton(1L);
        when(groupExpander.expandUuid(oid1)).thenReturn(expandedUuids);

        final ProjectedStatsRequest request = ProjectedStatsRequest.getDefaultInstance();
        when(statsMapper.toProjectedStatsRequest(expandedUuids, inputDto)).thenReturn(request);

        final ProjectedStatsResponse response = ProjectedStatsResponse.newBuilder()
                .setSnapshot(STAT_SNAPSHOT)
                .build();
        when(statsHistoryServiceSpy.getProjectedStats(request)).thenReturn(response);

        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        dto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(response.getSnapshot())).thenReturn(dto);

        // act
        final List<StatSnapshotApiDTO> retDto = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsHistoryServiceSpy, times(0)).getAveragedEntityStats(anyObject(),
                anyObject());
        verify(statsHistoryServiceSpy, times(1)).getProjectedStats(request);

        assertThat(retDto, containsInAnyOrder(dto));
        // Make sure that the date in the returned DTO is set properly.
        assertThat(retDto.get(0).getDate(), is(DateTimeUtil.toString(DateTimeUtil.parseTime(inputDto.getEndDate()))));
    }

    @Test
    public void testGetStatsByUuidsQueryHistoricalNoGlobalEntityType() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(period);

        final Set<Long> expandedUids = Sets.newHashSet(1L);
        when(groupExpander.getGroup("1")).thenReturn(Optional.empty());
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(expandedUids);

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));

        when(repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids)
                .build())).thenReturn(ImmutableMap.of(1L, Optional.of(ENTITY_DESCRIPTOR)));

        final GetEntityStatsRequest request = GetEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toEntityStatsRequest(any(), eq(period), eq(paginationRequest)))
            .thenReturn(request);

        final String nextCursor = "you're next!";
        when(statsHistoryServiceSpy.getEntityStats(request)).thenReturn(
            GetEntityStatsResponse.newBuilder()
                .addEntityStats(ENTITY_STATS)
                .setPaginationResponse(PaginationResponse.newBuilder()
                        .setNextCursor(nextCursor))
                .build());

        final List<StatSnapshotApiDTO> statDtos = Collections.singletonList(new StatSnapshotApiDTO());
        when(statsMapper.toStatsSnapshotApiDtoList(ENTITY_STATS)).thenReturn(statDtos);

        // act
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(statsHistoryServiceSpy, times(0)).getProjectedStats(anyObject(),
                anyObject());

        verify(groupExpander).expandUuids(Collections.singleton("1"));
        verify(repositoryApi).getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids).build());
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                    .addEntities(1L))
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(statsMapper).toStatsSnapshotApiDtoList(ENTITY_STATS);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor));


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), is(statDtos));
        assertThat(entityStatDto.getUuid(), is(ENTITY_DESCRIPTOR.getUuid()));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(ENTITY_DESCRIPTOR.getClassName()));

    }

    @Test
    public void testGetStatsByUuidsQueryHistoricalWithGlobalEntityType() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(period);

        final Set<Long> expandedUids = Sets.newHashSet(1L);
        when(groupExpander.getGroup("1")).thenReturn(Optional.of(Group.newBuilder()
            .setTempGroup(TempGroupInfo.newBuilder()
                    .setEntityType(ServiceEntityMapper.fromUIEntityType(PHYSICAL_MACHINE_TYPE))
                    .setIsGlobalScopeGroup(true))
            .build()));

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));

        when(repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids)
                .build())).thenReturn(ImmutableMap.of(1L, Optional.of(ENTITY_DESCRIPTOR)));

        final GetEntityStatsRequest request = GetEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toEntityStatsRequest(any(), eq(period), eq(paginationRequest)))
                .thenReturn(request);

        final String nextCursor = "you're next!";
        when(statsHistoryServiceSpy.getEntityStats(request)).thenReturn(
                GetEntityStatsResponse.newBuilder()
                        .addEntityStats(ENTITY_STATS)
                        .setPaginationResponse(PaginationResponse.newBuilder()
                                .setNextCursor(nextCursor))
                        .build());

        final List<StatSnapshotApiDTO> statDtos = Collections.singletonList(new StatSnapshotApiDTO());
        when(statsMapper.toStatsSnapshotApiDtoList(ENTITY_STATS)).thenReturn(statDtos);

        // act
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(statsHistoryServiceSpy, times(0)).getProjectedStats(anyObject(),
                anyObject());

        verify(groupExpander, never()).expandUuids(any());
        verify(repositoryApi).getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids).build());
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityType(ServiceEntityMapper.fromUIEntityType(PHYSICAL_MACHINE_TYPE))
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(statsMapper).toStatsSnapshotApiDtoList(ENTITY_STATS);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor));


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), is(statDtos));
        assertThat(entityStatDto.getUuid(), is(ENTITY_DESCRIPTOR.getUuid()));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(ENTITY_DESCRIPTOR.getClassName()));

    }

    @Test
    public void testGetStatsByUuidsQueryProjected() throws Exception {

        // Arrange
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");
        inputDto.setPeriod(period);

        final Set<Long> expandedUids = Sets.newHashSet(1L);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(expandedUids);

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));

        when(repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids)
                .build())).thenReturn(ImmutableMap.of(1L, Optional.of(ENTITY_DESCRIPTOR)));

        final ProjectedEntityStatsRequest request = ProjectedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toProjectedEntityStatsRequest(expandedUids, period, paginationRequest))
            .thenReturn(request);

        final String nextCursor = "you're next!";

        when(statsHistoryServiceSpy.getProjectedEntityStats(request))
            .thenReturn(ProjectedEntityStatsResponse.newBuilder()
                    .addEntityStats(ENTITY_STATS)
                    .setPaginationResponse(PaginationResponse.newBuilder()
                            .setNextCursor(nextCursor))
                    .build());

        final StatSnapshotApiDTO statDto = new StatSnapshotApiDTO();
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(statDto);

        // Act
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(statsHistoryServiceSpy, times(0)).getEntityStats(anyObject(),
                anyObject());
        verify(groupExpander).expandUuids(Collections.singleton("1"));
        verify(repositoryApi).getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedUids).build());
        verify(statsMapper).toProjectedEntityStatsRequest(expandedUids, period, paginationRequest);
        verify(statsHistoryServiceSpy).getProjectedEntityStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor));


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), containsInAnyOrder(statDto));
        assertThat(entityStatDto.getUuid(), is(ENTITY_DESCRIPTOR.getUuid()));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(ENTITY_DESCRIPTOR.getClassName()));
    }

    @Test
    public void testGetStatsByUuidsClusterStats() throws Exception {
        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();

        // Add a cluster stat request.
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Collections.singletonList("7"));
        inputDto.setPeriod(periodApiInputDTO);

        final ClusterInfo clusterInfo = ClusterInfo.newBuilder()
                .setName("Winter woede")
                .build();
        when(groupServiceSpy.getGroups(GetGroupsRequest.newBuilder().addId(7).build()))
                .thenReturn(Collections.singletonList(Group.newBuilder()
                        .setId(7)
                        .setCluster(clusterInfo)
                        .build()));

        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.getDefaultInstance();
        when(statsMapper.toClusterStatsRequest("7", periodApiInputDTO))
            .thenReturn(clusterStatsRequest);

        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest))
                .thenReturn(Collections.singletonList(STAT_SNAPSHOT));
        final StatSnapshotApiDTO apiSnapshot = new StatSnapshotApiDTO();
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(apiSnapshot);

        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        verify(groupServiceSpy).getGroups(GetGroupsRequest.newBuilder().addId(7).build());
        verify(statsMapper).toClusterStatsRequest("7", periodApiInputDTO);
        verify(statsHistoryServiceSpy).getClusterStats(clusterStatsRequest);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        verify(paginationRequest).allResultsResponse(any());

        assertThat(response.getRawResults().size(), is(1));
        final EntityStatsApiDTO clusterStats = response.getRawResults().get(0);
        assertThat(clusterStats.getUuid(), is("7"));
        assertThat(clusterStats.getDisplayName(), is(clusterInfo.getName()));
        assertThat(clusterStats.getStats(), containsInAnyOrder(apiSnapshot));
        assertThat(clusterStats.getClassName(), is(GroupMapper.CLUSTER));
    }

    @Test
    public void testGetStatsByUuidsRelatedType() throws Exception {
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest("foo", 1, true, "order");
        final long vmId = 7;
        final long pmId = 1;
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Collections.emptyList());

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setRelatedType(UIEntityType.PHYSICAL_MACHINE.getValue());
        inputDto.setScopes(Collections.singletonList(Long.toString(vmId)));
        inputDto.setPeriod(period);

        final Map<String, SupplyChainNode> supplyChainQueryResult = ImmutableMap.of(
                UIEntityType.PHYSICAL_MACHINE.getValue(),
                SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(pmId)
                        .build())
                    .build());

        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder = mock(SupplyChainNodeFetcherBuilder.class);
        when(nodeFetcherBuilder.addSeedUuids(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.entityTypes(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.fetch()).thenReturn(supplyChainQueryResult);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);

        final GetEntityStatsRequest request = GetEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toEntityStatsRequest(any(), eq(period), eq(paginationRequest))).thenReturn(request);
        when(statsMapper.normalizeRelatedType(inputDto.getRelatedType())).thenReturn(inputDto.getRelatedType());

        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());

        // We don't care about the result - for this test we just want to make sure
        // that the vm ID gets expanded into the PM id.
        try {
            statsService.getStatsByUuidsQuery(inputDto, paginationRequest);
        } catch (UnknownObjectException e) {
            // This is expected, since we didn't mock out the call to the repository API.
        }

        // Make sure we normalize the related entity type.
        verify(statsMapper, atLeastOnce()).normalizeRelatedType(inputDto.getRelatedType());
        // Make sure that the stats mapper got called with the right IDs.
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(pmId))
                .build(), period, paginationRequest);
    }

    @Test
    public void testStatsByQueryEmptyGroupEarlyReturn() throws Exception {
        final String groupId = "1";
        when(groupExpander.getGroup(eq(groupId))).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(groupId)).thenReturn(Collections.emptySet());
        final List<StatSnapshotApiDTO> dto =
                statsService.getStatsByEntityQuery("1", new StatPeriodApiInputDTO());
        assertTrue(dto.isEmpty());
        // Shouldn't have called any RPCs, because there should be an early return
        // if there are no entities to look for (since group is empty)
        verify(supplyChainFetcherFactory, never()).newNodeFetcher();
        verify(repositoryApi, never()).getSearchResults(any(), any(), any(), any(), any());
        verify(statsHistoryServiceSpy, never()).getAveragedEntityStats(any());
    }

    @Test
    public void testStatsByQueryEmptySupplyChainEarlyReturn() throws Exception {
        final String dcId = "1";
        final ServiceEntityApiDTO dcDto = new ServiceEntityApiDTO();
        dcDto.setClassName(UIEntityType.DATACENTER.getValue());
        dcDto.setUuid(dcId);
        when(groupExpander.getGroup(eq(dcId))).thenReturn(Optional.empty());
        // Query for entities of type DC return the dcDto
        when(repositoryApi.getSearchResults(null,
                Collections.singletonList(UIEntityType.DATACENTER.getValue()),
                UuidMapper.UI_REAL_TIME_MARKET_STR, null, null))
                .thenReturn(Collections.singletonList(dcDto));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = Mockito.mock(SupplyChainNodeFetcherBuilder.class);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(anyList())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.addSeedUuid(anyString())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.fetch())
                .thenReturn(ImmutableMap.of(UIEntityType.PHYSICAL_MACHINE.getValue(),
                        // Empty node!
                        SupplyChainNode.getDefaultInstance()));

        final List<StatSnapshotApiDTO> dto =
                statsService.getStatsByEntityQuery("1", new StatPeriodApiInputDTO());
        assertTrue(dto.isEmpty());
        // Expect to have had a supply chain lookup for PMs related to the DC.
        verify(fetcherBuilder).entityTypes(Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.getValue()));
        verify(fetcherBuilder).addSeedUuid(dcId);

        // Shouldn't have called history service, because there should be an early return
        // if there are no entities to look for.
        verify(statsHistoryServiceSpy, never()).getAveragedEntityStats(any());
    }

    @Test
    public void testGetPlanStats() throws Exception {
        // Arrange
        final Long planOid = 999L;
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList(Long.toString(planOid)));

        final PlanId planIdProto = PlanId.newBuilder().setPlanId(planOid).build();
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(planOid)
                .setStatus(PlanInstance.PlanStatus.SUCCEEDED)
                .build();

        when(planServiceSpy.getPlan(planIdProto))
                .thenReturn(PlanDTO.OptionalPlanInstance.newBuilder()
                        .setPlanInstance(planInstance)
                        .build());

        final PlanTopologyStatsRequest request = PlanTopologyStatsRequest.getDefaultInstance();
        when(statsMapper.toPlanTopologyStatsRequest(eq(planInstance), eq(inputDto), any()))
                .thenReturn(request);

        final PlanEntityStats retStats = PlanEntityStats.newBuilder()
                .setPlanEntity(TopologyEntityDTO.newBuilder()
                        .setEntityType(10)
                        .setDisplayName("foo")
                        .setOid(7L))
                .setPlanEntityStats(EntityStats.newBuilder()
                    .addStatSnapshots(STAT_SNAPSHOT))
                .build();

        when(repositoryServiceSpy.getPlanTopologyStats(any()))
                .thenReturn(PlanTopologyStatsResponse.newBuilder()
                        .addEntityStats(retStats)
                        .build());

        final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
        retDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(retDto);

        // Act
        final List<EntityStatsApiDTO> result = getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        verify(planServiceSpy).getPlan(planIdProto);
        verify(repositoryServiceSpy).getPlanTopologyStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        assertThat(result.size(), equalTo(1));
        final EntityStatsApiDTO resultForEntity = result.get(0);
        assertThat(resultForEntity.getDisplayName(), is("foo"));
        assertThat(resultForEntity.getStats(), containsInAnyOrder(retDto));
    }

    @Test
    public void testGetStatsByUuidsFullMarket() throws Exception {
        // Arrange
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        inputDto.setRelatedType(PHYSICAL_MACHINE_TYPE);
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        inputDto.setPeriod(period);

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));

        when(statsMapper.normalizeRelatedType(PHYSICAL_MACHINE_TYPE)).thenReturn(PHYSICAL_MACHINE_TYPE);

        final Set<Long> expandedIds = Sets.newHashSet(1L);

        final GetEntityStatsRequest request = GetEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toEntityStatsRequest(any(), eq(period), eq(paginationRequest)))
            .thenReturn(request);

        final EntityStats entityStats = EntityStats.newBuilder()
                .setOid(1L)
                .addStatSnapshots(STAT_SNAPSHOT)
                .build();

        final String nextCursor = "you're next!";
        when(statsHistoryServiceSpy.getEntityStats(request))
            .thenReturn(GetEntityStatsResponse.newBuilder()
                    .addEntityStats(entityStats)
                    .setPaginationResponse(PaginationResponse.newBuilder()
                            .setNextCursor(nextCursor))
                    .build());

        final ServiceEntityApiDTO entityDescriptor = new ServiceEntityApiDTO();
        entityDescriptor.setUuid("1");
        entityDescriptor.setDisplayName("hello japan");
        entityDescriptor.setClassName(PHYSICAL_MACHINE_TYPE);

        when(repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedIds)
                .build())).thenReturn(ImmutableMap.of(1L, Optional.of(entityDescriptor)));

        final List<StatSnapshotApiDTO> statsList =
                Collections.singletonList(new StatSnapshotApiDTO());
        when(statsMapper.toStatsSnapshotApiDtoList(entityStats)).thenReturn(statsList);

        // Act
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(statsMapper).normalizeRelatedType(PHYSICAL_MACHINE_TYPE);
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityType(ServiceEntityMapper.fromUIEntityType(PHYSICAL_MACHINE_TYPE))
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(repositoryApi).getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(expandedIds).build());
        verify(statsMapper).toStatsSnapshotApiDtoList(entityStats);
        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor));

        assertThat(response.getRawResults().size(), equalTo(1));
        final EntityStatsApiDTO resultEntity = response.getRawResults().get(0);
        assertThat(resultEntity.getDisplayName(), is(entityDescriptor.getDisplayName()));
        assertThat(resultEntity.getUuid(), is(entityDescriptor.getUuid()));
        assertThat(resultEntity.getClassName(), is(entityDescriptor.getClassName()));
        assertThat(resultEntity.getStats(), is(statsList));
    }

    /**
     * Test that the 'relatedType' argument is required if the scope is "Market"
     *
     * @throws Exception as expected, with IllegalArgumentException since no 'relatedType'
     */
    @Test(expected = IllegalArgumentException.class)
    public void testFullMarketStatsNoRelatedType() throws Exception {
        // Arrange
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(new StatPeriodApiInputDTO());
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        // Act
        getStatsByUuidsQuery(statsService, inputDto);
    }
}
