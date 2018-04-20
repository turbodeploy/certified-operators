package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.DATACENTER;
import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.PHYSICAL_MACHINE;
import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.reports.db.RelationType;
import com.vmturbo.repository.api.RepositoryClient;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

    public static final String PHYSICAL_MACHINE_TYPE = "PhysicalMachine";

    private StatsService statsService;

    private UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private PlanServiceMole planServiceSpy = spy(new PlanServiceMole());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private RepositoryClient repositoryClient = Mockito.mock(RepositoryClient.class);
    private SupplyChainFetcherFactory supplyChainFetcherFactory =
            Mockito.mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private TargetsService targetsService = Mockito.mock(TargetsService.class);

    private Clock mockClock = Mockito.mock(Clock.class);


    private final String oid1 = "1";
    private final ApiId apiId1 = mock(ApiId.class);
    private final String oid2 = "2";
    private final ApiId apiId2 = mock(ApiId.class);

    private final ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO se2 = new ServiceEntityApiDTO();

    final private static ImmutableList<String> commodityList1 = ImmutableList.of(
                                    "CPU",
                                    "StorageLatency",
                                    "nextStepRoi",
                                    "ApplicationCommodity");

    final private static ImmutableList<String> commodityList2 = ImmutableList.of(
                                    "nextStepRoi",
                                    "currentProfitMargin",
                                    "currentExpenses",
                                    "nextStepExpenses",
                                    "ActionPermit",
                                    "Space",
                                    "Extent",
                                    "ApplicationCommodity",
                                    "ClusterCommodity",
                                    "DataCenterCommodity",
                                    "DatastoreCommodity",
                                    "DSPMAccessCommodity",
                                    "NetworkCommodity",
                                    "SegmentationCommodity",
                                    "DrsSegmentationCommodity",
                                    "StorageClusterCommodity",
                                    "VAppAccessCommodity",
                                    "VDCCommodity",
                                    "VMPMAccessCommodity");

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy,
            groupServiceSpy, planServiceSpy);

    @Before
    public void setUp() throws IOException {
        StatsHistoryServiceBlockingStub statsServiceRpc =
                StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        PlanServiceGrpc.PlanServiceBlockingStub planRpcService =
                PlanServiceGrpc.newBlockingStub(testServer.getChannel());

        groupExpander = Mockito.mock(GroupExpander.class);
        GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());

        statsService = new StatsService(statsServiceRpc, planRpcService, repositoryApi,
                repositoryClient, supplyChainFetcherFactory, groupExpander, mockClock,
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
        when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(ImmutableMap.of(1L, Optional.of(se1)));
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);
        when(statsHistoryServiceSpy.getAveragedEntityStats(EntityStatsRequest.newBuilder()
                .addEntities(Long.parseLong(oid1))
                .setFilter(StatsFilter.getDefaultInstance())
                .build()))
            .thenReturn(Collections.singletonList(StatSnapshot.newBuilder().addAllStatRecords(
                records(commodityList1))
                .build()));

        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid1, inputDto);

        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        // The returned stats contain cpu, latency, roi, and app.
        // Should only keep cpu and latency and filter out roi and app.
        assertEquals(1, resp.size());
        List<StatApiDTO> stats = resp.get(0).getStatistics();
        assertEquals(2, stats.size());
        assertEquals(Sets.newHashSet("CPU", "StorageLatency"),
                            Sets.newHashSet(stats.get(0).getName(), stats.get(1).getName()));
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
        when(statsHistoryServiceSpy.getAveragedEntityStats(EntityStatsRequest.newBuilder()
                .addAllEntities(Collections.emptySet()).setFilter(StatsFilter.getDefaultInstance()).build()))
                .thenReturn(Collections.singletonList(StatSnapshot.newBuilder().addAllStatRecords(
                        records(commodityList1))
                        .build()));


        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(
                UuidMapper.UI_REAL_TIME_MARKET_STR, inputDto);
        assertEquals(1, resp.size());
        List<StatApiDTO> stats = resp.get(0).getStatistics();
        assertEquals(2, stats.size());

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
    public void testGetClusterStats() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        final Set<Long> listOfOidsInGroup = Sets.newHashSet(apiId2.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);
        when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(ImmutableMap.of(2L, Optional.of(se2)));

        statsService.getStatsByEntityQuery(oid1, inputDto);

        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);

        verify(statsHistoryServiceSpy).getAveragedEntityStats(requestCaptor.capture(), any());
        assertEquals(apiId1.oid(), requestCaptor.getValue().getEntitiesList().size());
        assertEquals(apiId2.oid(), (long)requestCaptor.getValue().getEntitiesList().get(0));
    }

    @Test
    public void testGetGroupStats() throws Exception {
        // arrange
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        final Set<Long> listOfOidsInGroup = Sets.newHashSet(7L, 8L);
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);

        ServiceEntityApiDTO se7 = new ServiceEntityApiDTO();
        se7.setUuid("7");
        se7.setClassName("classname-7");
        ServiceEntityApiDTO se8 = new ServiceEntityApiDTO();
        se8.setUuid("8");
        se8.setClassName("classname-8");
        when(repositoryApi.getServiceEntitiesById(Mockito.any()))
                .thenReturn(ImmutableMap.of(7L, Optional.of(se7), 8L, Optional.of(se8)));

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(statsHistoryServiceSpy).getAveragedEntityStats(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().getEntitiesList(), containsInAnyOrder(7L, 8L));
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
        when(repositoryApi.getSearchResults(any(), any(),  eq(UuidMapper.UI_REAL_TIME_MARKET_STR),
                any(), any()))
                .thenReturn(Lists.newArrayList(se7));

        // set up the supplychainfetcherfactory for DC 7
        SupplyChainNodeFetcherBuilder fetcherBuilder = Mockito.mock(SupplyChainNodeFetcherBuilder.class);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(anyList())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.addSeedUuid(anyString())).thenReturn(fetcherBuilder);
        // set up supply chain result for DC 7
        Map<String, SupplyChain.SupplyChainNode> supplyChainNodeMap1 = ImmutableMap.of(
                PHYSICAL_MACHINE.getValue(), SupplyChain.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addMemberOids(101L)
                            .addMemberOids(102L)
                            .build())
                        .build());
        when(fetcherBuilder.fetch()).thenReturn(supplyChainNodeMap1);

        // act
        statsService.getStatsByEntityQuery("7", inputDto);

        // assert
        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(statsHistoryServiceSpy).getAveragedEntityStats(requestCaptor.capture(), any());
        System.out.println(requestCaptor.getValue().getEntitiesList());
        assertThat(requestCaptor.getValue().getEntitiesList(), containsInAnyOrder(101L, 102L));
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

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(statsHistoryServiceSpy).getAveragedEntityStats(requestCaptor.capture(), any());
        System.out.println(requestCaptor.getValue().getEntitiesList());
        assertThat(requestCaptor.getValue().getEntitiesList(), containsInAnyOrder(101L, 102L, 103L, 104L));
    }

    @Test
    public void testGetHistoricalStatsByEntityQuery() throws Exception {
        // arrange
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "1000",
                "1500", "a");

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.empty());
        // just a simple SE, not group or cluster; expanded list is just the input OID
        when(groupExpander.expandUuid(oid1)).thenReturn(Sets.newHashSet(1L));
        when(repositoryApi.getServiceEntitiesById(Mockito.any()))
                .thenReturn(ImmutableMap.of(1L, Optional.of(se1)));

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        ArgumentCaptor<EntityStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(statsHistoryServiceSpy).getAveragedEntityStats(entityRequestCaptor.capture(),
                anyObject());
        assertThat(entityRequestCaptor.getAllValues().size(), equalTo(1));
        EntityStatsRequest entityStatsRequest = entityRequestCaptor.getAllValues().iterator().next();
        assertThat(entityStatsRequest.getEntitiesList().size(), equalTo(1));
        assertThat(entityStatsRequest.getEntitiesList().iterator().next(), equalTo(1L));

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
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");

        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.empty());
        // just a simple SE, not group or cluster; expanded list is just the input OID
        when(groupExpander.expandUuid(oid1)).thenReturn(Sets.newHashSet(1L));
        when(repositoryApi.getServiceEntitiesById(Mockito.any()))
                .thenReturn(ImmutableMap.of(1L, Optional.of(se1)));

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsHistoryServiceSpy, times(0)).getAveragedEntityStats(anyObject(),
                anyObject());

        ArgumentCaptor<ProjectedStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(ProjectedStatsRequest.class);

        verify(statsHistoryServiceSpy, times(1)).getProjectedStats(entityRequestCaptor.capture(),
                anyObject());
    }

    @Test
    public void testGetStatsByUuidsQueryHistorical() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(period);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(Sets.newHashSet(1L, 2L));

        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(
                1L, Optional.of(new ServiceEntityApiDTO()),
                2L, Optional.of(new ServiceEntityApiDTO()));
        when(repositoryApi.getServiceEntitiesById(any())).thenReturn(serviceEntityMap);

        // act
        List<EntityStatsApiDTO> result = getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        assertThat(result.size(), equalTo(2));
        ArgumentCaptor<EntityStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(statsHistoryServiceSpy, times(1)).getEntityStats(entityRequestCaptor.capture(),
                anyObject());
        EntityStatsRequest entityStatsRequest = entityRequestCaptor.getValue();
        assertThat(entityStatsRequest.getEntitiesList().size(), equalTo(2));
        assertThat(entityStatsRequest.getEntitiesList(), contains(1L, 2L));


        verify(statsHistoryServiceSpy, times(0)).getProjectedStats(anyObject(),
                anyObject());

    }

    @Test
    public void testGetStatsByUuidsQueryProjected() throws Exception {

        // Arrange
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");
        inputDto.setPeriod(period);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(Sets.newHashSet(1L, 2L));

        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(
                1L, Optional.of(new ServiceEntityApiDTO()),
                2L, Optional.of(new ServiceEntityApiDTO()));
        when(repositoryApi.getServiceEntitiesById(any())).thenReturn(serviceEntityMap);

        // Act
        List<EntityStatsApiDTO> result = getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        assertThat(result.size(), equalTo(2));
        ArgumentCaptor<ProjectedStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(ProjectedStatsRequest.class);
        verify(statsHistoryServiceSpy, times(1)).getProjectedEntityStats(entityRequestCaptor.capture(),
                anyObject());
        ProjectedStatsRequest projectedStatsRequest = entityRequestCaptor.getValue();
        assertThat(projectedStatsRequest.getEntitiesList().size(), equalTo(2));
        assertThat(projectedStatsRequest.getEntitiesList(), contains(1L, 2L));

        verify(statsHistoryServiceSpy, times(0)).getEntityStats(anyObject(),
                anyObject());

    }

    @Test
    public void testGetStatsByUuidsRelatedType() throws Exception {
        final long vmId = 7;
        final long pmId = 9;
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

        // We don't care about the result - for this test we just want to make sure
        // that the vm ID gets expanded into the PM id.
        try {
            getStatsByUuidsQuery(statsService, inputDto);
        } catch (UnknownObjectException e) {
            // This is expected, since we didn't mock out the call to the repository API.
        }

        final ArgumentCaptor<ServiceEntitiesRequest> requestCaptor =
                ArgumentCaptor.forClass(ServiceEntitiesRequest.class);
        verify(repositoryApi).getServiceEntitiesById(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getEntityIds(), contains(pmId));
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
        Long planOid = 999L;
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList(Long.toString(planOid)));

        final PlanId planIdProto = PlanId.newBuilder().setPlanId(planOid).build();
        when(planServiceSpy.getPlan(planIdProto))
                .thenReturn(PlanDTO.OptionalPlanInstance.newBuilder()
                        .setPlanInstance(PlanDTO.PlanInstance.newBuilder()
                                .setPlanId(planOid)
                                .setStatus(PlanDTO.PlanInstance.PlanStatus.SUCCEEDED)
                                .build())
                        .build());
        final long oid1 = 1;
        final String entityName1 = "entity-1";
        int entityType1 = CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE;
        final String date1 = "snapshot-1-date";

        long oid2 = 2;
        int entityType2 = CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
        String entityName2 = "entity-2";
        final String date2 = "snapshot-2-date";


        final PlanEntityStats entity1Stats = buildPlanEntityStats(oid1, entityName1,
                entityType1, date1, commodityList1);
        final PlanEntityStats entity2Stats = buildPlanEntityStats(oid2, entityName2,
                entityType2, date2, commodityList2);
        List<PlanEntityStats> entitStatsToReturn = Lists.newArrayList(
                entity1Stats,
                entity2Stats
        );
        when(repositoryClient.getPlanStats(any())).thenReturn(entitStatsToReturn.iterator());

        // Act
        List<EntityStatsApiDTO> result = getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        verify(planServiceSpy).getPlan(planIdProto);
        verify(repositoryClient).getPlanStats(any());

        assertThat(result.size(), equalTo(2));
        final EntityStatsApiDTO resultForEntity1;
        final EntityStatsApiDTO resultForEntity2;
        if (result.get(0).getDisplayName().equals(entityName1)) {
            resultForEntity1 = result.get(0);
            resultForEntity2 = result.get(1);
        } else {
            resultForEntity1 = result.get(1);
            resultForEntity2 = result.get(0);
        }
        testEntityStats(resultForEntity1, oid1, entityName1, commodityList1.size());
        testEntityStats(resultForEntity2, oid2, entityName2, commodityList2.size());
    }

    /**
     * Build a {@link PlanEntityStats} for this test.
     *
     * @param uid the uid for the entity
     * @param entityName a string name for the entity
     * @param entityType the int entity type
     * @param date a string representing the date (not interpreted as a date)
     * @param commodityList a list of commodity names to return; values are not set
     * @return
     */
    private PlanEntityStats buildPlanEntityStats(long uid, String entityName, int entityType, String date, List<String> commodityList) {
        return PlanEntityStats.newBuilder()
                    .setPlanEntity(TopologyDTO.TopologyEntityDTO.newBuilder()
                            .setOid(uid)
                            .setEntityType(entityType)
                            .setDisplayName(entityName)
                            .build())
                    .setPlanEntityStats(EntityStats.newBuilder()
                            .addStatSnapshots(StatSnapshot.newBuilder()
                                    .setSnapshotDate(date)
                                    .addAllStatRecords(records(commodityList))
                                    .build())
                            .build())
                    .build();
    }

    /**
     * Check that the EntityStatsApiDTO has the correct values.
     *
     * @param resultForEntity the EntityStatsApiDTO to check
     * @param uid the id to expect
     * @param entityName the entity name to expect
     * @param numStats the number of stats to expect
     */
    private void testEntityStats(EntityStatsApiDTO resultForEntity, long uid, String entityName, int numStats) {
        assertThat(resultForEntity.getDisplayName(), equalTo(entityName));
        assertThat(resultForEntity.getUuid(), equalTo(Long.toString(uid)));
        assertThat(resultForEntity.getStats().size(), equalTo(1));
        assertThat(resultForEntity.getStats().get(0).getStatistics().size(), equalTo(numStats));
    }

    @Test
    public void testFullMarketStats() throws Exception {
        // Arrange
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        inputDto.setRelatedType(PHYSICAL_MACHINE_TYPE);
        inputDto.setPeriod(new StatPeriodApiInputDTO());

        // two PMs in the search results
        final ServiceEntityApiDTO pm1 = new ServiceEntityApiDTO();
        pm1.setUuid("1");
        final ServiceEntityApiDTO pm2 = new ServiceEntityApiDTO();
        pm2.setUuid("2");
        Collection<ServiceEntityApiDTO> searchResults = Lists.newArrayList(pm1, pm2);
        List<String> expectedTypes = Lists.newArrayList(PHYSICAL_MACHINE_TYPE);
        when(repositoryApi.getSearchResults(null, expectedTypes,
                UuidMapper.UI_REAL_TIME_MARKET_STR, null, null)).thenReturn(searchResults);

        // Act
        List<EntityStatsApiDTO> result = getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        // expect stats for two PMs in the search response
        assertThat(result.size(), equalTo(2));
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
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        // Act
        getStatsByUuidsQuery(statsService, inputDto);

        // Assert
        Assert.fail("Should never get here");

    }

    private List<StatRecord> records(final List<String> recordlist) {
        return recordlist.stream()
                         .map(name -> StatRecord.newBuilder().setName(name)
                              .setRelation(RelationType.COMMODITIES.getLiteral()).build())
                         .collect(Collectors.toList());
    }
}
