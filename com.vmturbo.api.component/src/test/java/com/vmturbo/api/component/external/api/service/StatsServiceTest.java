package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.rmi.activation.UnknownObjectException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
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
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

    private static final long LIVE_STATS_RETRIEVAL_WINDOW_MS = 60_000;

    private static final long REALTIME_CONTEXT_ID = 777777;

    private static final String PHYSICAL_MACHINE_TYPE = UIEntityType.PHYSICAL_MACHINE.apiStr();

    private static final long TARGET_ID = 10L;

    private static final String TARGET_DISPLAY_NAME = "display name";

    private static final String PROBE_TYPE = "probe type";

    private final TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);

    private final ServiceEntityMapper serviceEntityMapper = new ServiceEntityMapper(topologyProcessor);

    private StatsService statsService;

    private UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private CostServiceMole costServiceSpy = spy(new CostServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private PlanServiceMole planServiceSpy = spy(new PlanServiceMole());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    private SupplyChainFetcherFactory supplyChainFetcherFactory =
            Mockito.mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private StatsMapper statsMapper = Mockito.mock(StatsMapper.class);

    private final SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder supplychainApiDTOFetcherBuilder =
            Mockito.mock(SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder.class);

    private TargetsService targetsService = Mockito.mock(TargetsService.class);

    private Clock mockClock = Mockito.mock(Clock.class);

    private ReservedInstanceUtilizationCoverageServiceMole riUtilizationCoverageSpy =
            spy(new ReservedInstanceUtilizationCoverageServiceMole());

    private ReservedInstancesService riService = Mockito.mock(ReservedInstancesService.class);

    private MagicScopeGateway magicScopeGateway = mock(MagicScopeGateway.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final String oid1 = "1";
    private final ApiId apiId1 = mock(ApiId.class);
    private final String oid2 = "2";
    private final ApiId apiId2 = mock(ApiId.class);

    private final ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO se2 = new ServiceEntityApiDTO();

    private static final StatSnapshot STAT_SNAPSHOT = StatSnapshot.newBuilder()
            .setSnapshotDate(DateTimeUtil.toString(Clock.systemUTC().millis()))
            .build();

    private static final EntityStats ENTITY_STATS = EntityStats.newBuilder()
            .setOid(1L)
            .addStatSnapshots(STAT_SNAPSHOT)
            .build();

    private static final ServiceEntityApiDTO ENTITY_DESCRIPTOR = new ServiceEntityApiDTO();

    static {
        ENTITY_DESCRIPTOR.setUuid("1");
        ENTITY_DESCRIPTOR.setDisplayName("hello japan");
        ENTITY_DESCRIPTOR.setClassName(UIEntityType.PHYSICAL_MACHINE.apiStr());
    }

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy,
            groupServiceSpy, planServiceSpy, repositoryServiceSpy, costServiceSpy, riUtilizationCoverageSpy);

    @Before
    public void setUp() throws Exception {
        final StatsHistoryServiceBlockingStub statsServiceRpc =
            StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        final PlanServiceGrpc.PlanServiceBlockingStub planRpcService =
            PlanServiceGrpc.newBlockingStub(testServer.getChannel());
        final RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService =
            RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());
        final SearchServiceBlockingStub searchServiceClient = SearchServiceGrpc.newBlockingStub(
            testServer.getChannel());

        mockTopologyProcessorOutputs();

        groupExpander = Mockito.mock(GroupExpander.class);
        GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        CostServiceBlockingStub costService = CostServiceGrpc.newBlockingStub(testServer.getChannel());

        when(magicScopeGateway.enter(anyString())).thenAnswer(invocation -> invocation.getArgumentAt(0, String.class));
        when(magicScopeGateway.enter(anyList())).thenAnswer(invocation -> invocation.getArgumentAt(0, List.class));
        when(riService.fetchPlanInstance(oid1)).thenReturn(Optional.empty());
        when(riService.fetchPlanInstance(oid2)).thenReturn(Optional.empty());
        when(riService.fetchPlanInstance("11111")).thenReturn(Optional.empty());
        when(riService.fetchPlanInstance(StatsService.MARKET)).thenReturn(Optional.empty());
        when(repositoryApi.getServiceEntityById(anyLong(), any())).thenReturn(new ServiceEntityApiDTO());

        statsService = spy(new StatsService(statsServiceRpc, planRpcService, repositoryApi,
            repositoryRpcService, searchServiceClient, supplyChainFetcherFactory, statsMapper,
            groupExpander, mockClock, targetsService, groupService, Duration.ofMillis(LIVE_STATS_RETRIEVAL_WINDOW_MS),
            costService, magicScopeGateway, userSessionContext, riService,
            serviceEntityMapper, REALTIME_CONTEXT_ID));
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
        when(statsMapper.toStatSnapshotApiDTO(any())).thenReturn(apiDto);

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid1, inputDto);

        verify(statsMapper).toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper, times(1)).toStatSnapshotApiDTO(any());
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        assertTrue(resp.contains(apiDto));
    }

    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostTypeCSP() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setGroupBy(Lists.newArrayList(StatsService.CSP));
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CSP)
                .build();
        verifyCall(request, inputDto);
    }

    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostTarget() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setGroupBy(Lists.newArrayList(StatsService.TARGET));
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.TARGET)
                .build();
        verifyCall(request, inputDto);
    }

    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostCloudService() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setGroupBy(Lists.newArrayList(StatsService.CLOUD_SERVICE));
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        inputDto.setEndDate("15000");
        GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CLOUD_SERVICE)
                .build();
        verifyCall(request, inputDto);
    }


    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostTargetWithScope() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setGroupBy(Lists.newArrayList(StatsService.TARGET));
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.TARGET)
                .setEntityFilter(EntityFilter.newBuilder().addEntityId(111l).build())
                .build();
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.newBuilder().setId(111l).build()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(ImmutableSet.of(111l));

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, 1))
                .build();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(cloudStatRecord);

        when(costServiceSpy.getCloudCostStats(any()))
                .thenReturn(builder.build());

        when(costServiceSpy.getAccountExpenseStats(any()))
                .thenReturn(builder.build());

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(any(), any(), any(), any(), any(), any())).thenReturn(apiDto);
        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));
        when(riService.fetchPlanInstance("111")).thenReturn(Optional.empty());

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery("111", inputDto);

        verify(costServiceSpy).getAccountExpenseStats(
                GetCloudExpenseStatsRequest.newBuilder().setGroupBy(GroupByType.TARGET).build());
        verify(statsMapper).toStatSnapshotApiDTO(any(), any(), any(), any(), any(), any());
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        assertEquals(1, resp.size());
    }

    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostOthers() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName("unknown");
        statApiInputDTO.setGroupBy(Lists.newArrayList("unknown"));
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder()
                        .addGroupBy("unknown").build())
                        .build())
                .build();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        when(statsMapper.toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty()))
                .thenReturn(request);

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(any(), any(), any(), any(), any(), any())).thenReturn(apiDto);

        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(StatsService.MARKET, inputDto);

        verify(statsMapper).toAveragedEntityStatsRequest(eq(Collections.EMPTY_SET), anyObject(),
            eq(Optional.empty()));
    }

    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostBottomUpWorkload() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setRelatedEntityType("Workload");
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder().build())
                        .build())
                .build();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);
        when(statsMapper.toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty()))
                .thenReturn(request);
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, 1))
                .build();

        builder.addCloudStatRecord(cloudStatRecord);

        when(costServiceSpy.getCloudCostStats(any()))
                .thenReturn(builder.build());

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toCloudStatSnapshotApiDTO(any())).thenReturn(apiDto);
        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));
        when(riService.fetchPlanInstance(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID)).thenReturn(Optional.empty());

        final List<StatSnapshotApiDTO> resp = statsService
                .getStatsByEntityQuery(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID, inputDto);

        GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                    .addAllEntityTypeId(StatsService.ENTITY_TYPES_COUNTED_AS_WORKLOAD.stream()
                        .map(UIEntityType::fromString)
                        .map(UIEntityType::typeNumber)
                        .collect(Collectors.toSet())))
                .build();
        verify(costServiceSpy).getCloudCostStats(cloudCostStatsRequest);
        verify(statsMapper).toCloudStatSnapshotApiDTO(any());
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);
        assertEquals(1, resp.size());
    }

    //custom group and entityid are both in id form, so this test also cover the getting stat from an entity
    @Test
    public void testGetStatsByEntityQueryWithFilteringForCostBottomUpWorkloadWithCustomGroupOrEntity() throws Exception {

        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setRelatedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder().build())
                        .build())
                .build();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);
        expectedEntityIdsAfterSupplyChainTraversal(expandedOidList);

        when(statsMapper.toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty()))
                .thenReturn(request);
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final int value1 = 1;
        final int value2 = 2;
        final int value3 = 3;

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, value1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, value2))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, value3))
                .build();

        builder.addCloudStatRecord(cloudStatRecord);

        when(costServiceSpy.getCloudCostStats(any()))
                .thenReturn(builder.build());

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toCloudStatSnapshotApiDTO(any())).thenReturn(apiDto);
        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));

        final List<StatSnapshotApiDTO> resp = statsService
                .getStatsByEntityQuery("11111", inputDto);

        verifySupplyChainTraversal();

        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);
        assertEquals(0, resp.size());
    }

    // Verify when request has both Cloud and non-Cloud stats, the result stats will be combined
    // from both Cost and History component .
    @Test
    public void testGetStatsByEntityQueryWithBothCloudCostAndNonCloudStats() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        // Cloud cost stat
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.COST_PRICE);
        statApiInputDTO.setRelatedEntityType("Workload");
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO));

        // other stats
        final StatApiInputDTO statApiInputDTO1 = new StatApiInputDTO();
        statApiInputDTO1.setName("VMem");
        statApiInputDTO1.setRelatedEntityType("VirtualMachine");
        inputDto.setStatistics(Lists.newArrayList(statApiInputDTO, statApiInputDTO1));
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder().build())
                        .build())
                .build();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);
        when(supplyChainFetcherFactory.expandScope(any(), any())).thenReturn(expandedOidList);

        when(statsMapper.toAveragedEntityStatsRequest(expandedOidList, inputDto, Optional.empty()))
                .thenReturn(request);
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final int value1 = 1;
        final int value2 = 2;
        final int value3 = 3;

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, value1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, value2))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, value3))
                .build();

        builder.addCloudStatRecord(cloudStatRecord);

        when(costServiceSpy.getCloudCostStats(any()))
                .thenReturn(builder.build());

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toCloudStatSnapshotApiDTO(any())).thenReturn(apiDto);
        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));
        when(repositoryApi.getServiceEntityById(anyLong(), any())).thenReturn(new ServiceEntityApiDTO());

        final List<StatSnapshotApiDTO> resp = statsService
                .getStatsByEntityQuery("11111", inputDto);

        verifySupplyChainTraversal();

        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);
        assertEquals(0, resp.size());

        // verify retrieving stats from history component
        verify(statsHistoryServiceSpy).getAveragedEntityStats(anyObject());
    }

    @Test
    public void testGetStatsByEntityQueryWithUserScope() throws Exception {
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(apiId1.oid())), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // verify that the request will not get interrupted on a request for entity 1
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final List<StatSnapshotApiDTO> response = statsService.getStatsByEntityQuery(oid1, inputDto);
        Assert.assertEquals(0, response.size());
    }

    @Test(expected = UserAccessScopeException.class)
    public void testGetStatsByEntityQueryBlockedByUserScope() throws Exception {
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid(), apiId2.oid());
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(apiId1.oid())), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // verify that the request for for oid 2 will result in an UserAccessScopeException
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final List<StatSnapshotApiDTO> response = statsService.getStatsByEntityQuery(oid2, inputDto);
    }

    private CloudCostStatRecord.StatRecord.Builder getStatRecordBuilder(CostCategory costCategory, float value) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits(StringConstants.DOLLARS_PER_HOUR);
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        statRecordBuilder.setCategory(costCategory);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg(value);
        statValueBuilder.setTotal(value);
        statValueBuilder.setMax(value);
        statValueBuilder.setMin(value);

        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder;
    }

    public void verifyCall(final GetCloudExpenseStatsRequest request, final StatPeriodApiInputDTO inputDto) throws Exception {

        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, 1))
                .build();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(cloudStatRecord);

        when(costServiceSpy.getCloudCostStats(any()))
                .thenReturn(builder.build());

        when(costServiceSpy.getAccountExpenseStats(any()))
                .thenReturn(builder.build());

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(any(), any(), any(), any(), any(), any())).thenReturn(apiDto);

        when(targetsService.getTargets(null)).thenReturn(ImmutableList.of(new TargetApiDTO()));

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(StatsService.MARKET, inputDto);

        verify(costServiceSpy).getAccountExpenseStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(any(), any(), any(), any(), any(), any());
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        assertEquals(1, resp.size());
    }

    /**
     * A null period should be treated the same way as a default period, i.e. retrieve the current stats
     * @throws Exception
     */
    @Test
    public void testGetStatsByEntityQueryWithFilteringAndNullPeriod() throws Exception {
        final StatPeriodApiInputDTO inputDto = null;
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        // Match on 'any' StatPeriodApiInputDTO because the service may replace null with an empty DTO
        when(statsMapper.toAveragedEntityStatsRequest(eq(expandedOidList),
                any(StatPeriodApiInputDTO.class),
                eq(Optional.empty())))
                    .thenReturn(request);

        when(statsHistoryServiceSpy.getAveragedEntityStats(request))
                .thenReturn(Collections.singletonList(STAT_SNAPSHOT));

        final StatSnapshotApiDTO apiDto = new StatSnapshotApiDTO();
        apiDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(any())).thenReturn(apiDto);

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid1, inputDto);

        // Match on 'any' StatPeriodApiInputDTO because the service may replace null with an empty DTO
        verify(statsMapper).toAveragedEntityStatsRequest(eq(expandedOidList),
                any(StatPeriodApiInputDTO.class),
                eq(Optional.empty()));
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper, times(1)).toStatSnapshotApiDTO(any());
        // Should have called targets service to get a list of targets.
        verify(targetsService).getTargets(null);

        assertTrue(resp.contains(apiDto));
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
        when(statsMapper.toStatSnapshotApiDTO(any())).thenReturn(dto);

        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(
                UuidMapper.UI_REAL_TIME_MARKET_STR, inputDto);

        verify(groupExpander).getGroup(UuidMapper.UI_REAL_TIME_MARKET_STR);
        verify(statsMapper).toAveragedEntityStatsRequest(Collections.emptySet(), inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        verify(statsMapper, times(1)).toStatSnapshotApiDTO(any());

        assertEquals(1, resp.size());
        assertTrue(resp.contains(dto));
    }

    /**
     * A null period should be treated the same way as a default period, i.e. retrieve the current stats
     * @throws Exception
     */
    @Test
    public void testGetStatsByEntityQueryWithAllFilteredAndNullPeriod() throws Exception {
        StatPeriodApiInputDTO inputDto = null;
        when(groupExpander.getGroup(anyObject())).thenReturn(Optional.of(Group.getDefaultInstance()));
        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid2, inputDto);

        // The returned stats will be all filtered out.
        assertEquals(0, resp.size());
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
    public void testGetStatsByEntityQueryForPlanId() throws Exception {
        // verify that a request for plan stats does NOT request projected stats or cost stats, only
        // averaged entity stats.
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        inputDto.setEndDate("1M"); // the UI is sending an end date now -- we need to make sure this
        // does NOT result in requesting projected realtime stats or cost stats

        // configure the mock plan info
        String planIdString = "111";
        long planId = 111L;
        when(groupExpander.getGroup(planIdString)).thenReturn(Optional.empty());
        when(planServiceSpy.getPlan(PlanId.newBuilder().setPlanId(planId).build()))
                .thenReturn(OptionalPlanInstance.newBuilder()
                        .setPlanInstance(PlanInstance.newBuilder()
                                .setPlanId(planId)
                                .setStatus(PlanStatus.SUCCEEDED))
                        .build());
        final List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(planIdString, inputDto);
        verify(statsHistoryServiceSpy, never()).getProjectedStats(any());
        verifyZeroInteractions(costServiceSpy);
        verify(statsHistoryServiceSpy, times(1)).getAveragedEntityStats(any());
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
        when(statsMapper.toStatSnapshotApiDTO(any())).thenReturn(dto);

        // act
        final List<StatSnapshotApiDTO> retDtos = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(listOfOidsInGroup, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);

        assertTrue(retDtos.contains(dto));
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
        se7.setClassName(UIEntityType.DATACENTER.apiStr());
        when(repositoryApi.getSearchResults(any(), any(), any()))
                .thenReturn(Lists.newArrayList(se7));

        final Set<Long> oids = Sets.newHashSet(101L, 102L);

        // set up supply chain result for DC 7
        Map<String, SupplyChainProto.SupplyChainNode> supplyChainNodeMap1 = ImmutableMap.of(
                UIEntityType.PHYSICAL_MACHINE.apiStr(), SupplyChainProto.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addAllMemberOids(oids)
                            .build())
                        .build());
        // set up the supplychainfetcherfactory for DC 7
        SupplyChainNodeFetcherBuilder fetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(supplyChainNodeMap1);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        final GetAveragedEntityStatsRequest request =
                GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(oids, inputDto, Optional.empty()))
                .thenReturn(request);
        when(riService.fetchPlanInstance("7")).thenReturn(Optional.empty());

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
        se7.setClassName(UIEntityType.DATACENTER.apiStr());
        ServiceEntityApiDTO se8 = new ServiceEntityApiDTO();
        se8.setUuid("8");
        se8.setClassName(UIEntityType.DATACENTER.apiStr());
        when(repositoryApi.getSearchResults(any(), any(), any()))
                .thenReturn(Lists.newArrayList(se7, se8));

        // first req, for DC 7, return PMs 101 and 102 for supply chain
        Map<String, SupplyChainProto.SupplyChainNode> supplyChainNodeMap1 = ImmutableMap.of(
                UIEntityType.PHYSICAL_MACHINE.apiStr(), SupplyChainProto.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                                .addMemberOids(101L)
                                .addMemberOids(102L)
                                .build())
                        .build()
        );
        // second call, for DC8, return PMs 103 and 104.
        Map<String, SupplyChainProto.SupplyChainNode> supplyChainNodeMap2 = ImmutableMap.of(
                UIEntityType.PHYSICAL_MACHINE.apiStr(), SupplyChainProto.SupplyChainNode.newBuilder()
                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                                .addMemberOids(103L)
                                .addMemberOids(104L)
                                .build())
                        .build()
        );
        // set up the supplychainfetcherfactory
        SupplyChainNodeFetcherBuilder fetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(supplyChainNodeMap1, supplyChainNodeMap2);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        final Set<Long> expandedOids = Sets.newHashSet(101L, 102L, 103L, 104L);
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toAveragedEntityStatsRequest(expandedOids, inputDto, Optional.empty()))
                .thenReturn(request);
        expectedEntityIdsAfterSupplyChainTraversal(expandedOids);

        // act
        statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(expandedOids, inputDto, Optional.empty());
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
    }

    @Test
    public void testGetHistoricalStatsByEntityQuery() throws Exception {
        // arrange
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(LIVE_STATS_RETRIEVAL_WINDOW_MS + 2000L,
            "1000",
            Long.toString(LIVE_STATS_RETRIEVAL_WINDOW_MS + 1500), "a");

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
        when(statsMapper.toStatSnapshotApiDTO(any())).thenReturn(retDto);

        // act
        final List<StatSnapshotApiDTO> results = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(statsMapper).toAveragedEntityStatsRequest(eq(expandedOid), anyObject(),
            eq(Optional.empty()));
        verify(statsHistoryServiceSpy).getAveragedEntityStats(request);
        // we will return an extra data point to represent current record which may not be in DB
        verify(statsMapper, times(2)).toStatSnapshotApiDTO(any());

        assertTrue(results.contains(retDto));

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
        final StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(100000L, "162000",
                "162000", "a");

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
                    .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
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
                .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
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
        assertThat(clusterStats.getDisplayName(), is(clusterInfo.getDisplayName()));
        assertThat(clusterStats.getStats(), containsInAnyOrder(apiSnapshot));
        assertThat(clusterStats.getClassName(), is(StringConstants.CLUSTER));
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
        inputDto.setRelatedType(UIEntityType.PHYSICAL_MACHINE.apiStr());
        inputDto.setScopes(Collections.singletonList(Long.toString(vmId)));
        inputDto.setPeriod(period);

        final Map<String, SupplyChainNode> supplyChainQueryResult = ImmutableMap.of(
                UIEntityType.PHYSICAL_MACHINE.apiStr(),
                SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(pmId)
                        .build())
                    .build());

        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(supplyChainQueryResult);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);

        final GetEntityStatsRequest request = GetEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toEntityStatsRequest(any(), eq(period), eq(paginationRequest))).thenReturn(request);
        when(statsMapper.normalizeRelatedType(inputDto.getRelatedType())).thenReturn(inputDto.getRelatedType());

        when(groupExpander.getGroup(any())).thenReturn(Optional.empty());
        when(repositoryApi.getServiceEntityById(anyLong(), any())).thenThrow(UnknownObjectException.class);
        expectedEntityIdsAfterSupplyChainTraversal(Collections.singleton(pmId));

        // We don't care about the result - for this test we just want to make sure
        // that the vm ID gets expanded into the PM id.
        try {
            statsService.getStatsByUuidsQuery(inputDto, paginationRequest);
        } catch (UnknownObjectException e) {
            // this is expected
        }

        verifySupplyChainTraversal();

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
        verify(repositoryApi, never()).getSearchResults(any(), any(), any());
        verify(statsHistoryServiceSpy, never()).getAveragedEntityStats(any());
    }

    @Test
    public void testStatsByQueryEmptySupplyChainEarlyReturn() throws Exception {
        final String dcId = "1";
        final ServiceEntityApiDTO dcDto = new ServiceEntityApiDTO();
        dcDto.setClassName(UIEntityType.DATACENTER.apiStr());
        dcDto.setUuid(dcId);
        when(groupExpander.getGroup(eq(dcId))).thenReturn(Optional.empty());
        // Query for entities of type DC return the dcDto
        when(repositoryApi.getSearchResults(null, Collections.singletonList(UIEntityType.DATACENTER.apiStr()), null))
                .thenReturn(Collections.singletonList(dcDto));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                // Empty node!
                SupplyChainNode.getDefaultInstance()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        final List<StatSnapshotApiDTO> dto =
                statsService.getStatsByEntityQuery("1", new StatPeriodApiInputDTO());
        assertTrue(dto.isEmpty());
        // Expect to have had a supply chain lookup for PMs related to the DC.
        verify(fetcherBuilder).entityTypes(Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.apiStr()));
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
                .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
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

    /**
     * Test that the 'relatedType' argument is required if the scope is "Market"
     * This should behave equivalently to the same call with a default period object
     * @throws Exception as expected, with IllegalArgumentException since no 'relatedType'
     */
    @Test(expected = IllegalArgumentException.class)
    public void testFullMarketStatsNoRelatedTypeWithNullPeriod() throws Exception {
        // Arrange
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(null);
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        // Act
        getStatsByUuidsQuery(statsService, inputDto);
    }

    @Ignore
    @Test(expected = UserAccessScopeException.class)
    public void testGetStatsByUuidsQueryBlockedByUserScope() throws Exception {
        // Arrange
        // configure the user to only have access to entity 1
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // not a temp group and has an entity that's out of scope
        when(groupExpander.getGroup(eq("2"))).thenReturn(
                Optional.of(Group.newBuilder()
                    .setGroup(GroupInfo.newBuilder()
                    .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(2L)
                        .addStaticMemberOids(1L)))
                    .build()));

        Set<Long> groupMembers = new HashSet<>(Arrays.asList(2L));
        when(groupExpander.expandUuids(eq(new HashSet<>(Arrays.asList("2"))))).thenReturn(
                groupMembers);

        // request scope 2
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(null);
        inputDto.setScopes(Lists.newArrayList("2"));

        // Act
        // request will fail with access denied because group member 2 is out of scope
        List<EntityStatsApiDTO> stats = getStatsByUuidsQuery(statsService, inputDto);
    }

    // test that a temp group request for a scoped user is translated to an entity-specific request.
    @Test
    public void testGetStatsByUuidsQueryTempGroupWithUserScope() throws Exception {
        // Arrange
        // configure the user to only have access to entity 1 of entity type "VirtualMachine"
        when(userSessionContext.isUserScoped()).thenReturn(true);
        Map<String, OidSet> oidsByEntityType = new HashMap<>();
        oidsByEntityType.put("VirtualMachine", new ArrayOidSet(Arrays.asList(1L)));
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), oidsByEntityType);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // not a temp group and has an entity that's out of scope
        when(groupExpander.getGroup(eq("temp"))).thenReturn(
                Optional.of(Group.newBuilder()
                        .setTempGroup(TempGroupInfo.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setIsGlobalScopeGroup(true))
                        .build()));

        // the temp group will have oids 1 and 2
        Set<Long> groupMembers = new HashSet<>(Arrays.asList(1L,2L));
        when(groupExpander.expandUuids(eq(new HashSet<>(Arrays.asList("temp"))))).thenReturn(
                groupMembers);

        // request scope 2
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(null);
        inputDto.setScopes(Lists.newArrayList("temp"));

        final ArgumentCaptor<EntityStatsScope> argumentCaptor = ArgumentCaptor.forClass(EntityStatsScope.class);
        // Act
        // request should get scoped to only entity 1, even though 2 is also in the temp group
        getStatsByUuidsQuery(statsService, inputDto);
        verify(statsMapper).toEntityStatsRequest(argumentCaptor.capture(), any(), any());
        EntityList entityList = argumentCaptor.getValue().getEntityList();
        Assert.assertEquals(1, entityList.getEntitiesCount());
        Assert.assertEquals(1, entityList.getEntities(0));
    }

    private void expectedEntityIdsAfterSupplyChainTraversal(Set<Long> entityIds)
            throws OperationFailedException, InterruptedException {
        when(supplyChainFetcherFactory.expandScope(any(), any())).thenReturn(entityIds);
    }

    private void verifySupplyChainTraversal() throws OperationFailedException, InterruptedException {
        verify(supplyChainFetcherFactory).expandScope(any(), any());
    }

    private void mockTopologyProcessorOutputs() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        final ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);
        final AccountValue accountValue = Mockito.mock(AccountValue.class);
        Mockito.when(targetInfo.getId()).thenReturn(TARGET_ID);
        final long probeId = 11L;
        Mockito.when(targetInfo.getProbeId()).thenReturn(probeId);
        Mockito.when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        Mockito.when(accountValue.getName()).thenReturn(TargetInfo.TARGET_ADDRESS);
        Mockito.when(accountValue.getStringValue()).thenReturn(TARGET_DISPLAY_NAME);
        Mockito.when(probeInfo.getId()).thenReturn(probeId);
        Mockito.when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        Mockito.when(topologyProcessor.getProbe(Mockito.eq(probeId))).thenReturn(probeInfo);
        Mockito.when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);
    }
}
