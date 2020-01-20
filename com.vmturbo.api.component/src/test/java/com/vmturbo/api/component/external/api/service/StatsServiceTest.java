package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.rmi.activation.UnknownObjectException;
import java.time.Clock;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroup;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

    private static final String PHYSICAL_MACHINE_TYPE = UIEntityType.PHYSICAL_MACHINE.apiStr();

    private final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);

    private final PolicyDTOMoles.PolicyServiceMole policyMole = spy(new PolicyServiceMole());

    private final CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    private final CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
                    spy(new CostMoles.ReservedInstanceBoughtServiceMole());
    /**
     * Rule to provide GRPC server and channels for GRPC services for test purposes.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyMole, costServiceMole, reservedInstanceBoughtServiceMole);

    private ServiceEntityMapper serviceEntityMapper;

    private StatsService statsService;

    private UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private PlanServiceMole planServiceSpy = spy(new PlanServiceMole());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    private StatsQueryExecutor statsQueryExecutor = mock(StatsQueryExecutor.class);

    private PlanEntityStatsFetcher planEntityStatsFetcher = mock(PlanEntityStatsFetcher.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory =
            Mockito.mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private StatsMapper statsMapper = Mockito.mock(StatsMapper.class);

    private Clock mockClock = Mockito.mock(Clock.class);

    private MagicScopeGateway magicScopeGateway = mock(MagicScopeGateway.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final String oid1 = "1";
    private final ApiId apiId1 = mock(ApiId.class);
    private final String oid2 = "2";
    private final ApiId apiId2 = mock(ApiId.class);
    private final String marketUuid = "Market";
    private final ApiId marketApiId = mock(ApiId.class);

    private final ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO se2 = new ServiceEntityApiDTO();

    private static final StatSnapshot STAT_SNAPSHOT = StatSnapshot.newBuilder()
            .setSnapshotDate(Clock.systemUTC().millis())
            .build();

    private static final EntityStats ENTITY_STATS = EntityStats.newBuilder()
            .setOid(1L)
            .addStatSnapshots(STAT_SNAPSHOT)
            .build();

    private static final MinimalEntity ENTITY_DESCRIPTOR = MinimalEntity.newBuilder()
        .setOid(1)
        .setDisplayName("hello japan")
        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
        .build();

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy,
            groupServiceSpy, planServiceSpy, repositoryServiceSpy);

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Before
    public void setUp() throws Exception {
        serviceEntityMapper = new ServiceEntityMapper(targetCache,
                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        supplyChainRpcService());
        final StatsHistoryServiceBlockingStub statsServiceRpc =
            StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        final PlanServiceGrpc.PlanServiceBlockingStub planRpcService =
            PlanServiceGrpc.newBlockingStub(testServer.getChannel());
        final RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService =
            RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());

        MultiEntityRequest req = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        groupExpander = Mockito.mock(GroupExpander.class);
        GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());

        when(magicScopeGateway.enter(anyString())).thenAnswer(invocation -> invocation.getArgumentAt(0, String.class));
        when(magicScopeGateway.enter(anyList())).thenAnswer(invocation -> invocation.getArgumentAt(0, List.class));

        statsService = spy(new StatsService(statsServiceRpc, planRpcService, repositoryApi,
            repositoryRpcService, supplyChainFetcherFactory, statsMapper,
            groupExpander, mockClock, groupService,
            magicScopeGateway, userSessionContext,
            serviceEntityMapper, uuidMapper, statsQueryExecutor, planEntityStatsFetcher));
        when(uuidMapper.fromUuid(oid1)).thenReturn(apiId1);
        when(apiId1.uuid()).thenReturn(oid1);
        when(apiId1.oid()).thenReturn(Long.parseLong(oid1));
        when(apiId1.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        UIEntityType.PHYSICAL_MACHINE)));
        when(apiId1.isGroup()).thenReturn(false);

        when(uuidMapper.fromUuid(oid2)).thenReturn(apiId2);
        when(apiId2.uuid()).thenReturn(oid2);
        when(apiId2.oid()).thenReturn(Long.parseLong(oid2));
        when(apiId2.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        UIEntityType.PHYSICAL_MACHINE)));
        when(apiId2.isGroup()).thenReturn(false);

        when(uuidMapper.fromUuid(marketUuid)).thenReturn(marketApiId);
        when(marketApiId.getScopeTypes()).thenReturn(Optional.empty());
        when(marketApiId.isGroup()).thenReturn(false);

        se1.setUuid(apiId1.uuid());
        se1.setClassName("ClassName-1");
        se2.setUuid(apiId2.uuid());
        se2.setClassName("ClassName-2");

        final SearchRequest dcReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(dcReq);
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

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

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

        verify(repositoryApi).entitiesRequest(expandedUids);
        verify(groupExpander).expandUuids(Collections.singleton("1"));
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                    .addEntities(1L))
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(statsMapper).toStatsSnapshotApiDtoList(ENTITY_STATS);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor), any());


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), is(statDtos));
        assertThat(entityStatDto.getUuid(), is(Long.toString(ENTITY_DESCRIPTOR.getOid())));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(UIEntityType.fromType(ENTITY_DESCRIPTOR.getEntityType()).apiStr()));

    }

    @Test
    public void testGetStatsByUuidsQueryHistoricalWithGlobalEntityType() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(period);

        final Set<Long> expandedUids = Sets.newHashSet(1L);
        when(groupExpander.getGroup("1")).thenReturn(Optional.of(Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                                        .setIsGlobalScope(true))
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.PHYSICAL_MACHINE
                                                                                                        .typeNumber())))))
                        .build()));

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));


        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        when(repositoryApi.entitiesRequest(expandedUids)).thenReturn(req);

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
        verify(repositoryApi).entitiesRequest(expandedUids);

        verify(groupExpander, never()).expandUuids(any());
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(statsMapper).toStatsSnapshotApiDtoList(ENTITY_STATS);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor), any());


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), is(statDtos));
        assertThat(entityStatDto.getUuid(), is(Long.toString(ENTITY_DESCRIPTOR.getOid())));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(UIEntityType.fromType(ENTITY_DESCRIPTOR.getEntityType()).apiStr()));

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
        final EntityStatsScope entityStatsScope = StatsTestUtil.createEntityStatsScope(expandedUids);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(expandedUids);

        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest("foo", 1, true, "order"));

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        when(repositoryApi.entitiesRequest(expandedUids)).thenReturn(req);

        final ProjectedEntityStatsRequest request = ProjectedEntityStatsRequest.getDefaultInstance();
        when(statsMapper.toProjectedEntityStatsRequest(entityStatsScope, period, paginationRequest))
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
        verify(repositoryApi).entitiesRequest(expandedUids);
        verify(statsMapper).toProjectedEntityStatsRequest(entityStatsScope, period, paginationRequest);
        verify(statsHistoryServiceSpy).getProjectedEntityStats(request);
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor), any());


        assertThat(response.getRawResults().size(), equalTo(1));

        final EntityStatsApiDTO entityStatDto = response.getRawResults().get(0);
        assertThat(entityStatDto.getStats(), containsInAnyOrder(statDto));
        assertThat(entityStatDto.getUuid(), is(Long.toString(ENTITY_DESCRIPTOR.getOid())));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(UIEntityType.fromType(ENTITY_DESCRIPTOR.getEntityType()).apiStr()));
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

        final GroupDefinition clusterInfo = GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName("Winter woede")
                .build();
        when(groupServiceSpy.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder().addId(7)).build())).thenReturn(
                                        Collections.singletonList(Grouping.newBuilder().setId(7)
                                                        .setDefinition(clusterInfo).build()));

        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.getDefaultInstance();
        when(statsMapper.toClusterStatsRequest("7", periodApiInputDTO))
            .thenReturn(clusterStatsRequest);

        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest))
                .thenReturn(Collections.singletonList(STAT_SNAPSHOT));
        final StatSnapshotApiDTO apiSnapshot = new StatSnapshotApiDTO();
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(apiSnapshot);

        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        verify(groupServiceSpy).getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder().addId(7)).build());
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
        when(groupExpander.expandUuids(any())).thenReturn(new HashSet<>(Arrays.asList(1L)));
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityEmptyRequest();
        when(repositoryApi.entityRequest(anyLong())).thenReturn(req);
        expectedEntityIdsAfterSupplyChainTraversal(Collections.singleton(pmId));

        // We don't care about the result - for this test we just want to make sure
        // that the vm ID gets expanded into the PM id.
        try {
            statsService.getStatsByUuidsQuery(inputDto, paginationRequest);
        } catch (UnknownObjectException e) {
            // this is expected
        }

        verifySupplyChainTraversal();

        // Make sure that the stats mapper got called with the right IDs.
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(pmId))
                .build(), period, paginationRequest);
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

        final PlanTopologyStatsRequest repositoryRequest = PlanTopologyStatsRequest.getDefaultInstance();
        when(statsMapper.toPlanTopologyStatsRequest(anyLong(), eq(inputDto), any()))
                .thenReturn(repositoryRequest);

        final PlanEntityStats retStats = PlanEntityStats.newBuilder()
            .setPlanEntity(PartialEntity.newBuilder()
                .setApi(ApiPartialEntity.newBuilder()
                    .setEntityType(10)
                    .setDisplayName("foo")
                    .setOid(7L)))
            .setPlanEntityStats(EntityStats.newBuilder()
                .addStatSnapshots(STAT_SNAPSHOT))
            .build();

        when(repositoryServiceSpy.getPlanTopologyStats(any()))
                .thenReturn(Collections.singletonList(PlanTopologyStatsResponse.newBuilder()
                    .setEntityStatsWrapper(PlanEntityStatsChunk.newBuilder().addEntityStats(retStats).build())
                    .build()));

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        final EntityStatsPaginationResponse paginationResponse =
            Mockito.mock(EntityStatsPaginationResponse.class);

        when(planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest))
            .thenReturn(paginationResponse);

        final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
        retDto.setStatistics(Collections.emptyList());
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(retDto);

        // Act
        statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(planServiceSpy).getPlan(planIdProto);
        verify(planEntityStatsFetcher).getPlanEntityStats(planInstance, inputDto, paginationRequest);
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

        final MinimalEntity entityDescriptor = MinimalEntity.newBuilder()
            .setOid(1)
            .setDisplayName("hello japan")
            .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
            .build();


        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(entityDescriptor));
        when(repositoryApi.entitiesRequest(expandedIds)).thenReturn(req);

        final List<StatSnapshotApiDTO> statsList =
                Collections.singletonList(new StatSnapshotApiDTO());
        when(statsMapper.toStatsSnapshotApiDtoList(entityStats)).thenReturn(statsList);

        // Act
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // Assert
        verify(statsMapper).toEntityStatsRequest(EntityStatsScope.newBuilder()
                .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                .build(), period, paginationRequest);
        verify(statsHistoryServiceSpy).getEntityStats(request);
        verify(repositoryApi).entitiesRequest(expandedIds);
        verify(statsMapper).toStatsSnapshotApiDtoList(entityStats);
        verify(paginationRequest).nextPageResponse(any(), eq(nextCursor), any());

        assertThat(response.getRawResults().size(), equalTo(1));
        final EntityStatsApiDTO resultEntity = response.getRawResults().get(0);
        assertThat(resultEntity.getDisplayName(), is(entityDescriptor.getDisplayName()));
        assertThat(resultEntity.getUuid(), is(Long.toString(entityDescriptor.getOid())));
        assertThat(resultEntity.getClassName(), is(UIEntityType.fromType(entityDescriptor.getEntityType()).apiStr()));
        assertThat(resultEntity.getStats(), is(statsList));
    }

    /**
     * Test that the 'relatedType' argument is required if the scope is "Market".
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
     * Test that the 'relatedType' argument is required if the scope is "Market".
     * This should behave equivalently to the same call with a default period object.
     *
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
                Optional.of(Grouping.newBuilder()
                                .setDefinition(
                                GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                .setIsTemporary(true)
                                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                                .setIsGlobalScope(true))
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType
                                                        .newBuilder()
                                                        .setType(MemberType
                                                            .newBuilder()
                                                            .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                            .typeNumber()))
                                                        .addMembers(2L)
                                                        .addMembers(1L)
                                                        ))

                                ).build()
                                  ));

        Set<Long> groupMembers = new HashSet<>(Arrays.asList(2L));
        when(groupExpander.expandUuids(eq(new HashSet<>(Arrays.asList("2"))))).thenReturn(
                groupMembers);

        // request scope 2
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(null);
        inputDto.setScopes(Lists.newArrayList("2"));

        // Act
        // request will fail with access denied because group member 2 is out of scope
        getStatsByUuidsQuery(statsService, inputDto);
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
        final String tempGroupUuid = "temp";
        when(groupExpander.getGroup(eq(tempGroupUuid))).thenReturn(Optional.of(Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                        .setIsTemporary(true)
                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                        .setIsGlobalScope(true))
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType
                                                .newBuilder()
                                                .setType(MemberType
                                                                .newBuilder()
                                                                .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                .typeNumber())))))
                        .build()));

        ApiId tempGroupApiId = mock(ApiId.class);
        when(uuidMapper.fromUuid(tempGroupUuid)).thenReturn(tempGroupApiId);
        when(tempGroupApiId.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.VIRTUAL_MACHINE)));
        when(tempGroupApiId.isGroup()).thenReturn(true);

        // the temp group will have oids 1 and 2
        Set<Long> groupMembers = new HashSet<>(Arrays.asList(1L, 2L));
        when(groupExpander.expandUuids(eq(new HashSet<>(Arrays.asList(tempGroupUuid))))).thenReturn(
                groupMembers);

        // request scope 2
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(null);
        inputDto.setScopes(Lists.newArrayList(tempGroupUuid));
        inputDto.setRelatedType(UIEntityType.VIRTUAL_MACHINE.apiStr());

        final ArgumentCaptor<EntityStatsScope> argumentCaptor = ArgumentCaptor.forClass(EntityStatsScope.class);
        // Act
        // request should get scoped to only entity 1, even though 2 is also in the temp group
        getStatsByUuidsQuery(statsService, inputDto);
        verify(statsMapper).toEntityStatsRequest(argumentCaptor.capture(), any(), any());
        EntityList entityList = argumentCaptor.getValue().getEntityList();
        Assert.assertEquals(1, entityList.getEntitiesCount());
        Assert.assertEquals(1, entityList.getEntities(0));
    }

    /**
     * Test the case that scope is a DC group, relatedType is DataCenter, it should expand to DCs
     * first, then expand each DC to PMs, and fetch aggregated stats for each DC using the related
     * PMs.
     *
     * @throws Exception any exception thrown in the unit test
     */
    @Test
    public void testGetStatsByUuidsQueryHistoricalWithRelatedTypeDataCenter() throws Exception {
        final String dcGroupOid = "12345";
        final Long dcOid1 = 111L;
        final Long dcOid2 = 112L;
        final Set<Long> pmsForDC1 = Sets.newHashSet(1111L, 1112L);
        final Set<Long> pmsForDC2 = Sets.newHashSet(1121L);

        // mock
        ApiId dcGroupApiId = mock(ApiId.class);
        when(uuidMapper.fromUuid(dcGroupOid)).thenReturn(dcGroupApiId);
        when(dcGroupApiId.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        UIEntityType.DATACENTER)));
        when(dcGroupApiId.isGroup()).thenReturn(true);

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList(dcGroupOid));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000", "1000", "a");
        inputDto.setPeriod(period);
        inputDto.setRelatedType(UIEntityType.DATACENTER.apiStr());

        final Set<String> seedUuids = Sets.newHashSet(dcGroupOid);
        Set<Long> dcMembers = Sets.newHashSet(dcOid1, dcOid2);
        when(groupExpander.expandUuids(eq(seedUuids))).thenReturn(dcMembers);
        when(supplyChainFetcherFactory.expandScope(eq(Sets.newHashSet(dcOid1, dcOid2)), any()))
                .thenReturn(Sets.newHashSet(dcOid1, dcOid2));
        when(supplyChainFetcherFactory.expandScope(eq(Sets.newHashSet(dcOid1)), any())).thenReturn(pmsForDC1);
        when(supplyChainFetcherFactory.expandScope(eq(Sets.newHashSet(dcOid2)), any())).thenReturn(pmsForDC2);

        when(statsMapper.toEntityStatsRequest(any(), any(), any())).thenReturn(
                GetEntityStatsRequest.getDefaultInstance());
        when(statsHistoryServiceSpy.getEntityStats(any())).thenReturn(
                GetEntityStatsResponse.newBuilder()
                        .addEntityStats(ENTITY_STATS)
                        .build());

        final List<StatSnapshotApiDTO> statDtos = Collections.singletonList(new StatSnapshotApiDTO());
        when(statsMapper.toStatsSnapshotApiDtoList(ENTITY_STATS)).thenReturn(statDtos);
        when(statsMapper.shouldNormalize(UIEntityType.DATACENTER.apiStr())).thenReturn(true);

        // act
        statsService.getStatsByUuidsQuery(inputDto,
                new EntityStatsPaginationRequest("foo", 1, true, "order"));

        // Assert
        // verify DC group is expanded to DCs first
        verify(groupExpander).expandUuids(seedUuids);

        // verify each DC is expanded to related PMs
        verify(supplyChainFetcherFactory).expandScope(eq(Sets.newHashSet(dcOid1)), any());
        verify(supplyChainFetcherFactory).expandScope(eq(Sets.newHashSet(dcOid2)), any());

        // verify that AggregatedEntity list is created correctly
        final ArgumentCaptor<EntityStatsScope> captor = ArgumentCaptor.forClass(EntityStatsScope.class);
        verify(statsMapper).toEntityStatsRequest(captor.capture(), any(), any());
        EntityStatsScope entityStatsScope = captor.getValue();
        assertTrue(entityStatsScope.hasEntityGroupList());

        List<EntityGroup> groupsList = entityStatsScope.getEntityGroupList().getGroupsList();
        assertThat(groupsList.size(), is(2));
        final Map<Long, List<Long>> aggregatedEntitiesMap = groupsList.stream()
                .collect(Collectors.toMap(EntityGroup::getSeedEntity, EntityGroup::getEntitiesList));
        assertThat(aggregatedEntitiesMap.get(dcOid1), containsInAnyOrder(pmsForDC1.toArray()));
        assertThat(aggregatedEntitiesMap.get(dcOid2), containsInAnyOrder(pmsForDC2.toArray()));
    }

    /**
     * test "is cluster stats request" logic on a cluster-exclusive stat.
     */
    @Test
    public void testIsClusterStatsRequest() {
        Assert.assertTrue(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("1"), StringConstants.CPU_HEADROOM)));
    }

    /**
     * test "is cluster stats request" logic on a non-cluster stats.
     */
    @Test
    public void testIsClusterStatsRequestNonClusterStat() {
        Assert.assertFalse(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("1"), StringConstants.PRICE_INDEX)));
    }

    /**
     * test "is cluster stats request" logic on a "possible" cluster stat.
     */
    @Test
    public void testIsClusterStatsRequestAmbiguous() {
        long clusterId = 1;
        long nonclusterId = 2;
        ApiId clusterApiId = mock(ApiId.class);
        when(clusterApiId.isGroup()).thenReturn(true);
        when(clusterApiId.getGroupType()).thenReturn(Optional.of(GroupType.COMPUTE_HOST_CLUSTER));
        when(uuidMapper.fromOid(clusterId)).thenReturn(clusterApiId);

        ApiId nonClusterApiId = mock(ApiId.class);
        when(nonClusterApiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromOid(nonclusterId)).thenReturn(nonClusterApiId);

        // requesting CPU for a cluster is a cluster stat request
        Assert.assertTrue(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("1"), StringConstants.CPU)));

        // requesting CPU for a non-cluster is NOT a cluster stat request
        Assert.assertFalse(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("2"), StringConstants.CPU)));

        // requesting CPU (which is a cluster stat) and NetThroughput (which is not) should NOT
        // be treated as a cluster stats request.
        Assert.assertFalse(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("1"),
                        StringConstants.CPU, StringConstants.NET_THROUGHPUT)));
    }

    /**
     * test "is cluster stats request" logic on a non-numeric input.
     */
    @Test
    public void testIsClusterStatsRequestAmbiguousNonnumeric() {
        Assert.assertFalse(statsService.isClusterStatsRequest(
                createClusterStatsRequest(Arrays.asList("Market"), StringConstants.CPU)));
    }

    private StatScopesApiInputDTO createClusterStatsRequest(List<String> scopes, String...args) {
        StatScopesApiInputDTO request = new StatScopesApiInputDTO();
        request.setScopes(scopes);
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Arrays.stream(args)
                .map(name -> {
                    StatApiInputDTO input = new StatApiInputDTO();
                    input.setName(name);
                    return input;
                })
                .collect(Collectors.toList()));
        request.setPeriod(period);
        return request;
    }

    private void expectedEntityIdsAfterSupplyChainTraversal(Set<Long> entityIds)
            throws OperationFailedException, InterruptedException {
        when(supplyChainFetcherFactory.expandScope(any(), any())).thenReturn(entityIds);
    }

    private void verifySupplyChainTraversal() throws OperationFailedException, InterruptedException {
        verify(supplyChainFetcherFactory).expandScope(any(), any());
    }

    /**
     * Tests {@link EntityStatsPaginationRequest}.finalResponse called with totalRecordCount.
     *
     * <p>Total Record Count set from {@link Pagination.PaginationResponse}</p>
     *
     * @throws OperationFailedException If any part of the operation failed.
     */
    @Test
    public void testGetLiveEntityStatsSetsTotalRecordCountWhenNoNextCursorPresent() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO statScopesApiInputDTO = mock(StatScopesApiInputDTO.class);
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);

        doReturn(EntityStatsScope.newBuilder().build()).when(statsService).createEntityStatsScope(statScopesApiInputDTO);

        Pagination.PaginationResponse pagResponse = PaginationResponse.newBuilder().setTotalRecordCount(100).build();

        GetEntityStatsResponse getEntityStatsResponse = GetEntityStatsResponse.newBuilder()
                .setPaginationResponse(pagResponse)
                .build();

        doReturn(getEntityStatsResponse).when(statsHistoryServiceSpy).getEntityStats(any());

        //WHEN
        statsService.getLiveEntityStats(statScopesApiInputDTO, paginationRequest);

        //THEN
        verify(paginationRequest).finalPageResponse(any(), eq(100));
    }

    /**
     * Tests {@link EntityStatsPaginationRequest}.nextPageResponse called with totalRecordCount.
     *
     * <p>Total Record Count set from {@link Pagination.PaginationResponse}</p>
     *
     * @throws OperationFailedException If any part of the operation failed.
     */
    @Test
    public void testGetLiveEntityStatsSetsTotalRecordCountWhenNextCursorPresent() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO statScopesApiInputDTO = mock(StatScopesApiInputDTO.class);
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);

        doReturn(EntityStatsScope.newBuilder().build()).when(statsService).createEntityStatsScope(statScopesApiInputDTO);

        Pagination.PaginationResponse pagResponse = PaginationResponse.newBuilder()
                .setTotalRecordCount(100)
                .setNextCursor("NextCursor")
                .build();

        GetEntityStatsResponse getEntityStatsResponse = GetEntityStatsResponse.newBuilder()
                .setPaginationResponse(pagResponse)
                .build();

        doReturn(getEntityStatsResponse).when(statsHistoryServiceSpy).getEntityStats(any());

        //WHEN
        statsService.getLiveEntityStats(statScopesApiInputDTO, paginationRequest);

        //THEN
        verify(paginationRequest).nextPageResponse(any(), eq("NextCursor"), eq(100));
    }

    /**
     * Test globalTempGroup of environmentType Hyrid returns non empty optional of relatedType.
     */
    @Test
    public void testGetGlobalTempGroupEntityTypeWithTempGlobalHybridGroup() {
        //GIVEN
        Grouping grouping = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                .setIsGlobalScope(true)
                        .setEnvironmentType(EnvironmentType.HYBRID))
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType
                                        .newBuilder()
                                        .setType(MemberType
                                                .newBuilder()
                                                .setEntity(UIEntityType.PHYSICAL_MACHINE
                                                        .typeNumber())))))
                .build();

        //THEN
        assertTrue(statsService.getGlobalTempGroupEntityType(Optional.of(grouping)).isPresent());
    }

    /**
     * Test globalTempGroup of environmentType non Hybrid returns empty optional.
     */
    @Test
    public void testGetGlobalTempGroupEntityTypeWithTempGlobalGroupWithNonHybridGroup() {
        //GIVEN
        Grouping grouping = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                .setIsGlobalScope(true)
                                .setEnvironmentType(EnvironmentType.ON_PREM))
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType
                                        .newBuilder()
                                        .setType(MemberType
                                                .newBuilder()
                                                .setEntity(UIEntityType.PHYSICAL_MACHINE
                                                        .typeNumber())))))
                .build();

        //THEN
        assertFalse(statsService.getGlobalTempGroupEntityType(Optional.of(grouping)).isPresent());
    }

}
