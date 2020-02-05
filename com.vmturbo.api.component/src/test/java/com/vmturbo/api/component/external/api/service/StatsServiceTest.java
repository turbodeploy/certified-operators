package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.PaginatedStatsExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
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
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

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


    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    private StatsQueryExecutor statsQueryExecutor = mock(StatsQueryExecutor.class);

    private PlanEntityStatsFetcher planEntityStatsFetcher = mock(PlanEntityStatsFetcher.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory =
            Mockito.mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private StatsMapper statsMapper = Mockito.mock(StatsMapper.class);

    private MagicScopeGateway magicScopeGateway = mock(MagicScopeGateway.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private PaginatedStatsExecutor paginatedStatsExecutor = mock(PaginatedStatsExecutor.class);

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

        groupExpander = Mockito.mock(GroupExpander.class);
        GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());

        when(magicScopeGateway.enter(anyString())).thenAnswer(invocation -> invocation.getArgumentAt(0, String.class));
        when(magicScopeGateway.enter(anyList())).thenAnswer(invocation -> invocation.getArgumentAt(0, List.class));

        statsService = spy(new StatsService(statsServiceRpc, planRpcService, statsMapper,
            groupService,
            magicScopeGateway, userSessionContext, uuidMapper, statsQueryExecutor, planEntityStatsFetcher,
            paginatedStatsExecutor));
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
