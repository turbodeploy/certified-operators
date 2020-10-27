package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.PaginationTestUtil.getStatsByUuidsQuery;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
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
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsChunk;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private static final Duration toleranceTime = Duration.ofSeconds(60);

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
            paginatedStatsExecutor, toleranceTime, groupExpander));
        when(uuidMapper.fromUuid(oid1)).thenReturn(apiId1);
        when(apiId1.uuid()).thenReturn(oid1);
        when(apiId1.oid()).thenReturn(Long.parseLong(oid1));
        when(apiId1.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        ApiEntityType.PHYSICAL_MACHINE)));
        when(apiId1.isGroup()).thenReturn(false);

        when(uuidMapper.fromUuid(oid2)).thenReturn(apiId2);
        when(apiId2.uuid()).thenReturn(oid2);
        when(apiId2.oid()).thenReturn(Long.parseLong(oid2));
        when(apiId2.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        ApiEntityType.PHYSICAL_MACHINE)));
        when(apiId2.isGroup()).thenReturn(false);

        when(uuidMapper.fromUuid(marketUuid)).thenReturn(marketApiId);
        when(marketApiId.getScopeTypes()).thenReturn(Optional.empty());
        when(marketApiId.isGroup()).thenReturn(false);

        se1.setUuid(apiId1.uuid());
        se1.setClassName("ClassName-1");
        se2.setUuid(apiId2.uuid());
        se2.setClassName("ClassName-2");
    }

    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * <p>In this test:
     *    <ul>
     *        <li>the scopes list is empty (which translates to the full market)</li>
     *        <li>the user is not restricted</li>
     *        <li>we ask for 2 records and give a null offset</li>
     *        <li>we get 2 records back and a final pagination response</li>
     *    </ul>
     *    We test correctness of all intermediate results and the fact that
     *    the stats mapper service and the history service are called with
     *    appropriate parameters and that the group service is <i>not</i>
     *    called at all.
     * </p>
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClusterStatsEmptyScope() throws Exception {
        // constants
        final String statRequested = "foo";
        final int limit = 2;
        final long clusterId1 = 1L;
        final long clusterId2 = 2L;

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, limit, true, statRequested);
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(periodApiInputDTO);

        // expected input to the internal gRPC call
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                            .setStatName(statRequested))
                                    .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                                                                .setCursor("0")
                                                                .setAscending(true)
                                                                .setLimit(limit)
                                                                .setOrderBy(orderBy)
                                                                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                                            .addCommodityRequests(CommodityRequest.newBuilder()
                                                    .setCommodityName(statRequested))
                                            .build();
        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.newBuilder()
                                                            .setStats(statsFilter)
                                                            .setPaginationParams(paginationParameters)
                                                            .build();

        // mock mapping from API input to internal input
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true)).thenReturn(statsFilter);

        // mock mapping from internal output to external output
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setClassName(StringConstants.CLUSTER);
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(statSnapshotApiDTO);

        // mock internal group gRPC call
        final Grouping cluster1 = makeCluster(clusterId1, Collections.singleton(11L));
        final Grouping cluster2 = makeCluster(clusterId2, Collections.singleton(12L));
        when(groupServiceSpy.getGroups(any())).thenReturn(ImmutableList.of(cluster1, cluster2));

        // mock internal history gRPC call
        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                .setOid(clusterId1)
                .addStatSnapshots(STAT_SNAPSHOT)).build()).build());

        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
                .addSnapshots(EntityStats.newBuilder()
                    .setOid(clusterId2)
                    .addStatSnapshots(STAT_SNAPSHOT))).build());

        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setPaginationResponse(PaginationResponse.newBuilder()
            .setTotalRecordCount(2)).build());
        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest)).thenReturn(clusterStatsResponse);

        // call service
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // mapping from external input to internal input happened
        verify(statsMapper).newPeriodStatsFilter(periodApiInputDTO, true);

        // proper use of the history service
        verify(statsHistoryServiceSpy).getClusterStats(clusterStatsRequest);

        // mapping from internal output to external output happened
        verify(statsMapper, atLeast(2)).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        // verify values of the result
        assertThat(response.getRawResults().size(), is(2));
        final EntityStatsApiDTO clusterStats1 = response.getRawResults().get(0);
        assertEquals(Long.toString(clusterId1), clusterStats1.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats1.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats1.getStats());
        final EntityStatsApiDTO clusterStats2 = response.getRawResults().get(1);
        assertEquals(Long.toString(clusterId2), clusterStats2.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats2.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats2.getStats());
        assertEquals(Collections.singletonList(""),
                     response.getRestResponse().getHeaders().get("X-Next-Cursor"));
    }

    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * <p>In this test:
     *    <ul>
     *        <li>the scopes list contains the string "Market"</li>
     *        <li>the user is not restricted</li>
     *        <li>we ask for 2 records and give an offset of 3</li>
     *        <li>we get 2 records back and a new cursor 4</li>
     *        <li>one of the two records contains 2 snapshots</li>
     *    </ul>
     *    We test correctness of all intermediate results and the fact that
     *    the stats mapper service and the history service are called with
     *    appropriate parameters and that the group service is <i>not</i>
     *    called at all. Furthermore, we make sure that all snapshots are returned.
     * </p>
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClusterStatsMarketScope() throws Exception {
        // constants
        final String statRequested = "foo";
        final int limit = 2;
        final String cursor = "3";
        final String nextCursor = "4";
        final long clusterId1 = 1L;
        final long clusterId2 = 2L;

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(cursor, limit, true, statRequested);
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(periodApiInputDTO);
        inputDto.setScopes(ImmutableList.of("Market", "0"));

        // expected input to the internal gRPC call
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                            .setStatName(statRequested))
                                    .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                                                                .setCursor(cursor)
                                                                .setAscending(true)
                                                                .setLimit(limit)
                                                                .setOrderBy(orderBy)
                                                                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                                            .addCommodityRequests(CommodityRequest.newBuilder()
                                                    .setCommodityName(statRequested))
                                            .build();
        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.newBuilder()
                                                            .setStats(statsFilter)
                                                            .setPaginationParams(paginationParameters)
                                                            .build();

        // mock mapping from API input to internal input
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true)).thenReturn(statsFilter);

        // mock mapping from internal output to external output
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setClassName(StringConstants.CLUSTER);
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(statSnapshotApiDTO);

        // mock internal group gRPC call
        final Grouping cluster1 = makeCluster(clusterId1, Collections.singleton(11L));
        final Grouping cluster2 = makeCluster(clusterId2, Collections.singleton(12L));
        when(groupServiceSpy.getGroups(any())).thenReturn(ImmutableList.of(cluster1, cluster2));

        // mock internal history gRPC call
        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                                    .setOid(clusterId1)
                                    .addStatSnapshots(STAT_SNAPSHOT).build())).build());

        clusterStatsResponse.add(ClusterStatsResponse.newBuilder()
                                    .setSnapshotsChunk(EntityStatsChunk.newBuilder()
                                                        .addSnapshots(EntityStats.newBuilder()
                                                                        .setOid(clusterId2)
                                                                        .addStatSnapshots(STAT_SNAPSHOT)
                                                                        .addStatSnapshots(STAT_SNAPSHOT)))
                                    .build());

        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setPaginationResponse(PaginationResponse.newBuilder()
                                                .setTotalRecordCount(2)
                                                .setNextCursor(nextCursor)).build());
        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest)).thenReturn(clusterStatsResponse);

        // call service
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // mapping from external input to internal input happened
        verify(statsMapper).newPeriodStatsFilter(periodApiInputDTO, true);

        // proper use of the history service
        verify(statsHistoryServiceSpy).getClusterStats(clusterStatsRequest);

        // mapping from internal output to external output happened
        verify(statsMapper, atLeast(3)).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        // verify values of the result
        assertThat(response.getRawResults().size(), is(2));
        final EntityStatsApiDTO clusterStats1 = response.getRawResults().get(0);
        assertEquals(Long.toString(clusterId1), clusterStats1.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats1.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats1.getStats());
        final EntityStatsApiDTO clusterStats2 = response.getRawResults().get(1);
        assertEquals(Long.toString(clusterId2), clusterStats2.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats2.getClassName());
        assertEquals(ImmutableList.of(statSnapshotApiDTO, statSnapshotApiDTO), clusterStats2.getStats());
        assertEquals(Collections.singletonList(nextCursor),
                     response.getRestResponse().getHeaders().get("X-Next-Cursor"));
    }

    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * <p>In this test:
     *    <ul>
     *        <li>the scopes list ids of a cluster and a group of clusters</li>
     *        <li>the user is not restricted</li>
     *    </ul>
     *    We test correctness the internal call to the group component
     *    and the history component.
     * </p>
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClusterStatsWithClusterAndGroupOfClusters() throws Exception {
        // constants
        final String statRequested = "foo";
        final long scopeId1 = 1L;
        final long scopeId2 = 2L;

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 4, true, statRequested);
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(periodApiInputDTO);
        inputDto.setScopes(ImmutableList.of(Long.toString(scopeId1), Long.toString(scopeId2)));

        // mock cluster of hosts and mock group of clusters
        // cluster 1 contains PH 1
        // group of clusters contains clusters 2 and 3
        final long ph1Id = 100L;
        final long ph2Id = 200L;
        final long ph3Id = 300L;
        final long clusterId1 = scopeId1;
        final long clusterId2 = 3L;
        final long clusterId3 = 4L;
        final Grouping cluster1 = makeCluster(clusterId1, Collections.singleton(ph1Id));
        final Grouping groupOfClusters = makeGroupOfClusters(scopeId2,
                                                             ImmutableList.of(clusterId2, clusterId3));
        final Grouping cluster2 = makeCluster(clusterId2, Collections.singleton(ph2Id));
        final Grouping cluster3 = makeCluster(clusterId3, Collections.singleton(ph3Id));

        // expected input / output to the group gRPC calls
        final GetGroupsRequest groupRequest = GetGroupsRequest.newBuilder()
                                                    .setGroupFilter(GroupFilter.newBuilder()
                                                                        .addId(scopeId1)
                                                                        .addId(scopeId2))
                                                    .build();
        final List<Grouping> groupResponse = ImmutableList.of(cluster1, groupOfClusters);
        when(groupServiceSpy.getGroups(groupRequest)).thenReturn(groupResponse);
        final GetGroupsRequest secondGroupRequest = GetGroupsRequest.newBuilder()
                                                        .setGroupFilter(GroupFilter.newBuilder()
                                                                            .addId(clusterId2)
                                                                            .addId(clusterId3))
                                                        .build();
        final List<Grouping> secondGroupResponse = ImmutableList.of(cluster2, cluster3);
        when(groupServiceSpy.getGroups(secondGroupRequest)).thenReturn(secondGroupResponse);

        List<Long> membersList = new ArrayList<Long>() {{
            add(clusterId2);
            add(clusterId3);
        }};
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(secondGroupResponse.get(0))
                .members(membersList)
                .entities(membersList)
                .build();

        when(groupExpander.getMembersForGroup(any())).thenReturn(groupAndMembers);
        // expected input / output to the internal history gRPC call
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                            .setStatName(statRequested))
                                    .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                                                                .setCursor("0")
                                                                .setAscending(true)
                                                                .setOrderBy(orderBy)
                                                                .setLimit(4)
                                                                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                                            .addCommodityRequests(CommodityRequest.newBuilder()
                                                    .setCommodityName(statRequested))
                                            .build();
        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.newBuilder()
                                                            .addClusterIds(clusterId1)
                                                            .addClusterIds(clusterId2)
                                                            .addClusterIds(clusterId3)
                                                            .setStats(statsFilter)
                                                            .setPaginationParams(paginationParameters)
                                                            .build();
        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                                            .setOid(clusterId1)
                                            .addStatSnapshots(STAT_SNAPSHOT).build()).build()).build());
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                                            .setOid(clusterId2)
                                            .addStatSnapshots(STAT_SNAPSHOT).build()).build()).build());
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                                            .setOid(clusterId3)
                                            .addStatSnapshots(STAT_SNAPSHOT).build()).build()).build());
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true)).thenReturn(statsFilter);
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setClassName(StringConstants.CLUSTER);
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(statSnapshotApiDTO);
        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest)).thenReturn(clusterStatsResponse);

        // call service
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // mapping from external input to internal input happened
        verify(statsMapper).newPeriodStatsFilter(periodApiInputDTO, true);

        // proper use of the group service
        verify(groupServiceSpy).getGroups(groupRequest);

        // proper use of the history service
        verify(statsHistoryServiceSpy).getClusterStats(clusterStatsRequest);

        // mapping from internal output to external output happened
        verify(statsMapper, atLeast(3)).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        // verify values of the result
        assertThat(response.getRawResults().size(), is(3));
        final EntityStatsApiDTO clusterStats1 = response.getRawResults().get(0);
        assertEquals(Long.toString(clusterId1), clusterStats1.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats1.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats1.getStats());
        final EntityStatsApiDTO clusterStats2 = response.getRawResults().get(1);
        assertEquals(Long.toString(clusterId2), clusterStats2.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats2.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats2.getStats());
        final EntityStatsApiDTO clusterStats3 = response.getRawResults().get(2);
        assertEquals(Long.toString(clusterId3), clusterStats3.getUuid());
        assertEquals(StringConstants.CLUSTER, clusterStats2.getClassName());
        assertEquals(Collections.singletonList(statSnapshotApiDTO), clusterStats2.getStats());
        assertEquals(Collections.singletonList(""),
                     response.getRestResponse().getHeaders().get("X-Next-Cursor"));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * <p>In this test:
     *    <ul>
     *        <li>the scopes list two ids</li>
     *        <li>the user is not restricted</li>
     *        <li>the group component returns only one cluster</li>
     *    </ul>
     *    We test correctness of all the internal calls to the group component.
     *    The API response should only have the present cluster.
     * </p>
     *
     * @throws Exception should happen because not all ids are accounted for
     */
    @Test
    public void testGetClusterStatsWithMissingClusters() throws Exception {
        // constants
        final String statRequested = "foo";
        final long scopeId1 = 1L;
        final long scopeId2 = 2L;

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 4, true, statRequested);
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(periodApiInputDTO);
        inputDto.setScopes(ImmutableList.of(Long.toString(scopeId1), Long.toString(scopeId2)));

        // mock cluster of hosts
        // cluster 1 contains PH 1
        final long ph1Id = 100L;
        final long clusterId = scopeId1;
        final Grouping cluster = makeCluster(clusterId, Collections.singleton(ph1Id));

        // expected input / output to the group gRPC call
        final GetGroupsRequest groupRequest = GetGroupsRequest.newBuilder()
                                                    .setGroupFilter(GroupFilter.newBuilder()
                                                                        .addId(scopeId1)
                                                                        .addId(scopeId2))
                                                    .build();
        final List<Grouping> groupResponse = ImmutableList.of(cluster);
        when(groupServiceSpy.getGroups(groupRequest)).thenReturn(groupResponse);

        // mock internal input mapping
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true))
                .thenReturn(StatsFilter.getDefaultInstance());

        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
                .addSnapshots(EntityStats.newBuilder()
                        .setOid(scopeId1)
                        .addStatSnapshots(STAT_SNAPSHOT).build()).build()).build());
        when(statsHistoryServiceSpy.getClusterStats(any())).thenReturn(clusterStatsResponse);

        // call service
        EntityStatsPaginationResponse response = statsService.getStatsByUuidsQuery(inputDto, paginationRequest);
        assertThat(response.getRawResults().size(), is(1));
    }

    /**
     * If ALL clusters explicitly specified in the cluster stats request are not found, ensure
     * we throw an exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetClusterStatsWithAllMissingClusters() throws Exception {
        final String statRequested = "foo";
        final long scopeId1 = 1L;
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 4, true, statRequested);

        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));

        final StatScopesApiInputDTO missingClusterOnly = new StatScopesApiInputDTO();
        missingClusterOnly.setPeriod(periodApiInputDTO);
        missingClusterOnly.setScopes(ImmutableList.of(Long.toString(scopeId1)));

        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true))
                .thenReturn(StatsFilter.getDefaultInstance());

        statsService.getStatsByUuidsQuery(missingClusterOnly, paginationRequest);
    }


    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * <p>In this test, the scope contains a single id
     *    that does not correspond to a cluster.  An exception must be thrown.
     * </p>
     *
     * @throws Exception should happen because the scope
     *                   contains groups that are not clusters
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetClusterStatsWithBadId() throws Exception {
        // constants
        final String statRequested = "foo";
        final long scopeId = 1L;

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 4, true, statRequested);
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setPeriod(periodApiInputDTO);
        inputDto.setScopes(Collections.singletonList(Long.toString(scopeId)));

        // expected input / output to the group gRPC call
        final GetGroupsRequest groupRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                                    .addId(scopeId))
                .build();
        final List<Grouping> groupResponse = ImmutableList.of(Grouping.getDefaultInstance());
        when(groupServiceSpy.getGroups(groupRequest)).thenReturn(groupResponse);

        // mock internal input mapping
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true))
                .thenReturn(StatsFilter.getDefaultInstance());

        // call service
        statsService.getStatsByUuidsQuery(inputDto, paginationRequest);
    }

    @Nonnull
    private Grouping makeCluster(long id, @Nonnull Collection<Long> ids) {
        final StaticMembersByType staticMembersByType = StaticMembersByType.newBuilder()
                                                            .setType(MEMBER_TYPE_HOSTS)
                                                            .addAllMembers(ids)
                                                            .build();
        return Grouping.newBuilder()
                .setId(id)
                .addExpectedTypes(MEMBER_TYPE_HOSTS)
                .setDefinition(GroupDefinition.newBuilder()
                                    .setDisplayName("name")
                                    .setStaticGroupMembers(StaticMembers.newBuilder()
                                                               .addMembersByType(staticMembersByType))
                                    .setType(GroupType.COMPUTE_HOST_CLUSTER))
                .build();
    }

    @Nonnull
    private Grouping makeGroupOfClusters(long id, @Nonnull Collection<Long> ids) {
        final StaticMembersByType staticMembersByType = StaticMembersByType.newBuilder()
                                                            .setType(MEMBER_TYPE_CLUSTERS)
                                                            .addAllMembers(ids)
                                                            .build();
        return Grouping.newBuilder()
                .setId(id)
                .addExpectedTypes(MEMBER_TYPE_CLUSTERS)
                .setDefinition(GroupDefinition.newBuilder()
                                    .setStaticGroupMembers(StaticMembers.newBuilder()
                                                                .addMembersByType(staticMembersByType)))
                .build();
    }

    private static final MemberType MEMBER_TYPE_CLUSTERS = MemberType.newBuilder()
                                                                .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                                                .build();

    private static final MemberType MEMBER_TYPE_HOSTS = MemberType.newBuilder()
                                                            .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                                            .build();

    /**
     * Tests {@link StatsService} for cluster stats queries.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetStatsByUuidsClusterStats() throws Exception {
        // constants
        final String statRequested = "foo";
        final long id = 7;
        final String idString = Long.toString(id);

        // input to the API method
        final EntityStatsPaginationRequest paginationRequest =
                spy(new EntityStatsPaginationRequest(null, 1, true, statRequested));
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(StringConstants.CPU_HEADROOM);
        periodApiInputDTO.setStatistics(Collections.singletonList(statApiInputDTO));
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Collections.singletonList(idString));
        inputDto.setPeriod(periodApiInputDTO);

        // input to the internal gRPC call
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                        .setStatName(statRequested))
                                    .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                                                                .setAscending(true)
                                                                .setCursor("0")
                                                                .setLimit(1)
                                                                .setOrderBy(orderBy)
                                                                .build();
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                                            .addCommodityRequests(CommodityRequest.newBuilder()
                                                                        .setCommodityName(statRequested))
                                            .build();
        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.newBuilder()
                                                            .addClusterIds(id)
                                                            .setStats(statsFilter)
                                                            .setPaginationParams(paginationParameters)
                                                            .build();

        // mock mapping from API input to internal input
        when(statsMapper.newPeriodStatsFilter(periodApiInputDTO, true)).thenReturn(statsFilter);


        // mock internal group gRPC call
        final GroupDefinition clusterInfo = GroupDefinition.newBuilder()
                                                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                                                .setDisplayName("Winter woede")
                                                .build();
        when(groupServiceSpy.getGroups(GetGroupsRequest.newBuilder()
                                            .setGroupFilter(GroupFilter.newBuilder()
                                                    .addId(id))
                                                    .build()))
                .thenReturn(Collections.singletonList(Grouping.newBuilder()
                                                            .setId(id)
                                                            .setDefinition(clusterInfo)
                                                            .build()));

        // mock internal history gRPC call
        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();

        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                .setOid(id).addStatSnapshots(STAT_SNAPSHOT)).build()).build());
        when(statsHistoryServiceSpy.getClusterStats(clusterStatsRequest)).thenReturn(clusterStatsResponse);

        // mock mapping from internal output to external output
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setUuid(idString);
        statSnapshotApiDTO.setClassName(StringConstants.CLUSTER);
        when(statsMapper.toStatSnapshotApiDTO(STAT_SNAPSHOT)).thenReturn(statSnapshotApiDTO);

        // call service
        final EntityStatsPaginationResponse response =
                statsService.getStatsByUuidsQuery(inputDto, paginationRequest);

        // internal group gRPC call happened
        verify(groupServiceSpy).getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addId(7)).build());
        // mapping from external input to internal input happened
        verify(statsMapper).newPeriodStatsFilter(periodApiInputDTO, true);
        // internal history gRPC call happened
        verify(statsService).getStatsByUuidsQuery(inputDto, paginationRequest);
        // mapping from internal output to external output happened
        verify(statsMapper).toStatSnapshotApiDTO(STAT_SNAPSHOT);

        // verify values of the result
        assertThat(response.getRawResults().size(), is(1));
        final EntityStatsApiDTO clusterStats = response.getRawResults().get(0);
        assertThat(clusterStats.getUuid(), is(idString));
        assertThat(clusterStats.getClassName(), is(StringConstants.CLUSTER));
        assertThat(clusterStats.getStats(), is(Collections.singletonList(statSnapshotApiDTO)));
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
                null, null);
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
                                                            .setEntity(ApiEntityType.VIRTUAL_MACHINE
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
}
