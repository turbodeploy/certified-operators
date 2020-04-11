package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceBoughtServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceCostServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link RIStatsSubQuery}.
 */
public class RIStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;
    private static final long TIER_ID = 1111L;
    private static final String TIER_NAME = "compute_medium";
    private static final TimeWindow TIME_WINDOW =
            ImmutableTimeWindow.builder().startTime(500_000).endTime(600_000).build();

    private static final Set<Long> SCOPE_ENTITIES = ImmutableSet.of(1L, 2L);
    private static final Set<Long> SCOPE_ACCOUNT_ENTITY = ImmutableSet.of(1L);
    private static final StatApiInputDTO CVG_INPUT =
            StatsTestUtil.statInput(StringConstants.RI_COUPON_COVERAGE);
    private static final StatApiInputDTO UTL_INPUT =
            StatsTestUtil.statInput(StringConstants.RI_COUPON_UTILIZATION);

    private final ReservedInstanceUtilizationCoverageServiceMole backend =
            Mockito.spy(ReservedInstanceUtilizationCoverageServiceMole.class);

    private final ReservedInstanceBoughtServiceMole riBoughtBackend =
            Mockito.spy(ReservedInstanceBoughtServiceMole.class);

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(backend);

    @Rule
    public GrpcTestServer riBoughtServer = GrpcTestServer.newServer(riBoughtBackend);

    private RIStatsSubQuery riStatsSubQuery;
    private final ApiId scope = Mockito.mock(ApiId.class);
    private final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);
    private final BuyRiScopeHandler buyRiScopeHandler = Mockito.mock(BuyRiScopeHandler.class);
    private final UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);
    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    @Before
    public void setup() throws Exception {
        final MultiEntityRequest request = Mockito.mock(MultiEntityRequest.class);
        final ServiceEntityApiDTO tierApiDto = new ServiceEntityApiDTO();
        tierApiDto.setDisplayName(TIER_NAME);
        Mockito.when(request.getSEMap()).thenReturn(Collections.singletonMap(TIER_ID, tierApiDto));
        Mockito.when(repositoryApi.entitiesRequest(org.mockito.Matchers.any())).thenReturn(request);
        riStatsSubQuery = new RIStatsSubQuery(
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(
                        testServer.getChannel()),
                ReservedInstanceBoughtServiceGrpc.newBlockingStub(riBoughtServer.getChannel()),
                repositoryApi,
                ReservedInstanceCostServiceGrpc.newBlockingStub(testServer.getChannel()),
                buyRiScopeHandler, userSessionContext);

        final StatsQueryScope queryScope = Mockito.mock(StatsQueryScope.class);
        Mockito.when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        Mockito.when(queryScope.getExpandedOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getQueryScope()).thenReturn(queryScope);
        Mockito.when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        Mockito.when(context.getInputScope()).thenReturn(scope);
        Mockito.when(context.getInputScope().getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));
    }

    @Test
    public void testApplicableInNotPlan() {
        Mockito.when(scope.isPlan()).thenReturn(false);
        MatcherAssert.assertThat(riStatsSubQuery.applicableInContext(context), Matchers.is(true));
    }

    @Test
    public void testNotApplicableInPlan() {
        Mockito.when(scope.isPlan()).thenReturn(true);
        MatcherAssert.assertThat(riStatsSubQuery.applicableInContext(context), Matchers.is(false));
    }

    @Test
    public void testNotApplicableNoTimeWindow() {
        Mockito.when(scope.isPlan()).thenReturn(false);
        Mockito.when(context.getTimeWindow()).thenReturn(Optional.empty());
        MatcherAssert.assertThat(riStatsSubQuery.applicableInContext(context), Matchers.is(true));
    }

    @Test
    public void testAggregateStats() throws Exception {
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        final ReservedInstanceStatsRecord cvgRecord =
                ReservedInstanceStatsRecord.newBuilder().setSnapshotDate(MILLIS).build();
        Mockito.doReturn(GetReservedInstanceCoverageStatsResponse.newBuilder()
                .addReservedInstanceStatsRecords(cvgRecord)
                .build())
                .when(backend)
                .getReservedInstanceCoverageStats(org.mockito.Matchers.any());

        final ReservedInstanceStatsRecord record =
                ReservedInstanceStatsRecord.newBuilder().setSnapshotDate(MILLIS).build();
        Mockito.doReturn(GetReservedInstanceUtilizationStatsResponse.newBuilder()
                .addReservedInstanceStatsRecords(record)
                .build())
                .when(backend)
                .getReservedInstanceUtilizationStats(org.mockito.Matchers.any());

        final List<StatSnapshotApiDTO> results =
                riStatsSubQuery.getAggregateStats(Sets.newHashSet(CVG_INPUT, UTL_INPUT), context);

        Mockito.verify(backend)
                .getReservedInstanceCoverageStats(
                        GetReservedInstanceCoverageStatsRequest.newBuilder()
                                .setStartDate(TIME_WINDOW.startTime())
                                .setEndDate(TIME_WINDOW.endTime())
                                .setRegionFilter(RegionFilter.newBuilder().addAllRegionId(SCOPE_ENTITIES))
                                .setIncludeBuyRiCoverage(false)
                                .build());
        Mockito.verify(backend)
                .getReservedInstanceUtilizationStats(
                        GetReservedInstanceUtilizationStatsRequest.newBuilder()
                                .setStartDate(TIME_WINDOW.startTime())
                                .setEndDate(TIME_WINDOW.endTime())
                                .setRegionFilter(RegionFilter.newBuilder().addAllRegionId(SCOPE_ENTITIES))
                                .setIncludeBuyRiUtilization(false)
                                .build());

        // Should be merged into one time
        Assert.assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        Assert.assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        final List<StatApiDTO> stats = resultSnapshot.getStatistics();
        MatcherAssert.assertThat(stats.size(), Matchers.is(2));
        final Map<String, StatApiDTO> stringStatApiDTOMap =
                stats.stream().collect(Collectors.toMap(StatApiDTO::getName, e -> e));
        Assert.assertTrue(stringStatApiDTOMap.containsKey(StringConstants.RI_COUPON_UTILIZATION));
        Assert.assertTrue(stringStatApiDTOMap.containsKey(StringConstants.RI_COUPON_COVERAGE));
    }

    /**
     * Tests for getAggregatedStats when the account does not support RI as in Azure PAYG.
     *
     * @throws Exception exception thrown by the getAggregatesStats.
     */
    @Test
    public void testAggregateStatsForNoRISupport() throws Exception {

        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)));
        final StatsQueryScope queryScope = Mockito.mock(StatsQueryScope.class);
        Mockito.when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        Mockito.when(queryScope.getExpandedOids()).thenReturn(SCOPE_ACCOUNT_ENTITY);
        Mockito.when(queryScope.getScopeOids()).thenReturn(SCOPE_ACCOUNT_ENTITY);
        Mockito.when(context.getQueryScope()).thenReturn(queryScope);
        SingleEntityRequest accountRequest = Mockito.mock(SingleEntityRequest.class);
        TopologyEntityDTO accountDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(1L)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                .setRiSupported(false).build()).build())
                .build();
        Mockito.when(accountRequest.getFullEntity()).thenReturn(Optional.of(accountDTO));
        Mockito.when(repositoryApi.entityRequest(SCOPE_ACCOUNT_ENTITY.iterator().next()))
                .thenReturn(accountRequest);

        final List<StatSnapshotApiDTO> results =
                riStatsSubQuery.getAggregateStats(Sets.newHashSet(CVG_INPUT, UTL_INPUT), context);

        Assert.assertTrue(results.isEmpty());
    }

    /**
     * Test that getAggregateStats with NUM_RI request returns response with template name by count.
     *
     * @throws Exception if getAggregateStats throws OperationFailedException.
     */
    @Test
    public void testRiBoughtCountByTemplateType() throws Exception {
        final long riBoughtCount = 3L;
        final Map<Long, Long> riBoughtCountByTierId =
                Collections.singletonMap(TIER_ID, riBoughtCount);
        final GetReservedInstanceBoughtCountByTemplateResponse response =
                GetReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                        .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                        .build();
        Mockito.when(riBoughtBackend.getReservedInstanceBoughtCountByTemplateType(
                org.mockito.Matchers.any())).thenReturn(response);
        final StatApiInputDTO request = new StatApiInputDTO();
        request.setName(StringConstants.NUM_RI);

        final List<StatSnapshotApiDTO> results =
                riStatsSubQuery.getAggregateStats(Collections.singleton(request), context);

        Assert.assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        final List<StatApiDTO> statApiDTOS = resultSnapshot.getStatistics();
        Assert.assertFalse(statApiDTOS.isEmpty());
        final StatApiDTO statApiDTO = statApiDTOS.iterator().next();
        Assert.assertEquals(TIER_NAME, statApiDTO.getFilters().iterator().next().getValue());
        Assert.assertEquals((float)riBoughtCount, statApiDTO.getValues().getAvg(), 0);
    }

    @Test
    public void testCreateUtilizationRequestGlobalScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes()).thenReturn(Optional.empty());
        Mockito.when(context.isGlobalScope()).thenReturn(true);

        final GetReservedInstanceUtilizationStatsRequest req =
                riStatsSubQuery.createUtilizationRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
    }

    @Test
    public void testCreateUtilizationRequestRegionScope() throws OperationFailedException {
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));

        final GetReservedInstanceUtilizationStatsRequest req =
                riStatsSubQuery.createUtilizationRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getRegionFilter().getRegionIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateUtilizationRequestAzScope() throws OperationFailedException {
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.AVAILABILITY_ZONE)));

        final GetReservedInstanceUtilizationStatsRequest req =
                riStatsSubQuery.createUtilizationRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getAvailabilityZoneFilter().getAvailabilityZoneIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateUtilizationRequestBaScope() throws OperationFailedException {
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);

        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)));

        final GetReservedInstanceUtilizationStatsRequest req =
                riStatsSubQuery.createUtilizationRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getAccountFilter().getAccountIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test(expected = OperationFailedException.class)
    public void testUtilizationNoEntityTypeNonGlobalScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes()).thenReturn(Optional.empty());
        Mockito.when(context.isGlobalScope()).thenReturn(false);

        riStatsSubQuery.createUtilizationRequest(context);
    }

    @Test
    public void testCreateCoverageRequestGlobalScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes()).thenReturn(Optional.empty());
        Mockito.when(context.isGlobalScope()).thenReturn(true);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
    }

    @Test
    public void testCreateCoverageRequestRegionScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getRegionFilter().getRegionIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateCoverageRequestAzScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.AVAILABILITY_ZONE)));
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getAvailabilityZoneFilter().getAvailabilityZoneIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCoverageOtherEntityType() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE)));
        Mockito.when(context.isGlobalScope()).thenReturn(false);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getEntityFilter().getEntityIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateCoverageRequestBaScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)));
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        MatcherAssert.assertThat(req.getAccountFilter().getAccountIdList(),
                Matchers.containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    /**
     * Test create coverage request for service provider scope.
     *
     * @throws OperationFailedException when failed
     */
    @Test
    public void testCreateCoverageRequestServiceProviderScope() throws OperationFailedException {
        final MultiEntityRequest multiEntityRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(multiEntityRequest.getEntitiesWithConnections())
                .thenReturn(SCOPE_ENTITIES.stream()
                        .map(oid -> EntityWithConnections.newBuilder()
                                .setOid(oid)
                                .addConnectedEntities(ConnectedEntity.newBuilder()
                                        .setConnectedEntityId(7L)
                                        .setConnectedEntityType(EntityType.REGION_VALUE)
                                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                                .build()));
        Mockito.when(repositoryApi.entitiesRequest(SCOPE_ENTITIES)).thenReturn(multiEntityRequest);
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.SERVICE_PROVIDER)));
        final CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(context.getQueryScope().getScopeOids()).thenReturn(SCOPE_ENTITIES);
        Mockito.when(context.getInputScope().getCachedGroupInfo())
                .thenReturn(Optional.of(cachedGroupInfo));
        Mockito.when(context.getInputScope().isGroup()).thenReturn(true);

        final GetReservedInstanceCoverageStatsRequest req =
                riStatsSubQuery.createCoverageRequest(context);

        MatcherAssert.assertThat(req.getStartDate(), Matchers.is(TIME_WINDOW.startTime()));
        MatcherAssert.assertThat(req.getEndDate(), Matchers.is(TIME_WINDOW.endTime()));
        Assert.assertEquals(Collections.singletonList(7L), req.getRegionFilter().getRegionIdList());
    }

    @Test(expected = OperationFailedException.class)
    public void testCoverageNoEntityTypeNonGlobalScope() throws OperationFailedException {
        Mockito.when(scope.getScopeTypes()).thenReturn(Optional.empty());
        Mockito.when(context.isGlobalScope()).thenReturn(false);
        riStatsSubQuery.createCoverageRequest(context);
    }

    @Test
    public void testConvertStatCoverage() {
        final StatValue capacity =
                StatValue.newBuilder().setAvg(10).setMax(20).setMin(1).setTotal(30).build();
        final StatValue value =
                StatValue.newBuilder().setAvg(11).setMax(21).setMin(2).setTotal(31).build();

        final ReservedInstanceStatsRecord record = ReservedInstanceStatsRecord.newBuilder()
                .setSnapshotDate(MILLIS)
                .setCapacity(capacity)
                .setValues(value)
                .build();

        final List<StatSnapshotApiDTO> snapshots =
                AbstractRIStatsSubQuery.internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                        Collections.singletonList(record), true);

        MatcherAssert.assertThat(snapshots.size(), Matchers.is(1));
        final StatSnapshotApiDTO snapshot = snapshots.get(0);
        MatcherAssert.assertThat(snapshot.getDate(), Matchers.is(DateTimeUtil.toString(MILLIS)));
        MatcherAssert.assertThat(snapshot.getStatistics().size(), Matchers.is(1));

        final StatApiDTO stat = snapshot.getStatistics().get(0);
        MatcherAssert.assertThat(stat.getValue(), Matchers.is(value.getAvg()));
        MatcherAssert.assertThat(stat.getName(), Matchers.is(StringConstants.RI_COUPON_COVERAGE));
        MatcherAssert.assertThat(stat.getUnits(), Matchers.is(StringConstants.RI_COUPON_UNITS));
        MatcherAssert.assertThat(stat.getValues().getMax(), Matchers.is(value.getMax()));
        MatcherAssert.assertThat(stat.getValues().getMin(), Matchers.is(value.getMin()));
        MatcherAssert.assertThat(stat.getValues().getAvg(), Matchers.is(value.getAvg()));
        MatcherAssert.assertThat(stat.getValues().getTotal(), Matchers.is(value.getTotal()));
        MatcherAssert.assertThat(stat.getCapacity().getMax(), Matchers.is(capacity.getMax()));
        MatcherAssert.assertThat(stat.getCapacity().getMin(), Matchers.is(capacity.getMin()));
        MatcherAssert.assertThat(stat.getCapacity().getAvg(), Matchers.is(capacity.getAvg()));
        MatcherAssert.assertThat(stat.getCapacity().getTotal(), Matchers.is(capacity.getTotal()));
    }

    @Test
    public void testConvertStatUtilization() {
        final ReservedInstanceStatsRecord record =
                ReservedInstanceStatsRecord.newBuilder().setSnapshotDate(MILLIS).build();

        final List<StatSnapshotApiDTO> snapshots =
                AbstractRIStatsSubQuery.internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                        Collections.singletonList(record), false);

        MatcherAssert.assertThat(snapshots.size(), Matchers.is(1));
        final StatSnapshotApiDTO snapshot = snapshots.get(0);
        MatcherAssert.assertThat(snapshot.getDate(), Matchers.is(DateTimeUtil.toString(MILLIS)));
        MatcherAssert.assertThat(snapshot.getStatistics().size(), Matchers.is(1));

        final StatApiDTO stat = snapshot.getStatistics().get(0);
        MatcherAssert.assertThat(stat.getName(),
                Matchers.is(StringConstants.RI_COUPON_UTILIZATION));
    }

    @Test
    public void testObserverUser() throws Exception {
        Mockito.when(userSessionContext.isUserObserver()).thenReturn(true);

        final List<StatSnapshotApiDTO> results =
                riStatsSubQuery.getAggregateStats(Sets.newHashSet(CVG_INPUT, UTL_INPUT), context);

        MatcherAssert.assertThat(results.size(), Matchers.is(0));
    }
}
