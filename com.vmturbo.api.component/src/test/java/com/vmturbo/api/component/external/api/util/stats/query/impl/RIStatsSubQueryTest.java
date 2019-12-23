package com.vmturbo.api.component.external.api.util.stats.query.impl;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.component.external.api.util.stats.query.impl.RIStatsSubQuery.RIStatsMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceBoughtServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;

public class RIStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;
    private static final long TIER_ID = 1111L;
    private static final String TIER_NAME = "compute_medium";
    private static final TimeWindow TIME_WINDOW = ImmutableTimeWindow.builder()
        .startTime(500_000)
        .endTime(600_000)
        .build();

    private static final Set<Long> SCOPE_ENTITIES = ImmutableSet.of(1L, 2L);

    private static final StatApiInputDTO CVG_INPUT = StatsTestUtil.statInput(StringConstants.RI_COUPON_COVERAGE);
    private static final StatApiInputDTO UTL_INPUT = StatsTestUtil.statInput(StringConstants.RI_COUPON_UTILIZATION);

    private ReservedInstanceUtilizationCoverageServiceMole backend =
        spy(ReservedInstanceUtilizationCoverageServiceMole.class);

    private ReservedInstanceBoughtServiceMole riBoughtBackend =
            spy(ReservedInstanceBoughtServiceMole.class);

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(backend);

    @Rule
    public GrpcTestServer riBoughtServer = GrpcTestServer.newServer(riBoughtBackend);

    private RIStatsMapper mapper = mock(RIStatsMapper.class);

    private RIStatsSubQuery query;

    private StatsQueryContext context = mock(StatsQueryContext.class);

    private ApiId scope = mock(ApiId.class);

    @Before
    public void setup() {
        final RepositoryApi repositoryApi = mock(RepositoryApi.class);
        final MultiEntityRequest request = mock(MultiEntityRequest.class);
        final ServiceEntityApiDTO tierApiDto = new ServiceEntityApiDTO();
        tierApiDto.setDisplayName(TIER_NAME);
        when(request.getSEMap()).thenReturn(Collections.singletonMap(TIER_ID, tierApiDto));
        when(repositoryApi.entitiesRequest(any())).thenReturn(request);
        query = new RIStatsSubQuery(
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(testServer.getChannel()),
                ReservedInstanceBoughtServiceGrpc.newBlockingStub(riBoughtServer.getChannel()),
                mapper, repositoryApi);

        when(context.getInputScope()).thenReturn(scope);
        Set<UIEntityType> inputScopeTypes = new HashSet<>();
        inputScopeTypes.add(UIEntityType.REGION);
        when(context.getInputScope().getScopeTypes()).thenReturn(Optional.of(inputScopeTypes));
        StatsQueryScope queryScope = StatsQueryScope.some(SCOPE_ENTITIES);
        when(context.getQueryScope()).thenReturn(queryScope);
    }

    @Test
    public void testApplicableInNotPlan() {
        when(scope.isPlan()).thenReturn(false);
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testNotApplicableInPlan() {
        when(scope.isPlan()).thenReturn(true);
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableNoTimeWindow() {
        when(scope.isPlan()).thenReturn(false);
        when(context.getTimeWindow()).thenReturn(Optional.empty());

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testAggregateStats() throws OperationFailedException {
        // Arrange coverage
        final GetReservedInstanceCoverageStatsRequest cvgReq = GetReservedInstanceCoverageStatsRequest.newBuilder()
            .setStartDate(1L)
            .build();
        when(mapper.createCoverageRequest(any())).thenReturn(cvgReq);

        final ReservedInstanceStatsRecord cvgRecord = ReservedInstanceStatsRecord.newBuilder()
            .setSnapshotDate(1)
            .build();
        doReturn(GetReservedInstanceCoverageStatsResponse.newBuilder()
            .addReservedInstanceStatsRecords(cvgRecord)
            .build()).when(backend).getReservedInstanceCoverageStats(any());
        final StatSnapshotApiDTO cvgMappedSnapshot = new StatSnapshotApiDTO();
        cvgMappedSnapshot.setDate(DateTimeUtil.toString(MILLIS));
        cvgMappedSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));

        when(mapper.convertRIStatsRecordsToStatSnapshotApiDTO(any(), eq(true)))
            .thenReturn(Collections.singletonList(cvgMappedSnapshot));

        // Arrange utilization
        final GetReservedInstanceUtilizationStatsRequest utilReq = GetReservedInstanceUtilizationStatsRequest.newBuilder()
            .setStartDate(1L)
            .build();
        when(mapper.createUtilizationRequest(any())).thenReturn(utilReq);

        final ReservedInstanceStatsRecord record = ReservedInstanceStatsRecord.newBuilder()
            .setSnapshotDate(1)
            .build();
        doReturn(GetReservedInstanceUtilizationStatsResponse.newBuilder()
            .addReservedInstanceStatsRecords(record)
            .build()).when(backend).getReservedInstanceUtilizationStats(any());
        final StatSnapshotApiDTO utilMappedSnapshot = new StatSnapshotApiDTO();
        utilMappedSnapshot.setDate(DateTimeUtil.toString(MILLIS));
        utilMappedSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("bar")));

        when(mapper.convertRIStatsRecordsToStatSnapshotApiDTO(any(), eq(false)))
            .thenReturn(Collections.singletonList(utilMappedSnapshot));

        // ACT
        Map<Long, List<StatApiDTO>> ret = query.getAggregateStats(Sets.newHashSet(CVG_INPUT, UTL_INPUT), context);

        // VERIFY
        verify(mapper).createCoverageRequest(context);
        verify(mapper).convertRIStatsRecordsToStatSnapshotApiDTO(Collections.singletonList(record), true);
        verify(backend).getReservedInstanceCoverageStats(cvgReq);
        verify(mapper).createUtilizationRequest(context);
        verify(mapper).convertRIStatsRecordsToStatSnapshotApiDTO(Collections.singletonList(record), false);
        verify(backend).getReservedInstanceUtilizationStats(utilReq);

        // Should be merged into one time
        assertThat(ret.keySet(), containsInAnyOrder(MILLIS));
        assertThat(ret.get(MILLIS), containsInAnyOrder(cvgMappedSnapshot.getStatistics().get(0), utilMappedSnapshot.getStatistics().get(0)));
    }

    /**
     * Test that getAggregateStats with NUM_RI request returns response with template name by count.
     *
     * @throws OperationFailedException if getAggregateStats throws OperationFailedException.
     */
    @Test
    public void testRiBoughtCountByTemplateType() throws OperationFailedException {
        final long riBoughtCount = 3L;
        final Map<Long, Long> riBoughtCountByTierId = Collections.singletonMap(TIER_ID,
                riBoughtCount);
        GetReservedInstanceBoughtCountByTemplateResponse response =
                GetReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                        .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                        .build();
        when(riBoughtBackend.getReservedInstanceBoughtCountByTemplateType(any()))
                .thenReturn(response);
        when(mapper.convertNumRIStatsRecordsToStatSnapshotApiDTO(any())).thenCallRealMethod();
        final StatApiInputDTO request = new StatApiInputDTO();
        request.setName(StringConstants.NUM_RI);
        final Map<Long, List<StatApiDTO>> result =
                query.getAggregateStats(Collections.singleton(request),
                context);
        Assert.assertFalse(result.isEmpty());
        final List<StatApiDTO> statApiDTOS = result.values().iterator().next();
        Assert.assertFalse(statApiDTOS.isEmpty());
        final StatApiDTO statApiDTO = statApiDTOS.iterator().next();
        Assert.assertEquals(TIER_NAME, statApiDTO.getFilters().iterator().next().getValue());
        Assert.assertEquals((float)riBoughtCount, statApiDTO.getValues().getAvg(), 0);
    }

    @Test
    public void testCreateUtilizationRequestGlobalScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.empty());
        when(context.isGlobalScope()).thenReturn(true);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceUtilizationStatsRequest req = mapper.createUtilizationRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
    }

    @Test
    public void testCreateUtilizationRequestRegionScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(context.getInputScope().isGroup()).thenReturn(true);
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.REGION)));

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceUtilizationStatsRequest req = mapper.createUtilizationRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getRegionFilter().getRegionIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateUtilizationRequestAzScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope().isGroup()).thenReturn(true);
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.AVAILABILITY_ZONE)));

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceUtilizationStatsRequest req = mapper.createUtilizationRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getAvailabilityZoneFilter().getAvailabilityZoneIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateUtilizationRequestBaScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope().isGroup()).thenReturn(true);

        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceUtilizationStatsRequest req = mapper.createUtilizationRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getAccountFilter().getAccountIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test(expected = OperationFailedException.class)
    public void testUtilizationNoEntityTypeNonGlobalScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.empty());
        when(context.isGlobalScope()).thenReturn(false);

        new RIStatsMapper().createUtilizationRequest(context);
    }

    @Test(expected = OperationFailedException.class)
    public void testUtilizationUnsupportedEntityType() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.VIRTUAL_MACHINE)));
        when(context.isGlobalScope()).thenReturn(false);

        new RIStatsMapper().createUtilizationRequest(context);
    }


    @Test
    public void testCreateCoverageRequestGlobalScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.empty());
        when(context.isGlobalScope()).thenReturn(true);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceCoverageStatsRequest req = mapper.createCoverageRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
    }

    @Test
    public void testCreateCoverageRequestRegionScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.REGION)));
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope().isGroup()).thenReturn(true);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceCoverageStatsRequest req = mapper.createCoverageRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getRegionFilter().getRegionIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateCoverageRequestAzScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.AVAILABILITY_ZONE)));
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope().isGroup()).thenReturn(true);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceCoverageStatsRequest req = mapper.createCoverageRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getAvailabilityZoneFilter().getAvailabilityZoneIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCoverageOtherEntityType() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.VIRTUAL_MACHINE)));
        when(context.isGlobalScope()).thenReturn(false);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceCoverageStatsRequest req = mapper.createCoverageRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getEntityFilter().getEntityIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test
    public void testCreateCoverageRequestBaScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.of(
                        Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));
        CachedGroupInfo cachedGroupInfo = Mockito.mock(CachedGroupInfo.class);
        Mockito.when(cachedGroupInfo.getEntityIds()).thenReturn(SCOPE_ENTITIES);
        when(context.getInputScope().getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope().isGroup()).thenReturn(true);

        final RIStatsMapper mapper = new RIStatsMapper();
        final GetReservedInstanceCoverageStatsRequest req = mapper.createCoverageRequest(context);
        assertThat(req.getStartDate(), is(TIME_WINDOW.startTime()));
        assertThat(req.getEndDate(), is(TIME_WINDOW.endTime()));
        assertThat(req.getAccountFilter().getAccountIdList(), containsInAnyOrder(SCOPE_ENTITIES.toArray()));
    }

    @Test(expected = OperationFailedException.class)
    public void testCoverageNoEntityTypeNonGlobalScope() throws OperationFailedException {
        when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
        when(scope.getScopeTypes()).thenReturn(Optional.empty());
        when(context.isGlobalScope()).thenReturn(false);

        new RIStatsMapper().createCoverageRequest(context);
    }

    @Test
    public void testConvertStatCoverage() {
        StatValue capacity = StatValue.newBuilder()
            .setAvg(10)
            .setMax(20)
            .setMin(1)
            .setTotal(30)
            .build();
        StatValue value = StatValue.newBuilder()
            .setAvg(11)
            .setMax(21)
            .setMin(2)
            .setTotal(31)
            .build();

        ReservedInstanceStatsRecord record = ReservedInstanceStatsRecord.newBuilder()
            .setSnapshotDate(MILLIS)
            .setCapacity(capacity)
            .setValues(value)
            .build();

        final List<StatSnapshotApiDTO> snapshots = new RIStatsMapper()
            .convertRIStatsRecordsToStatSnapshotApiDTO(Collections.singletonList(record), true);

        assertThat(snapshots.size(), is(1));
        StatSnapshotApiDTO snapshot = snapshots.get(0);
        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshot.getStatistics().size(), is(1));

        StatApiDTO stat = snapshot.getStatistics().get(0);
        assertThat(stat.getValue(), is(value.getAvg()));
        assertThat(stat.getName(), is(StringConstants.RI_COUPON_COVERAGE));
        assertThat(stat.getUnits(), is(StringConstants.RI_COUPON_UNITS));
        assertThat(stat.getValues().getMax(), is(value.getMax()));
        assertThat(stat.getValues().getMin(), is(value.getMin()));
        assertThat(stat.getValues().getAvg(), is(value.getAvg()));
        assertThat(stat.getValues().getTotal(), is(value.getTotal()));
        assertThat(stat.getCapacity().getMax(), is(capacity.getMax()));
        assertThat(stat.getCapacity().getMin(), is(capacity.getMin()));
        assertThat(stat.getCapacity().getAvg(), is(capacity.getAvg()));
        assertThat(stat.getCapacity().getTotal(), is(capacity.getTotal()));
    }

    @Test
    public void testConvertStatUtilization() {
        ReservedInstanceStatsRecord record = ReservedInstanceStatsRecord.newBuilder()
            .setSnapshotDate(MILLIS)
            .build();

        final List<StatSnapshotApiDTO> snapshots = new RIStatsMapper()
            .convertRIStatsRecordsToStatSnapshotApiDTO(Collections.singletonList(record), false);
        assertThat(snapshots.size(), is(1));
        StatSnapshotApiDTO snapshot = snapshots.get(0);
        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshot.getStatistics().size(), is(1));

        StatApiDTO stat = snapshot.getStatistics().get(0);
        assertThat(stat.getName(), is(StringConstants.RI_COUPON_UTILIZATION));
    }
}