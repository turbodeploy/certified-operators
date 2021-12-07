package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilization;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse.CloudCommitmentUtilizationRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesMoles.CloudCommitmentStatsServiceMole;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Testing the CloudCommitmentStatsSubQuery.
 */
public class CloudCommitmentStatsSubQueryTest {
    private final CloudCommitmentStatsServiceMole backend = Mockito.spy(CloudCommitmentStatsServiceMole.class);

    /**
     * The test server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(backend);
    private final ApiId scope = Mockito.mock(ApiId.class);
    private final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);

    private CloudCommitmentStatsSubQuery cloudCommitmentStatsSubQuery;

    private final Long timestamp = 1609525802L;

    private static final TimeWindow TIME_WINDOW = ImmutableTimeWindow.builder()
            .startTime(500_000)
            .endTime(600_000)
            .includeCurrent(true)
            .includeHistorical(true)
            .includeProjected(false)
            .build();

    private static final StatValue capacity = StatValue.newBuilder().setAvg(CloudCommitmentAmount.newBuilder().setAmount(
            CurrencyAmount.newBuilder().setAmount(2.5d).build()).build()).setMax(
            CloudCommitmentAmount.newBuilder().setAmount(CurrencyAmount.newBuilder().setAmount(5.0d).build())).build();

    private static final StatValue value = StatValue.newBuilder().setAvg(CloudCommitmentAmount.newBuilder().setAmount(
            CurrencyAmount.newBuilder().setAmount(1.0d).build()).build()).setMax(
            CloudCommitmentAmount.newBuilder().setAmount(CurrencyAmount.newBuilder().setAmount(3.0d).build()).build()).build();

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        cloudCommitmentStatsSubQuery = new CloudCommitmentStatsSubQuery(
                CloudCommitmentStatsServiceGrpc.newBlockingStub(testServer.getChannel()));
        final StatsQueryScope queryScope = Mockito.mock(StatsQueryScope.class);
        Mockito.when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        Mockito.when(context.getQueryScope()).thenReturn(queryScope);
        Mockito.when(context.getInputScope()).thenReturn(scope);
        Mockito.when(context.getInputScope().getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.SERVICE_PROVIDER)));
        Mockito.when(context.getTimeWindow()).thenReturn(Optional.of(TIME_WINDOW));
    }

    /**
     * Test that the query is not applicable in plan.
     */
    @Test
    public void testNotApplicableInPlan() {
        Mockito.when(scope.isPlan()).thenReturn(false);
        MatcherAssert.assertThat(cloudCommitmentStatsSubQuery.applicableInContext(context), Matchers.is(true));
    }

    /**
     * Test returning of the cloud commitment stats when the request just contains the service provider.
     *
     * @throws OperationFailedException An operation failed exception.
     * @throws InterruptedException An interrupted exception.
     * @throws ConversionException A conversion exception.
     */
    @Test
    public void testUtilizationAggregateStatsByCsp()
            throws OperationFailedException, InterruptedException, ConversionException {

        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.SERVICE_PROVIDER)));
        final StatsQueryScope queryScope = Mockito.mock(StatsQueryScope.class);
        Mockito.when(context.getQueryScope()).thenReturn(queryScope);
        final CloudCommitmentStatRecord statRecord = CloudCommitmentStatRecord.newBuilder().setCapacity(capacity)
                .setValues(value)
                .setSnapshotDate(timestamp)
                .build();

        GetHistoricalCloudCommitmentUtilizationResponse response = GetHistoricalCloudCommitmentUtilizationResponse.newBuilder().addCommitmentStatRecordChunk(statRecord).build();
        List<GetHistoricalCloudCommitmentUtilizationResponse> responses = new ArrayList<>();
        responses.add(response);
        Mockito.doReturn(responses).when(backend).getHistoricalCommitmentUtilization(org.mockito.Matchers.any());

        StatApiInputDTO cloudCommitmentInput =
                StatsTestUtil.statInput(StringConstants.CLOUD_COMMITMENT_UTILIZATION);
        final List<StatSnapshotApiDTO> results = cloudCommitmentStatsSubQuery.getAggregateStats(
                Sets.newHashSet(cloudCommitmentInput), context);

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        final List<StatApiDTO> stats = resultSnapshot.getStatistics();
        final Map<String, StatApiDTO> stringStatApiDTOMap =
                stats.stream().collect(Collectors.toMap(StatApiDTO::getName, e -> e));
        assertTrue(stringStatApiDTOMap.containsKey(StringConstants.CLOUD_COMMITMENT_UTILIZATION));
        StatApiDTO stat = stringStatApiDTOMap.get(StringConstants.CLOUD_COMMITMENT_UTILIZATION);
        assertEquals(Float.valueOf(5.0f), stat.getCapacity().getMax());
        assertEquals(Float.valueOf(2.5f), stat.getCapacity().getAvg());
        assertEquals(Float.valueOf(3.0f), stat.getValues().getMax());
        assertEquals(Float.valueOf(1.0f), stat.getValues().getAvg());
    }

    /**
     * Test fetching of cloud commitment coverage stats from the cost component.
     *
     * @throws OperationFailedException An operation failed exception.
     * @throws InterruptedException An interrupted exception.
     * @throws ConversionException A Conversion Exception.
     */
    @Test
    public void testCoverageStatsByCsp()
            throws OperationFailedException, InterruptedException, ConversionException {
        Mockito.when(scope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(ApiEntityType.SERVICE_PROVIDER)));
        final StatsQueryScope queryScope = Mockito.mock(StatsQueryScope.class);
        Mockito.when(context.getQueryScope()).thenReturn(queryScope);
        final CloudCommitmentStatRecord statRecord = CloudCommitmentStatRecord.newBuilder().setCapacity(capacity)
                .setValues(value)
                .setSnapshotDate(timestamp)
                .build();

        GetHistoricalCommitmentCoverageStatsResponse response = GetHistoricalCommitmentCoverageStatsResponse.newBuilder()
                .addCommitmentStatRecordChunk(statRecord).build();
        List<GetHistoricalCommitmentCoverageStatsResponse> responses = new ArrayList<>();
        responses.add(response);

        StatApiInputDTO cloudCommitmentInput =
                StatsTestUtil.statInput(StringConstants.CLOUD_COMMITMENT_COVERAGE);
        Mockito.doReturn(responses).when(backend).getHistoricalCommitmentCoverageStats(org.mockito.Matchers.any());
        final List<StatSnapshotApiDTO> results = cloudCommitmentStatsSubQuery.getAggregateStats(
                Sets.newHashSet(cloudCommitmentInput), context);

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        final List<StatApiDTO> stats = resultSnapshot.getStatistics();
        final Map<String, StatApiDTO> stringStatApiDTOMap =
                stats.stream().collect(Collectors.toMap(StatApiDTO::getName, e -> e));
        assertTrue(stringStatApiDTOMap.containsKey(StringConstants.CLOUD_COMMITMENT_COVERAGE));
        StatApiDTO stat = stringStatApiDTOMap.get(StringConstants.CLOUD_COMMITMENT_COVERAGE);
        assertEquals(Float.valueOf(5.0f), stat.getCapacity().getMax());
        assertEquals(Float.valueOf(2.5f), stat.getCapacity().getAvg());
        assertEquals(Float.valueOf(3.0f), stat.getValues().getMax());
        assertEquals(Float.valueOf(1.0f), stat.getValues().getAvg());
    }

    /**
     * Test fetching topology cloud commitment coverage stats for current or projected timeframe.
     *
     * @throws ConversionException never
     * @throws OperationFailedException never
     * @throws InterruptedException never
     */
    @Test
    public void testTopologyCoverageStats() throws ConversionException, OperationFailedException, InterruptedException {
        final double capacity = 100.1;
        final double used = 1.5;
        final GetTopologyCommitmentCoverageStatsResponse response =
                GetTopologyCommitmentCoverageStatsResponse.newBuilder()
                        .addCommitmentCoverageStatChunk(CloudCommitmentCoverage.newBuilder()
                                .setCapacity(CloudCommitmentAmount.newBuilder().setCoupons(capacity))
                                .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(used))
                                .build())
                        .addCommitmentCoverageStatChunk(CloudCommitmentCoverage.newBuilder()
                                .setCapacity(CloudCommitmentAmount.newBuilder().setAmount(
                                        CurrencyAmount.newBuilder().setCurrency(1).setAmount(capacity)))
                                .setUsed(CloudCommitmentAmount.newBuilder().setAmount(
                                        CurrencyAmount.newBuilder().setCurrency(1).setAmount(used)))
                                .build())
                        .build();
        final ArgumentCaptor<GetTopologyCommitmentCoverageStatsRequest> captor =
                ArgumentCaptor.forClass(GetTopologyCommitmentCoverageStatsRequest.class);
        Mockito.doReturn(Collections.singletonList(response)).when(backend)
                .getTopologyCommitmentCoverage(captor.capture());
        Mockito.when(context.requestProjected()).thenReturn(true);

        final StatApiInputDTO input = StatsTestUtil.statInput(StringConstants.CLOUD_COMMITMENT_COVERAGE);

        final List<StatSnapshotApiDTO> result1 =
                cloudCommitmentStatsSubQuery.getAggregateStats(Collections.singleton(input), context);
        verifyInternalTopologyCommitmentCoverageRequest(1, TopologyType.TOPOLOGY_TYPE_PROJECTED, captor);
        assertEquals(2, result1.size());
        result1.forEach(stat -> {
            assertEquals(Epoch.PROJECTED, stat.getEpoch());
            assertEquals(1, stat.getStatistics().size());
            final StatApiDTO innerStat = stat.getStatistics().get(0);
            verifyInnerTopologyStat(capacity, used, innerStat, StringConstants.CLOUD_COMMITMENT_COVERAGE);
        });

        Mockito.when(context.requestProjected()).thenReturn(false);
        Mockito.when(context.includeCurrent()).thenReturn(true);

        final List<StatSnapshotApiDTO> result2 =
                cloudCommitmentStatsSubQuery.getAggregateStats(Collections.singleton(input), context);
        verifyInternalTopologyCommitmentCoverageRequest(2, TopologyType.TOPOLOGY_TYPE_SOURCE, captor);
        assertEquals(2, result2.size());
        result2.forEach(stat -> {
            assertEquals(Epoch.CURRENT, stat.getEpoch());
            assertEquals(1, stat.getStatistics().size());
            final StatApiDTO innerStat = stat.getStatistics().get(0);
            verifyInnerTopologyStat(capacity, used, innerStat, StringConstants.CLOUD_COMMITMENT_COVERAGE);
        });
    }

    /**
     * Test fetching topology cloud commitment utilization stats for current or projected timeframe.
     *
     * @throws ConversionException never
     * @throws OperationFailedException never
     * @throws InterruptedException never
     */
    @Test
    public void testTopologyUtilizationStats() throws ConversionException, OperationFailedException, InterruptedException {
        final double capacity = 200.1;
        final double used = 10.8;
        final double overhead = 50.7;
        final long oid = 123456789;
        final GetTopologyCommitmentUtilizationStatsResponse response =
                GetTopologyCommitmentUtilizationStatsResponse.newBuilder()
                        .addCommitmentUtilizationRecordChunk(
                                CloudCommitmentUtilizationRecord.newBuilder()
                                        .setCommitmentOid(oid)
                                        .setUtilization(CloudCommitmentUtilization.newBuilder()
                                                .setCapacity(CloudCommitmentAmount.newBuilder()
                                                        .setCoupons(capacity))
                                                .setOverhead(CloudCommitmentAmount.newBuilder()
                                                        .setCoupons(overhead))
                                                .setUsed(CloudCommitmentAmount.newBuilder()
                                                        .setCoupons(used))))
                        .addCommitmentUtilizationRecordChunk(
                                CloudCommitmentUtilizationRecord.newBuilder()
                                        .setCommitmentOid(oid)
                                        .setUtilization(CloudCommitmentUtilization.newBuilder()
                                                .setCapacity(CloudCommitmentAmount.newBuilder()
                                                        .setAmount(CurrencyAmount.newBuilder()
                                                                .setCurrency(1)
                                                                .setAmount(capacity)))
                                                .setOverhead(CloudCommitmentAmount.newBuilder()
                                                        .setAmount(CurrencyAmount.newBuilder()
                                                                .setCurrency(1)
                                                                .setAmount(overhead)))
                                                .setUsed(CloudCommitmentAmount.newBuilder()
                                                        .setAmount(CurrencyAmount.newBuilder()
                                                                .setCurrency(1)
                                                                .setAmount(used)))))
                        .build();
        final ArgumentCaptor<GetTopologyCommitmentUtilizationStatsRequest> captor =
                ArgumentCaptor.forClass(GetTopologyCommitmentUtilizationStatsRequest.class);
        Mockito.doReturn(Collections.singletonList(response)).when(backend)
                .getTopologyCommitmentUtilization(captor.capture());
        Mockito.when(context.requestProjected()).thenReturn(true);

        final StatApiInputDTO input = StatsTestUtil.statInput(StringConstants.CLOUD_COMMITMENT_UTILIZATION);

        final List<StatSnapshotApiDTO> result1 =
                cloudCommitmentStatsSubQuery.getAggregateStats(Collections.singleton(input), context);
        verifyInternalTopologyCommitmentUtilizationRequest(1, TopologyType.TOPOLOGY_TYPE_PROJECTED, captor);
        assertEquals(2, result1.size());
        result1.forEach(stat -> {
            assertEquals(Epoch.PROJECTED, stat.getEpoch());
            assertEquals(1, stat.getStatistics().size());
            final StatApiDTO innerStat = stat.getStatistics().get(0);
            verifyInnerTopologyStat(capacity, used, innerStat, StringConstants.CLOUD_COMMITMENT_UTILIZATION);
            verifyStatValue(overhead, innerStat.getReserved());
            assertNotNull(innerStat.getRelatedEntity());
            assertEquals(oid, Long.valueOf(innerStat.getRelatedEntity().getUuid()).longValue());
            assertEquals(StringConstants.CLOUD_COMMITMENT, innerStat.getRelatedEntity().getClassName());
            assertEquals(StringConstants.CLOUD_COMMITMENT, innerStat.getRelatedEntityType());
        });
        Mockito.when(context.requestProjected()).thenReturn(false);
        Mockito.when(context.includeCurrent()).thenReturn(true);

        final List<StatSnapshotApiDTO> result2 =
                cloudCommitmentStatsSubQuery.getAggregateStats(Collections.singleton(input), context);
        verifyInternalTopologyCommitmentUtilizationRequest(2, TopologyType.TOPOLOGY_TYPE_SOURCE, captor);
        assertEquals(2, result2.size());
        result2.forEach(stat -> {
            assertEquals(Epoch.CURRENT, stat.getEpoch());
            assertEquals(1, stat.getStatistics().size());
            final StatApiDTO innerStat = stat.getStatistics().get(0);
            verifyInnerTopologyStat(capacity, used, innerStat, StringConstants.CLOUD_COMMITMENT_UTILIZATION);
            verifyStatValue(overhead, innerStat.getReserved());
            assertNotNull(innerStat.getRelatedEntity());
            assertEquals(oid, Long.valueOf(innerStat.getRelatedEntity().getUuid()).longValue());
            assertEquals(StringConstants.CLOUD_COMMITMENT, innerStat.getRelatedEntity().getClassName());
            assertEquals(StringConstants.CLOUD_COMMITMENT, innerStat.getRelatedEntityType());
        });
    }

    private void verifyInnerTopologyStat(double capacity, double used, StatApiDTO innerStat, String topologyCommitmentCoverage) {
        assertEquals(topologyCommitmentCoverage, innerStat.getName());
        assertNotNull(innerStat.getUnits());
        verifyStatValue(used, innerStat.getValues());
        verifyStatValue(capacity, innerStat.getCapacity());
    }

    private void verifyStatValue(double value, StatValueApiDTO dto) {
        assertNotNull(dto);
        assertEquals(value, dto.getAvg(), .001);
        assertEquals(value, dto.getTotal(), .001);
        assertEquals(value, dto.getMin(), .001);
        assertEquals(value, dto.getMax(), .001);
    }

    private void verifyInternalTopologyCommitmentCoverageRequest(int times, TopologyType type,
            ArgumentCaptor<GetTopologyCommitmentCoverageStatsRequest> captor) {
        Mockito.verify(backend, Mockito.times(times)).getTopologyCommitmentCoverage(Mockito.any());
        final GetTopologyCommitmentCoverageStatsRequest internalRequest = captor.getValue();
        assertTrue(internalRequest.hasTopologyType());
        assertEquals(type, internalRequest.getTopologyType());
    }

    private void verifyInternalTopologyCommitmentUtilizationRequest(int times, TopologyType topologyType,
            ArgumentCaptor<GetTopologyCommitmentUtilizationStatsRequest> captor) {
        Mockito.verify(backend, Mockito.times(times)).getTopologyCommitmentUtilization(Mockito.any());
        final GetTopologyCommitmentUtilizationStatsRequest internalRequest = captor.getValue();
        assertTrue(internalRequest.hasTopologyType());
        assertEquals(topologyType, internalRequest.getTopologyType());
    }
}
