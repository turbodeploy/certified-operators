package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesMoles.CloudCommitmentStatsServiceMole;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Testing the CLoudCommitmentStatsSubQuery.
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

    private static final StatApiInputDTO CLOUD_COMMITMENT_INPUT =
            StatsTestUtil.statInput(StringConstants.CLOUD_COMMITMENT_UTILIZATION);

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
    public void testAggregateStatsByCsp()
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

        final List<StatSnapshotApiDTO> results = cloudCommitmentStatsSubQuery.getAggregateStats(
                Sets.newHashSet(CLOUD_COMMITMENT_INPUT), context);

        Assert.assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        final List<StatApiDTO> stats = resultSnapshot.getStatistics();
        final Map<String, StatApiDTO> stringStatApiDTOMap =
                stats.stream().collect(Collectors.toMap(StatApiDTO::getName, e -> e));
        Assert.assertTrue(stringStatApiDTOMap.containsKey(StringConstants.CLOUD_COMMITMENT_UTILIZATION));
        StatApiDTO stat = stringStatApiDTOMap.get(StringConstants.CLOUD_COMMITMENT_UTILIZATION);
        Assert.assertEquals(Float.valueOf(5.0f), stat.getCapacity().getMax());
        Assert.assertEquals(Float.valueOf(2.5f), stat.getCapacity().getAvg());
        Assert.assertEquals(Float.valueOf(3.0f), stat.getValues().getMax());
        Assert.assertEquals(Float.valueOf(1.0f), stat.getValues().getAvg());
    }
}
