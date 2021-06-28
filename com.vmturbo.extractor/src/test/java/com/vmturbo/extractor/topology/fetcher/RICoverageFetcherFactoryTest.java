package com.vmturbo.extractor.topology.fetcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetProjectedEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetProjectedEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory.RICoverageData;

/**
 * Tests for {@link RICoverageFetcherFactory}.
 */
public class RICoverageFetcherFactoryTest {

    private final ReservedInstanceUtilizationCoverageServiceMole riServiceMole =
            spy(ReservedInstanceUtilizationCoverageServiceMole.class);
    private final MultiStageTimer timer = mock(MultiStageTimer.class);

    /**
     * Mock tests for gRPC services.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(riServiceMole);

    private RICoverageFetcherFactory riCoverageFetcherFactory;

    /**
     * Set up RI service and fetcher.
     */
    @Before
    public void before() {
        final ReservedInstanceUtilizationCoverageServiceBlockingStub riService =
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(server.getChannel());
        riCoverageFetcherFactory = new RICoverageFetcherFactory(riService);
    }

    /**
     * Test that current RI Coverage are fetched correctly.
     */
    @Test
    public void testFetchCurrentRICoverage() {
        GetEntityReservedInstanceCoverageResponse response =
                GetEntityReservedInstanceCoverageResponse.newBuilder()
                        .putCoverageByEntityId(111L, EntityReservedInstanceCoverage.newBuilder()
                                .setEntityId(111L)
                                .setEntityCouponCapacity(10)
                                .putCouponsCoveredByRi(991, 1)
                                .putCouponsCoveredByRi(992, 2)
                                .build())
                        .build();

        doReturn(response).when(riServiceMole).getEntityReservedInstanceCoverage(
                any(GetEntityReservedInstanceCoverageRequest.class));

        final RICoverageData riCoverageData = riCoverageFetcherFactory.newCurrentRiCoverageFetcher(
                timer, t -> { }).fetch();

        assertThat(riCoverageData.size(), is(1));
        assertThat(riCoverageData.getRiCoveragePercentage(111), is(30F));
        // return -1 for an entity without ri coverage
        assertThat(riCoverageData.getRiCoveragePercentage(888), is(-1.0F));
    }

    /**
     * Test that projected RI Coverage are fetched correctly.
     */
    @Test
    public void testFetchProjectedRICoverage() {
        GetProjectedEntityReservedInstanceCoverageResponse response =
                GetProjectedEntityReservedInstanceCoverageResponse.newBuilder()
                        .putCoverageByEntityId(111L, EntityReservedInstanceCoverage.newBuilder()
                                .setEntityId(111L)
                                .setEntityCouponCapacity(10)
                                .putCouponsCoveredByRi(991, 1)
                                .putCouponsCoveredByRi(992, 2)
                                .build())
                        .build();

        doReturn(response).when(riServiceMole).getProjectedEntityReservedInstanceCoverageStats(
                any(GetProjectedEntityReservedInstanceCoverageRequest.class));

        final RICoverageData riCoverageData = riCoverageFetcherFactory.newProjectedRiCoverageFetcher(
                timer, t -> { }, 213).fetch();

        assertThat(riCoverageData.size(), is(1));
        assertThat(riCoverageData.getRiCoveragePercentage(111), is(30F));
        // return -1 for an entity without ri coverage
        assertThat(riCoverageData.getRiCoveragePercentage(888), is(-1.0F));
    }
}