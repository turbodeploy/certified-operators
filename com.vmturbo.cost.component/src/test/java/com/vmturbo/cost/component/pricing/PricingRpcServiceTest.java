package com.vmturbo.cost.component.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChecksum;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableHeader;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbeRISpecPriceChunk;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;

/**
 * Test class for the PricingRpc Service.
 */
public class PricingRpcServiceTest {

    private static final String PROBE_TYPE = "foo";

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private static final PriceTableKey PRICE_TABLE_KEY = PriceTableKey.newBuilder()
            .putProbeKeyMaterial("enrollmentId", "123")
            .putProbeKeyMaterial("offerId", "234")
            .setProbeType("aws").build();

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);

    private PricingRpcService backend = new PricingRpcService(priceTableStore, riSpecStore);

    /**
     * Create a grpc instance of the pricing rpc service.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PricingServiceBlockingStub pricingServiceBlockingStub;

    private PricingServiceStub pricingServiceStub;

    /**
     * An instance of the expected expection.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Setup for the test class.
     */
    @Before
    public void setup() {
        pricingServiceBlockingStub = PricingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        pricingServiceStub = PricingServiceGrpc.newStub(grpcTestServer.getChannel());

    }

    /**
     * Test the getter for the price table.
     */
    @Test
    public void testGet() {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
        final GetPriceTableResponse response = pricingServiceBlockingStub.getPriceTable(
                GetPriceTableRequest.newBuilder()
                        .build());
        assertTrue(response.hasGlobalPriceTable());
        assertThat(response.getGlobalPriceTable(), is(priceTable));
    }

    /**
     * Test the price table checksum.
     */
    @Test
    public void testGetPriceTableChecksum() {
        final PriceTableKey priceTableKey = PRICE_TABLE_KEY;
        Map<PriceTableKey, Long> test = new HashMap<>();
        test.put(priceTableKey, 99999L);
        when(priceTableStore.getChecksumByPriceTableKeys(anyList())).thenReturn(test);
        GetPriceTableChecksumResponse getPriceTableChecksumResponse =
                pricingServiceBlockingStub.getPriceTableChecksum(
                        GetPriceTableChecksumRequest.newBuilder().addPriceTableKey(PRICE_TABLE_KEY)
                                .build());
        assertEquals(getPriceTableChecksumResponse
                .getPricetableToChecksum(0).getPriceTableKey(), priceTableKey);
    }

    /**
     * Testing the update of price tables.
     *
     * @throws InterruptedException an Interrupted Exception
     */
    @Test
    public void testUpdatePriceTables() throws InterruptedException {
        ProbePriceTableSegment probePriceTableSegmentHeader = ProbePriceTableSegment.newBuilder()
                .setHeader(ProbePriceTableHeader.newBuilder()
                        .setCreatedTime(111L)
                        .addPriceTableChecksums(PriceTableChecksum.newBuilder()
                                .setPriceTableKey(PRICE_TABLE_KEY).setCheckSum(999L).build()).build()).build();

        ProbePriceTableSegment probePriceTableSegmentPriceTable = ProbePriceTableSegment.newBuilder()
                .setProbePriceTable(ProbePriceTableChunk.newBuilder()
                        .setPriceTableKey(PRICE_TABLE_KEY).build()).build();

        ProbePriceTableSegment probePriceTableSegmentRI = ProbePriceTableSegment.newBuilder().setProbeRiSpecPrices(ProbeRISpecPriceChunk.newBuilder()
                .setPriceTableKey(PRICE_TABLE_KEY).build()).build();

        final List<ProbePriceTableSegment> requests = Arrays.asList(probePriceTableSegmentHeader, probePriceTableSegmentRI, probePriceTableSegmentPriceTable);
        final CountDownLatch completedLatch = new CountDownLatch(1);
        final CountDownLatch onNextLatch = new CountDownLatch(requests.size());

        StreamObserver<UploadPriceTablesResponse> responseObserver = new StreamObserver<UploadPriceTablesResponse>() {
            public void onCompleted() {
                completedLatch.countDown();
            }

            public void onError(Throwable ex) {
            }

            public void onNext(final UploadPriceTablesResponse uploadPriceTablesResponse) {
                onNextLatch.countDown();
            }
        };
        final StreamObserver<ProbePriceTableSegment> requestStream = pricingServiceStub.updatePriceTables(responseObserver);
        requests.forEach(requestStream::onNext);
        requestStream.onCompleted();
        completedLatch.await(30, TimeUnit.SECONDS);
        verify(priceTableStore).putProbePriceTables(any());
    }
}
