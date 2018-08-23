package com.vmturbo.cost.component.pricing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.grpc.Status.Code;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTableRequest;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;

public class PricingRpcServiceTest {

    private static final String PROBE_TYPE = "foo";

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private PricingRpcService backend = new PricingRpcService(priceTableStore);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PricingServiceBlockingStub clientStub;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        clientStub = PricingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
    }

    @Test
    public void testUpload() {
        clientStub.uploadPriceTable(UploadPriceTableRequest.newBuilder()
                .setProbeType(PROBE_TYPE)
                .setPriceTable(PRICE_TABLE)
                .build());

        Mockito.verify(priceTableStore).putPriceTable(PROBE_TYPE, PRICE_TABLE);
    }

    @Test
    public void testUploadNoProbeType() {
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("probe type"));
        clientStub.uploadPriceTable(UploadPriceTableRequest.newBuilder()
                // Not setting probe type.
                .setPriceTable(PRICE_TABLE)
                .build());
    }

    @Test
    public void testUploadNoPriceTable() {
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("price table"));
        clientStub.uploadPriceTable(UploadPriceTableRequest.newBuilder()
                .setProbeType(PROBE_TYPE)
                // Not setting price table.
                .build());
    }

    @Test
    public void testGet() {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
        final GetPriceTableResponse response = clientStub.getPriceTable(
                GetPriceTableRequest.newBuilder()
                        .build());
        assertTrue(response.hasGlobalPriceTable());
        assertThat(response.getGlobalPriceTable(), is(priceTable));
    }
}
