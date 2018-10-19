package com.vmturbo.cost.component.pricing;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesRequest;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;

@ThreadSafe
public class PricingRpcService extends PricingServiceImplBase {

    private final PriceTableStore priceTableStore;

    // track the hash of the current price table we are storing. Note that this needs to come from
    // the original upload request.
    private long lastConfirmedChecksum = 0;

    public PricingRpcService(@Nonnull final PriceTableStore priceTableStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
    }

    @Override
    public void getPriceTable(GetPriceTableRequest request,
                              StreamObserver<GetPriceTableResponse> responseObserver) {
        responseObserver.onNext(GetPriceTableResponse.newBuilder()
                .setGlobalPriceTable(priceTableStore.getMergedPriceTable())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPriceTableChecksum(final GetPriceTableChecksumRequest request, final StreamObserver<GetPriceTableChecksumResponse> responseObserver) {
        responseObserver.onNext(GetPriceTableChecksumResponse.newBuilder()
                .setPriceTableChecksum(lastConfirmedChecksum).build());
        responseObserver.onCompleted();;
    }

    @Override
    public void uploadPriceTables(UploadPriceTablesRequest request,
                                 StreamObserver<UploadPriceTablesResponse> responseObserver) {
        priceTableStore.putProbePriceTables(request.getProbePriceTablesMap());
        lastConfirmedChecksum = request.getChecksum();
        responseObserver.onNext(UploadPriceTablesResponse.newBuilder()
                .build());
        responseObserver.onCompleted();
    }
}
