package com.vmturbo.cost.component.pricing;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTableResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;

@ThreadSafe
public class PricingRpcService extends PricingServiceImplBase {

    private final PriceTableStore priceTableStore;

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
    public void uploadPriceTable(UploadPriceTableRequest request,
                                 StreamObserver<UploadPriceTableResponse> responseObserver) {
        if (!request.hasProbeType() || !request.hasPriceTable()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Upload must have probe type and a price table.")
                .asException());
            return;
        }

        priceTableStore.putPriceTable(request.getProbeType(), request.getPriceTable());
        responseObserver.onNext(UploadPriceTableResponse.newBuilder()
                .build());
        responseObserver.onCompleted();
    }
}
