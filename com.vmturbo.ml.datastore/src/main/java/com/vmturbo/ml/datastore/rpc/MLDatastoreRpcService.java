package com.vmturbo.ml.datastore.rpc;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.CommodityTypeWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetCommodityTypeWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetMetricTypeWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetCommodityTypeWhitelistResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetMetricTypeWhitelistResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastoreServiceGrpc.MLDatastoreServiceImplBase;
import com.vmturbo.ml.datastore.influx.MetricsStoreWhitelist;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * RPC service for configuring the machine-learning data store at runtime.
 */
public class MLDatastoreRpcService extends MLDatastoreServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final MetricsStoreWhitelist metricsStoreWhitelist;

    public MLDatastoreRpcService(@Nonnull final MetricsStoreWhitelist metricsStoreWhitelist) {
        this.metricsStoreWhitelist = Objects.requireNonNull(metricsStoreWhitelist);
    }


    @Override
    public void getCommodityTypeWhitelist(GetCommodityTypeWhitelistRequest request,
                                          StreamObserver<CommodityTypeWhitelist> responseObserver) {
        final CommodityTypeWhitelist.Builder whitelistBuilder = CommodityTypeWhitelist.newBuilder();
        metricsStoreWhitelist.getWhitelistCommodityTypes().stream()
            .map(Object::toString)
            .forEach(whitelistBuilder::addCommodityTypeNames);

        responseObserver.onNext(whitelistBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setCommodityTypeWhitelist(CommodityTypeWhitelist whitelist,
                                          StreamObserver<SetCommodityTypeWhitelistResponse> responseObserver) {
        final Set<CommodityType> commodityTypes = new HashSet<>();
        for (String commodityName : whitelist.getCommodityTypeNamesList()) {
            try {
                commodityTypes.add(CommodityType.valueOf(commodityName));
            } catch (IllegalArgumentException e) {
                responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("Illegal commodity type: " + commodityName)
                        .asException());
                return;
            }
        }

        metricsStoreWhitelist.setWhitelistCommodityTypes(commodityTypes);
        responseObserver.onNext(SetCommodityTypeWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMetricTypeWhitelist(GetMetricTypeWhitelistRequest request,
                                       StreamObserver<MetricTypeWhitelist> responseObserver) {
        final MetricTypeWhitelist.Builder whitelistBuilder = MetricTypeWhitelist.newBuilder();
        metricsStoreWhitelist.getWhitelistMetricTypes()
            .forEach(whitelistBuilder::addMetricTypes);

        responseObserver.onNext(whitelistBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setMetricTypeWhitelist(MetricTypeWhitelist whitelist,
                                       StreamObserver<SetMetricTypeWhitelistResponse> responseObserver) {
        metricsStoreWhitelist.setWhitelistMetricTypes(new HashSet<>(whitelist.getMetricTypesList()));
        responseObserver.onNext(SetMetricTypeWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
