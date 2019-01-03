package com.vmturbo.ml.datastore.rpc;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ClusterSupport;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.CommodityTypeWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetClusterSupportRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetCommodityTypeWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetMetricTypeWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetActionTypeWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.GetActionStateWhitelistRequest;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetClusterSupportResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetCommodityTypeWhitelistResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetMetricTypeWhitelistResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetActionTypeWhitelistResponse;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.SetActionStateWhitelistResponse;
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
        metricsStoreWhitelist.getWhitelist(MetricsStoreWhitelist.COMMODITY_TYPE).stream()
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

        metricsStoreWhitelist.storeWhitelist(commodityTypes, MetricsStoreWhitelist.COMMODITY_TYPE);
        responseObserver.onNext(SetCommodityTypeWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMetricTypeWhitelist(GetMetricTypeWhitelistRequest request,
                                       StreamObserver<MetricTypeWhitelist> responseObserver) {
        final MetricTypeWhitelist.Builder whitelistBuilder = MetricTypeWhitelist.newBuilder();
        metricsStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE)
            .forEach(whitelistBuilder::addMetricTypes);

        responseObserver.onNext(whitelistBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setMetricTypeWhitelist(MetricTypeWhitelist whitelist,
                                       StreamObserver<SetMetricTypeWhitelistResponse> responseObserver) {
        metricsStoreWhitelist.storeWhitelist(new HashSet<>(whitelist.getMetricTypesList()), MetricsStoreWhitelist.METRIC_TYPE);
        responseObserver.onNext(SetMetricTypeWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClusterSupport(GetClusterSupportRequest request,
                                  StreamObserver<ClusterSupport> responseObserver) {
        responseObserver.onNext(ClusterSupport.newBuilder()
            .setWriterClusterMemberships(metricsStoreWhitelist.getClusterSupport())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void setClusterSupport(ClusterSupport clusterSupport,
                                  StreamObserver<SetClusterSupportResponse> responseObserver) {
        metricsStoreWhitelist.setClusterSupport(clusterSupport.getWriterClusterMemberships());
        responseObserver.onNext(SetClusterSupportResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void setActionTypeWhitelist(ActionTypeWhitelist whitelist,
                                          StreamObserver<SetActionTypeWhitelistResponse> responseObserver) {
        metricsStoreWhitelist.storeWhitelist(new HashSet<>(whitelist.getActionTypesList()), MetricsStoreWhitelist.ACTION_TYPE);
        responseObserver.onNext(SetActionTypeWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getActionTypeWhitelist(GetActionTypeWhitelistRequest request,
                                       StreamObserver<ActionTypeWhitelist> responseObserver) {
        final ActionTypeWhitelist.Builder whitelistBuilder = ActionTypeWhitelist.newBuilder();
        metricsStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE)
                .forEach(whitelistBuilder::addActionTypes);

        responseObserver.onNext(whitelistBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getActionStateWhitelist(GetActionStateWhitelistRequest request,
                                        StreamObserver<ActionStateWhitelist> responseObserver) {
        final ActionStateWhitelist.Builder whitelistBuilder = ActionStateWhitelist.newBuilder();
        metricsStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE)
                .forEach(whitelistBuilder::addActionStates);

        responseObserver.onNext(whitelistBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setActionStateWhitelist(ActionStateWhitelist whitelist,
                                       StreamObserver<SetActionStateWhitelistResponse> responseObserver) {
        metricsStoreWhitelist.storeWhitelist(new HashSet<>(whitelist.getActionStatesList()), MetricsStoreWhitelist.ACTION_STATE);
        responseObserver.onNext(SetActionStateWhitelistResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
