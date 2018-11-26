package com.vmturbo.topology.processor.probes;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableResponse;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc.ProbeRpcServiceImplBase;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Service for getting probe information and handling probe properties.
 * It is currently a placeholder for the implementations of OM-40405 (persistence of probe properties)
 * and OM-40407 (communication of probe properties to the mediation clients).
 */
public class ProbeRpcService extends ProbeRpcServiceImplBase {
    private final ProbeStore probeStore;
    private final KeyValueStore keyValueStore;

    private final Logger logger = LogManager.getLogger(ProbeRpcService.class);

    public ProbeRpcService(@Nonnull ProbeStore probeStore, @Nonnull KeyValueStore keyValueStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
    }

    @Override
    public void getProbeInfo(
            @Nonnull GetProbeInfoRequest request,
            @Nonnull StreamObserver<GetProbeInfoResponse> response) {
        logger.info("*** getProbeInfo was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(GetProbeInfoResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void getAllProbeProperties(
            @Nonnull GetAllProbePropertiesRequest request,
            @Nonnull StreamObserver<GetAllProbePropertiesResponse> response) {
        logger.info("*** getAllProbeProperties was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(GetAllProbePropertiesResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void getTableOfProbeProperties(
            @Nonnull GetTableOfProbePropertiesRequest request,
            @Nonnull StreamObserver<GetTableOfProbePropertiesResponse> response) {
        logger.info("*** getTableOfProbeProperties was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(GetTableOfProbePropertiesResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void getProbePropertyValue(
            @Nonnull GetProbePropertyValueRequest request,
            @Nonnull StreamObserver<GetProbePropertyValueResponse> response) {
        logger.info("*** getProbePropertyValue was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(GetProbePropertyValueResponse.newBuilder().setValue("sample").build());
        response.onCompleted();
    }

    @Override
    public void updateProbePropertyTable(
            @Nonnull UpdateProbePropertyTableRequest request,
            @Nonnull StreamObserver<UpdateProbePropertyTableResponse> response) {
        logger.info("*** updateProbePropertyTable was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(UpdateProbePropertyTableResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void updateOneProbeProperty(
            @Nonnull UpdateOneProbePropertyRequest request,
            @Nonnull StreamObserver<UpdateOneProbePropertyResponse> response) {
        logger.info("*** updateOneProbeProperty was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(UpdateOneProbePropertyResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void deleteProbeProperty(
            @Nonnull DeleteProbePropertyRequest request,
            @Nonnull StreamObserver<DeleteProbePropertyResponse> response) {
        logger.info("*** deleteOneProbeProperty was called with request:\n" + request.toString());
        // TODO: implement OM-40405 & OM-40407
        response.onNext(DeleteProbePropertyResponse.newBuilder().build());
        response.onCompleted();
    }
}
