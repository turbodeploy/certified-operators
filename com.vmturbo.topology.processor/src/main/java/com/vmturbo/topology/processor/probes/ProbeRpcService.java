package com.vmturbo.topology.processor.probes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
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
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbeOrTarget;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyInfo;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyNameValuePair;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableResponse;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc.ProbeRpcServiceImplBase;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.probeproperties.KVBackedProbePropertyStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore.ProbePropertyKey;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Service for getting probe information and handling probe properties.
 */
public class ProbeRpcService extends ProbeRpcServiceImplBase {
    private final ProbePropertyStore probePropertyStore;

    /**
     * Construct the service.  Access to probes, targets, and a key/value store (for persistence) is
     * required.
     *
     * @param probeStore probe store.
     * @param targetStore target store.
     * @param keyValueStore persistence.
     */
    public ProbeRpcService(
            @Nonnull ProbeStore probeStore,
            @Nonnull TargetStore targetStore,
            @Nonnull KeyValueStore keyValueStore) {
        probePropertyStore =
            new KVBackedProbePropertyStore(
                Objects.requireNonNull(probeStore),
                Objects.requireNonNull(targetStore),
                Objects.requireNonNull(keyValueStore));
    }

    @Override
    public void getProbeInfo(
            @Nonnull GetProbeInfoRequest request,
            @Nonnull StreamObserver<GetProbeInfoResponse> response) {
        // TODO: implement and remove the corresponding implementation from the /probes REST endpoint (part of OM-40987)
        response.onNext(GetProbeInfoResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void getAllProbeProperties(
            @Nonnull GetAllProbePropertiesRequest request,
            @Nonnull StreamObserver<GetAllProbePropertiesResponse> response) {
        final GetAllProbePropertiesResponse.Builder
            responseBuilder = GetAllProbePropertiesResponse.newBuilder();

        probePropertyStore.getAllProbeProperties().forEach(
            e -> {
                final ProbeOrTarget.Builder probePropertyTableBuilder =
                    ProbeOrTarget.newBuilder().setProbeId(e.getKey().getProbeId());
                e.getKey().getTargetId().map(probePropertyTableBuilder::setTargetId);
                responseBuilder.addProbeProperties(
                    ProbePropertyInfo.newBuilder()
                        .setProbePropertyTable(probePropertyTableBuilder.build())
                        .setProbePropertyNameAndValue(
                            ProbePropertyNameValuePair.newBuilder()
                                .setName(e.getKey().getName())
                                .setValue(e.getValue()))
                        .build());
            });

        response.onNext(responseBuilder.build());
        response.onCompleted();
    }

    @Override
    public void getTableOfProbeProperties(
            @Nonnull GetTableOfProbePropertiesRequest request,
            @Nonnull StreamObserver<GetTableOfProbePropertiesResponse> response) {
        // TODO: implement OM-40407

        // read store
        final ProbeOrTarget table = request.getProbePropertyTable();
        final List<ProbePropertyNameValuePair>
            listOfNameValuePairs =
                commonProbePropertyErrorHandler(
                    response,
                    () -> {
                        if (table.hasTargetId()) {
                            return
                                probePropertyStore
                                    .getTargetSpecificProbeProperties(
                                        table.getProbeId(),
                                        table.getTargetId())
                                    .map(
                                        e ->
                                            ProbePropertyNameValuePair.newBuilder()
                                                .setName(e.getKey())
                                                .setValue(e.getValue())
                                                .build())
                                    .collect(Collectors.toList());
                        } else {
                            return
                                probePropertyStore.getProbeSpecificProbeProperties(table.getProbeId())
                                    .map(
                                        e ->
                                            ProbePropertyNameValuePair.newBuilder()
                                                .setName(e.getKey())
                                                .setValue(e.getValue())
                                                .build())
                                    .collect(Collectors.toList());
                        }
                    });
        if (listOfNameValuePairs == null) {
            return;
        }

        // build and return response
        response.onNext(
            GetTableOfProbePropertiesResponse.newBuilder()
                .addAllProbeProperties(listOfNameValuePairs)
                .build());
        response.onCompleted();
    }

    @Override
    public void getProbePropertyValue(
            @Nonnull GetProbePropertyValueRequest request,
            @Nonnull StreamObserver<GetProbePropertyValueResponse> response) {
        // TODO: implement OM-40407

        // read store
        final ProbeOrTarget table = request.getProbePropertyTable();
        final ProbePropertyKey key;
        if (table.hasTargetId()) {
            key = new ProbePropertyKey(table.getProbeId(), table.getTargetId(), request.getName());
        } else {
            key = new ProbePropertyKey(table.getProbeId(), request.getName());
        }
        final Optional<String> result =
            commonProbePropertyErrorHandler(response, () -> probePropertyStore.getProbeProperty(key));
        if (result == null) {
            return;
        }

        // build and return response
        final GetProbePropertyValueResponse.Builder
            responseBuilder = GetProbePropertyValueResponse.newBuilder();
        result.map(responseBuilder::setValue);
        response.onNext(responseBuilder.build());
        response.onCompleted();
    }

    @Override
    public void updateProbePropertyTable(
            @Nonnull UpdateProbePropertyTableRequest request,
            @Nonnull StreamObserver<UpdateProbePropertyTableResponse> response) {
        // TODO: implement OM-40407

        // read request
        final Map<String, String> newProperties = new HashMap<>();
        for (ProbePropertyNameValuePair nv : request.getNewProbePropertiesList()) {
            newProperties.put(nv.getName(), nv.getValue());
        }
        final ProbeOrTarget table = request.getProbePropertyTable();

        // update store
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    if (table.hasTargetId()) {
                        probePropertyStore.putAllTargetSpecificProperties(
                            table.getProbeId(),
                            table.getTargetId(),
                            newProperties);
                    } else {
                        probePropertyStore.putAllProbeSpecificProperties(table.getProbeId(), newProperties);
                    }
                    // dummy return value
                    return 0;
                });
        if (result == null) {
            return;
        }

        // send void response
        response.onNext(UpdateProbePropertyTableResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void updateOneProbeProperty(
            @Nonnull UpdateOneProbePropertyRequest request,
            @Nonnull StreamObserver<UpdateOneProbePropertyResponse> response) {
        // TODO: implement OM-40407

        // read request
        final ProbeOrTarget table = request.getNewProbeProperty().getProbePropertyTable();
        final ProbePropertyNameValuePair
            nameValue = request.getNewProbeProperty().getProbePropertyNameAndValue();
        final ProbePropertyKey key;
        if (table.hasTargetId()) {
            key = new ProbePropertyKey(table.getProbeId(), table.getTargetId(), nameValue.getName());
        } else {
            key = new ProbePropertyKey(table.getProbeId(), nameValue.getName());
        }

        // update store
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    probePropertyStore.putProbeProperty(key, nameValue.getValue());
                    // dummy return value
                    return 0;
                });
        if (result == null) {
            return;
        }

        // send void response
        response.onNext(UpdateOneProbePropertyResponse.newBuilder().build());
        response.onCompleted();
    }

    @Override
    public void deleteProbeProperty(
            @Nonnull DeleteProbePropertyRequest request,
            @Nonnull StreamObserver<DeleteProbePropertyResponse> response) {
        // TODO: implement OM-40407

        // read request
        final ProbeOrTarget table = request.getProbePropertyTable();
        final ProbePropertyKey key;
        if (table.hasTargetId()) {
            key = new ProbePropertyKey(table.getProbeId(), table.getTargetId(), request.getName());
        } else {
            key = new ProbePropertyKey(table.getProbeId(), request.getName());
        }

        // update store
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    probePropertyStore.deleteProbeProperty(key);
                    // dummy return value
                    return 0;
                });
        if (result == null) {
            return;
        }

        // send void response
        response.onNext(DeleteProbePropertyResponse.newBuilder().build());
        response.onCompleted();
    }

    private <T> T commonProbePropertyErrorHandler(
            @Nonnull StreamObserver<?> responseStream,
            @Nonnull ProbePropertyStoreCall<T> wrappedCode) {
        try {
            return wrappedCode.call();
        } catch(ProbeException | TargetNotFoundException e) {
            responseStream.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return null;
        } catch (TargetStoreException e) {
            responseStream.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return null;
        }
    }

    @FunctionalInterface
    private interface ProbePropertyStoreCall<T> {
        T call() throws ProbeException, TargetStoreException;
    }
}
