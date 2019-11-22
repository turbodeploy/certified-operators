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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore.ProbePropertyKey;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Service for getting probe information and handling probe properties.
 */
public class ProbeRpcService extends ProbeRpcServiceImplBase {
    private final ProbePropertyStore probePropertyStore;
    private final RemoteMediationServer mediationServer;
    private final Logger logger = LogManager.getLogger();

    /**
     * Construct the service.  Access to probes, targets, mediation,
     * and a key/value store (for persistence) is required.
     *
     * @param probePropertyStore probe/target property store
     * @param mediationServer mediation service.
     */
    public ProbeRpcService(@Nonnull ProbePropertyStore probePropertyStore,
            @Nonnull RemoteMediationServer mediationServer) {
        this.mediationServer = Objects.requireNonNull(mediationServer);
        this.probePropertyStore = probePropertyStore;
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
        // read request
        final Map<String, String> newProperties = new HashMap<>();
        for (ProbePropertyNameValuePair nv : request.getNewProbePropertiesList()) {
            newProperties.put(nv.getName(), nv.getValue());
        }
        final ProbeOrTarget table = request.getProbePropertyTable();

        // update store and send mediation message
        final long probeId = table.getProbeId();
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    // update store
                    if (table.hasTargetId()) {
                        probePropertyStore.putAllTargetSpecificProperties(
                            probeId,
                            table.getTargetId(),
                            newProperties);
                    } else {
                        probePropertyStore.putAllProbeSpecificProperties(probeId, newProperties);
                    }

                    // send mediation message
                    sendProbePropertyMediationMessageForProbe(probeId);

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
        // read request
        final ProbeOrTarget table = request.getNewProbeProperty().getProbePropertyTable();
        final ProbePropertyNameValuePair
            nameValue = request.getNewProbeProperty().getProbePropertyNameAndValue();
        final ProbePropertyKey key;
        final long probeId = table.getProbeId();
        if (table.hasTargetId()) {
            key = new ProbePropertyKey(probeId, table.getTargetId(), nameValue.getName());
        } else {
            key = new ProbePropertyKey(probeId, nameValue.getName());
        }

        // update store and send mediation message
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    // update store
                    probePropertyStore.putProbeProperty(key, nameValue.getValue());

                    // send mediation message
                    sendProbePropertyMediationMessageForProbe(probeId);

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
        // read request
        final ProbeOrTarget table = request.getProbePropertyTable();
        final long probeId = table.getProbeId();
        final ProbePropertyKey key;
        if (table.hasTargetId()) {
            key = new ProbePropertyKey(probeId, table.getTargetId(), request.getName());
        } else {
            key = new ProbePropertyKey(probeId, request.getName());
        }

        // update store and send mediation message
        final Integer result =
            commonProbePropertyErrorHandler(
                response,
                () -> {
                    // update store
                    probePropertyStore.deleteProbeProperty(key);

                    // send mediation message
                    sendProbePropertyMediationMessageForProbe(probeId);

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

    private void sendProbePropertyMediationMessageForProbe(long probeId)
            throws
                InterruptedException,
                CommunicationException,
                ProbeException,
                TargetStoreException {
        mediationServer.sendSetPropertiesRequest(
            probeId,
            probePropertyStore.buildSetPropertiesMessageForProbe(probeId));
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
        } catch (InterruptedException | CommunicationException e) {
            responseStream.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return null;
        }
    }

    @FunctionalInterface
    private interface ProbePropertyStoreCall<T> {
        T call() throws ProbeException, TargetStoreException, InterruptedException, CommunicationException;
    }
}
