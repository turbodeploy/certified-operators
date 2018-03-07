package com.vmturbo.topology.processor.probes;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceImplBase;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.SdkToProbeActionsConverter;

/**
 * Service for getting action capabilities of probes.
 */
public class ProbeActionCapabilitiesRpcService extends ProbeActionCapabilitiesServiceImplBase {

    /**
     * Message in case of Probe will not have capabilities with provided entityType
     */
    private static final String NO_SUCH_CAPABILITIES_MESSAGE =
            "There are no action capabilities for specified probeId={} and EntityType={}";

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    public ProbeActionCapabilitiesRpcService(@Nonnull ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    /**
     * Gets action Capabilities of probe with the certain id.
     *
     * @param request provides probeId and entity type. Entity type is optional
     * so if it is presented then Capabilities will be filtered by entityType.
     * @param responseObserver response with probe Capabilities
     */
    @Override
    public void getProbeActionCapabilities(@Nonnull GetProbeActionCapabilitiesRequest request,
            @Nonnull StreamObserver<GetProbeActionCapabilitiesResponse> responseObserver) {
        final long probeId = request.getProbeId();
        final Optional<ProbeInfo> probeInfo = probeStore.getProbe(probeId);
        if (!probeInfo.isPresent()) {
            String errorMessage = String.format("There is no probe with probeId=%s", probeId);
            logger.warn(errorMessage);
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription
                    (errorMessage)));
            responseObserver.onCompleted();
            return;
        }
        final List<ProbeActionCapability> actionCapabilities =
                getProbeActionCapabilities(request, probeInfo.get());
        if (actionCapabilities.isEmpty()) {
            logger.warn(NO_SUCH_CAPABILITIES_MESSAGE, probeId, request.getEntityType());
        }
        responseObserver.onNext(GetProbeActionCapabilitiesResponse.newBuilder()
                .addAllActionCapabilities(actionCapabilities).build());
        responseObserver.onCompleted();
    }

    /**
     * Gets action capabilities for all probes provided in request. If there is not probe with
     * provided probeId then capabilities for this probe will be empty.
     * @param request contains probeIds list to get capabilities
     * @param responseObserver contains action capabilities for each probe
     */
    @Override
    public void listProbeActionCapabilities(@Nonnull ListProbeActionCapabilitiesRequest request,
            StreamObserver<ProbeActionCapabilities> responseObserver) {
        final List<Long> probeIds = request.getProbeIdsList();
        for (long id : probeIds) {
            addActionCapabilitiesOfProbeToResponse(responseObserver, id);
        }
        responseObserver.onCompleted();
    }

    private void addActionCapabilitiesOfProbeToResponse(
            StreamObserver<ProbeActionCapabilities> responseObserver, long id) {
        final ProbeActionCapabilities.Builder responseBuilder = ProbeActionCapabilities
                .newBuilder().setProbeId(id);
        final Optional<ProbeInfo> probeInfo = probeStore.getProbe(id);
        if (!probeInfo.isPresent()) {
            responseObserver.onNext(responseBuilder.build());
        } else {
            final List<ProbeActionCapability> actionCapabilities = SdkToProbeActionsConverter
                    .convert(probeInfo.get().getActionPolicyList());
            responseObserver.onNext(
                    responseBuilder.addAllActionCapabilities(actionCapabilities).build());
        }
    }

    private Optional<ProbeInfo> getProbeInfo(@Nonnull StreamObserver responseObserver, long probeId) {
        final Optional<ProbeInfo> probeInfo = probeStore.getProbe(probeId);
        if (!probeInfo.isPresent()) {
            String errorMessage = String.format("There is no probe with probeId=%s", probeId);
            logger.warn(errorMessage);
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription
                    (errorMessage)));
            responseObserver.onCompleted();
        }
        return probeInfo;
    }

    /**
     * Returnes action capabilities of probe converted from sdk-format action policies.
     *
     * @param request request contains probeId and may contain entityType
     * @param probe ProbeInfo
     * @return
     */
    private List<ProbeActionCapability> getProbeActionCapabilities
            (GetProbeActionCapabilitiesRequest request, ProbeInfo probe) {
        return SdkToProbeActionsConverter.convert(getSdkFormatProbeActionCapabilities(request,
                probe.getActionPolicyList()));
    }

    /**
     * Returnes action policies of the probe with sdk-specified Action types.
     *
     * @param request current request
     * @param probePolicies Sdk-fromat action policies of requested probe
     * @return action policies of the probe with sdk-specified Action types
     */
    @Nonnull
    private List<ActionPolicyDTO> getSdkFormatProbeActionCapabilities(
            @Nonnull GetProbeActionCapabilitiesRequest request, @Nonnull List<ActionPolicyDTO> probePolicies) {
        return request.hasEntityType()
                ? getFilteredProbeCapabilities(request.getEntityType(), probePolicies)
                : probePolicies;
    }

    /**
     * Filters sdk-format action policies of the probe.
     *
     * @param entityType entity type for filtering
     * @param probeCapabilities probeCapabilities of the probe
     * @return filtered policies
     */
    @Nonnull
    private List<ActionPolicyDTO> getFilteredProbeCapabilities(int entityType,
            @Nonnull List<ActionPolicyDTO> probeCapabilities) {
        return probeCapabilities.stream()
                .filter(policy -> policy.getEntityType().getNumber() == entityType)
                .collect(Collectors.toList());
    }
}
