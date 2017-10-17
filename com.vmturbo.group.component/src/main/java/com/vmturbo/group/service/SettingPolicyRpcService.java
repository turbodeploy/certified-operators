package com.vmturbo.group.service;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.group.persistent.DuplicateNameException;
import com.vmturbo.group.persistent.InvalidSettingPolicyException;
import com.vmturbo.group.persistent.SettingPolicyFilter;
import com.vmturbo.group.persistent.SettingPolicyNotFoundException;
import com.vmturbo.group.persistent.SettingStore;

/**
 * The SettingPolicyService provides RPC's for CRUD-type operations
 * related to SettingPolicy objects.
 */
public class SettingPolicyRpcService extends SettingPolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SettingStore settingStore;

    public SettingPolicyRpcService(@Nonnull final SettingStore settingStore) {
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSettingPolicy(CreateSettingPolicyRequest request,
                                    StreamObserver<CreateSettingPolicyResponse> responseObserver) {
        if (!request.hasSettingPolicyInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing setting policy!").asException());
            return;
        }

        try {
            final SettingPolicy policy =
                    settingStore.createSettingPolicy(request.getSettingPolicyInfo());
            responseObserver.onNext(CreateSettingPolicyResponse.newBuilder()
                    .setSettingPolicy(policy)
                    .build());
            responseObserver.onCompleted();
        } catch (InvalidSettingPolicyException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
        } catch (DuplicateNameException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSettingPolicy(UpdateSettingPolicyRequest request,
                                    StreamObserver<UpdateSettingPolicyResponse> responseObserver) {
        if (!request.hasId() || !request.hasNewInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Update request must have ID and new setting policy info.")
                    .asException());
            return;
        }

        try {
            final SettingPolicy policy =
                    settingStore.updateSettingPolicy(request.getId(), request.getNewInfo());
            responseObserver.onNext(UpdateSettingPolicyResponse.newBuilder()
                .setSettingPolicy(policy)
                .build());
            responseObserver.onCompleted();
        } catch (DuplicateNameException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage()).asException());
        } catch (InvalidSettingPolicyException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (SettingPolicyNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSettingPolicy(GetSettingPolicyRequest request,
                                 StreamObserver<GetSettingPolicyResponse> responseObserver) {
        if (!request.hasId() && !request.hasName()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Request must have one of ID and Name!").asException());
            return;
        }

        final Optional<SettingPolicy> foundPolicy = request.hasId() ?
                settingStore.getSettingPolicy(request.getId()) :
                settingStore.getSettingPolicy(request.getName());

        final GetSettingPolicyResponse response = foundPolicy.map(settingPolicy -> {
            final GetSettingPolicyResponse.Builder respBuilder = GetSettingPolicyResponse.newBuilder()
                    .setSettingPolicy(settingPolicy);
            if (request.getIncludeSettingSpecs()) {
                getSpecsForPolicy(settingPolicy).forEach(respBuilder::addSettingSpecs);
            }
            return respBuilder.build();
        }).orElse(GetSettingPolicyResponse.getDefaultInstance());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Get the {@link SettingSpec}s associated with settings in a {@link SettingPolicy}.
     * Any settings that no longer refer to valid specs will be ignored.
     *
     * @param settingPolicy The setting policy.
     * @return A stream of {@link SettingSpec}s.
     */
    private Stream<SettingSpec> getSpecsForPolicy(@Nonnull final SettingPolicy settingPolicy) {
        return settingPolicy.getInfo().getSettingsList().stream()
                .map(Setting::getSettingSpecName)
                .map(name -> {
                    Optional<SettingSpec> specOpt = settingStore.getSettingSpec(name);
                    if (!specOpt.isPresent()) {
                        logger.warn("Setting {} from setting policy ID: {} Name: {} does not" +
                                        " exist. Did it get deleted?", name, settingPolicy.getId(),
                                settingPolicy.getInfo().getName());
                    }
                    return specOpt;
                })
                .filter(Optional::isPresent).map(Optional::get);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listSettingPolicies(ListSettingPoliciesRequest request,
                                    StreamObserver<SettingPolicy> responseObserver) {
        final SettingPolicyFilter.Builder filterBuilder = SettingPolicyFilter.newBuilder();
        if (request.hasTypeFilter()) {
            filterBuilder.withType(request.getTypeFilter());
        }
        settingStore.getSettingPolicies(filterBuilder.build()).forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void uploadEntitySettings(final UploadEntitySettingsRequest request,
            final StreamObserver<UploadEntitySettingsResponse> responseObserver) {

        if (!request.hasTopologyId() || !request.hasTopologyContextId()) {
            logger.error("Missing topologId {} or topologyContexId argument {}",
                request.hasTopologyId(), request.hasTopologyContextId());
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing topologyId and/or topologyContexId argument!")
                .asException());
            return;
        }

        if (request.getEntitySettingsCount() > 0) {
            // TODO : karthikt - OM-25472. Store the settings
        }
        responseObserver.onNext(UploadEntitySettingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
