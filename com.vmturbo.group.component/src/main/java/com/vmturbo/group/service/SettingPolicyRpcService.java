package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableSettingPolicyUpdateException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.setting.EntitySettingStore;
import com.vmturbo.group.setting.EntitySettingStore.NoSettingsForTopologyException;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;

/**
 * The SettingPolicyService provides RPC's for CRUD-type operations
 * related to SettingPolicy objects.
 */
public class SettingPolicyRpcService extends SettingPolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SettingStore settingStore;

    private final SettingSpecStore settingSpecStore;

    private final EntitySettingStore entitySettingStore;

    private final ActionsServiceBlockingStub actionsServiceClient;

    public SettingPolicyRpcService(@Nonnull final SettingStore settingStore,
                                   @Nonnull final SettingSpecStore settingSpecStore,
                                   @Nonnull final EntitySettingStore entitySettingStore,
                                   @Nonnull final ActionsServiceBlockingStub actionsServiceClient) {
        this.settingStore = Objects.requireNonNull(settingStore);
        this.entitySettingStore = Objects.requireNonNull(entitySettingStore);
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.actionsServiceClient = actionsServiceClient;
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
                    settingStore.createUserSettingPolicy(request.getSettingPolicyInfo());
            responseObserver.onNext(CreateSettingPolicyResponse.newBuilder()
                    .setSettingPolicy(policy)
                    .build());
            responseObserver.onCompleted();
        } catch (InvalidItemException e) {
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
            cancelAutomationActions(policy);
            responseObserver.onNext(UpdateSettingPolicyResponse.newBuilder()
                .setSettingPolicy(policy)
                .build());
            responseObserver.onCompleted();
        } catch (DuplicateNameException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage()).asException());
        } catch (InvalidItemException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (SettingPolicyNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void resetSettingPolicy(ResetSettingPolicyRequest request,
                                   StreamObserver<ResetSettingPolicyResponse> responseObserver) {
        if (!request.hasSettingPolicyId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Update request must have ID and new setting policy info.")
                    .asException());
            return;
        }

        try {
            final SettingPolicy policy =
                    settingStore.resetSettingPolicy(request.getSettingPolicyId());
            cancelAutomationActions(policy);
            responseObserver.onNext(ResetSettingPolicyResponse.newBuilder()
                    .setSettingPolicy(policy)
                    .build());
            responseObserver.onCompleted();
        } catch (SettingPolicyNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSettingPolicy(DeleteSettingPolicyRequest request,
                                    StreamObserver<DeleteSettingPolicyResponse> responseObserver) {
        if (!request.hasId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Delete request must have ID.")
                    .asException());
            return;
        }

        try {
            logger.info("Attempting to delete setting policy {}...", request.getId());
            SettingPolicy deletedSettingPolicy = settingStore.deleteUserSettingPolicy(request.getId());
            logger.info("Deleted setting policy: {}", request.getId());
            cancelAutomationActions(deletedSettingPolicy);
            responseObserver.onNext(DeleteSettingPolicyResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (SettingPolicyNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        } catch (ImmutableSettingPolicyUpdateException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * If user is changing Automation Settings, send message to ActionOrchestrator
     * to purge the outstanding actions which are in the execution queue.
     */
    private void cancelAutomationActions(SettingPolicy policy) {
        if (hasAutomationSetting(policy))
            try {
                actionsServiceClient.cancelQueuedActions(
                        CancelQueuedActionsRequest.getDefaultInstance());
            } catch (StatusRuntimeException e) {
                // Exception is fine as it is a best-effort call.
                logger.warn("Failed to cancel outstanding automation actions", e);
            }
    }

    private boolean hasAutomationSetting(final SettingPolicy settingPolicy) {
        return (settingPolicy!=null &&
                settingPolicy.getInfo().getSettingsList().stream()
                        .anyMatch(setting ->
                                EntitySettingSpecs.isAutomationSetting(
                                        setting.getSettingSpecName())));
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
        return settingPolicy.getInfo().getSettingsList().stream().map(Setting::getSettingSpecName)
                .map(name -> {
                    Optional<SettingSpec> specOpt = settingSpecStore.getSettingSpec(name);
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

        entitySettingStore.storeEntitySettings(request.getTopologyContextId(),
                request.getTopologyId(), request.getEntitySettingsList().stream());

        responseObserver.onNext(UploadEntitySettingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntitySettings(final GetEntitySettingsRequest request,
                          final StreamObserver<GetEntitySettingsResponse> responseObserver) {
        try {
            final Map<Long, Collection<Setting>> results = entitySettingStore.getEntitySettings(
                    request.getTopologySelection(),
                    request.getSettingFilter());

            final GetEntitySettingsResponse.Builder respBuilder =
                    GetEntitySettingsResponse.newBuilder();
            results.forEach((oid, settings) -> respBuilder.addSettings(
                SettingsForEntity.newBuilder()
                    .setEntityId(oid)
                    .addAllSettings(settings)
                    .build()));

            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (NoSettingsForTopologyException e) {
            logger.error("Topology not found for entity settings request: {}", e.getMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getMessage()).asException());
        }
    }
}
