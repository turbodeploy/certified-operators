package com.vmturbo.group.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupResponse.GroupSettingPolicies;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesUsingScheduleRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.Context;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.api.SetOnce;
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

    private final long realtimeTopologyContextId;

    private final int entitySettingsResponseChunkSize;

    private static final Set<String> IMMUTABLE_ACTION_SETTINGS = ImmutableSet.<String>builder()
            .add(EntitySettingSpecs.Move.getSettingName()).add(EntitySettingSpecs.StorageMove.getSettingName())
            .add(EntitySettingSpecs.Provision.getSettingName()).add(EntitySettingSpecs.Suspend.getSettingName())
            .add(EntitySettingSpecs.Activate.getSettingName()).add(EntitySettingSpecs.Resize.getSettingName())
            .add(EntitySettingSpecs.Reconfigure.getSettingName()).build();

    public SettingPolicyRpcService(@Nonnull final SettingStore settingStore,
                                   @Nonnull final SettingSpecStore settingSpecStore,
                                   @Nonnull final EntitySettingStore entitySettingStore,
                                   @Nonnull final ActionsServiceBlockingStub actionsServiceClient,
                                   final long realtimeTopologyContextId,
                                   final int entitySettingsResponseChunkSize) {
        this.settingStore = Objects.requireNonNull(settingStore);
        this.entitySettingStore = Objects.requireNonNull(entitySettingStore);
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.actionsServiceClient = Objects.requireNonNull(actionsServiceClient);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.entitySettingsResponseChunkSize = entitySettingsResponseChunkSize;
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
        if (hasAutomationSetting(policy)) {
            try {
                actionsServiceClient.cancelQueuedActions(
                        CancelQueuedActionsRequest.getDefaultInstance());
            } catch (StatusRuntimeException e) {
                // Exception is fine as it is a best-effort call.
                logger.warn("Failed to cancel outstanding automation actions", e);
            }
        }
    }

    private boolean hasAutomationSetting(final SettingPolicy settingPolicy) {
        return (settingPolicy != null &&
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
        if (!request.getIdFilterList().isEmpty()) {
            request.getIdFilterList().forEach(filterBuilder::withId);
        }
        Stream<SettingPolicy> settingPolicies = settingStore.getSettingPolicies(filterBuilder.build());
        if (request.hasContextId() && request.getContextId() != realtimeTopologyContextId) {
            // we need to create new policies for plan, because plan has a different set of action settings
            // as default. we assume all actions are in automatic mode in plan.
            settingPolicies.forEach(p -> {
                if (p.hasInfo()) {
                    SettingPolicy.Builder newPolicyBuilder = p.toBuilder();
                    for (Setting.Builder setting : newPolicyBuilder.getInfoBuilder().getSettingsBuilderList()) {
                        if (IMMUTABLE_ACTION_SETTINGS.contains(setting.getSettingSpecName())) {
                            setting.setEnumSettingValue(EnumSettingValue.newBuilder()
                                    .setValue(ActionMode.AUTOMATIC.toString())
                                    .build())
                                    .build();
                        }
                    }
                    responseObserver.onNext(newPolicyBuilder.build());
                }
            });
            responseObserver.onCompleted();
        } else {
            settingPolicies.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSettingPoliciesUsingSchedule(final GetSettingPoliciesUsingScheduleRequest request,
                                                final StreamObserver<SettingPolicy> responseObserver) {
        if (!request.hasScheduleId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Request must have a schedule ID!").asException());
            return;
        }
        Stream<SettingPolicy> settingPolicies = settingStore.getSettingPoliciesUsingSchedule(
            request.getScheduleId());
        settingPolicies.forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    /**
     * Handles receiving and storing the entity settings.
     * @param responseObserver Let the topology processor know when all the entity settings have
     *                         been read or if an error occurred.
     * @return A stream of {@link UploadEntitySettingsRequest} containing chunks of the entity
     * settings request.
     */
    public StreamObserver<UploadEntitySettingsRequest> uploadEntitySettings(
        final StreamObserver<UploadEntitySettingsResponse> responseObserver) {
        return new EntitySettingsRequest(responseObserver);
    }

    /**
     * Get an EntitySettingsRequest that contains the logic to write chunks of the request in the
     * StreamObserver.
     **/
    private class EntitySettingsRequest implements StreamObserver<UploadEntitySettingsRequest> {

        private final StreamObserver<UploadEntitySettingsResponse> responseObserver;

        /**
         * All entity settings, across all chunks.
         */
        private final List<EntitySettings> allEntitySettings = new LinkedList<>();

        /**
         * The context message received as the first message in the upload request.
         */
        private final SetOnce<Context> context = new SetOnce<>();

        EntitySettingsRequest(StreamObserver<UploadEntitySettingsResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final UploadEntitySettingsRequest request) {
            switch (request.getTypeCase()) {
                case CONTEXT:
                    final Context reqContext = request.getContext();
                    if (!reqContext.hasTopologyId() || !reqContext.hasTopologyContextId()) {
                        logger.error("Missing topologyId {} or topologyContextId argument {}",
                            reqContext.hasTopologyId(), reqContext.hasTopologyContextId());
                        responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("Missing topologyId and/or topologyContextId argument!")
                            .asException());
                        return;
                    }
                    context.trySetValue(request.getContext());
                    break;
                case SETTINGS_CHUNK:
                    allEntitySettings.addAll(request.getSettingsChunk().getEntitySettingsList());
                    break;
                default:
                    final String error = "Unexpected request chunk: " + request.getTypeCase();
                    logger.error(error);
                    responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription(error)
                        .asException());
                    break;
            }
        }

        @Override
        public void onError(final Throwable t) {
            logger.error("Error in processing UploadEntitySettingsRequest ");
        }

        @Override
        public void onCompleted() {
            if (!context.getValue().isPresent()) {
                String error = "No valid context message received as part of the input stream.";
                logger.error(error);
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(error)
                    .asException());
            } else {
                final Context contextVal = context.getValue().get();
                logger.info("Completed uploading of {} entity settings in context: {}",
                    allEntitySettings.size(), contextVal);
                entitySettingStore.storeEntitySettings(contextVal.getTopologyContextId(),
                    contextVal.getTopologyId(), allEntitySettings.stream());
                responseObserver.onNext(UploadEntitySettingsResponse.newBuilder().build());
                responseObserver.onCompleted();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntitySettings(final GetEntitySettingsRequest request,
                                  final StreamObserver<GetEntitySettingsResponse> responseObserver) {
        try {
            final Map<Long, Collection<SettingToPolicyId>> results = entitySettingStore.getEntitySettings(
                    request.getTopologySelection(),
                    request.getSettingFilter());

            if (request.getIncludeSettingPolicies()) {
                final SettingPolicyFilter.Builder settingPolicyFilter = SettingPolicyFilter.newBuilder()
                    .withType(Type.USER)
                    .withType(Type.DISCOVERED)
                    .withType(Type.DEFAULT);

                results.values().stream()
                    .flatMap(Collection::stream)
                    .map(SettingToPolicyId::getSettingPolicyIdList)
                    .flatMap(Collection::stream)
                    .forEach(settingPolicyFilter::withId);

                final Map<Long, SettingPolicyId> settingPolicyById =
                    settingStore.getSettingPolicies(settingPolicyFilter.build())
                        .collect(Collectors.toMap(SettingPolicy::getId,
                            policy -> SettingPolicyId.newBuilder()
                                .setPolicyId(policy.getId())
                                .setDisplayName(policy.getInfo().getName())
                                .setType(policy.getSettingPolicyType())
                                .build()));

                final Map<SettingToPolicyId, Set<Long>> entitiesBySettingAndPolicy = new HashMap<>();
                results.forEach((entityId, settingsForEntity) -> {
                    settingsForEntity.forEach(settingToPolicyId -> {
                        final Set<Long> entitiesByPolicy = entitiesBySettingAndPolicy.computeIfAbsent(
                            settingToPolicyId, k -> new HashSet<>());
                        entitiesByPolicy.add(entityId);
                    });
                });

                Iterators.partition(entitiesBySettingAndPolicy.entrySet().iterator(), entitySettingsResponseChunkSize)
                    .forEachRemaining(settingGroupChunk -> {
                        final GetEntitySettingsResponse.Builder chunkResponse =
                            GetEntitySettingsResponse.newBuilder();
                        settingGroupChunk.forEach(settingGroup -> {
                            final List<Long> settingPolicyIdList = settingGroup.getKey().getSettingPolicyIdList();
                            final List<SettingPolicyId> identifiers = settingPolicyIdList.stream()
                                .filter(settingPolicyById::containsKey)
                                .map(settingPolicyById::get).collect(Collectors.toList());
                            if (!identifiers.isEmpty()) {
                                chunkResponse.addSettingGroup(EntitySettingGroup.newBuilder()
                                    .setSetting(settingGroup.getKey().getSetting())
                                    .addAllPolicyId(identifiers)
                                    .addAllEntityOids(settingGroup.getValue()));
                            }
                        });
                        responseObserver.onNext(chunkResponse.build());
                    });
            } else {
                final Map<Setting, Set<Long>> entitiesBySetting = new HashMap<>();
                results.forEach((entityId, settingsForEntity) -> {
                    settingsForEntity.forEach(settingToPolicyId -> {
                        final Set<Long> entitiesByPolicy = entitiesBySetting.computeIfAbsent(
                            settingToPolicyId.getSetting(), k -> new HashSet<>());
                        entitiesByPolicy.add(entityId);
                    });
                });

                Iterators.partition(entitiesBySetting.entrySet().iterator(), entitySettingsResponseChunkSize)
                    .forEachRemaining(settingGroupChunk -> {
                        final GetEntitySettingsResponse.Builder chunkResponse =
                            GetEntitySettingsResponse.newBuilder();
                        settingGroupChunk.forEach(settingAndEntities -> {
                            chunkResponse.addSettingGroup(EntitySettingGroup.newBuilder()
                                .setSetting(settingAndEntities.getKey())
                                .addAllEntityOids(settingAndEntities.getValue()));
                        });
                        responseObserver.onNext(chunkResponse.build());
                    });
            }

            responseObserver.onCompleted();
        } catch (NoSettingsForTopologyException e) {
            logger.error("Topology not found for entity settings request: {}", e.getMessage());
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntitySettingPolicies(final GetEntitySettingPoliciesRequest request,
                                         final StreamObserver<GetEntitySettingPoliciesResponse> responseStreamObserver) {

        if (!request.hasEntityOid()) {
            responseStreamObserver.onNext(GetEntitySettingPoliciesResponse.getDefaultInstance());
            responseStreamObserver.onCompleted();
            return;
        }

        GetEntitySettingPoliciesResponse.Builder response =
                GetEntitySettingPoliciesResponse.newBuilder();

        entitySettingStore.getEntitySettingPolicies(request.getEntityOid())
                .forEach(settingPolicy -> response.addSettingPolicies(settingPolicy));

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSettingPoliciesForGroup(final GetSettingPoliciesForGroupRequest request,
                                           final StreamObserver<GetSettingPoliciesForGroupResponse> responseStreamObserver) {

        Map<Long, List<SettingPolicy>> groupIdToSettingPolicies =
                settingStore.getSettingPoliciesForGroups(new HashSet<>(request.getGroupIdsList()));

        GetSettingPoliciesForGroupResponse.Builder response =
                GetSettingPoliciesForGroupResponse.newBuilder();

        groupIdToSettingPolicies.entrySet()
                .forEach(entry ->
                        response.putSettingPoliciesByGroupId(entry.getKey(),
                                GroupSettingPolicies.newBuilder()
                                        .addAllSettingPolicies(entry.getValue())
                                        .build()));

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }
}
