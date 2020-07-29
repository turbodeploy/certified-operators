package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Collections;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.RemoveActionsAcceptancesAndRejectionsRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto;
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
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.EntitySettingStore;
import com.vmturbo.group.setting.EntitySettingStore.NoSettingsForTopologyException;
import com.vmturbo.group.setting.ISettingPolicyStore;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingPolicyFilter.Builder;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.platform.sdk.common.util.Pair;

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
    private final IdentityProvider identityProvider;
    private final GrpcTransactionUtil grpcTransactionUtil;

    private static final Set<String> IMMUTABLE_ACTION_SETTINGS = ImmutableSet.<String>builder()
            .add(EntitySettingSpecs.Move.getSettingName()).add(EntitySettingSpecs.StorageMove.getSettingName())
            .add(EntitySettingSpecs.Provision.getSettingName()).add(EntitySettingSpecs.Suspend.getSettingName())
            .add(EntitySettingSpecs.DisabledSuspend.getSettingName())
            .add(EntitySettingSpecs.DisabledProvision.getSettingName())
            .add(EntitySettingSpecs.Activate.getSettingName()).add(EntitySettingSpecs.Resize.getSettingName())
            .add(EntitySettingSpecs.Reconfigure.getSettingName()).build();

    /**
     * Constructs setting policy gRPC service.
     *
     * @param settingStore setting store to use
     * @param settingSpecStore setting spec store
     * @param entitySettingStore entity setting store
     * @param actionsServiceClient actions gRPC client
     * @param identityProvider identity provider
     * @param transactionProvider transaction provider to operate with DB objects
     * @param realtimeTopologyContextId realtimme topology context ID
     * @param entitySettingsResponseChunkSize entity setttings chunk size
     */
    public SettingPolicyRpcService(@Nonnull final SettingStore settingStore,
                                   @Nonnull final SettingSpecStore settingSpecStore,
                                   @Nonnull final EntitySettingStore entitySettingStore,
                                   @Nonnull final ActionsServiceBlockingStub actionsServiceClient,
                                   @Nonnull final IdentityProvider identityProvider,
                                   @Nonnull final TransactionProvider transactionProvider,
                                   final long realtimeTopologyContextId,
                                   final int entitySettingsResponseChunkSize) {
        this.settingStore = Objects.requireNonNull(settingStore);
        this.entitySettingStore = Objects.requireNonNull(entitySettingStore);
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.actionsServiceClient = Objects.requireNonNull(actionsServiceClient);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.entitySettingsResponseChunkSize = entitySettingsResponseChunkSize;
        this.grpcTransactionUtil = new GrpcTransactionUtil(transactionProvider, logger);
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

        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setInfo(request.getSettingPolicyInfo())
                .setId(identityProvider.next())
                .setSettingPolicyType(Type.USER)
                .build();
        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> createSettingPolicy(stores.getSettingPolicyStore(), policy,
                        responseObserver));
    }

    private void createSettingPolicy(@Nonnull ISettingPolicyStore settingPolicyStore,
            @Nonnull SettingPolicy policy,
            @Nonnull StreamObserver<CreateSettingPolicyResponse> responseObserver)
            throws StoreOperationException {
        settingPolicyStore.createSettingPolicies(Collections.singletonList(policy));
        responseObserver.onNext(
                CreateSettingPolicyResponse.newBuilder().setSettingPolicy(policy).build());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSettingPolicy(UpdateSettingPolicyRequest request,
            StreamObserver<UpdateSettingPolicyResponse> responseObserver) {
        if (!request.hasId() || !request.hasNewInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    "Update request must have ID and new setting policy info.").asException());
            return;
        }
        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> updateSettingPolicyInternal(request, responseObserver,
                        stores.getSettingPolicyStore()));
    }

    private void updateSettingPolicyInternal(UpdateSettingPolicyRequest request,
            StreamObserver<UpdateSettingPolicyResponse> responseObserver,
            ISettingPolicyStore settingPolicyStore) throws StoreOperationException {
        final Pair<SettingPolicy, Boolean> updateSettingPolicyPair =
                settingPolicyStore.updateSettingPolicy(request.getId(), request.getNewInfo());
        final SettingPolicy policy = updateSettingPolicyPair.getFirst();
        final Boolean shouldRemoveAcceptancesAndRejections = updateSettingPolicyPair.getSecond();
        if (shouldRemoveAcceptancesAndRejections) {
            removeAcceptancesAndRejectionsForAssociatedActions(policy.getId());
        }
        cancelAutomationActions(policy);
        responseObserver.onNext(
                UpdateSettingPolicyResponse.newBuilder().setSettingPolicy(policy).build());
        responseObserver.onCompleted();
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
                    settingStore.resetSettingPolicy(request.getSettingPolicyId()).getFirst();
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
        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> deleteSettingPolicy(request, responseObserver,
                        stores.getSettingPolicyStore()));
    }

    private void deleteSettingPolicy(@Nonnull DeleteSettingPolicyRequest request,
            @Nonnull StreamObserver<DeleteSettingPolicyResponse> responseObserver,
            @Nonnull ISettingPolicyStore settingPolicyStore) throws StoreOperationException {
        logger.info("Attempting to delete setting policy {}...", request.getId());
        final SettingPolicy policy = settingPolicyStore.getPolicy(request.getId())
                .orElseThrow(() -> new StoreOperationException(Status.NOT_FOUND,
                        "Policy not found by id " + request.getId()));
        settingPolicyStore.deletePolicies(Collections.singletonList(request.getId()), Type.USER);
        logger.info("Deleted setting policy: {}", request.getId());
        cancelAutomationActions(policy);
        removeAcceptancesAndRejectionsForAssociatedActions(policy.getId());
        responseObserver.onNext(DeleteSettingPolicyResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Remove acceptance and rejection for all actions associated with this policy.
     *
     * @param policyId the policy id
     */
    private void removeAcceptancesAndRejectionsForAssociatedActions(long policyId) {
        try {
            actionsServiceClient.removeActionsAcceptancesAndRejections(
                    RemoveActionsAcceptancesAndRejectionsRequest.newBuilder()
                            .setPolicyId(policyId)
                            .build());
        } catch (StatusRuntimeException e) {
            logger.warn("Failed to remove actions associate with policy {}", policyId, e);
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
        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> getSettingPolicy(request, responseObserver,
                        stores.getSettingPolicyStore()));
    }

    private void getSettingPolicy(@Nonnull GetSettingPolicyRequest request,
            @Nonnull StreamObserver<GetSettingPolicyResponse> responseObserver,
            @Nonnull ISettingPolicyStore settingPolicyStore) throws StoreOperationException {
        final Optional<SettingPolicy> foundPolicy;
        if (request.hasId()) {
            foundPolicy = settingPolicyStore.getPolicy(request.getId());
        } else {
            final Collection<SettingProto.SettingPolicy> policies = settingPolicyStore.getPolicies(
                    SettingPolicyFilter.newBuilder().withName(request.getName()).build());
            foundPolicy =
                    policies.isEmpty() ? Optional.empty() : Optional.of(policies.iterator().next());
        }
        final GetSettingPolicyResponse response = foundPolicy.map(settingPolicy -> {
            final GetSettingPolicyResponse.Builder respBuilder =
                    GetSettingPolicyResponse.newBuilder().setSettingPolicy(settingPolicy);
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

        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> listSettingPolicies(request, responseObserver,
                        stores.getSettingPolicyStore()));
    }

    private void listSettingPolicies(@Nonnull ListSettingPoliciesRequest request,
            @Nonnull StreamObserver<SettingPolicy> responseObserver,
            @Nonnull ISettingPolicyStore settingPolicyStore) throws StoreOperationException {
        final Collection<SettingPolicy> settingPolicies =
                settingPolicyStore.getPolicies(buildFilter(request));
        if (request.hasContextId() && request.getContextId() != realtimeTopologyContextId) {
            // we need to create new policies for plan, because plan has a different set of action settings
            // as default. we assume all actions are in automatic mode in plan.
            settingPolicies.forEach(p -> {
                if (p.hasInfo()) {
                    SettingPolicy.Builder newPolicyBuilder = p.toBuilder();
                    for (Setting.Builder setting : newPolicyBuilder.getInfoBuilder()
                            .getSettingsBuilderList()) {
                        if (IMMUTABLE_ACTION_SETTINGS.contains(setting.getSettingSpecName())) {
                            setting.setEnumSettingValue(EnumSettingValue.newBuilder()
                                    .setValue(ActionMode.AUTOMATIC.toString())
                                    .build()).build();
                        }
                    }
                    responseObserver.onNext(newPolicyBuilder.build());
                }
            });
        } else {
            settingPolicies.forEach(responseObserver::onNext);
        }
        responseObserver.onCompleted();
    }

    @Nonnull
    private SettingPolicyFilter buildFilter(@Nonnull ListSettingPoliciesRequest request) {
        final Builder filterBuilder = SettingPolicyFilter.newBuilder();
        if (request.hasTypeFilter()) {
            filterBuilder.withType(request.getTypeFilter());
        }
        request.getIdFilterList().forEach(filterBuilder::withId);
        request.getWorkflowIdList().forEach(filterBuilder::withWorkflowId);
        return filterBuilder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSettingPoliciesUsingSchedule(final GetSettingPoliciesUsingScheduleRequest request,
            final StreamObserver<SettingPolicy> responseObserver) {
        if (!request.hasScheduleId()) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("Request must have a schedule ID!")
                            .asException());
            return;
        }
        grpcTransactionUtil.executeOperation(responseObserver,
                stores -> getSettingPoliciesUsingSchedule(request, responseObserver,
                        stores.getSettingPolicyStore()));
    }

    private void getSettingPoliciesUsingSchedule(
            final GetSettingPoliciesUsingScheduleRequest request,
            final StreamObserver<SettingPolicy> responseObserver,
            @Nonnull ISettingPolicyStore settingPolicyStore) throws StoreOperationException {
        final Collection<SettingPolicy> policiesWithActivationSchedules =
                settingPolicyStore.getPolicies(SettingPolicyFilter.newBuilder()
                        .withActivationScheduleId(request.getScheduleId())
                        .build());
        final Collection<SettingPolicy> policiesWithExecutionSchedules =
                settingPolicyStore.getPolicies(SettingPolicyFilter.newBuilder()
                        .withExecutionScheduleId(request.getScheduleId())
                        .build());
        for (SettingPolicy policy : Iterables.concat(policiesWithActivationSchedules,
                policiesWithExecutionSchedules)) {
            responseObserver.onNext(policy);
        }
        responseObserver.onCompleted();
    }

    /**
     * Handles receiving and storing the entity settings.
     * @param responseObserver Let the topology processor know when all the entity settings have
     *                         been read or if an error occurred.
     * @return A stream of {@link UploadEntitySettingsRequest} containing chunks of the entity
     * settings request.
     */
    @Override
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
                try {
                    if (contextVal.getTopologyContextId() == realtimeTopologyContextId) {
                        entitySettingStore.storeEntitySettings(contextVal.getTopologyContextId(),
                                contextVal.getTopologyId(), allEntitySettings.stream());
                    } else {
                        // save allEntitySettings to database
                        entitySettingStore.savePlanEntitySettings(contextVal.getTopologyContextId(),
                                allEntitySettings);
                    }
                    responseObserver.onNext(UploadEntitySettingsResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } catch (StoreOperationException e) {
                    logger.error(
                            "Failed to process UploadEntitySettingsRequest request for context " +
                                    contextVal.getTopologyContextId() + " topology " +
                                    contextVal.getTopologyId(), e);
                    responseObserver.onError(
                            e.getStatus().withDescription(e.getMessage()).asException());
                }
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
                        .stream()
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
        } catch (StoreOperationException e) {
            logger.error("Failed to get entity settings for " + request, e);
            responseObserver.onError(e.getStatus().withDescription(e.getMessage()).asException());
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to load setting from plan setting database.", e);
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

        if (request.getEntityOidListList().isEmpty()) {
            responseStreamObserver.onNext(GetEntitySettingPoliciesResponse.getDefaultInstance());
            responseStreamObserver.onCompleted();
            return;
        }

        GetEntitySettingPoliciesResponse.Builder response = GetEntitySettingPoliciesResponse.newBuilder();
        Set<SettingPolicy> policies = new HashSet<>();
        try {
            entitySettingStore.getEntitySettingPolicies(request.getEntityOidListList()
                .stream().collect(Collectors.toSet()), request.getIncludeInactive())
                .forEach(p -> policies.add(p));
            response.addAllSettingPolicies(policies);
            responseStreamObserver.onNext(response.build());
            responseStreamObserver.onCompleted();
        } catch (StoreOperationException e) {
            logger.error("Failed processing entity setting policies request for " +
                request.getEntityOidListList(), e);
            responseStreamObserver.onError(
                e.getStatus().withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSettingPoliciesForGroup(final GetSettingPoliciesForGroupRequest request,
                                           final StreamObserver<GetSettingPoliciesForGroupResponse> responseStreamObserver) {

        Map<Long, Set<SettingPolicy>> groupIdToSettingPolicies =
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
