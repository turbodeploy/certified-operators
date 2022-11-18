package com.vmturbo.topology.processor.operation;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.exception.DataAccessException;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeaturesRequiredException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DuplicationErrorType;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportResponse;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportResponse.PlanExportResponseState;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionAuditRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionListRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionListResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest.Builder;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionUpdateStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportResult;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.RequestTargetId;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.common.util.SetOnce;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ActionRecordNotFoundException;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumperImpl;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionExecutionState;
import com.vmturbo.topology.processor.operation.action.ActionList;
import com.vmturbo.topology.processor.operation.action.ActionListMessageHandler;
import com.vmturbo.topology.processor.operation.action.ActionListMessageHandler.ActionListOperationCallback;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler.ActionOperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApprovalMessageHandler;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateStateMessageHandler;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionStateMessageHandler;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAuditMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.planexport.PlanExport;
import com.vmturbo.topology.processor.operation.planexport.PlanExportMessageHandler;
import com.vmturbo.topology.processor.operation.planexport.PlanExportMessageHandler.PlanExportOperationCallback;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.operation.validation.ValidationResult;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;
import com.vmturbo.topology.processor.workflow.WorkflowExecutionResult;

/**
 * Responsible for managing all operations.
 * Any individual target may only have a single ongoing operation of a particular type
 * associated with it.
 * TODO (roman, June 16 2016): Should we allow multiple operations of different types?
 */
@ThreadSafe
public class OperationManager implements ProbeStoreListener, TargetStoreListener,
        IOperationManager, AutoCloseable {

    private static final Logger logger = LogManager.getLogger(OperationManager.class);

    // Mapping from OperationID -> Ongoing Operations
    private final ConcurrentMap<Long, OperationMessageHandler<?, ?>> ongoingOperations =
            new ConcurrentHashMap<>();

    // Mapping from TargetID -> Target operation context (validation/discovery/discoveryContext/...)
    protected final ConcurrentMap<Long, TargetOperationContext> targetOperationContexts = new ConcurrentHashMap<>();

    // Control number of concurrent target discoveries per probe per discovery type.
    // Mapping from ProbeId -> Semaphore
    private final ConcurrentMap<Long, Map<DiscoveryType, Semaphore>> probeOperationPermits = new ConcurrentHashMap<>();

    protected final TargetStore targetStore;

    protected final ProbeStore probeStore;

    protected final RemoteMediation remoteMediationServer;

    protected final IdentityProvider identityProvider;

    private final List<OperationListener> operationListeners = new CopyOnWriteArrayList<>();

    private final EntityStore entityStore;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DerivedTargetParser derivedTargetParser;

    protected final GroupScopeResolver groupScopeResolver;

    private final TargetDumpingSettings targetDumpingSettings;

    private DiscoveryDumper discoveryDumper = null;
    private BinaryDiscoveryDumper binaryDiscoveryDumper;

    private final SystemNotificationProducer systemNotificationProducer;

    protected final long discoveryTimeoutMs;

    protected final long validationTimeoutMs;

    protected final long actionTimeoutMs;

    protected final long planExportTimeoutMs;

    /**
     *  Executor service for handling async responses from the probe.
     */
    private final ExecutorService resultExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("result-handler")
                            .build());
    /**
     *  Executor service for sending async requests to the probe.
     *  This executor is only used for activating pending discoveries not for
     *  the discoveries triggered via the Scheduler.
     */
    private final ExecutorService discoveryExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("start-discovery")
                            .build());

    private final EntityActionDao entityActionDao;

    /**
     * Map that contain an action id with the respective affected entities. This is used only for
     * controllable actions, other actions should not have an entry in this map.
     */
    private final HashMap<Long, Set<Long>> actionToAffectedEntities = new HashMap<>();

    private static final ImmutableSet<ActionItemDTO.ActionType> ACTION_TYPES_AFFECTING_ENTITIES
            = ImmutableSet.of(ActionItemDTO.ActionType.MOVE, ActionItemDTO.ActionType.CHANGE,
            ActionItemDTO.ActionType.CROSS_TARGET_MOVE, ActionItemDTO.ActionType.MOVE_TOGETHER,
            ActionItemDTO.ActionType.START, ActionType.RIGHT_SIZE, ActionType.RESIZE, ActionType.SCALE);

    protected static final DataMetricGauge ONGOING_OPERATION_GAUGE = DataMetricGauge.builder()
        .withName("tp_ongoing_operation_total")
        .withHelp("Total number of ongoing operations in the topology processor.")
        .withLabelNames("type")
        .build()
        .register();

    protected static final DataMetricSummary DISCOVERY_SIZE_SUMMARY = DataMetricSummary.builder()
        .withName("tp_discovery_size_entities")
        .withHelp("The number of service entities in a discovery.")
        .withLabelNames("target_type")
        .build()
        .register();

    protected static final DataMetricHistogram DISCOVERY_TIMES = DataMetricHistogram.builder()
                    .withName("tp_discovery_time_seconds")
                    .withHelp("Total time of discoveries. Currently FULL and INCREMENTAL discovery types are supported.")
                    .withLabelNames("target_type", "discovery_type", "is_successful")
		    .withBuckets(new double[]{60.0, 120.0, 180.0, 300.0, 480.0, 600.0})
                    .build()
                    .register();

    protected static final DataMetricHistogram DISCOVERY_WAITING_TIMES = DataMetricHistogram.builder()
                    .withName("tp_discovery_wait_time_seconds")
                    .withHelp("Total time that discovery waits for permit.")
                    .withLabelNames("target_type", "discovery_type")
		    .withBuckets(new double[]{1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0})
                    .build()
                    .register();

    private DiscoveredWorkflowUploader discoveredWorkflowUploader;

    private DiscoveredCloudCostUploader discoveredCloudCostUploader;

    private final BilledCloudCostUploader billedCloudCostUploader;

    private DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader;

    private final int maxConcurrentTargetDiscoveriesPerProbeCount;

    private final int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;

    private final Random random = new Random();

    private final boolean enableDiscoveryResponsesCaching;

    private final LicenseCheckClient licenseCheckClient;

    private final int workflowExecutionTimeoutMillis;

    /**
     * Timeout for acquiring the permit for running a discovering operation
     * on a target.
     */
    protected final int probeDiscoveryPermitWaitTimeoutMins;

    /**
     * Total Permit timeout = probeDiscoveryPermitWaitTimeoutMins +
     *      rand(0, probeDiscoveryPermitWaitTimeoutIntervalMins).
     */
    private final int probeDiscoveryPermitWaitTimeoutIntervalMins;

    private final MatrixInterface matrix;

    public OperationManager(@Nonnull final IdentityProvider identityProvider,
                            @Nonnull final TargetStore targetStore,
                            @Nonnull final ProbeStore probeStore,
                            @Nonnull final RemoteMediation remoteMediationServer,
                            @Nonnull final OperationListener operationListener,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
                            @Nonnull final DiscoveredWorkflowUploader discoveredWorkflowUploader,
                            @Nonnull final DiscoveredCloudCostUploader discoveredCloudCostUploader,
                            @Nonnull final BilledCloudCostUploader billedCloudCostUploader,
                            @Nonnull final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader,
                            @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                            @Nonnull final EntityActionDao entityActionDao,
                            @Nonnull final DerivedTargetParser derivedTargetParser,
                            @Nonnull final GroupScopeResolver groupScopeResolver,
                            @Nonnull final TargetDumpingSettings targetDumpingSettings,
                            @Nonnull final SystemNotificationProducer systemNotificationProducer,
                            final long discoveryTimeoutSeconds,
                            final long validationTimeoutSeconds,
                            final long actionTimeoutSeconds,
                            final long planExportTimeoutSeconds,
                            final int maxConcurrentTargetDiscoveriesPerProbeCount,
                            final int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount,
                            final int probeDiscoveryPermitWaitTimeoutMins,
                            final int probeDiscoveryPermitWaitTimeoutIntervalMins,
                            final @Nonnull MatrixInterface matrix,
                            final BinaryDiscoveryDumper binaryDiscoveryDumper,
                            final boolean enableDiscoveryResponsesCaching,
                            final LicenseCheckClient licenseCheckClient,
                            final int workflowExecutionTimeoutMillis) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.remoteMediationServer = Objects.requireNonNull(remoteMediationServer);
        this.operationListeners.add(operationListener);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        this.discoveredWorkflowUploader = Objects.requireNonNull(discoveredWorkflowUploader);
        this.discoveredPlanDestinationUploader = Objects.requireNonNull(discoveredPlanDestinationUploader);
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
        this.derivedTargetParser = Objects.requireNonNull(derivedTargetParser);
        this.groupScopeResolver = Objects.requireNonNull(groupScopeResolver);
        this.systemNotificationProducer = Objects.requireNonNull(systemNotificationProducer);
        this.discoveredTemplateDeploymentProfileNotifier = Objects.requireNonNull(discoveredTemplateDeploymentProfileNotifier);
        this.discoveryTimeoutMs = TimeUnit.MILLISECONDS.convert(discoveryTimeoutSeconds, TimeUnit.SECONDS);
        this.validationTimeoutMs = TimeUnit.MILLISECONDS.convert(validationTimeoutSeconds, TimeUnit.SECONDS);
        this.actionTimeoutMs = TimeUnit.MILLISECONDS.convert(actionTimeoutSeconds, TimeUnit.SECONDS);
        this.planExportTimeoutMs = TimeUnit.MILLISECONDS.convert(planExportTimeoutSeconds, TimeUnit.SECONDS);
        this.maxConcurrentTargetDiscoveriesPerProbeCount = maxConcurrentTargetDiscoveriesPerProbeCount;
        this.maxConcurrentTargetIncrementalDiscoveriesPerProbeCount = maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;
        this.discoveredCloudCostUploader = discoveredCloudCostUploader;
        this.billedCloudCostUploader = Objects.requireNonNull(billedCloudCostUploader);
        this.probeDiscoveryPermitWaitTimeoutMins = probeDiscoveryPermitWaitTimeoutMins;
        this.probeDiscoveryPermitWaitTimeoutIntervalMins = probeDiscoveryPermitWaitTimeoutIntervalMins;
        this.targetDumpingSettings = targetDumpingSettings;
        this.enableDiscoveryResponsesCaching = enableDiscoveryResponsesCaching;
        this.binaryDiscoveryDumper = binaryDiscoveryDumper;
        try {
            this.discoveryDumper = new DiscoveryDumperImpl(DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY, targetDumpingSettings);

        } catch (IOException e) {
            logger.warn("Failed to initialized discovery dumper; discovery responses will not be dumped", e);
            this.discoveryDumper = null;
        }
        this.probeStore.addListener(this);
        this.targetStore.addListener(this);
        this.matrix = matrix;
        // On restart we notify any listeners that all previously existing operations are now
        // cleared.
        operationListener.notifyOperationsCleared();
        this.licenseCheckClient = licenseCheckClient;
        this.workflowExecutionTimeoutMillis = workflowExecutionTimeoutMillis;
    }

    @Override
    public void setTargetStatusTracker(@Nonnull TargetStatusTracker targetStatusTracker) {
        this.operationListeners.add(targetStatusTracker);
        this.targetStore.addListener(targetStatusTracker);
    }

    @Override
    public synchronized Action requestActions(
            @Nonnull final ActionOperationRequest request,
            final long targetId,
            @Nullable Long secondaryTargetId)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
        final String probeType = getProbeTypeWithCheck(target);

        final ActionExecutionDTO actionDto = request.getActionExecutionDTO();
        final Action action = new Action(actionDto.getActionOid(), actionDto.getActionStableId(),
                target.getProbeId(), targetId, identityProvider,
                actionDto.getActionType());

        final ActionOperationCallback callback = new ActionOperationCallback() {
            @Override
            public void onActionProgress(@Nonnull ActionProgress actionProgress) {
                notifyActionProgress(action, actionProgress);
            }

            @Override
            public void onSuccess(@Nonnull ActionResult response) {
                notifyActionResult(action, response);
            }

            @Override
            public void onFailure(@Nonnull String error) {
                final ActionResult result = ActionResult.newBuilder().setResponse(
                        ActionResponse.newBuilder()
                                .setActionResponseState(ActionResponseState.FAILED)
                                .setProgress(0)
                                .setResponseDescription(error)).build();
                notifyActionResult(action, result);
            }
        };
        final Builder actionRequestBuilder = ActionRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                .setActionExecutionDTO(actionDto);
        // If a secondary target is defined, at it to the ActionRequest
        if (secondaryTargetId != null) {
            // This action requires interaction with a second target.
            // Secondary account values must be set.
            final Target secondaryTarget = targetStore.getTarget(secondaryTargetId)
                    .orElseThrow(() -> new TargetNotFoundException(secondaryTargetId));
            actionRequestBuilder.addAllSecondaryAccountValue(
                    secondaryTarget.getMediationAccountVals(groupScopeResolver));
        }
        final ActionRequest sdkRequest = actionRequestBuilder.build();
        final ActionMessageHandler messageHandler = new ActionMessageHandler(
                action,
                remoteMediationServer.getMessageHandlerExpirationClock(),
                actionTimeoutMs, callback);

        // Update the ENTITY_ACTION table in preparation for executing the action
        insertControllableAndSuspendableState(actionDto.getActionOid(), actionDto.getActionType(),
                request.getControlAffectedEntities());

        logger.info("Sending action {} execution request to probe", actionDto.getActionOid());
        remoteMediationServer.sendActionRequest(target, sdkRequest, messageHandler);

        logger.info("Beginning {}", action);
        logger.debug("Action execution DTO:\n" + request.getActionExecutionDTO());
        operationStart(messageHandler);
        return action;
    }

    @Override
    public synchronized ActionList requestActions(
            @Nonnull List<ActionOperationRequest> requestList,
            long targetId,
            @Nullable Long secondaryTargetId)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        // Add primary and secondary target account values.
        final Target target = getTarget(targetId);
        final String probeType = getProbeTypeWithCheck(target);
        final ActionListRequest.Builder actionListRequestBuilder = ActionListRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver));
        // If a secondary target is defined, add it to the ActionListRequest
        if (secondaryTargetId != null) {
            // These actions requires interaction with a second target.
            // Secondary account values must be set.
            final Target secondaryTarget = getTarget(secondaryTargetId);
            actionListRequestBuilder.addAllSecondaryAccountValue(
                    secondaryTarget.getMediationAccountVals(groupScopeResolver));
        }

        final Map<Long, ActionExecutionState> actionMap = new HashMap<>();
        for (final ActionOperationRequest request : requestList) {
            final ActionExecutionDTO actionDto = request.getActionExecutionDTO();
            actionListRequestBuilder.addActionExecution(actionDto);
            final long actionOid = actionDto.getActionOid();
            final ActionType actionType = actionDto.getActionType();
            actionMap.put(actionOid, new ActionExecutionState(actionOid, actionType));

            // Update the ENTITY_ACTION table in preparation for executing the action
            insertControllableAndSuspendableState(actionOid, actionType,
                    request.getControlAffectedEntities());
        }

        final ActionList actionList = new ActionList(actionMap, target.getProbeId(),
                targetId, identityProvider);

        final ActionListOperationCallback callback = new ActionListOperationCallback() {
            @Override
            public void onActionProgress(@Nonnull ActionProgress actionProgress) {
                notifyActionListProgress(actionList, actionProgress);
            }

            @Override
            public void onSuccess(@Nonnull ActionListResponse response) {
                notifyActionListResult(actionList, response);
            }

            @Override
            public void onFailure(@Nonnull String error) {
                final ActionListResponse result = ActionListResponse.newBuilder()
                        .addAllResponse(Collections.nCopies(requestList.size(),
                                ActionResponse.newBuilder()
                                        .setActionResponseState(ActionResponseState.FAILED)
                                        .setProgress(0)
                                        .setResponseDescription(error)
                                        .build()))
                        .build();
                notifyActionListResult(actionList, result);
            }
        };
        final ActionListMessageHandler messageHandler = new ActionListMessageHandler(
                actionList,
                remoteMediationServer.getMessageHandlerExpirationClock(),
                actionTimeoutMs, callback);

        logger.info("Sending action list execution request to probe: {}",
                actionList.getActionIdsString());

        final ActionListRequest sdkRequest = actionListRequestBuilder.build();
        remoteMediationServer.sendActionListRequest(target, sdkRequest, messageHandler);

        logger.info("Beginning {}", actionList);
        if (logger.isDebugEnabled()) {
            for (final ActionOperationRequest request : requestList) {
                logger.debug("Action execution DTO:\n" + request.getActionExecutionDTO());
            }
        }

        operationStart(messageHandler);

        return actionList;
    }

    private Target getTarget(long targetId) throws TargetNotFoundException {
        return targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
    }

    /**
     * Request execution of a workflow without affecting action state.
     *
     * @param actionExecutionDTO The input action execution DTO containing the workflow details.
     * @param targetId The unique target ID.
     *
     * return WorkflowExecutionResult The workflow execution result.
     *
     * @throws ProbeException If an error occurs when connecting to the probe.
     * @throws TargetNotFoundException If the target is not found.
     * @throws CommunicationException If a communication error occurs.
     * @throws InterruptedException If the current thread is interrupted.
     */
    @Override
    public WorkflowExecutionResult requestWorkflow(ActionExecutionDTO actionExecutionDTO, final long targetId)
        throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final SetOnce<WorkflowExecutionResult> result = SetOnce.create();

        final String workflowID = actionExecutionDTO.getWorkflow().getId();
        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
        final String probeType = getProbeTypeWithCheck(target);

        final CountDownLatch latch = new CountDownLatch(1);

        final ActionOperationCallback callback = new ActionOperationCallback() {
            @Override
            public void onActionProgress(@Nonnull ActionProgress actionProgress) {
                logger.debug("Execution of workflow with {} ID is in progress", workflowID);
            }

            @Override
            public void onSuccess(@Nonnull ActionResult response) {
                final ActionResponseState actionResponseState =
                        response.getResponse().getActionResponseState();
                final Boolean isSucceeded;
                switch (actionResponseState) {
                    case SUCCEEDED:
                        logger.debug("Execution of workflow with {} ID was successful", workflowID);
                        isSucceeded = Boolean.TRUE;
                        break;
                    case FAILED:
                        logger.debug("Execution of workflow with {} ID was failed", workflowID);
                        isSucceeded = Boolean.FALSE;
                        break;
                    default:
                        logger.debug("{} unexpected state for completed action execution operation."
                                        + "Treat execution of workflow with {} ID was failed",
                                actionResponseState, workflowID);
                        isSucceeded = Boolean.FALSE;
                        break;
                }
                result.trySetValue(
                        new WorkflowExecutionResult(
                                isSucceeded, response.getResponse().getResponseDescription()));
                latch.countDown();
            }

            @Override
            public void onFailure(@Nonnull String error) {
                result.trySetValue(new WorkflowExecutionResult(
                        Boolean.FALSE, "Workflow execution has failed: " + error));
                logger.debug("Execution of workflow with {} ID has failed", workflowID);
                latch.countDown();
            }
        };

        final ActionRequest request  = ActionRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                .setActionExecutionDTO(actionExecutionDTO)
                .build();

        final Action action = new Action(0, 0, target.getProbeId(),
                targetId, identityProvider,
                ActionType.NONE);

        final ActionMessageHandler messageHandler = new ActionMessageHandler(
                action,
                remoteMediationServer.getMessageHandlerExpirationClock(),
                actionTimeoutMs, callback);

        logger.info("Sending workflow execution request to probe; workfowID = {}", workflowID);
        remoteMediationServer.sendActionRequest(target, request, messageHandler);

        latch.await(workflowExecutionTimeoutMillis, TimeUnit.MILLISECONDS);

        logger.debug("Action execution DTO after trying out the workflow with {} ID is: \n {}", workflowID,
                request.getActionExecutionDTO().toString());

        return result.getValue().orElse(new WorkflowExecutionResult(
                Boolean.FALSE, "Workflow execution has timed out after "
                + workflowExecutionTimeoutMillis + "msec"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized PlanExport exportPlan(@Nonnull PlanExportDTO planData,
                                              @Nonnull NonMarketEntityDTO planDestination,
                                              final long destinationOid,
                                              final long targetId)
        throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Target target = targetStore.getTarget(targetId)
            .orElseThrow(() -> new TargetNotFoundException(targetId));

        final PlanExport export = new PlanExport(destinationOid, target.getProbeId(),
            targetId, identityProvider);
        final PlanExportOperationCallback callback = new PlanExportOperationCallback() {
            @Override
            public void onPlanExportProgress(@NotNull final PlanExportProgress progress) {
                notifyPlanExportProgress(export, progress);
            }

            @Override
            public void onSuccess(@NotNull final PlanExportResult response) {
                notifyPlanExportResult(export, response);
            }

            @Override
            public void onFailure(@Nonnull String error) {
                final PlanExportResult result = PlanExportResult.newBuilder().setResponse(
                    PlanExportResponse.newBuilder()
                        .setState(PlanExportResponseState.FAILED)
                        .setDescription(error)
                        .setProgress(export.getProgress())
                        .build()).build();
                notifyPlanExportResult(export, result);
            }
        };
        final PlanExportRequest request = PlanExportRequest.newBuilder()
            .setPlanData(planData)
            .setPlanDestination(planDestination)
            .setTarget(createTargetId(target))
            .build();
        final PlanExportMessageHandler messageHandler = new PlanExportMessageHandler(
            export,
            remoteMediationServer.getMessageHandlerExpirationClock(),
            planExportTimeoutMs, callback);

        logger.info("Sending plan export request to destination {} (oid {}) to probe",
            planDestination.getId(), destinationOid);
        remoteMediationServer.sendPlanExportRequest(target, request, messageHandler);

        logger.info("Beginning {}", export);
        operationStart(messageHandler);

        return export;
    }

    /**
     * Request a validation on a target. There may be only a single ongoing validation
     * at a time for a given target. Attempting a validation for a target with a current
     * ongoing validation will return the current ongoing validation.
     *
     * Validations may take a long time and are performed asynchronously. This method throws
     * exceptions if the validation can't initiate. If a problem
     * occurs after the validation is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * @param targetId The id of the target to validate.
     * @return The {@link Validation} requested for the given target. If there was no ongoing validation
     *         for the target with the same type, a new one will be created. If there was an ongoing validation
     *         for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Override
    @Nonnull
    public synchronized Validation startValidation(final long targetId)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Optional<Validation> currentValidation = getInProgressValidationForTarget(targetId);
        if (currentValidation.isPresent()) {
            return currentValidation.get();
        }

        final TargetOperationContext targetOperationContext =
            targetOperationContexts.computeIfAbsent(targetId, k -> new TargetOperationContext());

        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
        final String probeType = getProbeTypeWithCheck(target);

        final Validation validation = new Validation(target.getProbeId(),
                target.getId(), identityProvider);

        // Check the license before validation starts
        try {
            ProbeLicense.create(target.getProbeInfo().getLicense())
                            .ifPresent(licenseCheckClient::checkFeatureAvailable);
        } catch (LicenseFeaturesRequiredException e) {
            logger.error("Failed to validate license for target {}", target.getId(), e);

            final ErrorDTO.Builder errorBuilder = ErrorDTO.newBuilder()
                            .setSeverity(ErrorSeverity.CRITICAL);
            errorBuilder.setDescription(e.getLocalizedMessage());

            operationComplete(validation, false, Collections.singletonList(errorBuilder.build()));
            targetOperationContext.operationCompleted(validation);
            targetOperationContext.setCurrentValidation(validation);
            return validation;
        }

        final OperationCallback<ValidationResponse> callback = new ValidationOperationCallback(
                validation);
        final ValidationRequest validationRequest = ValidationRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver)).build();
        final ValidationMessageHandler validationMessageHandler =
                new ValidationMessageHandler(
                        validation,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        validationTimeoutMs,
                        callback);
        remoteMediationServer.sendValidationRequest(target,
                validationRequest,
                validationMessageHandler);

        operationStart(validationMessageHandler);
        targetOperationContext.setCurrentValidation(validation);
        logger.info("Beginning {}", validation);
        return validation;
    }

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long, DiscoveryType,
     * boolean)}, with the following exceptions:
     * 1. If a discovery is already in progress, instead of returning the existing discovery,
     *    a pending discovery will be added for the target.
     * 2. If the probe associated with the target is not currently connected, a pending discovery
     *    will be added for the target.
     *
     * When a target's discovery completes or its probe registers, the pending discovery will
     * be removed and a new discovery will be initiated for the associated target.
     *
     * @param targetId The id of the target to discover.
     * @return An {@link Optional<Discovery>}. If there was no in progress discovery
     *         for the target and the target's probe is connected, a new discovery will be initiated
     *         and this new Discovery operation will be returned.
     *         If there was an in progress discovery for the target or the target's probe is disconnected,
     *         returns {@link Optional#empty()}.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Override
    @Nonnull
    public Optional<Discovery> addPendingDiscovery(long targetId, DiscoveryType discoveryType)
            throws TargetNotFoundException, CommunicationException, InterruptedException {
        final TargetOperationContext targetOperationContext =
            targetOperationContexts.computeIfAbsent(targetId, k -> new TargetOperationContext());
        // try to queue pending discovery, if there is discovery of same type in progress, then do
        // not trigger discovery, but mark it as pending
        if (targetOperationContext.tryQueuePendingDiscovery(discoveryType)) {
            return Optional.empty();
        }

        try {
            // if no discovery in progress, then start discovery immediately
            return startDiscovery(targetId, discoveryType, false);
        } catch (ProbeException e) {
            targetOperationContext.onProbeDisconnected(discoveryType);
            return Optional.empty();
        }
    }

    /**
     * Request a discovery on a target. There may be only a single ongoing discovery
     * at a time for a given target. Attempting a discovery for a target with a current
     * ongoing discovery will return the current ongoing discovery.
     *
     * Discoveries may take a long time and are performed asynchronously. This method
     * throws exceptions if the discovery can't initiate. If a problem
     * occurs after the discovery is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * Note: It is best to avoid holding unnecessary locks when calling this method, as it may
     *       block for an extended period while waiting for a probe operation permit.
     *
     * @param targetId The id of the target to discover.
     * @return The {@link Discovery} requested for the given target. If there was no ongoing discovery
     *         for the target with the same type, a new one will be created. If there was an ongoing discovery
     *         for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    @Override
    public Optional<Discovery> startDiscovery(final long targetId, DiscoveryType discoveryType,
            boolean runNow)
            throws TargetNotFoundException, ProbeException, CommunicationException,
            InterruptedException {

        // Discoveries are triggered 3 ways:
        //  a) Through the scheduler at scheduled intervals
        //  b) Initiated by the user from the UI/API.
        //  c) When the probe registers, it activates any pending discoveries.
        //
        // (a) and (b) directly call startDiscovery. (c) calls startDiscovery
        // via the discoveryExecutor.
        //
        // We use permits to control the number of concurrent target discoveries in-order
        // to limit the resource usage(specially memory) on the probe.
        //
        // The concurrency should be based on the resource i.e. the actual
        // process manifestation of the probe(e.g. whether probe is running
        // inside a container or a vm or as a regular OS process). Until we have the
        // resourceIdentity concept, we will keep using the probeId as the level of concurrency.

        final Discovery discovery;
        final long probeId;
        final DiscoveryRequest discoveryRequest;
        final DiscoveryMessageHandler discoveryMessageHandler;
        final Target target;
        final String probeType;
        synchronized (this) {
            logger.info("Starting discovery for target: {} ({})", targetId, discoveryType);
            final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId, discoveryType);
            if (currentDiscovery.isPresent()) {
                logger.info("Returning existing discovery for target: {} ({})", targetId, discoveryType);
                return currentDiscovery;
            }

            target = targetStore.getTarget(targetId)
                    .orElseThrow(() -> new TargetNotFoundException(targetId));
            probeType = getProbeTypeWithCheck(target);

            probeId = target.getProbeId();
            discovery = new Discovery(probeId, target.getId(), discoveryType, identityProvider);

            // Check the license before discovery starts
            try {
                ProbeLicense.create(target.getProbeInfo().getLicense())
                                .ifPresent(licenseCheckClient::checkFeatureAvailable);
            } catch (LicenseFeaturesRequiredException e) {
                final ErrorDTO.Builder errorBuilder = ErrorDTO.newBuilder()
                                .setSeverity(ErrorSeverity.CRITICAL);
                errorBuilder.setDescription(e.getLocalizedMessage());

                operationComplete(discovery, false, Collections.singletonList(errorBuilder.build()));

                final TargetOperationContext targetOperationContext =
                                targetOperationContexts.computeIfAbsent(targetId, k -> new TargetOperationContext());

                targetOperationContext.operationCompleted(discovery);
                targetOperationContext.setCurrentDiscovery(discoveryType, discovery);
                return Optional.of(discovery);
            }

            discoveryRequest = DiscoveryRequest.newBuilder()
                .setProbeType(probeType)
                .setDiscoveryType(discoveryType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                .setDiscoveryContext(Optional.ofNullable(targetOperationContexts.get(targetId))
                    .map(TargetOperationContext::getCurrentDiscoveryContext)
                    .orElse(DiscoveryContextDTO.getDefaultInstance()))
                .build();
        }

        // If the probe has not yet registered, the semaphore won't be initialized.
        final Optional<Semaphore> semaphore = Optional.ofNullable(probeOperationPermits.get(probeId))
            .map(semaphoreByDiscoveryType -> semaphoreByDiscoveryType.get(discoveryType));
        logger.info("Number of permits before acquire: {}, queueLength: {} by targetId: {}({}) ({})",
            () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
            () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
            target::getDisplayName,
            () -> targetId,
            () -> discoveryType);

        try (DataMetricTimer waitingTimer = DISCOVERY_WAITING_TIMES.labels(probeType, discoveryType.toString()).startTimer()) {
            if (semaphore.isPresent()) {
                // Threads will be blocked if there are not enough permits.
                // If due to some bug, the permits are never released, there will be a deadlock and
                // any new discoveries will be blocked. To get out of such situations, we add a safety
                // measure of timing out the acquisition of the semaphore. After the timeout, there
                // will be an implicit release. We add a random delay to prevent thundering herd
                // effect.
                // Acquire timeout doesn't necessarily mean that there is a deadlock. It can also be
                // the case that all the concurrent requests didn't finish within the timeout. But
                // since this case is less likely, it is ok to assume that the acquire timeout is
                // due to a deadlock.
                // todo: should we have different timeout for incremental discovery?
                final long waitTimeout = probeDiscoveryPermitWaitTimeoutMins + random.nextInt(probeDiscoveryPermitWaitTimeoutIntervalMins);
                logger.info("Set permit acquire timeout to: {} for target: {}({}) ({})",
                            waitTimeout,
                            target.getDisplayName(),
                            targetId,
                            discoveryType);
                boolean gotPermit = semaphore.get().tryAcquire(1, waitTimeout, TimeUnit.MINUTES);
                if (!gotPermit) {
                    logger.warn("Permit acquire timeout of: {} {} exceeded for targetId: {} ({})."
                                + " Continuing with discovery",
                                waitTimeout,
                                TimeUnit.MINUTES,
                                targetId,
                                discoveryType);
                }
            }
            logger.info("Number of permits after acquire: {}, queueLength: {} by target: {}({}) ({}). Took {} seconds.",
                        () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                        () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                        target::getDisplayName,
                        () -> targetId,
                        () -> discoveryType,
                        waitingTimer::getTimeElapsedSecs);
        }

        synchronized (this) {
            try {
                // check again if there was a discovery triggered for this target by another thread
                // between the executions of the 1st and the 2nd synchronized blocks.
                final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId, discoveryType);
                if (currentDiscovery.isPresent()) {
                    logger.info("Discovery is in progress. Returning existing discovery for target: {} ({})",
                        targetId, discoveryType);
                    semaphore.ifPresent(Semaphore::release);
                    logger.info("Number of permits after release: {}, queueLength: {} by target: {}({}) ({})",
                            () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                            () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                            () -> target.getDisplayName(),
                            () -> targetId,
                            () -> discoveryType);
                    return currentDiscovery;
                }
                final OperationCallback<DiscoveryResponse> callback =
                        new DiscoveryOperationCallback(discovery);
                discoveryMessageHandler =
                    new DiscoveryMessageHandler(
                        discovery,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        discoveryTimeoutMs,
                            callback);
                operationStart(discoveryMessageHandler);
                targetOperationContexts.computeIfAbsent(targetId, k -> new TargetOperationContext())
                    .setCurrentDiscovery(discoveryType, discovery);
                // associate the mediation message id with the Discovery object which will be used
                // while processing discovery responses
                final int messageId = remoteMediationServer.sendDiscoveryRequest(target,
                    discoveryRequest, discoveryMessageHandler);
                discovery.setMediationMessageId(messageId);
            } catch (Exception ex) {
                if (semaphore.isPresent()) {
                    semaphore.get().release();
                    logger.warn("Releasing permit on exception for targetId: {} ({})" +
                            " After release permits: {}, queueLength: {}, exception:{}",
                        targetId, discoveryType, semaphore.map(Semaphore::availablePermits).orElse(-1),
                        semaphore.map(Semaphore::getQueueLength).orElse(-1), ex.toString());
                }

                final ErrorDTO.Builder errorBuilder = ErrorDTO.newBuilder()
                    .setSeverity(ErrorSeverity.CRITICAL);
                if (ex.getLocalizedMessage() != null) {
                    errorBuilder.setDescription(ex.getLocalizedMessage());
                } else {
                    errorBuilder.setDescription(ex.getClass().getSimpleName());
                }
                operationComplete(discovery, false, Collections.singletonList(errorBuilder.build()));
                throw ex;
            }
        }

        logger.info("Beginning {}", discovery);
        return Optional.of(discovery);
    }

    @Override
    @Nonnull
    public Optional<Discovery> getInProgressDiscovery(final long id) {
        return getInProgress(id, Discovery.class);
    }

    @Override
    @Nonnull
    public Optional<Discovery> getInProgressDiscoveryForTarget(final long targetId, DiscoveryType discoveryType) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .flatMap(targetOperationContext -> targetOperationContext.getInProgressDiscovery(discoveryType));
    }

    @Override
    @Nonnull
    public Optional<Discovery> getLastDiscoveryForTarget(final long targetId, DiscoveryType discoveryType) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .map(targetOperationContext -> targetOperationContext.getLastCompletedDiscovery(discoveryType));
    }

    @Override
    @Nonnull
    public List<Discovery> getInProgressDiscoveries() {
        return getAllInProgress(Discovery.class);
    }

    @Override
    @Nonnull
    public Optional<Validation> getInProgressValidation(final long id) {
        return getInProgress(id, Validation.class);
    }

    @Override
    @Nonnull
    public Optional<Validation> getInProgressValidationForTarget(final long targetId) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .map(TargetOperationContext::getCurrentValidation)
            .filter(currentValidation -> currentValidation.getStatus() == Status.IN_PROGRESS);
    }

    @Override
    @Nonnull
    public Optional<Validation> getLastValidationForTarget(final long targetId) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .map(TargetOperationContext::getLastCompletedValidation);
    }

    @Override
    @Nonnull
    public List<Validation> getAllInProgressValidations() {
        return getAllInProgress(Validation.class);
    }

    @Override
    @Nonnull
    public Optional<Action> getInProgressAction(final long id) {
        return getInProgress(id, Action.class);
    }

    @Nonnull
    public List<Action> getInProgressActions() {
        return getAllInProgress(Action.class);
    }

    @Override
    @Nonnull
    public Optional<ActionList> getInProgressActionList(final long id) {
        return getInProgress(id, ActionList.class);
    }

    @Override
    @Nonnull
    public Optional<PlanExport> getInProgressPlanExport(final long id) {
        return getInProgress(id, PlanExport.class);
    }

    @Override
    @Nonnull
    public List<PlanExport> getInProgressPlanExports() {
        return getAllInProgress(PlanExport.class);
    }

    /**
     * Get the latest validation result for a target.
     *
     * @param targetId The ID of the target.
     * @return A {@link ValidationResult} for the given target, or empty if
     *         no validation result exists.
     */
    @Nonnull
    public Optional<ValidationResult> getValidationResult(final long targetId) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .map(TargetOperationContext::getLastValidationResult);
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     * @return a Future representing pending completion of the task
     */
    @Override
    public Future<?> notifyDiscoveryResult(@Nonnull final Discovery operation,
                             @Nonnull final DiscoveryResponse message) {
        return resultExecutor.submit(() -> {
            processDiscoveryResponse(operation, message, true);
        });
    }

    @Override
    public Future<?> notifyLoadedDiscovery(@Nonnull Discovery operation,
            @Nonnull DiscoveryResponse message) {
        return resultExecutor.submit(() -> {
            processDiscoveryResponse(operation, message, false);
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     * @return a Future representing pending completion of the task
     */
    public Future<?> notifyValidationResult(@Nonnull final Validation operation,
            @Nonnull final ValidationResponse message) {
        return resultExecutor.submit(() -> {
            processValidationResponse(operation, message);
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyActionResult(@Nonnull final Action operation,
            @Nonnull final ActionResult message) {
        resultExecutor.execute(() -> {
            processActionResponse(operation, message);
            if (shouldUpdateEntityActionTable(operation)) {
                updateControllableAndSuspendableState(operation);
            }
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyActionListResult(
            @Nonnull final ActionList operation,
            @Nonnull final ActionListResponse message) {
        resultExecutor.execute(() -> {
            processActionListResponse(operation, message);
            for (final ActionExecutionState action : operation.getActions()) {
                final Status status = action.getStatus();
                if (shouldUpdateEntityActionTable(action.getActionId(), action.getActionType())
                        && status != null) {
                    updateControllableAndSuspendableState(action.getActionId(), status);
                }
            }
        });
    }

    /**
     * Notifies action execution progress.
     *
     * @param action operation
     * @param progress action progress message
     */
    public void notifyActionProgress(@Nonnull final Action action,
            @Nonnull ActionProgress progress) {
        resultExecutor.execute(() -> {
            action.updateProgress(progress.getResponse());
            operationListeners.forEach(operationListener -> operationListener.notifyOperationState(action));
            if (shouldUpdateEntityActionTable(action)) {
                updateControllableAndSuspendableState(action);
            }
        });
    }

    /**
     * Notifies action list execution progress.
     *
     * @param actionList operation
     * @param progress action progress message
     */
    public void notifyActionListProgress(
            @Nonnull final ActionList actionList,
            @Nonnull ActionProgress progress) {
        resultExecutor.execute(() -> {
            actionList.updateProgress(progress.getResponse());
            operationListeners.forEach(operationListener ->
                    operationListener.notifyOperationState(actionList));
            for (final ActionExecutionState action : actionList.getActions()) {
                final Status status = action.getStatus();
                if (shouldUpdateEntityActionTable(action.getActionId(), action.getActionType())
                        && status != null) {
                    updateControllableAndSuspendableState(action.getActionId(), status);
                }
            }
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyPlanExportResult(@Nonnull final PlanExport operation,
                                       @Nonnull final PlanExportResult message) {
        resultExecutor.execute(() -> {
            processPlanExportResponse(operation, message);
        });
    }

    /**
     * Notifies plan export progress.
     *
     * @param export operation
     * @param progress action progress message
     */
    public void notifyPlanExportProgress(@Nonnull final PlanExport export,
                                         @Nonnull PlanExportProgress progress) {
        resultExecutor.execute(() -> {
            export.updateProgress(progress.getResponse());
            operationListeners.forEach(operationListener -> operationListener.notifyOperationState(export));
        });
    }

    /**
     * Handles operation failed event.
     *
     * @param operation operation that is failed
     * @param targetFailure target failure details
     */
    public void notifyOperationFailed(@Nonnull final Operation operation,
            @Nonnull String targetFailure) {
        resultExecutor.execute(() -> {
            logger.info("Operation {} failed: {}", operation, targetFailure);
            operationComplete(operation, false,
                    Collections.singletonList(SDKUtil.createCriticalError(targetFailure)));
        });
    }

    /**
     * Notifies action state update response from a probe.
     *
     * @param operation operation
     * @param message message received
     */
    public void notifyExternalActionState(@Nonnull final GetActionState operation,
            @Nonnull GetActionStateResponse message) {
        resultExecutor.execute(() -> {
            logger.info("Reported external action states updated {}", message);
            operationComplete(operation, true, Collections.emptyList());
        });
    }

    /**
     * Check whether a target is associated with a pending FULL discovery.
     * A pending discovery can be added via a call to {@link #addPendingDiscovery(long, DiscoveryType)}
     * when there is already an in progress discovery for a target.
     *
     * @param targetId The id of the target to check for a pending discovery.
     * @param discoveryType type of the discovery to check.
     * @return True if the target is associated with a pending discovery for the given
     *         discovery type, false otherwise.
     */
    @VisibleForTesting
    public boolean hasPendingDiscovery(long targetId, DiscoveryType discoveryType) {
        return Optional.ofNullable(targetOperationContexts.get(targetId))
            .map(targetOperationContext -> targetOperationContext.hasPendingDiscovery(discoveryType))
            .orElse(false);
    }

    protected void setPermitsForProbe(long probeId, ProbeInfo probe) {
        Map<DiscoveryType, Semaphore> semaphoreByDiscoveryType =
                probeOperationPermits.computeIfAbsent(probeId, k -> new HashMap<>());
        semaphoreByDiscoveryType.put(DiscoveryType.FULL,
                new Semaphore(maxConcurrentTargetDiscoveriesPerProbeCount, true));
        logger.info("Setting number of permits for probe: {}, discovery type {} to: {}",
                probeId, DiscoveryType.FULL,
                semaphoreByDiscoveryType.get(DiscoveryType.FULL).availablePermits());

        if (probe.hasIncrementalRediscoveryIntervalSeconds()) {
            semaphoreByDiscoveryType.put(DiscoveryType.INCREMENTAL,
                    new Semaphore(maxConcurrentTargetIncrementalDiscoveriesPerProbeCount, true));
            logger.info("Setting number of permits for probe: {}, discovery type {} to: {}",
                    probeId, DiscoveryType.INCREMENTAL,
                    semaphoreByDiscoveryType.get(DiscoveryType.INCREMENTAL).availablePermits());
        }
    }

    /**
     * When a probe is registered, check a discovery for any targets associated
     * with the probes that have pending discoveries.
     * Executes pending target activation in a separate thread because this callback
     * occurs in the communication transport thread context and starting discoveries
     * may be an expensive operation.
     *
     * @param probeId The ID of the probe that was registered.
     * @param probe The info for the probe that was registered with the {@link ProbeStore}.
     */
    @Override
    public void onProbeRegistered(long probeId, ProbeInfo probe) {
        logger.info("Registration of probe {}", probeId);
        setPermitsForProbe(probeId, probe);

        // activate pending full discovery if any, no need to activate pending incremental discovery
        // here since we don't gain from incremental if full and incremental happen at the same time
        targetStore.getProbeTargets(probeId).stream()
            .map(Target::getId)
            .forEach(targetId -> activatePendingDiscovery(targetId, DiscoveryType.FULL));
    }

    /**
     * When a target is removed, cancel all ongoing operations for that target.
     * Also cancel the pending discovery for the target if one exists.
     * Also upload the newly emptied group list for this target, effectively deleting any previously
     * discovered groups for that target.
     * Also cancel the upload of any discovered 'workflow' NonMarketEntities for this target.
     *
     * @param target The target that was removed from the {@link TargetStore}.
     */
    @Override
    public synchronized void onTargetRemoved(@Nonnull final Target target) {
        final long targetId = target.getId();
        try {
            TargetUpdateRequest request = TargetUpdateRequest.newBuilder()
                            .setProbeType(getProbeTypeWithCheck(target))
                            .setUpdateType(UpdateType.DELETED)
                            .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                            .build();
            remoteMediationServer.handleTargetRemoval(target, request);
        } catch (CommunicationException | InterruptedException | ProbeException e) {
            logger.warn("Failed to clean up target " + targetId
                         + " data in remote mediation container", e);
        }

        List<OperationMessageHandler<?, ?>> targetOperations = ongoingOperations.values().stream()
            .filter(handler -> handler.getOperation().getTargetId() == targetId)
            .collect(Collectors.toList());

        for (OperationMessageHandler<?, ?> operationHandler : targetOperations) {
            operationHandler.onTargetRemoved(targetId);
        }
        targetOperationContexts.remove(targetId);
        discoveredGroupUploader.targetRemoved(targetId);
        discoveredTemplateDeploymentProfileNotifier.deleteTemplateDeploymentProfileByTarget(targetId);
        discoveredWorkflowUploader.targetRemoved(targetId);
        // Must generate category from ProbeInfo and not use targetStore method since target has
        // already been removed from targetStore.
        final Optional<ProbeCategory> probeCategory = probeStore.getProbe(target.getProbeId())
                .flatMap(
                probe -> Optional.ofNullable(ProbeCategory.create(probe.getProbeCategory())));
        discoveredCloudCostUploader.targetRemoved(targetId, probeCategory);
        billedCloudCostUploader.targetRemoved(targetId);
        discoveredPlanDestinationUploader.targetRemoved(targetId);
    }

    @Override
    public void onTargetAdded(@NotNull Target target) {
        try {
            remoteMediationServer.handleTargetAddition(target);
        } catch (ProbeException e) {
            logger.warn("Error when adding target {} in remote mediation server", target.getId(), e);
        }
    }

    /**
     * Check for and clear expired operations.
     */
    @Override
    public void checkForExpiredOperations() {
        remoteMediationServer.checkForExpiredHandlers();
    }

    @Override
    public long getDiscoveryTimeoutMs() {
        return discoveryTimeoutMs;
    }

    @Override
    public long getValidationTimeoutMs() {
        return validationTimeoutMs;
    }

    @Override
    public long getActionTimeoutMs() {
        return actionTimeoutMs;
    }

    private void processValidationResponse(@Nonnull final Validation validation,
                                           @Nonnull final ValidationResponse response) {
        // Store the errors encountered during validation.
        final ValidationResult result = new ValidationResult(validation.getTargetId(), response);
        final Optional<TargetOperationContext> targetOperationContext =
            getTargetOperationContextOrLogError(validation.getTargetId());
        if (targetOperationContext.isPresent()) {
            targetOperationContext.get().setLastValidationResult(result);
        }

        logger.trace("Received validation result from target {}: {}", validation.getTargetId(), response);
        validation.addStagesReports(response.getStagesDetailList());
        operationComplete(validation,
                          result.isSuccess(),
                          response.getErrorDTOList());
    }

    protected void releaseSemaphore(long probeId, long targetId,
            @Nonnull DiscoveryType discoveryType) {
        Optional<Semaphore> semaphore = Optional.ofNullable(probeOperationPermits.get(probeId))
                .map(semaphoreByDiscoveryType -> semaphoreByDiscoveryType.get(discoveryType));
        logger.info("Number of permits before release: {}, queueLength: {} by targetId: {} ({})",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId,
                () -> discoveryType);

        semaphore.ifPresent(Semaphore::release);

        logger.info("Number of permits after release: {}, queueLength: {} by targetId: {} ({})",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId,
                () -> discoveryType);
    }

    private void processDiscoveryResponse(@Nonnull final Discovery discovery,
            @Nonnull final DiscoveryResponse response, boolean processDerivedTargets) {
        boolean success = !hasGeneralCriticalError(response.getErrorDTOList());
        // Discovery response changed since last discovery
        final boolean change = !response.hasNoChange();
        final long targetId = discovery.getTargetId();
        final DiscoveryType discoveryType = discovery.getDiscoveryType();
        // pjs: these discovery results can be pretty huge, (i.e. the cloud price discovery is over
        // 100 mb of json), so I'm splitting this into two messages, a debug and trace version so
        // you don't get large response dumps by accident. Maybe we should have a toggle that
        // controls whether the actual response is logged instead.
        logger.debug("Received {} discovery result from target {}: {} bytes", discoveryType,
                targetId, response.getSerializedSize());
        logger.trace("{} discovery result from target {}: {}", discoveryType, targetId, response);
        if (!change) {
            logger.info("No change since last {} discovery of target {}", discoveryType, targetId);
        }

        releaseSemaphore(discovery.getProbeId(), targetId, discoveryType);

        /**
         * We can't detect a duplicate target until we process the response. If we detect a
         * duplicate target, we will alter responseUsed to reflect the duplicate target error.
         */
        DiscoveryResponse responseUsed = response;

        try {
            // Ensure this target hasn't been deleted since the discovery began
            final Optional<Target> target = targetStore.getTarget(targetId);
            if (change) {
                if (target.isPresent()) {
                    if (success) {
                        try {
                            boolean duplicateTarget = false;
                            // TODO: (DavidBlinn 3/14/2018) if information makes it into the entityStore but fails later
                            // the topological information will be inconsistent. (ie if the entities are placed in the
                            // entityStore but the discoveredGroupUploader throws an exception, the entity and group
                            // information will be inconsistent with each other because we do not roll back on failure.
                            // these operations apply to all discovery types (FULL and INCREMENTAL for now)
                            try {
                                entityStore.entitiesDiscovered(discovery.getProbeId(), targetId,
                                        discovery.getMediationMessageId(), discoveryType,
                                        response.getEntityDTOList());
                                final List<EntityIdentifyingPropertyValues> entityIdentifyingPropertyValues =
                                    response.getEntityIdentifyingPropertyValuesList();
                                if (!entityIdentifyingPropertyValues.isEmpty()) {
                                    entityStore.entityIdentifyingPropertyValuesDiscovered(discovery.getProbeId(),
                                        discovery.getTargetId(), entityIdentifyingPropertyValues);
                                }
                            } catch (DuplicateTargetException e) {
                                logger.error("Detected duplicate for target {}: {}", targetId,
                                        e.getMessage());
                                duplicateTarget = true;
                                DiscoveryResponse.Builder responseBuilder =
                                        // add an error DTO so that message will appear on targets
                                        // page
                                        DiscoveryResponse.newBuilder().addErrorDTO(
                                                ErrorDTO.newBuilder()
                                                        .setDescription(e.getLocalizedMessage())
                                                        .setSeverity(ErrorSeverity.CRITICAL)
                                                        .addErrorTypeInfo(
                                                                ErrorTypeInfo.newBuilder()
                                                                        .setDuplicationErrorType(DuplicationErrorType.getDefaultInstance())
                                                                        .build())
                                                        .build());
                                responseUsed = responseBuilder.build();
                                success = false;
                            }
                            DISCOVERY_SIZE_SUMMARY.labels(target.map(Target::getProbeInfo)
                                                                          .map(ProbeInfo::getProbeType)
                                                                          .orElse("UNKNOWN"))
                                            .observe((double)response.getEntityDTOCount());
                            // dump discovery response if required
                            final Optional<ProbeInfo> probeInfo = probeStore.getProbe(discovery.getProbeId());
                            if (discoveryDumper != null && !duplicateTarget) {
                                String displayName = target.map(Target::getDisplayName).orElseGet(() -> "targetID-" + targetId);
                                String targetName =
                                        probeInfo.get().getProbeType() + "_" + displayName;
                                if (discovery.getUserInitiated()) {
                                    // make sure we have up-to-date settings if this is a user-initiated discovery
                                    targetDumpingSettings.refreshSettings();
                                }
                                discoveryDumper.dumpDiscovery(targetName, discoveryType, response,
                                        probeInfo.get().getAccountDefinitionList());
                            }
                            if (enableDiscoveryResponsesCaching) {
                                binaryDiscoveryDumper.dumpDiscovery(String.valueOf(targetId),
                                    discoveryType,
                                    response,
                                    probeInfo.get().getAccountDefinitionList());
                            }
                            // set discovery context
                            if (response.hasDiscoveryContext()) {
                                getTargetOperationContextOrLogError(targetId).ifPresent(
                                        targetOperationContext -> targetOperationContext.setCurrentDiscoveryContext(
                                                response.getDiscoveryContext()));
                            }
                            // send notification from probe
                            systemNotificationProducer.sendSystemNotification(
                                    responseUsed.getNotificationList(), target.get());

                            // these operations only apply to FULL discovery response for now
                            if (discoveryType == DiscoveryType.FULL) {

                                // This must be done before processing discovered groups to support group references across linked targets
                                if (processDerivedTargets) {
                                    derivedTargetParser.instantiateDerivedTargets(targetId, responseUsed.getDerivedTargetList());
                                }

                                discoveredGroupUploader.setTargetDiscoveredGroups(targetId,
                                        responseUsed.getDiscoveredGroupList());
                                discoveredTemplateDeploymentProfileNotifier.recordTemplateDeploymentInfo(
                                        targetId, response.getEntityProfileList(),
                                        responseUsed.getDeploymentProfileList(),
                                        response.getEntityDTOList());
                                discoveredWorkflowUploader.setTargetWorkflows(targetId, responseUsed.getWorkflowList());
                                discoveredCloudCostUploader.recordTargetCostData(targetId,
                                        targetStore.getProbeTypeForTarget(targetId), targetStore.getProbeCategoryForTarget(targetId), discovery,
                                        responseUsed.getNonMarketEntityDTOList(),
                                        responseUsed.getCostDTOList(),
                                        responseUsed.getPriceTable(),
                                        responseUsed.getCloudBillingDataList());
                                if (FeatureFlags.PARTITIONED_BILLED_COST_UPLOAD.isEnabled()) {
                                    billedCloudCostUploader.enqueueTargetBillingData(targetId,
                                            targetStore.getProbeTypeForTarget(targetId)
                                                    .orElse(null),
                                            responseUsed.getCloudBillingDataList());
                                }
                                discoveredPlanDestinationUploader.recordPlanDestinations(targetId,
                                    responseUsed.getNonMarketEntityDTOList());
                                // Flows
                                matrix.update(responseUsed.getFlowDTOList());
                            }
                        } catch (TargetNotFoundException e) {
                            final String message = "Failed to process " + discoveryType +
                                    " discovery for target " + targetId +
                                    ", which does not exist. " +
                                    "The target may have been deleted during discovery processing.";
                            // Logging at warn level--this is unexpected, but should not cause any harm
                            logger.warn(message);
                            failDiscovery(discovery, message);
                        }
                    } else {
                        // send failure notification from probe.
                        // TODO:  Except in specific cases, the UI notification will only show failure, so as to
                        // not expose details to users.  Add user-friendly messages in the probes where needed.
                        // In OpsMgr, call createDiscoveryErrorAndNotification() instead of createDiscoveryError()
                        // to create a user-friendly UI notification.
                        systemNotificationProducer.sendSystemNotification(response.getNotificationList(), target.get());
                    }
                } else {
                    final String message = discoveryType + " discovery completed for a target, "
                                    + targetId + ", that no longer exists.";
                                // Logging at info level--this is just poor timing and will happen occasionally
                                logger.info(message);
                                failDiscovery(discovery, message);
                                return;
                }
            }
            discovery.addStagesReports(response.getStagesDetailList());
            operationComplete(discovery, success, responseUsed.getErrorDTOList());
            if (discovery.getCompletionTime() != null) {
                try {
                    DISCOVERY_TIMES.labels(getProbeTypeWithCheck(target.get()), discoveryType.toString(), Boolean.toString(success))
                            .observe((double)discovery.getStartTime().until(
                                    discovery.getCompletionTime(),
                                    ChronoUnit.SECONDS));
                } catch (ProbeException e) {
                    logger.warn("Probe type is missing for target {}. Exception is {}", target, e);
                }
            }
        } catch (IdentityServiceException | RuntimeException e) {
            final String messageDetail = e.getLocalizedMessage() != null
                ? e.getLocalizedMessage()
                : e.getClass().getSimpleName();
            final String message = "Error processing " + discoveryType + " discovery response: " + messageDetail;
            logger.error(message, e);
            failDiscovery(discovery, message);
        }
        // Only activate a pending discovery if the probe is connected
        if (probeStore.isProbeConnected(discovery.getProbeId())) {
            activatePendingDiscovery(targetId, discoveryType);
        }
    }

    protected String getProbeTypeWithCheck(Target target) throws ProbeException {
        return probeStore.getProbe(target.getProbeId())
                        .map(ProbeInfo::getProbeType)
                        .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                                + " corresponding to target '" + target.getDisplayName()
                                + "' (" + target.getId() + ") is not registered"));
    }

    private void failDiscovery(@Nonnull final Discovery discovery,
                               @Nonnull final String failureMessage) {
        // It takes an error to cancel the discovery without processing it
        final ErrorDTO error = ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.CRITICAL)
            .setDescription(failureMessage)
            .build();
        operationComplete(discovery, false, Collections.singletonList(error));
    }

    private void processActionResponse(@Nonnull final Action action,
                                       @Nonnull final ActionResult result) {
        final ActionResponse response = result.getResponse();
        action.updateProgress(response);
        final boolean success = response.getActionResponseState() == ActionResponseState.SUCCEEDED;
        List<ErrorDTO> errors = success ?
            Collections.emptyList() :
            Collections.singletonList(SDKUtil.createCriticalError(response.getResponseDescription()));

        logger.trace("Received action result from target {}: {}", action.getTargetId(), result);
        operationComplete(action, success, errors);
    }

    private void processActionListResponse(
            @Nonnull final ActionList actionList,
            @Nonnull final ActionListResponse response) {
        final List<ActionResponse> responseList = response.getResponseList();
        boolean success = false;
        final List<ErrorDTO> errors = new ArrayList<>();
        for (final ActionResponse actionResponse : responseList) {
            actionList.updateProgress(actionResponse);
            if (actionResponse.getActionResponseState() == ActionResponseState.SUCCEEDED) {
                // If at least one action succeeded consider this operation as successful
                success = true;
            } else {
                errors.add(SDKUtil.createCriticalError(actionResponse.getResponseDescription()));
            }
        }

        logger.trace("Received action list response from target {}: {}",
                actionList::getTargetId, () -> response);

        operationComplete(actionList, success, errors);
    }

    private void processPlanExportResponse(@Nonnull final PlanExport export,
                                           @Nonnull final PlanExportResult result) {
        final PlanExportResponse response = result.getResponse();
        export.updateProgress(response);
        final boolean success = response.getState() == PlanExportResponseState.SUCCEEDED;
        List<ErrorDTO> errors = success ?
            Collections.emptyList() :
            Collections.singletonList(SDKUtil.createCriticalError(response.getDescription()));

        logger.trace("Received plan export result from target {}: {}", export.getTargetId(), result);
        operationComplete(export, success, errors);
    }

    /**
     * Activate the pending discovery for the target.
     *
     * @param targetId id of the target to active pending discovery for
     */
    private void activatePendingDiscovery(long targetId, DiscoveryType discoveryType) {
        // check if the target has any valid transport connected (with the right communication
        // binding channel for example).  If not, no need to activate pending discovery.
        if (!targetStore.getTarget(targetId).map(probeStore::isAnyTransportConnectedForTarget).orElse(false)) {
            logger.debug("Skipping activation of pending discovery for {} ({}) as no valid"
                    + " transport for this target is connected", targetId, discoveryType);
            return;
        }
        final Optional<TargetOperationContext> targetOperationContext =
            getTargetOperationContextOrLogError(targetId);
        if (!targetOperationContext.isPresent()) {
            return;
        }
        // if no pending discovery, do not need to activate
        if (!targetOperationContext.get().tryToClearPendingDiscovery(discoveryType)) {
            return;
        }

        logger.info("Activating pending discovery for {} ({})", targetId, discoveryType);
        // Execute the discovery in the background.
        discoveryExecutor.execute(() -> {
            try {
                logger.debug("Trigger startDiscovery for target {} ({})", targetId, discoveryType);
                startDiscovery(targetId, discoveryType, false);
            } catch (Exception e) {
                logger.error("Failed to activate discovery for {} ({})", targetId, discoveryType, e);
            }
        });
    }

    protected void operationStart(OperationMessageHandler<? extends Operation, ?> handler) {
        final Operation operation = handler.getOperation();
        ongoingOperations.put(operation.getId(), handler);
        // Send the same notification as the complete notification,
        // just that completionTime is not yet set.
        operationListeners.forEach(operationListener -> operationListener.notifyOperationState(operation));
        ONGOING_OPERATION_GAUGE.labels(operation.getClass().getName().toLowerCase()).increment();
    }

    protected void operationComplete(@Nonnull final Operation operation,
                                   final boolean success,
                                   @Nonnull final List<ErrorDTO> errors) {
        operation.addErrors(errors);
        if (success) {
            operation.success();
        } else {
            operation.fail();
        }
        logger.info("Completed {}", operation);

        synchronized (this) {
            ongoingOperations.computeIfPresent(operation.getId(), (id, completedOperation) -> {
                getTargetOperationContextOrLogError(operation.getTargetId()).ifPresent(
                    targetOperationContext -> targetOperationContext.operationCompleted(operation));
                // remove the operation from map
                return null;
            });
        }
        operationListeners.forEach(operationListener -> operationListener.notifyOperationState(operation));
        ONGOING_OPERATION_GAUGE.labels(operation.getClass().getName().toLowerCase()).decrement();
    }

    /**
     * Check if there was an error of critical severity that applied to the entire target.
     *
     * @param errors The list of {@link ErrorDTO} objects to check.
     * @return True if there was a target-wide critical error, false otherwise.
     */
    private boolean hasGeneralCriticalError(@Nonnull final List<ErrorDTO> errors) {
        return errors.stream()
                .anyMatch(error -> error.getSeverity() == ErrorSeverity.CRITICAL && !error.hasEntityUuid());
    }

    private <T extends Operation> Optional<T> getInProgress(final long id, Class<T> type) {
        final OperationMessageHandler<?, ?> handler = ongoingOperations.get(id);
        if (handler == null) {
            return Optional.empty();
        }
        final Operation op = handler.getOperation();
        return type.isInstance(op) ? Optional.of(type.cast(op)) : Optional.empty();
    }

    private <T extends Operation> List<T> getAllInProgress(Class<T> type) {
        final ImmutableList.Builder<T> opBuilder = new ImmutableList.Builder<>();
        ongoingOperations.values().stream()
                .map(OperationMessageHandler::getOperation)
                .filter(type::isInstance)
                .forEach(op -> opBuilder.add(type.cast(op)));
        return opBuilder.build();
    }

    /**
     * Insert move and activate actions into ENTITY_ACTION table in topology processor component.
     * The ENTITY_ACTION table is used in {@link com.vmturbo.topology.processor.controllable.ControllableManager}
     * to decide the suspendable and controllable flags on
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}.
     *
     * @param actionId The id of the action that is going to be stored.
     * @param actionType The action type of the action that is going to be stored.
     * @param entities The set of entities affected by the action
     */
    private void insertControllableAndSuspendableState(final long actionId,
                                         @Nonnull final ActionItemDTO.ActionType actionType,
                                         @Nonnull final Set<Long> entities) {
        try {
            if (shouldInsertEntityActionTable(actionType, entities)) {
                logger.info("Insert controllable flag for action {} which is of type {}", actionId,
                    actionType);
                actionToAffectedEntities.put(actionId, entities);
                entityActionDao.insertAction(actionId, actionType, entities);
            }
        } catch (DataAccessException | IllegalArgumentException e) {
            logger.error("Failed to create queued activate action records for action: {}",
                    actionId);
        }
    }

    /**
     * Check if the sdk action type is START, MOVE, CHANGE, CROSS_TARGET_MOVE or MOVE_TOGETHER.
     * @param actionType The action type of the action that is going to be stored.
     * @param entities The set of entities affected by the action
     * @return if the action should be stored.
     */
    private boolean shouldInsertEntityActionTable(ActionType actionType, Set<Long> entities) {
        return ACTION_TYPES_AFFECTING_ENTITIES.contains(actionType) && entities.size() > 0;
    }

    /**
     * Check if the sdk action type is START, MOVE, CHANGE, CROSS_TARGET_MOVE or MOVE_TOGETHER.
     * @param action The action that is going to be updated.
     * @return if the action should be updated.
     */
    private boolean shouldUpdateEntityActionTable(Action action) {
        return shouldUpdateEntityActionTable(action.getActionInstanceId(), action.getActionType());
    }

    private boolean shouldUpdateEntityActionTable(
            final long actionId,
            final ActionType actionType) {
        return ACTION_TYPES_AFFECTING_ENTITIES.contains(actionType)
                && this.actionToAffectedEntities.containsKey(actionId);
    }

    /**
     * Update action status for records in ENTITY_ACTION table in topology processor component.
     * @param action The action that is going to be updated.
     * {@link com.vmturbo.topology.processor.controllable.ControllableManager}
     */
    private void updateControllableAndSuspendableState(@Nonnull final Action action) {
        updateControllableAndSuspendableState(action.getActionInstanceId(), action.getStatus());
    }

    /**
     * Update action status for records in ENTITY_ACTION table in topology processor component.
     *
     * @param actionId ID of the action that is going to be updated.
     * @param status Action status.
     * {@link com.vmturbo.topology.processor.controllable.ControllableManager}
     */
    private void updateControllableAndSuspendableState(
            final long actionId,
            @Nonnull final Status status) {
        try {
            final Optional<ActionState> actionState = getActionState(status);
            if (actionState.isPresent()) {
                final ActionState state = actionState.get();
                entityActionDao.updateActionState(actionId, state);
                logger.trace("Successfully set the state of action with {} ID to {}", actionId, state);
            }
        } catch (DataAccessException e) {
            logger.error("Failed to update controllable table for action {}: {}",
                    actionId, e.getMessage());
        } catch (ActionRecordNotFoundException e) {
            logger.error("Action with id {} does not exist. Failed to update controllable table.",
                    actionId);
        }
    }

    private Optional<ActionState> getActionState(@Nonnull final Status status) {
        switch (status) {
            case IN_PROGRESS:
                return Optional.of(ActionState.IN_PROGRESS);
            case SUCCESS:
                return Optional.of(ActionState.SUCCEEDED);
            case FAILED:
                return Optional.of(ActionState.FAILED);
            default:
                logger.warn("Not supported action state {} for controllable flag", status);
                return Optional.empty();
        }
    }

    /**
     * Get the operation context for the given target, and log an error if it doesn't exist.
     *
     * @param targetId id of the target to get operation context for
     * @return optional TargetOperationContext
     */
    private Optional<TargetOperationContext> getTargetOperationContextOrLogError(long targetId) {
        final TargetOperationContext targetOperationContext = targetOperationContexts.get(targetId);
        if (targetOperationContext == null) {
            // it should have been initialized in operation request stage
            logger.error("Operation context not found for target {}", targetId);
        }
        return Optional.ofNullable(targetOperationContext);
    }

    @Override
    public void close() {
        resultExecutor.shutdownNow();
        discoveryExecutor.shutdownNow();
    }

    @Nonnull
    private RequestTargetId createTargetId(@Nonnull Target target) {
        final RequestTargetId result = RequestTargetId.newBuilder().setProbeType(
                target.getProbeInfo().getProbeType()).addAllAccountValue(
                target.getMediationAccountVals(groupScopeResolver)).build();
        return result;
    }

    @Nonnull
    @Override
    public ActionApproval approveActions(long targetId,
            @Nonnull Collection<ActionExecutionDTO> requests,
            @Nonnull OperationCallback<ActionApprovalResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException {
        final Target target = targetStore.getTarget(targetId).orElseThrow(
                () -> new TargetNotFoundException(targetId));
        final ActionApprovalRequest request = ActionApprovalRequest.newBuilder().setTarget(
                createTargetId(target)).addAllAction(requests).build();
        final ActionApproval operation = new ActionApproval(target.getProbeId(), target.getId(),
                identityProvider);
        final OperationCallback<ActionApprovalResponse> internalCallback =
                new InternalOperationCallback<>(callback, operation);
        final ActionApprovalMessageHandler handler = new ActionApprovalMessageHandler(operation,
                remoteMediationServer.getMessageHandlerExpirationClock(), discoveryTimeoutMs,
                internalCallback);
        logger.info("Sending {} actions for approval to {} target", requests.size(), target.getDisplayName());
        remoteMediationServer.sendActionApprovalsRequest(target, request, handler);
        operationStart(handler);
        return operation;
    }

    @Nonnull
    @Override
    public GetActionState getExternalActionState(long targetId, @Nonnull Collection<Long> actions,
            @Nonnull OperationCallback<GetActionStateResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException {
        final Target target = targetStore.getTarget(targetId).orElseThrow(
                () -> new TargetNotFoundException(targetId));
        final GetActionStateRequest request = GetActionStateRequest.newBuilder()
            .setTarget(createTargetId(target))
            .addAllActionOid(actions)
            .setIncludeAllActionsInTransition(true)
            .build();
        final GetActionState operation = new GetActionState(target.getProbeId(), target.getId(),
                identityProvider);
        final OperationCallback<GetActionStateResponse> internalCallback =
                new InternalOperationCallback<>(callback, operation);
        final GetActionStateMessageHandler handler = new GetActionStateMessageHandler(operation,
                remoteMediationServer.getMessageHandlerExpirationClock(), discoveryTimeoutMs,
                internalCallback);
        logger.info("Getting the states of {} actions from {} target",
                actions.size(), target.getDisplayName());
        remoteMediationServer.sendGetActionStatesRequest(target, request, handler);
        operationStart(handler);
        return operation;
    }

    @Nonnull
    @Override
    public ActionUpdateState updateExternalAction(long targetId,
            @Nonnull Collection<ActionResponse> actions,
            @Nonnull OperationCallback<ActionErrorsResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException {
        final Target target = targetStore.getTarget(targetId).orElseThrow(
                () -> new TargetNotFoundException(targetId));
        final ActionUpdateStateRequest request = ActionUpdateStateRequest.newBuilder().setTarget(
                createTargetId(target)).addAllActionState(actions).build();
        final ActionUpdateState operation = new ActionUpdateState(target.getProbeId(), target.getId(),
                identityProvider);
        final OperationCallback<ActionErrorsResponse> internalCallback =
                new InternalOperationCallback<>(callback, operation);
        final ActionUpdateStateMessageHandler handler = new ActionUpdateStateMessageHandler(
                operation, remoteMediationServer.getMessageHandlerExpirationClock(),
                discoveryTimeoutMs, internalCallback);
        logger.info("Sending the states of {} actions to {}", actions.size(), target.getDisplayName());
        remoteMediationServer.sendActionUpdateStateRequest(target, request, handler);
        operationStart(handler);
        return operation;
    }

    @Nonnull
    @Override
    public ActionAudit sendActionAuditEvents(long targetId,
            @Nonnull Collection<ActionEventDTO> events,
            @Nonnull OperationCallback<ActionErrorsResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException {
        final Target target = targetStore.getTarget(targetId).orElseThrow(
                () -> new TargetNotFoundException(targetId));
        final ActionAuditRequest request = ActionAuditRequest
                .newBuilder()
                .setTarget(createTargetId(target))
                .addAllAction(events)
                .build();
        final ActionAudit operation = new ActionAudit(target.getProbeId(), target.getId(),
                identityProvider);
        final OperationCallback<ActionErrorsResponse> internalCallback =
                new InternalOperationCallback<>(callback, operation);
        final ActionAuditMessageHandler handler = new ActionAuditMessageHandler(
                operation, remoteMediationServer.getMessageHandlerExpirationClock(),
                discoveryTimeoutMs, internalCallback);
        remoteMediationServer.sendActionAuditRequest(target, request, handler);
        operationStart(handler);
        return operation;
    }

    /**
     * Wrapper class containing all operation status related to a target, like current/last
     * validation, current/last discovery, DiscoveryContextDTO, etc.
     */
    @ThreadSafe
    protected static class TargetOperationContext {
        // Current validation operation
        private volatile Validation currentValidation;

        // Current completed validation operation
        private volatile Validation lastCompletedValidation;

        /**
         * Human-readable errors encountered during the last validation. It may be null, which
         * means the validation was successful.
         */
        private volatile ValidationResult lastValidationResult;

        // Current full discovery operation
        private volatile Discovery currentFullDiscovery;

        // whether or not there is pending full discovery for this target
        // When a discovery completes, if a pending discovery exists for the target,
        // the pending discovery is reset and a new discovery is kicked off for the target.
        private volatile boolean pendingFullDiscovery;

        // Last completed full discovery operation
        private volatile Discovery lastCompletedFullDiscovery;

        // Current incremental discovery operation
        private volatile Discovery currentIncrementalDiscovery;

        // Last completed incremental discovery operation
        private volatile Discovery lastCompletedIncrementalDiscovery;

        // whether or not there is pending incremental discovery for this target
        private volatile boolean pendingIncrementalDiscovery;

        // DiscoveryContextDTO
        private volatile DiscoveryContextDTO currentDiscoveryContext;

        public synchronized Validation getCurrentValidation() {
            return currentValidation;
        }

        public synchronized void setCurrentValidation(Validation currentValidation) {
            this.currentValidation = currentValidation;
        }

        public synchronized Validation getLastCompletedValidation() {
            return lastCompletedValidation;
        }

        public synchronized ValidationResult getLastValidationResult() {
            return lastValidationResult;
        }

        public synchronized void setLastValidationResult(ValidationResult lastValidationResult) {
            this.lastValidationResult = lastValidationResult;
        }

        public synchronized boolean hasPendingDiscovery(DiscoveryType discoveryType) {
            switch (discoveryType) {
                case FULL:
                    return pendingFullDiscovery;
                case INCREMENTAL:
                    return pendingIncrementalDiscovery;
            }
            return false;
        }

        public synchronized DiscoveryContextDTO getCurrentDiscoveryContext() {
            return currentDiscoveryContext;
        }

        public synchronized void setCurrentDiscoveryContext(DiscoveryContextDTO currentDiscoveryContext) {
            this.currentDiscoveryContext = currentDiscoveryContext;
        }

        public synchronized void setCurrentDiscovery(DiscoveryType discoveryType, Discovery discovery) {
            if (discoveryType == DiscoveryType.FULL) {
                this.currentFullDiscovery = discovery;
            } else if (discoveryType == DiscoveryType.INCREMENTAL) {
                this.currentIncrementalDiscovery = discovery;
            }
        }

        /**
         * Update last completed discovery with current completed discovery, and clear current discovery.
         *
         * @param discoveryType type of the discovery
         */
        private synchronized void updateLastDiscoveryAndClearCurrent(DiscoveryType discoveryType) {
            if (discoveryType == DiscoveryType.FULL) {
                if (currentFullDiscovery != null) {
                    this.lastCompletedFullDiscovery = currentFullDiscovery;
                    this.currentFullDiscovery = null;
                }
            } else if (discoveryType == DiscoveryType.INCREMENTAL) {
                if (currentIncrementalDiscovery != null) {
                    this.lastCompletedIncrementalDiscovery = currentIncrementalDiscovery;
                    this.currentIncrementalDiscovery = null;
                }
            }
        }

        /**
         * Update last completed validation with current completed validation, and clear current validation.
         */
        private synchronized void updateLastValidationAndClearCurrent() {
            if (currentValidation != null) {
                this.lastCompletedValidation = currentValidation;
                setCurrentValidation(null);
            }
        }

        /**
         * Get the last completed discovery for the given type.
         *
         * @param discoveryType type of the discovery
         * @return last completed full discovery, or null if it doesn't exist
         */
        @Nullable
        public synchronized Discovery getLastCompletedDiscovery(@Nonnull DiscoveryType discoveryType) {
            switch (discoveryType) {
                case FULL:
                    return lastCompletedFullDiscovery;
                case INCREMENTAL:
                    return lastCompletedIncrementalDiscovery;
                default:
                    return null;
            }
        }

        /**
         * Get the current in-progress discovery for the given type.
         *
         * @param discoveryType type of the discovery
         * @return optional in-progress discovery, or empty if it doesn't exist
         */
        public synchronized Optional<Discovery> getInProgressDiscovery(@Nonnull DiscoveryType discoveryType) {
            final Optional<Discovery> currentDiscovery;
            switch (discoveryType) {
                case FULL:
                    currentDiscovery = Optional.ofNullable(currentFullDiscovery);
                    break;
                case INCREMENTAL:
                    currentDiscovery = Optional.ofNullable(currentIncrementalDiscovery);
                    break;
                default:
                    currentDiscovery = Optional.empty();
            }
            return currentDiscovery.filter(discovery -> discovery.getStatus() == Status.IN_PROGRESS);
        }

        /**
         * Update status of the current/last discovery/validation based on the completed operation.
         *
         * @param completedOperation the completed operation
         */
        public synchronized void operationCompleted(@Nonnull Operation completedOperation) {
            if (completedOperation.getClass() == Discovery.class) {
                updateLastDiscoveryAndClearCurrent(((Discovery)completedOperation).getDiscoveryType());
            } else if (completedOperation.getClass() == Validation.class) {
                updateLastValidationAndClearCurrent();
            }
        }

        /**
         * Try to queue pending discovery to this context. If there is discovery of same type in
         * progress, then mark it as pending and return true; otherwise return false.
         *
         * @param discoveryType type of the discovery
         * @return true if pending discovery is queued successfully, otherwise false
         */
        public synchronized boolean tryQueuePendingDiscovery(@Nonnull DiscoveryType discoveryType) {
            final Optional<Discovery> inProgressDiscovery = getInProgressDiscovery(discoveryType);
            // if no discovery in progress, then we don't need to queue it, return false
            if (!inProgressDiscovery.isPresent()) {
                return false;
            }
            // mark that that there is pending discovery for this target
            if (discoveryType == DiscoveryType.FULL) {
                this.pendingFullDiscovery = true;
            } else if (discoveryType == DiscoveryType.INCREMENTAL) {
                this.pendingIncrementalDiscovery = true;
            }
            return true;
        }

        /**
         * Perform operations on the context if encountering the issue that probe is disconnected.
         * like mark pending full discovery, and clear all the last completed operations.
         *
         * @param discoveryType type of the discovery
         */
        public synchronized void onProbeDisconnected(@Nonnull DiscoveryType discoveryType) {
            // mark that that there is pending full discovery for this target,
            // no need to mark it for incremental, since we don't want incremental discovery to
            // happen immediately same time as full discovery after probe connects again
            if (discoveryType == DiscoveryType.FULL) {
                this.pendingFullDiscovery = true;
            }
            this.lastCompletedFullDiscovery = null;
            this.lastCompletedIncrementalDiscovery = null;
            this.lastCompletedValidation = null;
        }

        /**
         * Try to clear pending full discovery (set flag to false), if it's true.
         *
         * @return true is there is pending full discovery, otherwise false
         */
        public synchronized boolean tryToClearPendingDiscovery(@Nonnull DiscoveryType discoveryType) {
            if (discoveryType == DiscoveryType.FULL) {
                if (pendingFullDiscovery) {
                    this.pendingFullDiscovery = false;
                    return true;
                }
            } else if (discoveryType == DiscoveryType.INCREMENTAL) {
                if (pendingIncrementalDiscovery) {
                    this.pendingIncrementalDiscovery = false;
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Internal operation callback is used to wrap the externally passed callback to implicitly
     * close operations (transmit to terminal state and remove from collection of running ones).
     *
     * @param <T> type of an operation result.
     */
    private class InternalOperationCallback<T> implements OperationCallback<T> {
        private final OperationCallback<T> callback;
        private final Operation operation;

        InternalOperationCallback(@Nonnull OperationCallback<T> callback, @Nonnull Operation operation) {
            this.callback = Objects.requireNonNull(callback);
            this.operation = Objects.requireNonNull(operation);
        }

        @Override
        public void onSuccess(@Nonnull T response) {
            resultExecutor.execute(() -> {
                logger.info("Operation {} finished successfully", operation);
                logger.debug("Received response for operation {}: {}", operation, response);
                operationComplete(operation, true, Collections.emptyList());
                callback.onSuccess(response);
            });
        }

        @Override
        public void onFailure(@Nonnull String error) {
            resultExecutor.execute(() -> {
                logger.info("Operation {} finished with error {}", operation, error);
                operationComplete(operation, true,
                        Collections.singletonList(SDKUtil.createCriticalError(error)));
                callback.onFailure(error);
            });
        }
    }

    /**
     * Operation callback for validation.
     */
    private class ValidationOperationCallback implements OperationCallback<ValidationResponse> {
        private final Validation validation;

        ValidationOperationCallback(@Nonnull Validation validation) {
            this.validation = Objects.requireNonNull(validation);
        }

        @Override
        public void onSuccess(@Nonnull ValidationResponse response) {
            notifyValidationResult(validation, response);
        }

        @Override
        public void onFailure(@Nonnull String error) {
            notifyValidationResult(validation, ValidationResponse.newBuilder()
                    .addErrorDTO(SDKUtil.createCriticalError(error))
                    .build());
        }
    }

    /**
     * Operation callback for discovery operation.
     */
    protected class DiscoveryOperationCallback implements OperationCallback<DiscoveryResponse> {
        private final Discovery discovery;

        DiscoveryOperationCallback(@Nonnull Discovery discovery) {
            this.discovery = Objects.requireNonNull(discovery);
        }

        @Override
        public void onSuccess(@Nonnull DiscoveryResponse response) {
            notifyDiscoveryResult(discovery, response);
        }

        @Override
        public void onFailure(@Nonnull String error) {
            final DiscoveryResponse response = DiscoveryResponse.newBuilder().addErrorDTO(
                    SDKUtil.createCriticalError(error)).build();
            notifyDiscoveryResult(discovery, response);
        }
    }
}
