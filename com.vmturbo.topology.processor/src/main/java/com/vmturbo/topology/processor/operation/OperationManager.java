package com.vmturbo.topology.processor.operation;

import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest.Builder;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ActionRecordNotFoundException;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumperImpl;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.operation.validation.ValidationResult;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * Responsible for managing all operations.
 * Any individual target may only have a single ongoing operation of a particular type
 * associated with it.
 * TODO (roman, June 16 2016): Should we allow multiple operations of different types?
 */
@ThreadSafe
public class OperationManager implements ProbeStoreListener, TargetStoreListener,
        IOperationManager {

    private static final Logger logger = LogManager.getLogger(OperationManager.class);

    // Mapping from OperationID -> Ongoing Operations
    private final ConcurrentMap<Long, Operation> ongoingOperations = new ConcurrentHashMap<>();

    // Mapping from TargetID -> Target operation context (validation/discovery/discoveryContext/...)
    private final ConcurrentMap<Long, TargetOperationContext> targetOperationContexts = new ConcurrentHashMap<>();

    // Control number of concurrent target discoveries per probe per discovery type.
    // Mapping from ProbeId -> Semaphore
    private final ConcurrentMap<Long, Map<DiscoveryType, Semaphore>> probeOperationPermits = new ConcurrentHashMap<>();

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final RemoteMediation remoteMediationServer;

    private final IdentityProvider identityProvider;

    private final OperationListener operationListener;

    private final EntityStore entityStore;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DerivedTargetParser derivedTargetParser;

    private final GroupScopeResolver groupScopeResolver;

    private final TargetDumpingSettings targetDumpingSettings;

    private DiscoveryDumper discoveryDumper = null;

    private final SystemNotificationProducer systemNotificationProducer;

    private final long discoveryTimeoutMs;

    private final long validationTimeoutMs;

    private final long actionTimeoutMs;

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

    private static final DataMetricGauge ONGOING_OPERATION_GAUGE = DataMetricGauge.builder()
        .withName("tp_ongoing_operation_total")
        .withHelp("Total number of ongoing operations in the topology processor.")
        .withLabelNames("type")
        .build()
        .register();

    private static final DataMetricSummary DISCOVERY_SIZE_SUMMARY = DataMetricSummary.builder()
        .withName("tp_discovery_size_entities")
        .withHelp("The number of service entities in a discovery.")
        .build()
        .register();

    private DiscoveredWorkflowUploader discoveredWorkflowUploader;

    private DiscoveredCloudCostUploader discoveredCloudCostUploader;

    private final int maxConcurrentTargetDiscoveriesPerProbeCount;

    private final int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;

    private final Random random = new Random();

    /**
     * Timeout for acquiring the permit for running a discovering operation
     * on a target.
     */
    private final int probeDiscoveryPermitWaitTimeoutMins;

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
                            @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                            @Nonnull final EntityActionDao entityActionDao,
                            @Nonnull final DerivedTargetParser derivedTargetParser,
                            @Nonnull final GroupScopeResolver groupScopeResolver,
                            @Nonnull final TargetDumpingSettings targetDumpingSettings,
                            @Nonnull final SystemNotificationProducer systemNotificationProducer,
                            final long discoveryTimeoutSeconds,
                            final long validationTimeoutSeconds,
                            final long actionTimeoutSeconds,
                            final int maxConcurrentTargetDiscoveriesPerProbeCount,
                            final int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount,
                            final int probeDiscoveryPermitWaitTimeoutMins,
                            final int probeDiscoveryPermitWaitTimeoutIntervalMins,
                            final @Nonnull MatrixInterface matrix) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.remoteMediationServer = Objects.requireNonNull(remoteMediationServer);
        this.operationListener = Objects.requireNonNull(operationListener);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        this.discoveredWorkflowUploader = Objects.requireNonNull(discoveredWorkflowUploader);
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
        this.derivedTargetParser = Objects.requireNonNull(derivedTargetParser);
        this.groupScopeResolver = Objects.requireNonNull(groupScopeResolver);
        this.systemNotificationProducer = Objects.requireNonNull(systemNotificationProducer);
        this.discoveredTemplateDeploymentProfileNotifier = Objects.requireNonNull(discoveredTemplateDeploymentProfileNotifier);
        this.discoveryTimeoutMs = TimeUnit.MILLISECONDS.convert(discoveryTimeoutSeconds, TimeUnit.SECONDS);
        this.validationTimeoutMs = TimeUnit.MILLISECONDS.convert(validationTimeoutSeconds, TimeUnit.SECONDS);
        this.actionTimeoutMs = TimeUnit.MILLISECONDS.convert(actionTimeoutSeconds, TimeUnit.SECONDS);
        this.maxConcurrentTargetDiscoveriesPerProbeCount = maxConcurrentTargetDiscoveriesPerProbeCount;
        this.maxConcurrentTargetIncrementalDiscoveriesPerProbeCount = maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;
        this.discoveredCloudCostUploader = discoveredCloudCostUploader;
        this.probeDiscoveryPermitWaitTimeoutMins = probeDiscoveryPermitWaitTimeoutMins;
        this.probeDiscoveryPermitWaitTimeoutIntervalMins = probeDiscoveryPermitWaitTimeoutIntervalMins;
        this.targetDumpingSettings = targetDumpingSettings;
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Action requestActions(final long actionId,
                                              final long targetId,
                                              @Nullable Long secondaryTargetId,
                                              @Nonnull final ActionType actionType,
                                              @Nonnull final List<ActionItemDTO> actionDtos,
                                              @Nonnull Set<Long> controlAffectedEntities,
                                              @Nonnull Optional<WorkflowDTO.WorkflowInfo> workflowInfoOpt)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));
        final String probeType = getProbeTypeWithCheck(target);

        final Action action = new Action(actionId, target.getProbeId(),
                targetId, identityProvider, actionType);
        final ActionExecutionDTO.Builder actionExecutionBuilder = ActionExecutionDTO.newBuilder()
                .setActionType(actionType)
                .addAllActionItem(actionDtos);
        // if a WorkflowInfo action execution override is present, translate it to a NonMarketEntity
        // and include it in the ActionExecution to be sent to the target
        workflowInfoOpt.ifPresent(workflowInfo ->
                actionExecutionBuilder.setWorkflow(buildWorkflow(workflowInfo)));
        final Builder actionRequestBuilder = ActionRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                .setActionExecutionDTO(actionExecutionBuilder);
        // If a secondary target is defined, at it to the ActionRequest
        if (secondaryTargetId != null) {
            // This action requires interaction with a second target.
            // Secondary account values must be set.
            final Target secondaryTarget = targetStore.getTarget(secondaryTargetId)
                    .orElseThrow(() -> new TargetNotFoundException(secondaryTargetId));
            actionRequestBuilder.addAllSecondaryAccountValue(
                    secondaryTarget.getMediationAccountVals(groupScopeResolver));
        }
        final ActionRequest request = actionRequestBuilder.build();
        final ActionMessageHandler messageHandler = new ActionMessageHandler(this,
                action,
                remoteMediationServer.getMessageHandlerExpirationClock(),
                actionTimeoutMs);

        // Update the ENTITY_ACTION table in preparation for executing the action
        insertControllableAndSuspendableState(actionId, actionType, controlAffectedEntities);

        logger.info("Sending action {} execution request to probe", actionId);
        remoteMediationServer.sendActionRequest(target,
                request, messageHandler);

        logger.info("Beginning {}", action);
        logger.debug("Action execution DTO:\n" + request.getActionExecutionDTO().toString());
        operationStart(action);
        return action;
    }

    /**
     * Create a Workflow DTO representing the given {@link WorkflowDTO.WorkflowInfo}.
     *
     * @param workflowInfo the information describing this Workflow, including ID, displayName,
     *                     and defining data - parameters and properties.
     * @return a newly created {@link }Workflow} DTO
     */
    private Workflow buildWorkflow(WorkflowDTO.WorkflowInfo workflowInfo) {
        final Workflow.Builder wfBuilder = Workflow.newBuilder();
        if (workflowInfo.hasDisplayName()) {
            wfBuilder.setDisplayName(workflowInfo.getDisplayName());
        }
        if (workflowInfo.hasName()) {
            wfBuilder.setId(workflowInfo.getName());
        }
        if (workflowInfo.hasDescription()) {
            wfBuilder.setDescription(workflowInfo.getDescription());
        }
        wfBuilder.addAllParam(workflowInfo.getWorkflowParamList().stream()
            .map(workflowParam -> {
                final Workflow.Parameter.Builder parmBuilder = Workflow.Parameter.newBuilder();
                if (workflowParam.hasDescription()) {
                    parmBuilder.setDescription(workflowParam.getDescription());
                }
                if (workflowParam.hasName()) {
                    parmBuilder.setName(workflowParam.getName());
                }
                if (workflowParam.hasType()) {
                    parmBuilder.setType(workflowParam.getType());
                }
                if (workflowParam.hasMandatory()) {
                    parmBuilder.setMandatory(workflowParam.getMandatory());
                }
                return parmBuilder.build();
            })
            .collect(Collectors.toList()));
        // include the 'property' entries from the Workflow
        wfBuilder.addAllProperty(workflowInfo.getWorkflowPropertyList().stream()
            .map(workflowProperty -> {
                final Workflow.Property.Builder propBUilder = Workflow.Property.newBuilder();
                if (workflowProperty.hasName()) {
                    propBUilder.setName(workflowProperty.getName());
                }
                if (workflowProperty.hasValue()) {
                    propBUilder.setValue(workflowProperty.getValue());
                }
                return propBUilder.build();
            })
            .collect(Collectors.toList()));
        if (workflowInfo.hasScriptPath()) {
            wfBuilder.setScriptPath(workflowInfo.getScriptPath());
        }
        if (workflowInfo.hasEntityType()) {
            wfBuilder.setEntityType(EntityDTO.EntityType.forNumber(workflowInfo.getEntityType()));
        }
        if (workflowInfo.hasActionType()) {

            ActionType converted = ActionConversions.convertActionType(workflowInfo.getActionType());
            if (converted != null) {
                wfBuilder.setActionType(converted);
            }
        }
        if (workflowInfo.hasActionPhase()) {
            final ActionScriptPhase converted = ActionConversions.convertActionPhase(workflowInfo.getActionPhase());
            if (converted != null) {
                wfBuilder.setPhase(converted);
            }
        }
        if (workflowInfo.hasTimeLimitSeconds()) {
            wfBuilder.setTimeLimitSeconds(workflowInfo.getTimeLimitSeconds());
        }
        return wfBuilder.build();
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
        final ValidationRequest validationRequest = ValidationRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver)).build();
        final ValidationMessageHandler validationMessageHandler =
                new ValidationMessageHandler(this,
                        validation,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        validationTimeoutMs);
        remoteMediationServer.sendValidationRequest(target,
                validationRequest,
                validationMessageHandler);

        operationStart(validation);
        targetOperationContext.setCurrentValidation(validation);
        logger.info("Beginning {}", validation);
        return validation;
    }

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long, DiscoveryType)},
     * with the following exceptions:
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
            return Optional.of(startDiscovery(targetId, discoveryType));
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
    public Discovery startDiscovery(final long targetId, DiscoveryType discoveryType)
            throws TargetNotFoundException, ProbeException, CommunicationException, InterruptedException {

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
        synchronized (this) {
            logger.info("Starting discovery for target: {} ({})", targetId, discoveryType);
            final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId, discoveryType);
            if (currentDiscovery.isPresent()) {
                logger.info("Returning existing discovery for target: {} ({})", targetId, discoveryType);
                return currentDiscovery.get();
            }

            target = targetStore.getTarget(targetId)
                    .orElseThrow(() -> new TargetNotFoundException(targetId));
            final String probeType = getProbeTypeWithCheck(target);

            probeId = target.getProbeId();
            discovery = new Discovery(probeId, target.getId(), discoveryType, identityProvider);

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
            () -> target.getDisplayName(),
            () -> targetId,
            () -> discoveryType);

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
            final long waitTimeout = probeDiscoveryPermitWaitTimeoutMins +
                    random.nextInt(probeDiscoveryPermitWaitTimeoutIntervalMins);
            logger.info("Set permit acquire timeout to: {} for target: {}({}) ({})",
                waitTimeout, target.getDisplayName(), targetId, discoveryType);
            boolean gotPermit = semaphore.get().tryAcquire(1, waitTimeout, TimeUnit.MINUTES);
            if (!gotPermit) {
                logger.warn("Permit acquire timeout of: {} {} exceeded for targetId: {} ({})." +
                    " Continuing with discovery", waitTimeout, TimeUnit.MINUTES,
                    targetId, discoveryType);
            }
        }
        logger.info("Number of permits after acquire: {}, queueLength: {} by target: {}({}) ({})",
            () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
            () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
            () -> target.getDisplayName(),
            () -> targetId,
            () -> discoveryType);

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
                    return currentDiscovery.get();
                }
                discoveryMessageHandler =
                    new DiscoveryMessageHandler(this,
                        discovery,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        discoveryTimeoutMs);
                operationStart(discovery);
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
        return discovery;
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

    private Future<?> notifyOperationError(@Nonnull final Operation operation, @Nonnull ErrorDTO error) {
        if (operation instanceof Discovery) {
            final DiscoveryResponse response =
                            DiscoveryResponse.newBuilder().addErrorDTO(error).build();
            return resultExecutor.submit(() -> processDiscoveryResponse((Discovery)operation, response));
        } else if (operation instanceof Validation) {
            final ValidationResponse response =
                            ValidationResponse.newBuilder().addErrorDTO(error).build();
            return resultExecutor.submit(
                            () -> processValidationResponse((Validation)operation, response));
        } else if (operation instanceof Action) {
            // It's obnoxious to have to translate from ErrorDTO -> ActionResult -> ErrorDTO
            // but unfortunately not easy to avoid due to the difference in the SDK.
            final ActionResult result = ActionResult.newBuilder()
                .setResponse(ActionResponse.newBuilder()
                    .setActionResponseState(ActionResponseState.FAILED)
                    .setProgress(0)
                    .setResponseDescription(error.getDescription())
                ).build();
            return resultExecutor.submit(
                () -> processActionResponse((Action)operation, result));
        }
        throw new UnsupportedOperationException("Unsupported operation: " + operation);
    }

    /**
     * Notify the {@link OperationManager} that an {@link Operation} timed out.
     *
     * @param operation The {@link Operation} that timed out.
     * @param secondsSinceStart The number of seconds the operation was active for.
     * @return a Future representing pending completion of the task
     */
    public Future<?> notifyTimeout(@Nonnull final Operation operation,
                              final long secondsSinceStart) {
        final ErrorDTO error = SDKUtil.createCriticalError(new StringBuilder()
                .append(operation.getClass().getSimpleName())
                .append(" ")
                .append(operation.getId())
                .append(" timed out after ")
                .append(secondsSinceStart)
                .append(" seconds.").toString());
        return notifyOperationError(operation, error);
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     * @return a Future representing pending completion of the task
     */
    public Future<?> notifyDiscoveryResult(@Nonnull final Discovery operation,
                             @Nonnull final DiscoveryResponse message) {
        return resultExecutor.submit(() -> {
            processDiscoveryResponse(operation, message);
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

    public void notifyProgress(@Nonnull final Operation operation,
                               @Nonnull final MediationClientMessage message) {
        if (operation instanceof Action) {
            resultExecutor.execute(() -> {
                Action action = (Action)operation;
                action.updateProgress(message.getActionProgress().getResponse());
                operationListener.notifyOperationState(operation);
                if (shouldUpdateEntityActionTable(action)) {
                    updateControllableAndSuspendableState(action);
                }
            });
        }
    }

    /**
     * Notify the {@link OperationManager} that an {@link Operation} is cancelled because transport
     * executing operation is closed.
     *
     * @param operation The {@link Operation} that timed out.
     * @param cancellationReason the reason why the operation was cancelled.
     * @return a Future representing pending completion of the task
     */
    public Future<?> notifyOperationCancelled(@Nonnull final Operation operation,
                                         @Nonnull final String cancellationReason) {
        final ErrorDTO error = SDKUtil.createCriticalError(operation.getClass().getSimpleName()
                        + " " + operation.getId() + " cancelled: " + cancellationReason);
        return notifyOperationError(operation, error);
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

    /**
     * When a probe is registered, check a discovery for any targets associated
     * with the probes that have pending discoveries.
     *
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
            remoteMediationServer.handleTargetRemoval(target.getProbeId(), targetId, request);
        } catch (CommunicationException | InterruptedException | ProbeException e) {
            logger.warn("Failed to clean up target " + targetId
                         + " data in remote mediation container", e);
        }

        List<Operation> targetOperations = ongoingOperations.values().stream()
            .filter(operation -> operation.getTargetId() == targetId)
            .collect(Collectors.toList());

        for (Operation operation : targetOperations) {
            notifyOperationCancelled(operation, "Target removed.");
        }
        targetOperationContexts.remove(targetId);
        discoveredGroupUploader.targetRemoved(targetId);
        discoveredTemplateDeploymentProfileNotifier.deleteTemplateDeploymentProfileByTarget(targetId);
        discoveredWorkflowUploader.targetRemoved(targetId);
        discoveredCloudCostUploader.targetRemoved(targetId, targetStore.getProbeCategoryForTarget(targetId));
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
        if (!targetOperationContext.isPresent()) {
            return;
        }

        targetOperationContext.get().setLastValidationResult(result);
        logger.trace("Received validation result from target {}: {}", validation.getTargetId(), response);
        operationComplete(validation,
                          result.isSuccess(),
                          response.getErrorDTOList());
    }

    private void processDiscoveryResponse(@Nonnull final Discovery discovery,
                                          @Nonnull final DiscoveryResponse response) {
        final boolean success = !hasGeneralCriticalError(response.getErrorDTOList());
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

        Optional<Semaphore> semaphore = Optional.ofNullable(probeOperationPermits.get(discovery.getProbeId()))
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

        try {
            // Ensure this target hasn't been deleted since the discovery began
            final Optional<Target> target = targetStore.getTarget(targetId);
            if (change) {
                if (target.isPresent()) {
                    if (success) {
                        try {
                            // TODO: (DavidBlinn 3/14/2018) if information makes it into the entityStore but fails later
                            // the topological information will be inconsistent. (ie if the entities are placed in the
                            // entityStore but the discoveredGroupUploader throws an exception, the entity and group
                            // information will be inconsistent with each other because we do not roll back on failure.
                            // these operations apply to all discovery types (FULL and INCREMENTAL for now)
                            entityStore.entitiesDiscovered(discovery.getProbeId(), targetId,
                                    discovery.getMediationMessageId(), discoveryType, response.getEntityDTOList());
                            DISCOVERY_SIZE_SUMMARY.observe((double)response.getEntityDTOCount());
                            // dump discovery response if required
                            if (discoveryDumper != null) {
                                final Optional<ProbeInfo> probeInfo = probeStore.getProbe(discovery.getProbeId());
                                String displayName = target.map(Target::getDisplayName).orElseGet(() -> "targetID-" + targetId);
                                String targetName =
                                        probeInfo.get().getProbeType() + "_" + displayName;
                                if (discovery.getUserInitiated()) {
                                    // make sure we have up-to-date settings if this is a user-initiated discovery
                                    targetDumpingSettings.refreshSettings();
                                }
                                discoveryDumper.dumpDiscovery(targetName, discoveryType, response,
                                        new ArrayList<>());
                            }
                            // set discovery context
                            if (response.hasDiscoveryContext()) {
                                getTargetOperationContextOrLogError(targetId).ifPresent(
                                        targetOperationContext -> targetOperationContext.setCurrentDiscoveryContext(
                                                response.getDiscoveryContext()));
                            }
                            // send notification from probe
                            systemNotificationProducer.sendSystemNotification(response.getNotificationList(), target.get());

                            // these operations only apply to FULL discovery response for now
                            if (discoveryType == DiscoveryType.FULL) {
                                discoveredGroupUploader.setTargetDiscoveredGroups(targetId, response.getDiscoveredGroupList());
                                discoveredTemplateDeploymentProfileNotifier.recordTemplateDeploymentInfo(
                                        targetId, response.getEntityProfileList(), response.getDeploymentProfileList(),
                                        response.getEntityDTOList());
                                discoveredWorkflowUploader.setTargetWorkflows(targetId, response.getWorkflowList());
                                derivedTargetParser.instantiateDerivedTargets(targetId, response.getDerivedTargetList());
                                discoveredCloudCostUploader.recordTargetCostData(targetId,
                                        targetStore.getProbeTypeForTarget(targetId), targetStore.getProbeCategoryForTarget(targetId), discovery,
                                        response.getNonMarketEntityDTOList(), response.getCostDTOList(),
                                        response.getPriceTable());
                                // Flows
                                matrix.update(response.getFlowDTOList());
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
            operationComplete(discovery, success, response.getErrorDTOList());
        } catch (IdentityServiceException | RuntimeException e) {
            final String messageDetail = e.getLocalizedMessage() != null
                ? e.getLocalizedMessage()
                : e.getClass().getSimpleName();
            final String message = "Error processing " + discoveryType + " discovery response: " + messageDetail;
            logger.error(message, e);
            failDiscovery(discovery, message);
        }
        activatePendingDiscovery(targetId, discoveryType);
    }

    private String getProbeTypeWithCheck(Target target) throws ProbeException {
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

    /**
     * Activate the pending discovery for the target.
     *
     * @param targetId id of the target to active pending discovery for
     */
    private void activatePendingDiscovery(long targetId, DiscoveryType discoveryType) {
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
                startDiscovery(targetId, discoveryType);
            } catch (Exception e) {
                logger.error("Failed to activate discovery for {} ({})", targetId, discoveryType, e);
            }
        });
    }

    private void operationStart(Operation operation) {
        ongoingOperations.put(operation.getId(), operation);
        // Send the same notification as the complete notification,
        // just that completionTime is not yet set.
        operationListener.notifyOperationState(operation);
        ONGOING_OPERATION_GAUGE.labels(operation.getClass().getName().toLowerCase()).increment();
    }

    private void operationComplete(@Nonnull final Operation operation,
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
        operationListener.notifyOperationState(operation);
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
        final Operation op = ongoingOperations.get(id);
        return op != null && type.isInstance(op) ? Optional.of(type.cast(op)) : Optional.empty();
    }

    private <T extends Operation> List<T> getAllInProgress(Class<T> type) {
        final ImmutableList.Builder<T> opBuilder = new ImmutableList.Builder<>();
        ongoingOperations.values().stream()
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
        return ACTION_TYPES_AFFECTING_ENTITIES.contains(action.getActionType())
            && this.actionToAffectedEntities.containsKey(action.getActionId());
    }

    /**
     * Update action status for records in ENTITY_ACTION table in topology processor component.
     * @param action The action that is going to be updated.
     * {@link com.vmturbo.topology.processor.controllable.ControllableManager}
     */
    private void updateControllableAndSuspendableState(@Nonnull final Action action) {
        try {
            final Optional<ActionState> actionState = getActionState(action.getStatus());
            if (actionState.isPresent()) {
                entityActionDao.updateActionState(action.getActionId(), actionState.get());
            }
        } catch (DataAccessException e) {
            logger.error("Failed to update controllable table for action {}: {}",
                    action.getActionId(), e.getMessage());
        } catch (ActionRecordNotFoundException e) {
            logger.error("Action with id {} does not exist. Failed to update controllable table.", action.getActionId());
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

    /**
     * Wrapper class containing all operation status related to a target, like current/last
     * validation, current/last discovery, DiscoveryContextDTO, etc.
     */
    @ThreadSafe
    private static class TargetOperationContext {
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
}
