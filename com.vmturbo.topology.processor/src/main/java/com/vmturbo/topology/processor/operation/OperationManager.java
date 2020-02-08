package com.vmturbo.topology.processor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
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
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ControllableRecordNotFoundException;
import com.vmturbo.topology.processor.conversions.AppComponentConverter;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumperImpl;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.operation.validation.ValidationResult;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;
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

    // Mapping from TargetID -> Current Discovery Operation
    private final ConcurrentMap<Long, Discovery> currentTargetDiscoveries = new ConcurrentHashMap<>();

    // Mapping from TargetID -> DiscoveryContextDTO
    private final ConcurrentMap<Long, DiscoveryContextDTO> currentTargetDiscoveryContext =
                    new ConcurrentHashMap<>();

    // Mapping from TargetID -> Current Validation Operation
    private final ConcurrentMap<Long, Validation> currentTargetValidations = new ConcurrentHashMap<>();

    // Mapping from TargetID -> Completed Discovery Operation
    private final ConcurrentMap<Long, Discovery> lastCompletedTargetDiscoveries = new ConcurrentHashMap<>();

    // Mapping from TargetID -> Completed Validation Operation
    private final ConcurrentMap<Long, Validation> lastCompletedTargetValidations = new ConcurrentHashMap<>();

    // Control number of concurrent target discoveries per probe.
    // Mapping from ProbeId -> Semaphore
    private final ConcurrentMap<Long, Semaphore> probeOperationPermits =
            new ConcurrentHashMap<>();

    /**
     * A set of targets for which there are pending discoveries.
     * When a discovery completes, if a pending discovery exists for the target,
     * the pending discovery is removed and a new discovery is kicked off for the target.
     */
    private final Set<Long> pendingDiscoveries = ConcurrentHashMap.newKeySet();

    /**
     * targetId -> List of human-readable errors encountered during the last validation.
     * The list may be empty, which means the validation was successful.
     * A null entry means no validation had run.
     */
    private final ConcurrentMap<Long, ValidationResult> validationResults = new ConcurrentHashMap<>();

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
    private final HashMap<Long, Set<Long>> actionToControlAffectedEntities = new HashMap<>();

    private static final ImmutableSet<ActionItemDTO.ActionType> CONTROLLABLE_OR_SUSPENDABLE_ACTION_TYPES
            = ImmutableSet.of(ActionItemDTO.ActionType.MOVE, ActionItemDTO.ActionType.CHANGE,
            ActionItemDTO.ActionType.CROSS_TARGET_MOVE, ActionItemDTO.ActionType.MOVE_TOGETHER,
            ActionItemDTO.ActionType.START);

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

        final String probeType = probeStore.getProbe(target.getProbeId())
                .map(ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                        + " corresponding to target " + targetId + " is not registered"));

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
        remoteMediationServer.sendActionRequest(target.getProbeId(),
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

        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));

        final String probeType = probeStore.getProbe(target.getProbeId())
                .map(ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                        + " corresponding to target '" + target.getDisplayName()
                        + "' (" + targetId + ") is not registered"));

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
        remoteMediationServer.sendValidationRequest(target.getProbeId(),
                validationRequest,
                validationMessageHandler);

        operationStart(validation);
        currentTargetValidations.put(targetId, validation);
        logger.info("Beginning {}", validation);
        return validation;
    }

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long)},
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
    public Optional<Discovery> addPendingDiscovery(long targetId)
        throws TargetNotFoundException, CommunicationException, InterruptedException {
        // TODO: Replace this with a dedicated lock object to synchronize on (see OM-51162)
        synchronized (this) {
            final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId);
            if (currentDiscovery.isPresent()) {
                // Add the target to the set of pending targets.
                pendingDiscoveries.add(targetId);
                return Optional.empty();
            }
        }
        try {
            // Avoid holding a lock while calling this, else that lock will continue to be held
            // while waiting on a probe operation permit.
            return Optional.of(startDiscovery(targetId));
        } catch (ProbeException e) {
            synchronized (this) {
                pendingDiscoveries.add(targetId);
                lastCompletedTargetDiscoveries.remove(targetId);
                lastCompletedTargetValidations.remove(targetId);
                return Optional.empty();
            }
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
    public Discovery startDiscovery(final long targetId)
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

        synchronized (this) {
            logger.info("Starting discovery for target: {}", targetId);
            final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId);
            if (currentDiscovery.isPresent()) {
                logger.info("Returning existing discovery for target: {}", targetId);
                return currentDiscovery.get();
            }

            final Target target = targetStore.getTarget(targetId)
                    .orElseThrow(() -> new TargetNotFoundException(targetId));

            final String probeType = probeStore.getProbe(target.getProbeId())
                    .map(ProbeInfo::getProbeType)
                    .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                            + " corresponding to target '" + target.getDisplayName()
                            + "' (" + targetId + ") is not registered"));


            probeId = target.getProbeId();
            discovery = new Discovery(probeId, target.getId(), identityProvider);

            discoveryRequest = DiscoveryRequest.newBuilder()
                    .setProbeType(probeType)
                    .setDiscoveryType(DiscoveryType.FULL)
                    .addAllAccountValue(target.getMediationAccountVals(groupScopeResolver))
                    .setDiscoveryContext(currentTargetDiscoveryContext.getOrDefault(
                        targetId, DiscoveryContextDTO.getDefaultInstance()))
                    .build();
        }

        // If the probe has not yet registered, the semaphore won't be initialized.
        Optional<Semaphore> semaphore =
                Optional.ofNullable(probeOperationPermits.get(probeId));

        logger.info("Number of permits before acquire: {}, queueLength: {} by targetId: {}",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId);

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
            final long waitTimeout = probeDiscoveryPermitWaitTimeoutMins +
                    random.nextInt(probeDiscoveryPermitWaitTimeoutIntervalMins);
            logger.info("Set permit acquire timeout to: {} for targetId: {}",
                    waitTimeout, targetId);
            boolean gotPermit = semaphore.get().tryAcquire(1, waitTimeout, TimeUnit.MINUTES);
            if (!gotPermit) {
                logger.warn("Permit acquire timeout of: {} {} exceeded for targetId: {}." +
                                " Continuing with discovery", probeId, waitTimeout,
                        TimeUnit.MINUTES, targetId);
            }
        }
        logger.info("Number of permits after acquire: {}, queueLength: {} by targetId: {}",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId);


        synchronized (this) {
            try {
                // check again if there was a discovery triggered for this target by another thread
                // between the executions of the 1st and the 2nd synchronized blocks.
                final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId);
                if (currentDiscovery.isPresent()) {
                    logger.info("Discovery is progress. Returning existing discovery for target: {}",
                            targetId);
                    return currentDiscovery.get();
                }
                discoveryMessageHandler =
                    new DiscoveryMessageHandler(this,
                        discovery,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        discoveryTimeoutMs);
                operationStart(discovery);
                currentTargetDiscoveries.put(targetId, discovery);
                remoteMediationServer.sendDiscoveryRequest(probeId,
                        discoveryRequest,
                        discoveryMessageHandler);
            } catch (Exception ex) {
                if (semaphore.isPresent()) {
                    semaphore.get().release();
                    logger.warn("Releasing permit on exception for targetId: {}" +
                                    " After release permits: {}, queueLength: {}, exception:{}",
                            targetId,
                            semaphore.map(Semaphore::availablePermits).orElse(-1),
                            semaphore.map(Semaphore::getQueueLength).orElse(-1),
                            ex.toString());
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
    public Optional<Discovery> getInProgressDiscoveryForTarget(final long targetId) {
        final Discovery currentDiscovery = currentTargetDiscoveries.get(targetId);
        return currentDiscovery == null || currentDiscovery.getStatus() != Status.IN_PROGRESS ?
                Optional.empty() : Optional.of(currentDiscovery);
    }

    @Override
    @Nonnull
    public Optional<Discovery> getLastDiscoveryForTarget(final long targetId) {
        return Optional.ofNullable(lastCompletedTargetDiscoveries.get(targetId));
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
        final Validation lastValidation = currentTargetValidations.get(targetId);
        return lastValidation == null || lastValidation.getStatus() != Status.IN_PROGRESS ?
                Optional.empty() : Optional.ofNullable(lastValidation);
    }

    @Override
    @Nonnull
    public Optional<Validation> getLastValidationForTarget(final long targetId) {
        return Optional.ofNullable(lastCompletedTargetValidations.get(targetId));
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
        return Optional.ofNullable(validationResults.get(targetId));
    }

    private void notifyOperationError(@Nonnull final Operation operation, @Nonnull ErrorDTO error) {
        if (operation instanceof Discovery) {
            final DiscoveryResponse response =
                            DiscoveryResponse.newBuilder().addErrorDTO(error).build();
            resultExecutor.execute(() -> processDiscoveryResponse((Discovery)operation, response));
        } else if (operation instanceof Validation) {
            final ValidationResponse response =
                            ValidationResponse.newBuilder().addErrorDTO(error).build();
            resultExecutor.execute(
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
            resultExecutor.execute(
                () -> processActionResponse((Action)operation, result));
        }
    }

    /**
     * Notify the {@link OperationManager} that an {@link Operation} timed out.
     *
     * @param operation The {@link Operation} that timed out.
     * @param secondsSinceStart The number of seconds the operation was active for.
     */
    public void notifyTimeout(@Nonnull final Operation operation,
                              final long secondsSinceStart) {
        final ErrorDTO error = SDKUtil.createCriticalError(new StringBuilder()
                .append(operation.getClass().getSimpleName())
                .append(" ")
                .append(operation.getId())
                .append(" timed out after ")
                .append(secondsSinceStart)
                .append(" seconds.").toString());
        notifyOperationError(operation, error);
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyDiscoveryResult(@Nonnull final Discovery operation,
                             @Nonnull final DiscoveryResponse message) {
        resultExecutor.execute(() -> {
            processDiscoveryResponse(operation, message);
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyValidationResult(@Nonnull final Validation operation,
            @Nonnull final ValidationResponse message) {
        resultExecutor.execute(() -> {
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
     */
    public void notifyOperationCancelled(@Nonnull final Operation operation,
                                         @Nonnull final String cancellationReason) {
        final ErrorDTO error = SDKUtil.createCriticalError(operation.getClass().getSimpleName()
                        + " " + operation.getId() + " cancelled: " + cancellationReason);
        notifyOperationError(operation, error);
    }

    /**
     * Check whether a target is associated with a pending discovery.
     * A pending discovery can be added via a call to {@link #addPendingDiscovery(long)}
     * when there is already an in progress discovery for a target. The
     *
     * @param targetId The id of the target to check for a pending discovery.
     * @return True if the target is associated with a pending discovery, false
     *         otherwise.
     */
    public boolean hasPendingDiscovery(long targetId) {
        return pendingDiscoveries.contains(targetId);
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
        probeOperationPermits.computeIfAbsent(probeId,
                k -> new Semaphore(maxConcurrentTargetDiscoveriesPerProbeCount, true /*fair*/));
        logger.info("Setting number of permits for probe: {} to: {}",
                probeId, probeOperationPermits.get(probeId).availablePermits());
        targetStore.getProbeTargets(probeId).stream()
            .map(Target::getId)
            .filter(this::hasPendingDiscovery)
            .forEach(this::activatePendingDiscovery);
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
        remoteMediationServer.removeMessageHandlers(operation -> operation.getTargetId() == targetId);

        List<Operation> targetOperations = ongoingOperations.values().stream()
            .filter(operation -> operation.getTargetId() == targetId)
            .collect(Collectors.toList());

        for (Operation operation : targetOperations) {
            notifyOperationCancelled(operation, "Target removed.");
        }
        pendingDiscoveries.remove(targetId);
        lastCompletedTargetValidations.remove(targetId);
        lastCompletedTargetDiscoveries.remove(targetId);
        discoveredGroupUploader.targetRemoved(targetId);
        discoveredTemplateDeploymentProfileNotifier.deleteTemplateDeploymentProfileByTarget(targetId);
        discoveredWorkflowUploader.targetRemoved(targetId);
        discoveredCloudCostUploader.targetRemoved(targetId);
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
        validationResults.put(validation.getTargetId(), result);

        logger.trace("Received validation result from target {}: {}", validation.getTargetId(), response);
        operationComplete(validation,
                          result.isSuccess(),
                          response.getErrorDTOList());
    }

    private void processDiscoveryResponse(@Nonnull final Discovery discovery,
                                          @Nonnull final DiscoveryResponse oldFormatResponse) {
        final boolean success = !hasGeneralCriticalError(oldFormatResponse.getErrorDTOList());
        final DiscoveryResponse response = new AppComponentConverter().convertResponse(oldFormatResponse);
        // Discovery response changed since last discovery
        final boolean change = !response.hasNoChange();
        final long targetId = discovery.getTargetId();
        // pjs: these discovery results can be pretty huge, (i.e. the cloud price discovery is over
        // 100 mb of json), so I'm splitting this into two messages, a debug and trace version so
        // you don't get large response dumps by accident. Maybe we should have a toggle that
        // controls whether the actual response is logged instead.
        logger.debug("Received discovery result from target {}: {} bytes",
                targetId, response.getSerializedSize());
        logger.trace("Discovery result from target {}: {}", targetId, response);
        if (!change) {
            logger.info("No change since last discovery of target {}", targetId);
        }

        Optional<Semaphore> semaphore =
                Optional.ofNullable(probeOperationPermits.get(discovery.getProbeId()));
        logger.info("Number of permits before release: {}, queueLength: {} by targetId: {}",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId);

        semaphore.ifPresent(Semaphore::release);

        logger.info("Number of permits after release: {}, queueLength: {} by targetId: {}",
                () -> semaphore.map(Semaphore::availablePermits).orElse(-1),
                () -> semaphore.map(Semaphore::getQueueLength).orElse(-1),
                () -> targetId);
        try {
            if (success && change) {
                // Ensure this target hasn't been deleted since the discovery began
                final Optional<Target> target = targetStore.getTarget(targetId);
                if (target.isPresent()) {
                    try {
                        // TODO: (DavidBlinn 3/14/2018) if information makes it into the entityStore but fails later
                        // the topological information will be inconsistent. (ie if the entities are placed in the
                        // entityStore but the discoveredGroupUploader throws an exception, the entity and group
                        // information will be inconsistent with each other because we do not roll back on failure.
                        entityStore.entitiesDiscovered(discovery.getProbeId(), targetId, response.getEntityDTOList());
                        discoveredGroupUploader.setTargetDiscoveredGroups(targetId, response.getDiscoveredGroupList());
                        discoveredTemplateDeploymentProfileNotifier.recordTemplateDeploymentInfo(targetId,
                            response.getEntityProfileList(), response.getDeploymentProfileList(),
                            response.getEntityDTOList());
                        discoveredWorkflowUploader.setTargetWorkflows(targetId,
                            response.getWorkflowList());
                        DISCOVERY_SIZE_SUMMARY.observe((double)response.getEntityDTOCount());
                        derivedTargetParser.instantiateDerivedTargets(targetId, response.getDerivedTargetList());
                        discoveredCloudCostUploader.recordTargetCostData(targetId,
                                targetStore.getProbeTypeForTarget(targetId), discovery,
                            response.getNonMarketEntityDTOList(), response.getCostDTOList(),
                            response.getPriceTable());
                        if (response.hasDiscoveryContext()) {
                            currentTargetDiscoveryContext.put(targetId, response.getDiscoveryContext());
                        }
                        systemNotificationProducer.sendSystemNotification(response.getNotificationList(), target.get());
                        if (discoveryDumper != null) {
                            final Optional<ProbeInfo> probeInfo = probeStore.getProbe(discovery.getProbeId());
                            String displayName = target.isPresent()
                                ? target.get().getDisplayName()
                                : "targetID-" + targetId;
                            String targetName = probeInfo.get().getProbeType() + "_" + displayName;
                            if (discovery.getUserInitiated()) {
                                // make sure we have up-to-date settings if this is a user-initiated discovery
                                targetDumpingSettings.refreshSettings();
                            }
                            discoveryDumper.dumpDiscovery(targetName, DiscoveryType.FULL, response, new ArrayList<>());
                        }
                        // Flows
                        matrix.update(response.getFlowDTOList());
                    } catch (TargetNotFoundException e) {
                        final String message = "Failed to process discovery for target "
                                + targetId
                                + ", which does not exist. "
                                + "The target may have been deleted during discovery processing.";
                        // Logging at warn level--this is unexpected, but should not cause any harm
                        logger.warn(message);
                        failDiscovery(discovery, message);
                    }
                } else {
                    final String message = "Discovery completed for a target, "
                        + targetId
                        + ", that no longer exists.";
                    // Logging at info level--this is just poor timing and will happen occasionally
                    logger.info(message);
                    failDiscovery(discovery, message);
                    return;
                }
            }
            operationComplete(discovery, success, response.getErrorDTOList());
        } catch (IdentityUninitializedException | IdentityMetadataMissingException |
                IdentityProviderException | RuntimeException e) {
            final String messageDetail = e.getLocalizedMessage() != null
                ? e.getLocalizedMessage()
                : e.getClass().getSimpleName();
            final String message = "Error processing discovery response: " + messageDetail;
            logger.error(message, e);
            failDiscovery(discovery, message);
        }
        activatePendingDiscovery(targetId);
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

    private void activatePendingDiscovery(long targetId) {
        if (pendingDiscoveries.remove(targetId)) {
            logger.info("Activating pending discovery for {}", targetId);
            // Execute the discovery in the background.
            discoveryExecutor.execute(() -> {
                try {
                    logger.debug("Trigger startDiscovery for target {}", targetId);
                    startDiscovery(targetId);
                } catch (Exception e) {
                    logger.error("Failed to activate discovery for {}", targetId, e);
                }
            });
        }
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

        Operation completedOperation = ongoingOperations.remove(operation.getId());
        if (completedOperation != null) {
            long targetId = operation.getTargetId();
            if (completedOperation.getClass() == Discovery.class) {
                Discovery lastDiscovery = currentTargetDiscoveries.remove(targetId);
                if (lastDiscovery !=null) {
                    lastCompletedTargetDiscoveries.put(targetId, lastDiscovery);
                }
            } else if (completedOperation.getClass() == Validation.class){
                Validation lastValidation = currentTargetValidations.remove(targetId);
                if (lastValidation !=null) {
                    lastCompletedTargetValidations.put(targetId, lastValidation);
                }
            }
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
                actionToControlAffectedEntities.put(actionId, entities);
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
        return CONTROLLABLE_OR_SUSPENDABLE_ACTION_TYPES.contains(actionType) && entities.size() > 0;
    }

    /**
     * Check if the sdk action type is START, MOVE, CHANGE, CROSS_TARGET_MOVE or MOVE_TOGETHER.
     * @param action The action that is going to be updated.
     * @return if the action should be updated.
     */
    private boolean shouldUpdateEntityActionTable(Action action) {
        return CONTROLLABLE_OR_SUSPENDABLE_ACTION_TYPES.contains(action.getActionType())
            && this.actionToControlAffectedEntities.containsKey(action.getActionId());
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
        } catch (ControllableRecordNotFoundException e) {
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
}
