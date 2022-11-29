package com.vmturbo.topology.processor.operation;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * Subclass of OperationManager that uses an AggregatingDiscoveryQueue to send discoveries to.
 * RemoteMediationWithDiscoveryWorkers will then pull discoveries off that queue to process in an
 * appropriate container.  This allows permitting for parallel discoveries to happen at the
 * container level instead of the probe type level.
 */
@ThreadSafe
public class OperationManagerWithQueue extends OperationManager {

    private static final Logger logger = LogManager.getLogger(OperationManagerWithQueue.class);

    private final AggregatingDiscoveryQueue discoveryQueue;

    /**
     * Create an instance of OperationManagerWithQueue.
     *
     * @param identityProvider IdentityProvider
     * @param targetStore TargetStore for querying target information.
     * @param probeStore ProbeStore for getting probe information.
     * @param remoteMediationServer RemoteMediationServer for interacting with mediation.
     * @param operationListener listener for operations.
     * @param entityStore EntityStore where discovered entities go.
     * @param discoveredGroupUploader DiscoveredGroupUploader where discovered groups go.
     * @param discoveredWorkflowUploader where discovered Workflows go.
     * @param discoveredCloudCostUploader where Cloud Cost goes.
     * @param aliasedOidsUploader where aliased OIDs mapping goes.
     * @param discoveredPlanDestinationUploader where Plan Destinations data goes.
     * @param discoveredTemplateDeploymentProfileNotifier where discovered templates go.
     * @param entityActionDao where entity actions are persisted.
     * @param derivedTargetParser where derived targets in the discovery response get handled.
     * @param groupScopeResolver needed for resolving account values.
     * @param targetDumpingSettings information about which targets to log discoveries for.
     * @param systemNotificationProducer for notifying about any issues encountered.
     * @param discoveryQueue where discoveries are queued for handling by RemoteMediationServer.
     * @param discoveryTimeoutSeconds discovery timeout.
     * @param validationTimeoutSeconds validation timeout.
     * @param actionTimeoutSeconds action timeout.
     * @param planExportTimeoutSeconds plan export timeout.
     * @param matrix MatrixInterface.
     * @param binaryDiscoveryDumper handles recording discovery responses in binary.
     * @param enableDiscoveryResponsesCaching whether or not to cache discovery responses.
     * @param licenseCheckClient license check client.
     * @param workflowExecutionTimeoutMillis workflow execution timeout.
     */
    public OperationManagerWithQueue(@Nonnull final IdentityProvider identityProvider,
                            @Nonnull final TargetStore targetStore,
                            @Nonnull final ProbeStore probeStore,
                            @Nonnull final RemoteMediation remoteMediationServer,
                            @Nonnull final OperationListener operationListener,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
                            @Nonnull final DiscoveredWorkflowUploader discoveredWorkflowUploader,
                            @Nonnull final DiscoveredCloudCostUploader discoveredCloudCostUploader,
                            @Nonnull final BilledCloudCostUploader billedCloudCostUploader,
                            @Nonnull final AliasedOidsUploader aliasedOidsUploader,
                            @Nonnull final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader,
                            @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                            @Nonnull final EntityActionDao entityActionDao,
                            @Nonnull final DerivedTargetParser derivedTargetParser,
                            @Nonnull final GroupScopeResolver groupScopeResolver,
                            @Nonnull final TargetDumpingSettings targetDumpingSettings,
                            @Nonnull final SystemNotificationProducer systemNotificationProducer,
                            @Nonnull final AggregatingDiscoveryQueue discoveryQueue,
                            final long discoveryTimeoutSeconds,
                            final long validationTimeoutSeconds,
                            final long actionTimeoutSeconds,
                            final long planExportTimeoutSeconds,
                            final @Nonnull MatrixInterface matrix,
                            final BinaryDiscoveryDumper binaryDiscoveryDumper,
                            final boolean enableDiscoveryResponsesCaching,
                            final LicenseCheckClient licenseCheckClient,
                            final int workflowExecutionTimeoutMillis) {
        super(identityProvider, targetStore, probeStore, remoteMediationServer, operationListener,
                entityStore, discoveredGroupUploader, discoveredWorkflowUploader,
                discoveredCloudCostUploader, billedCloudCostUploader, aliasedOidsUploader,
                discoveredPlanDestinationUploader, discoveredTemplateDeploymentProfileNotifier,
                entityActionDao, derivedTargetParser, groupScopeResolver, targetDumpingSettings,
                systemNotificationProducer, discoveryTimeoutSeconds, validationTimeoutSeconds,
                actionTimeoutSeconds, planExportTimeoutSeconds, 0, 0, 0, 0, matrix,
                binaryDiscoveryDumper, enableDiscoveryResponsesCaching, licenseCheckClient, workflowExecutionTimeoutMillis);

        this.discoveryQueue = discoveryQueue;
    }

    private DiscoveryBundle handleDiscovery(@Nonnull Runnable runAfterDiscoveryCompletes,
            long probeId, @Nonnull Target target, @Nonnull DiscoveryType discoveryType,
            @Nonnull DataMetricTimer waitingTimer) {
        try {
            logger.trace("In handleDiscovery with target {}", target.getId());
            final long targetId = target.getId();
            waitingTimer.close();
            String probeType = getProbeTypeWithCheck(target);
            Discovery discovery = new Discovery(probeId, targetId, discoveryType,
                    identityProvider);
            logger.trace("New discovery created.");
            DiscoveryRequest discoveryRequest = DiscoveryRequest.newBuilder().setProbeType(
                    probeType).setDiscoveryType(discoveryType).addAllAccountValue(
                    target.getMediationAccountVals(groupScopeResolver)).setDiscoveryContext(Optional
                    .ofNullable(targetOperationContexts.get(targetId))
                    .map(TargetOperationContext::getCurrentDiscoveryContext)
                    .orElse(DiscoveryContextDTO.getDefaultInstance())).build();
            synchronized (this) {
                // check again if there was a discovery triggered for this target by another
                // thread between the executions of the 1st and the 2nd synchronized blocks.
                final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(
                        targetId, discoveryType);
                if (currentDiscovery.isPresent()) {
                    logger.info("Discovery is in progress. Returning existing discovery for "
                                    + "target: {} ({})", targetId, discoveryType);
                    return new DiscoveryBundle(currentDiscovery.get(), null, null);
                }
                logger.trace("Current discovery is not present, creating callback.");
                final OperationCallback<DiscoveryResponse> callback =
                        new QueuedDiscoveryOperationCallback(discovery, runAfterDiscoveryCompletes);
                DiscoveryMessageHandler discoveryMessageHandler = new DiscoveryMessageHandler(
                        discovery, remoteMediationServer.getMessageHandlerExpirationClock(),
                        discoveryTimeoutMs, callback);
                logger.trace("Starting operation.");
                operationStart(discoveryMessageHandler);
                targetOperationContexts.computeIfAbsent(targetId, k -> new TargetOperationContext())
                        .setCurrentDiscovery(discoveryType, discovery);
                return new DiscoveryBundle(discovery, discoveryRequest, discoveryMessageHandler);
            }
        } catch (ProbeException e) {
            logger.error("Problem trying to run discovery for target {}", target.getId(), e);
        } catch (Exception e) {
            logger.error("Exception in handleDiscovery for target {}", target.getId(), e);
        }
        return null;
    }

    private void onDiscoveryError(@Nonnull Discovery discovery, @Nonnull Exception ex) {
        final ErrorDTO.Builder errorBuilder = ErrorDTO.newBuilder()
                .setSeverity(ErrorSeverity.CRITICAL);
        if (ex.getLocalizedMessage() != null) {
            errorBuilder.setDescription(ex.getLocalizedMessage());
        } else {
            errorBuilder.setDescription(ex.getClass().getSimpleName());
        }
        operationComplete(discovery, false, Collections.singletonList(errorBuilder.build()));

    }

    @Nonnull
    @Override
    public Optional<Discovery> startDiscovery(final long targetId,
            @Nonnull DiscoveryType discoveryType,
            boolean runNow)
            throws TargetNotFoundException, ProbeException {

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

        final long probeId;
        final Target target;
        IDiscoveryQueueElement element;
        synchronized (this) {
            logger.info("Starting discovery for target: {} ({})", targetId, discoveryType);
            final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId,
                    discoveryType);
            if (currentDiscovery.isPresent()) {
                logger.info("Returning existing discovery for target: {} ({})", targetId,
                        discoveryType);
                return currentDiscovery;
            }

            target = targetStore.getTarget(targetId).orElseThrow(() ->
                    new TargetNotFoundException(targetId));

            String probeType = getProbeTypeWithCheck(target);

            probeId = target.getProbeId();
            DataMetricTimer waitingTimer = DISCOVERY_WAITING_TIMES.labels(probeType,
                    discoveryType.toString()).startTimer();
            final LocalDateTime beforeQueueTime = LocalDateTime.now();
            element = discoveryQueue.offerDiscovery(target, discoveryType,
                    (runAfter) -> handleDiscovery(runAfter, probeId, target, discoveryType,
                            waitingTimer),
                    (discovery, exception) -> onDiscoveryError(discovery, exception), runNow);
            // if there was already a queued discovery for this target, end the timer.
            if (element.getQueuedTime().isBefore(beforeQueueTime)) {
                waitingTimer.close();
            }
        }
        if (runNow) {
            logger.debug("Target ID is {}", target.getId());
            logger.debug("Element is {}", element);
            // wait until discovery starts or target is deleted
            try {
                logger.trace("About to wait on element {}", element.toString());
                // wait until queued discovery is pulled off the queue
                return Optional.ofNullable(element.getDiscovery(TimeUnit.MINUTES
                        .toMillis(probeDiscoveryPermitWaitTimeoutMins)));
            } catch (InterruptedException e) {
                logger.error("Interruped while waiting for discovery to start for target {}",
                        element.getTarget().getId(), e);
            }
        }
        return Optional.empty();
     }

    @Override
    protected void setPermitsForProbe(long probeId, ProbeInfo probe) {
        // do nothing - semaphores are controlled at the container level
    }

    @Override
    protected void releaseSemaphore(long probeId, long targetId,
            @NotNull DiscoveryType discoveryType) {
        // do nothing - semaphores are controlled at the container level
    }

    /**
     * Subclass of DiscoveryOperationCallback that calls a runnable once the discovery completes.
     **/
    private class QueuedDiscoveryOperationCallback extends DiscoveryOperationCallback {

        private final Runnable cleanup;

        QueuedDiscoveryOperationCallback(Discovery discovery, Runnable cleanup) {
            super(discovery);
            this.cleanup = cleanup;
        }

        @Override
        public void onSuccess(@NotNull DiscoveryResponse response) {
            try {
                super.onSuccess(response);
            } finally {
                cleanup.run();
            }
        }

        @Override
        public void onFailure(@NotNull String error) {
            try {
                super.onFailure(error);
            } finally {
                cleanup.run();
            }
        }
    }
}
