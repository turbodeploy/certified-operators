package com.vmturbo.voltron.extensions.tp;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DuplicationErrorType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumpFilename;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumperImpl;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.DiscoveryDumperSettings;
import com.vmturbo.topology.processor.operation.OperationListener;
import com.vmturbo.topology.processor.operation.OperationManagerWithQueue;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.rest.OperationController;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * An {@link com.vmturbo.topology.processor.operation.IOperationManager} implementation that:
 * 1. Makes hidden derived targets visible so that can be explicitly discovered in the UI.
 * 2. Ignores any discovery requests that are NOT user initiated.
 *
 * <p>This class sublasses OperationManagerWithQueue and not OperationManager because the product
 * is configured to use OperationManagerWithQueue by default, see OperationConfig and ultimately
 * the property applyPermitsToContainers in SdkServerConfig which is set to true by default.
 *
 * <p>This class introduces duplication from OperationManager that could be avoided with an
 * interface for the BinaryDiscoveryDumper.restoreDiscoveryResponses and a small refactor of
 * OperationManager to allow overriding the saving of DRs to the cache.  However, in the review
 * of that code, there was concern expressed around changing the code at all and request for
 * regression testing. This approach was chosen to avoid any perceived risk to the TP code base.
 */
public class CacheOnlyOperationManager extends OperationManagerWithQueue {

    private static final Logger logger = LogManager.getLogger(CacheOnlyOperationManager.class);
    private static final int CACHED_DISCOVERY_RELOADED_MEDIATION_MSG_ID = 555555;

    private CacheOnlyDiscoveryDumper cacheOnlyDiscoveryDumper;
    private final EntityStore entityStore;
    private final TargetDumpingSettings targetDumpingSettings;
    private final SystemNotificationProducer systemNotificationProducer;
    private final DiscoveredGroupUploader discoveredGroupUploader;
    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;
    private final DiscoveredWorkflowUploader discoveredWorkflowUploader;
    private final DerivedTargetParser derivedTargetParser;
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader;
    private final BilledCloudCostUploader billedCloudCostUploader;
    private final AliasedOidsUploader aliasedOidsUploader;
    private final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader;
    private final MatrixInterface matrix;
    private DiscoveryDumper discoveryDumper;
    private final Map<Long, LocalDateTime> discoveryTimes = new HashMap<>();
    private final boolean isCacheDiscoveryModeOffline;

    private final ExecutorService resultExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("cache-only-dr-result-handler")
                            .build());

    /**
     * Create an instance of CacheOnlyOperationManager.
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
     * @param billedCloudCostUploader where Cloud Cost goes.
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
     * @param cacheDiscoveryDumper handles recording discovery responses in topology processor cache.
     * @param enableDiscoveryResponsesCaching whether or not to cache discovery responses.
     * @param licenseCheckClient license check client.
     * @param workflowExecutionTimeoutMillis workflow execution timeout.
     */
    public CacheOnlyOperationManager(@Nonnull final IdentityProvider identityProvider,
            @Nonnull final TargetStore targetStore, @Nonnull final ProbeStore probeStore,
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
            final long discoveryTimeoutSeconds, final long validationTimeoutSeconds,
            final long actionTimeoutSeconds, final long planExportTimeoutSeconds,
            final @Nonnull MatrixInterface matrix,
            final BinaryDiscoveryDumper binaryDiscoveryDumper,
            final CacheOnlyDiscoveryDumper cacheDiscoveryDumper,
            final boolean enableDiscoveryResponsesCaching,
            final LicenseCheckClient licenseCheckClient, final int workflowExecutionTimeoutMillis,
            final boolean isCacheDiscoveryModeOffline) {
        super(identityProvider, targetStore, probeStore, remoteMediationServer, operationListener,
                entityStore, discoveredGroupUploader, discoveredWorkflowUploader,
                discoveredCloudCostUploader, billedCloudCostUploader, aliasedOidsUploader,
                discoveredPlanDestinationUploader, discoveredTemplateDeploymentProfileNotifier,
                entityActionDao, derivedTargetParser, groupScopeResolver, targetDumpingSettings,
                systemNotificationProducer, discoveryQueue, discoveryTimeoutSeconds,
                validationTimeoutSeconds, actionTimeoutSeconds, planExportTimeoutSeconds, matrix,
                binaryDiscoveryDumper, enableDiscoveryResponsesCaching, licenseCheckClient,
                workflowExecutionTimeoutMillis);
        this.cacheOnlyDiscoveryDumper = cacheDiscoveryDumper;
        this.entityStore = entityStore;
        this.targetDumpingSettings = targetDumpingSettings;
        this.systemNotificationProducer = systemNotificationProducer;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.discoveredTemplateDeploymentProfileNotifier = discoveredTemplateDeploymentProfileNotifier;
        this.discoveredWorkflowUploader = discoveredWorkflowUploader;
        this.derivedTargetParser = derivedTargetParser;
        this.discoveredCloudCostUploader = discoveredCloudCostUploader;
        this.billedCloudCostUploader = billedCloudCostUploader;
        this.aliasedOidsUploader = aliasedOidsUploader;
        this.discoveredPlanDestinationUploader = discoveredPlanDestinationUploader;
        this.matrix = matrix;
        this.isCacheDiscoveryModeOffline = isCacheDiscoveryModeOffline;
        try {
            this.discoveryDumper = new DiscoveryDumperImpl(DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY, targetDumpingSettings);
        } catch (IOException e) {
            logger.warn("Failed to initialized discovery dumper; discovery responses will not be dumped", e);
            this.discoveryDumper = null;
        }
    }

    @Override
    public Optional<Discovery> startDiscovery(final long targetId, DiscoveryType discoveryType,
            boolean runNow) throws TargetNotFoundException, ProbeException {
        if (!isUserInitiated()) {
            Target target = targetStore.getTarget(targetId)
                    .orElseThrow(() -> new TargetNotFoundException(targetId));
            logger.info(
                    "Cache only discovery mode enabled, ignoring non user initiated discovery request for target: {} {}",
                    target.getDisplayName(), target.getProbeInfo().getProbeType());
            return Optional.empty();
        } else {
            if (shouldReloadCachedDiscovery(targetId)) {
                Target target = targetStore.getTarget(targetId)
                        .orElseThrow(() -> new TargetNotFoundException(targetId));
                logger.info("Reloading updated cached discovery file for target {}:{}",
                        target.getDisplayName(), target.getProbeInfo().getProbeType());
                Discovery discovery = reloadCachedDiscovery(targetId);
                return Optional.of(discovery);
            } else {
                return super.startDiscovery(targetId, discoveryType, runNow);
            }
        }
    }

    private boolean shouldReloadCachedDiscovery(final long targetId) {
        if (isCacheDiscoveryModeOffline) {
            return true;
        }
        // if the cached discovery last modified timestamp is newer than the last one loaded,
        // it indicates it was modified by hand, so reload it. Otherwise do a new discovery.
        LocalDateTime lastLoaded = discoveryTimes.get(targetId);
        LocalDateTime lastModified = cacheOnlyDiscoveryDumper.getCachedDiscoveryTimestamp(targetId);
        if (lastLoaded != null && lastModified != null && lastModified.isAfter(lastLoaded)) {
            return true;
        }
        return false;
    }

    private Discovery reloadCachedDiscovery(long targetId) throws TargetNotFoundException, ProbeException {
        Target target = targetStore.getTarget(targetId).orElseThrow(() ->
                new TargetNotFoundException(targetId));
        logger.info("Loading cached discovery response for target {}, {}",
                target.getDisplayName(), target.getProbeInfo().getProbeType());
        DiscoveryResponse dr = cacheOnlyDiscoveryDumper.getCachedDiscoveryResponse(target.getId());
        if (dr == null) {
            logger.error("No cached discovery response found for target {}, {}",
                    target.getDisplayName(), target.getProbeInfo().getProbeType());
            return null;
        }
        Discovery discovery = new Discovery(target.getProbeId(), targetId, identityProvider);
        discovery.setMediationMessageId(CACHED_DISCOVERY_RELOADED_MEDIATION_MSG_ID);
        try {
            notifyLoadedDiscovery(discovery, dr).get(20, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error(
                    "Error in notifying the discovery result for target {}  {}",
                    target.getDisplayName(), target.getProbeInfo().getProbeType(), e);
            return null;
        }
        return discovery;
    }

    private boolean isUserInitiated() {
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        for (int i = 0; i < stacktrace.length; i++) {
            if (stacktrace[i].getClassName().equals(OperationController.class.getName())
                    && stacktrace[i].getMethodName().equals("performDiscovery")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Future<?> notifyDiscoveryResult(@Nonnull final Discovery operation,
            @Nonnull final DiscoveryResponse dr) {
        discoveryTimes.put(operation.getTargetId(), LocalDateTime.now());
        // Make the hidden derived targets visible on the DR before processing
        return resultExecutor.submit(() -> {
            processDiscoveryResponse(operation, makeHiddenDerivedTargetsVisible(dr), true);
        });
    }

    @Override
    public Future<?> notifyLoadedDiscovery(@Nonnull Discovery operation,
            @Nonnull DiscoveryResponse dr) {
        discoveryTimes.put(operation.getTargetId(), LocalDateTime.now());
        return resultExecutor.submit(() -> {
            processDiscoveryResponse(operation, makeHiddenDerivedTargetsVisible(dr), false);
        });
    }

    /**
     * Method which replaces the derived targets of a parent target with copies
     * that have hidden set to false.  When using cache only mode, you need to allow the
     * users to manually rediscover ALL targets in the UI, including derived targets that are
     * normally hidden.
     *
     * @param dr The original discovery response
     * @return A new discovery response with all derived targets hidden attrib set to false.
     */
    private DiscoveryResponse makeHiddenDerivedTargetsVisible(DiscoveryResponse dr) {
        List<DerivedTargetSpecificationDTO> derivedTargetSpecs = dr.getDerivedTargetList();
        if (derivedTargetSpecs.size() > 0) {
            DiscoveryResponse.Builder drBuilder = dr.toBuilder();
            drBuilder.clearDerivedTarget();
            for (DerivedTargetSpecificationDTO derivedTargetSpec : derivedTargetSpecs) {
                drBuilder.addDerivedTarget(derivedTargetSpec.toBuilder().setHidden(false));
            }
            return drBuilder.build();
        }
        return dr;
    }

    /*
     * We are re-implementing this method to avoid changing anything in OperationManager for reasons
     * explained in the class javadoc comment above.  This method is copy/pasted with a few minor
     * changes as noted, so that we can compare & keep up with changes in the parent class.
     */
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

        // -- cached discovery mode --
        // Not needed in cached discovery mode since this is a no op for OperationManagerWithQueue
        // which this class extends.
        //releaseSemaphore(discovery.getProbeId(), targetId, discoveryType);

        /*
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
                                                                        .setDuplicationErrorType(
                                                                                DuplicationErrorType.getDefaultInstance())
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
                            // If this is a discovery based on an updated cached file, don't replace the cached file.
                            // It's unnecessary and would update the file timestamp which would prevent real discovery
                            // from ever happening.
                            if (discovery.getMediationMessageId() != CACHED_DISCOVERY_RELOADED_MEDIATION_MSG_ID) {
                                dumpDiscoveryToCache(target.get(), discoveryType, response, probeInfo.get());
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
                                discoveredGroupUploader.setTargetDiscoveredGroups(targetId,
                                        responseUsed.getDiscoveredGroupList());
                                discoveredTemplateDeploymentProfileNotifier.recordTemplateDeploymentInfo(
                                        targetId, response.getEntityProfileList(),
                                        responseUsed.getDeploymentProfileList(),
                                        response.getEntityDTOList());
                                discoveredWorkflowUploader.setTargetWorkflows(targetId, responseUsed.getWorkflowList());
                                if (processDerivedTargets) {
                                    derivedTargetParser.instantiateDerivedTargets(targetId, responseUsed.getDerivedTargetList());
                                }
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
                            final String message =
                                    "Failed to process " + discoveryType + " discovery for target "
                                            + targetId + ", which does not exist. The target may "
                                            + "have been deleted during discovery processing.";
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
        // -- cached discovery mode --
        // Not applicable in cached discovery mode
        // Only activate a pending discovery if the probe is connected
        //if (probeStore.isProbeConnected(discovery.getProbeId())) {
        //    activatePendingDiscovery(targetId, discoveryType);
        //}
    }

    private void dumpDiscoveryToCache(Target target, DiscoveryType discoveryType,
            DiscoveryResponse response, ProbeInfo probeInfo) {
        String displayName = target.getDisplayName();
        String tgId = DiscoveryDumpFilename.sanitize(
                target.getId() + "_" + probeInfo.getProbeType() + "_" + displayName);
        cacheOnlyDiscoveryDumper.dumpDiscovery(tgId, discoveryType, response,
                probeInfo.getAccountDefinitionList());
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

    private void failDiscovery(@Nonnull final Discovery discovery,
            @Nonnull final String failureMessage) {
        // It takes an error to cancel the discovery without processing it
        final ErrorDTO error = ErrorDTO.newBuilder()
                .setSeverity(ErrorSeverity.CRITICAL)
                .setDescription(failureMessage)
                .build();
        operationComplete(discovery, false, Collections.singletonList(error));
    }
}
