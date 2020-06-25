package com.vmturbo.integrations.intersight.targetsync;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.api.AssetApi;
import com.cisco.intersight.client.model.AssetTarget;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * This updater handles Intersight target changes under various scenarios and reports target status
 * back to Intersight.
 */
public class IntersightTargetUpdater {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Object type of the workload optimizer service class in the Intersight model.
     * Needed here because the Intersight Java SDK only casts it as AssetService during
     * deserialization and we compare this string with the object type field to classify it as a
     * workload optimizer service.
     */
    private static final String IWO_SERVICE_OBJECT_TYPE = "asset.WorkloadOptimizerService";

    /**
     * API client to access Intersight.
     */
    private final ApiClient apiClient;

    /**
     * Topology processor client.
     */
    private final TopologyProcessor topologyProcessor;

    /**
     * How long in seconds to hold off target status update since the target is created/modified
     * in Intersight.
     */
    private final long noUpdateOnChangePeriodSeconds;

    /**
     * Construct an updater to handle Intersight target changes.
     *
     * @param apiClient API client to access Intersight
     * @param topologyProcessor the topology processor client
     * @param noUpdateOnChangePeriodSeconds how long to hold off target status update upon target
     *                                      creation/modification
     */
    public IntersightTargetUpdater(@Nonnull final ApiClient apiClient,
                                   @Nonnull final TopologyProcessor topologyProcessor,
                                   final long noUpdateOnChangePeriodSeconds) {
        this.apiClient = Objects.requireNonNull(apiClient);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.noUpdateOnChangePeriodSeconds = noUpdateOnChangePeriodSeconds;
    }

    /**
     * Entry method to handle target updates.
     *
     * @param intersightAssetTarget the {@link AssetTarget} discovered from Intersight
     * @param tpTargetInfo the {@link TargetInfo} from topology processor
     * @throws ApiException when having problems fetching the list of targets from Intersight
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    public void update(@Nonnull final AssetTarget intersightAssetTarget,
                       @Nonnull final TargetInfo tpTargetInfo) throws InterruptedException,
            TopologyProcessorException, CommunicationException, ApiException {
        final WrappedTarget wrappedTarget = new WrappedTarget(intersightAssetTarget, tpTargetInfo);
        // Differentiate various scenarios in updating status for better user experience
        if (wrappedTarget.ifRecentlyCreated(noUpdateOnChangePeriodSeconds)) {
            onRecentlyCreated(wrappedTarget);
        } else if (wrappedTarget.ifRecentlyModified(noUpdateOnChangePeriodSeconds)) {
            onRecentlyModified(wrappedTarget);
        } else {
            onNormalUpdate(wrappedTarget);
        }
    }

    /**
     * Within a configurable period of target creation, we will keep calling validation until
     * validated.  Only when that happens, we will report back the target status.  This is
     * because real target credentials transported to the probe out-of-band via the equinox proxy
     * on the assist may not show up immediately for the probe to consume possibly due to that
     * the credentials volume is cached on the probe pod.
     *
     * @param target the {@link WrappedTarget} which has the combined Intersight and TP target info
     * @throws ApiException when having problems fetching the list of targets from Intersight
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    private void onRecentlyCreated(@Nonnull final WrappedTarget target) throws ApiException,
            InterruptedException, TopologyProcessorException, CommunicationException {
        if (target.isValidated()) {
            onNormalUpdate(target);
            topologyProcessor.discoverTarget(target.tp().getId());
            logger.info("Target {} is recently validated; kicking off a discovery...",
                    target.intersight().getMoid());
        } else {
            topologyProcessor.validateTarget(target.tp().getId());
            logger.info("Target {} is recently added; validating...", target.intersight().getMoid());
        }
    }

    /**
     * Within a configurable period of target modification, we will hold off any update until
     * enough time has elapsed to ensure the target info update will have shown up on the probe pod.
     *
     * @param target the {@link WrappedTarget} which has the combined Intersight and TP target info
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    private void onRecentlyModified(@Nonnull final WrappedTarget target)
            throws TopologyProcessorException, CommunicationException, InterruptedException {
        topologyProcessor.validateTarget(target.tp().getId());
        logger.info("Target {} is recently modified; validating...", target.intersight().getMoid());
    }

    /**
     * This executes the normal periodic target status update back to Intersight beyond the target
     * creation/modification periods.
     *
     * @param target the {@link WrappedTarget} which has the combined Intersight and TP target info
     * @throws ApiException thrown if updating the target status returns an API exception
     */
    private void onNormalUpdate(@Nonnull final WrappedTarget target) throws ApiException {
        final long changedCount = Optional.ofNullable(target.intersight().getServices())
                .orElse(Collections.emptyList()).stream()
                .filter(service -> Objects.equals(IWO_SERVICE_OBJECT_TYPE, service.getObjectType()))
                .filter(target::needsUpdate)
                .map(service -> service.status(target.getNewStatusEnum(service))) // update status enum
                .map(service -> service.statusErrorReason(target.getNewStatusErrorReason(service))) // update err string
                .count();
        if (changedCount > 0) {
            final AssetApi assetApi = new AssetApi(apiClient);
            try {
                assetApi.updateAssetTarget(target.intersight().getMoid(), target.intersight(), null);
            } catch (ApiException e) {
                logger.error("Attempted to update target {} status to {} but getting an error: {}",
                        target.intersight().getMoid(), target.tp().getStatus(), e.getResponseBody());
                throw e;
            }
            logger.info("Updated target {} status to {}", target.intersight().getMoid(), target.tp().getStatus());
        }
    }
}
