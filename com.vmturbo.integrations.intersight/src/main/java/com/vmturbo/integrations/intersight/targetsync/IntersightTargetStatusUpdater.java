package com.vmturbo.integrations.intersight.targetsync;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.api.AssetApi;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetWorkloadOptimizerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * This updater handles Intersight target changes under various scenarios and reports target status
 * back to Intersight.
 */
public class IntersightTargetStatusUpdater {
    private static final Logger logger = LogManager.getLogger();

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
    public IntersightTargetStatusUpdater(@Nonnull final ApiClient apiClient,
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
     * @throws JsonProcessingException thrown if there is a problem serializing the asset target
     */
    public void update(@Nonnull final AssetTarget intersightAssetTarget,
                       @Nonnull final TargetInfo tpTargetInfo)
            throws InterruptedException, TopologyProcessorException, CommunicationException,
            ApiException, JsonProcessingException {
        final IntersightWrappedTarget
                wrappedTarget = new IntersightWrappedTarget(intersightAssetTarget, tpTargetInfo);
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
     * @param target the {@link IntersightWrappedTarget} which has the combined Intersight and TP target info
     * @throws ApiException when having problems fetching the list of targets from Intersight
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     * @throws JsonProcessingException thrown if there is a problem serializing the asset target
     */
    private void onRecentlyCreated(@Nonnull final IntersightWrappedTarget target)
            throws ApiException, InterruptedException, TopologyProcessorException,
            CommunicationException, JsonProcessingException {
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
     * @param target the {@link IntersightWrappedTarget} which has the combined Intersight and TP target info
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    private void onRecentlyModified(@Nonnull final IntersightWrappedTarget target)
            throws TopologyProcessorException, CommunicationException, InterruptedException {
        topologyProcessor.validateTarget(target.tp().getId());
        logger.info("Target {} is recently modified; validating...", target.intersight().getMoid());
    }

    /**
     * This executes the normal periodic target status update back to Intersight beyond the target
     * creation/modification periods.
     *
     * @param target the {@link IntersightWrappedTarget} which has the combined Intersight and TP target info
     * @throws ApiException thrown if updating the target status returns an API exception
     * @throws JsonProcessingException thrown if there is a problem serializing the asset target
     */
    private void onNormalUpdate(@Nonnull final IntersightWrappedTarget target)
            throws ApiException, JsonProcessingException {
        final long changedCount = Optional.ofNullable(target.intersight().getServices())
                .orElse(Collections.emptyList()).stream()
                .filter(AssetWorkloadOptimizerService.class::isInstance)
                .map(AssetWorkloadOptimizerService.class::cast)
                .filter(target::needsUpdate)
                .map(service -> service.status(target.getNewStatusEnum(service))) // update status enum
                .map(service -> service.statusErrorReason(target.getNewStatusErrorReason(service))) // update err string
                .count();
        if (changedCount > 0) {
            final ObjectMapper jsonMapper = apiClient.getJSON().getMapper();
            final String targetJsonString = jsonMapper.writeValueAsString(target.intersight());
            final JSONObject editableJson = new JSONObject(targetJsonString);
            editableJson.remove("CreateTime");
            editableJson.remove("ModTime");
            final AssetTarget editableTarget = jsonMapper.readValue(editableJson.toString(),
                    AssetTarget.class);
            final AssetApi assetApi = new AssetApi(apiClient);
            // AssetTarget ManagementLocation is a enum with a default value "Unknown".
            // This value wont be used for updating AssetTarget. We need to make Target
            // ManagementLocation to be null for the AssetTarget other properties to be updated.
            editableTarget.setManagementLocation(null);

            final String moid = target.intersight().getMoid();
            try {
                assetApi.updateAssetTarget(moid, editableTarget, null);
            } catch (ApiException e) {
                logger.error("Attempted to update target {} status to {} but getting an error: {}",
                        moid, target.tp().getStatus(), e.getResponseBody());
                throw e;
            }
            logger.info("Updated target {} status to {}", moid, target.tp().getStatus());
        }
    }
}
