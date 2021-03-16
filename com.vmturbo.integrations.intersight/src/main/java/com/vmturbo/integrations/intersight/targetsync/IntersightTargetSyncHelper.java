package com.vmturbo.integrations.intersight.targetsync;

import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_ADDRESS;
import static com.vmturbo.integrations.intersight.targetsync.IntersightTargetConverter.INTERSIGHT_PORT;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetTarget.TargetTypeEnum;
import com.cisco.intersight.client.model.MoMoRef;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

/**
 * A helper class to assist Intersight target sync service, by caching the current set of probe
 * infos and target infos from Topology Processor, as well as keeping track of the set of stale
 * targets to remove after sync.
 */
public class IntersightTargetSyncHelper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The connection to access the Intersight instance.
     */
    private final IntersightConnection intersightConnection;

    /**
     * The entry point to access topology processor APIs.
     */
    private final TopologyProcessor topologyProcessor;

    /**
     * Cache the probe type to {@link ProbeInfo} map.
     */
    private final Map<String, ProbeInfo> probesByType;

    /**
     * Cache the probe id to the list of {@link TargetInfo}s map.
     */
    private final Map<Long, List<TargetInfo>> targetsByProbeId;

    /**
     * This keeps track of the set of stale targets to remove after sync.
     */
    private final Set<TargetInfo> staleTargets;

    /**
     * Construct a helper instance to assist target sync.
     *
     * @param intersightConnection provides connection to the Intersight instance
     * @param topologyProcessor    the entry point to access topology processor APIs
     * @throws CommunicationException when having problems communicating with the topology processor
     */
    protected IntersightTargetSyncHelper(@Nonnull IntersightConnection intersightConnection,
            @Nonnull final TopologyProcessor topologyProcessor) throws CommunicationException {
        this.intersightConnection = Objects.requireNonNull(intersightConnection);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);

        final Set<ProbeInfo> probeInfos = topologyProcessor.getAllProbes();
        probesByType = probeInfos.stream().collect(Collectors.toMap(ProbeInfo::getType,
                Function.identity(), (p1, p2) -> p1));
        final Set<Long> cloudNativeProbeIds = probeInfos.stream()
                .filter(probe -> Objects.equals(ProbeCategory.CLOUD_NATIVE.getCategory(),
                        probe.getCategory()))
                .map(ProbeInfo::getId)
                .collect(Collectors.toSet());
        final Set<TargetInfo> targets = topologyProcessor.getAllTargets()
                .stream()
                .filter(t -> !t.isHidden())
                .filter(t -> !t.isReadOnly())
                .filter(t -> !cloudNativeProbeIds.contains(t.getProbeId()))
                .collect(Collectors.toSet());
        staleTargets = new HashSet<>(targets);
        targetsByProbeId = targets.stream().collect(Collectors.groupingBy(TargetInfo::getProbeId));
        if (logger.isDebugEnabled()) {
            dumpAllTargetInfos();
        }
    }

    /**
     * Dump all target infos.
     */
    private void dumpAllTargetInfos() {
        logger.debug("Existing targets that are not hidden, nor read-only, nor cloud native");
        for (Entry<Long, List<TargetInfo>> entry : targetsByProbeId.entrySet()) {
            logger.debug("Grouped existing targets for {} ", entry.getKey());
            dumpTargetInfo(entry.getValue());
        }
    }

    /**
     * Dump target infos for a collection of targets.
     *
     * @param targets a collection of {@link TargetInfo}s
     */
    private void dumpTargetInfo(final Collection<TargetInfo> targets) {
        for (TargetInfo targetInfo : targets) {
            long targetProbeId = targetInfo.getProbeId();
            long targetId = targetInfo.getId();
            logger.debug("TargetInfo Probe Id {} Target Id {} DisplayName {}", targetProbeId,
                    targetId, targetInfo.getDisplayName());
        }
    }

    /**
     * Add a target corresponding to the Intersight endpoint to discover Intersight-hosted
     * topologies such as UCS/Hyperflex entities.
     *
     * @throws CommunicationException when having problems communicating with the topology processor
     */
    protected void addIntersightEndpointAsTarget() throws CommunicationException {
        final SDKProbeType intersightServerProbeType = SDKProbeType.INTERSIGHT;
        final ProbeInfo intersightProbeInfo =
                probesByType.get(intersightServerProbeType.getProbeType());
        if (intersightProbeInfo == null) {
            logger.info("No probe found for probe type {}; can't add Intersight target: {} [{}]",
                    intersightServerProbeType, intersightConnection.getAddress(),
                    intersightConnection.getClientId());
        }

        // Check if an Intersight server target already exists for this namespace
        final TargetInfo existingIntersightTargetInfo =
                getExistingIntersightTargetInfo(intersightProbeInfo);
        if (existingIntersightTargetInfo != null) {
            logger.debug("Found existing Intersight Server target {} with clientId {} ",
                    intersightConnection.getAddress(), intersightConnection.getClientId());
            // Target was already there
            staleTargets.remove(existingIntersightTargetInfo);
        } else {
            TargetData intersightTargetData =
                    IntersightTargetConverter.inputFields(intersightConnection, intersightProbeInfo);
            logger.info("Adding Intersight target {}:{} [{}]", intersightConnection.getAddress(),
                    intersightConnection.getPort(), intersightConnection.getClientId());
            try {
                // No need to remove target since staleTargets.remove(intersightTargetData);
                final long targetId = topologyProcessor.addTarget(intersightProbeInfo.getId(),
                        intersightTargetData);
            } catch (TopologyProcessorException e) {
                logger.error("Error adding or updating target {} of probe type {} due to: {}",
                        intersightTargetData, intersightServerProbeType, e);
            }
        }
    }

    /**
     * Fetch the Intersight {@link TargetInfo} from the topology processor if exists, or null
     * will be returned.
     *
     * @param intersightProbeInfo the Intersight probe info
     * @return the found {@link TargetInfo} or null if none found
     */
    @Nullable
    private TargetInfo getExistingIntersightTargetInfo(@Nonnull final ProbeInfo intersightProbeInfo) {
        Objects.requireNonNull(intersightProbeInfo);
        final List<TargetInfo> intersightTargets = targetsByProbeId.get(intersightProbeInfo.getId());
        logger.debug("Looking for ProbInfo for Id {} Found {} Intersight targets",
                intersightProbeInfo.getId(),
                !CollectionUtils.isEmpty(intersightTargets) ? intersightTargets.size() : 0);
        final List<String> targetIdFields = intersightProbeInfo.getIdentifyingFields();
        if (!CollectionUtils.isEmpty(intersightTargets)) {
            for (TargetInfo isTargetInfo : intersightTargets) {
                boolean matchedAddress = false;
                boolean matchedPort = false;
                for (AccountValue accountValue : isTargetInfo.getAccountData()) {
                    if (targetIdFields.contains(accountValue.getName())
                            && INTERSIGHT_ADDRESS.equals(accountValue.getName())
                            && Objects.equals(intersightConnection.getAddress(), accountValue.getStringValue())) {
                        matchedAddress = true;
                    }
                    if (targetIdFields.contains(accountValue.getName())
                            && INTERSIGHT_PORT.equals(accountValue.getName())
                            && Objects.equals(intersightConnection.getPort().toString(), accountValue.getStringValue())) {
                        matchedPort = true;
                    }
                }
                if (matchedAddress && matchedPort) {
                    return isTargetInfo;
                }
            }
        }
        return null;
    }

    /**
     * Loop through the list of {@link AssetTarget}s discovered from the Intersight endpoint and
     * synchronize them with what's in the topology processor.
     *
     * @param noUpdateOnChangePeriodSeconds how long to hold off target status update upon target
     *                                      creation/modification
     * @param injectAssistId                whether to inject the assist moid when calling the
     *                                      topology processor API to add targets
     * @throws IOException when having problems acquiring an auth token from the Intersight instance
     * @throws ApiException when having problems fetching the list of targets from Intersight
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    protected void syncAssetTargets(final long noUpdateOnChangePeriodSeconds,
            final boolean injectAssistId) throws InterruptedException, IOException, ApiException {
        // Technically we only need Moid, TargetType, Assist and Parent.  We are getting CreateTime
        // and ModTime too to carve out appropriate actions correspondingly to achieve better user
        // experience.  We are also getting "Services" and "Status" because:
        // "Services": updating status requires passing back the entire Services portion;
        //             get it and retain the other parts unchanged
        // "Status": this is a top-level status field and is an enum and read-only;
        //           To ensure accepted by the server, this has to be passed back unchanged.
        final String select = "$select=Moid,TargetType,Services,Status,CreateTime,ModTime,Assist,Parent";
        final List<AssetTarget> assetTargets =
                new IntersightAssetTargetQuery(select).getAllQueryInstancesOrElseThrow(intersightConnection);
        final Map<String, Optional<String>> assistToDeviceMap = getAssistToDeviceMap(assetTargets);
        final IntersightTargetStatusUpdater targetStatusUpdater = new IntersightTargetStatusUpdater(
                intersightConnection.getApiClient(), topologyProcessor, noUpdateOnChangePeriodSeconds);
        for (final AssetTarget assetTarget : assetTargets) {
            for (final SDKProbeType probeType : IntersightTargetConverter.findProbeType(assetTarget)) {
                final ProbeInfo probeInfo = probesByType.get(probeType.getProbeType());
                if (probeInfo == null) {
                    logger.error("No probe found for probe type {}; can't process asset target {}",
                            probeType, assetTarget.getMoid());
                    continue;
                }
                try {
                    TargetInfo targetInfo = findTargetInfo(assetTarget, probeInfo);
                    final Optional<String> assistId = injectAssistId
                            ? getAssistDeviceMoid(assetTarget, assistToDeviceMap) : Optional.empty();
                    if (targetInfo == null) {
                        final TargetInputFields targetInputFields =
                                IntersightTargetConverter.inputFields(assetTarget, assistId, probeInfo);
                        final long targetId = topologyProcessor.addTarget(probeInfo.getId(), targetInputFields);
                        logger.info("Added {} target {}", probeType, assetTarget.getMoid());
                        targetInfo = topologyProcessor.getTarget(targetId);
                    } else {
                        staleTargets.remove(targetInfo);
                        updateAssistIdIfNeeded(assetTarget, targetInfo, probeInfo, assistId);
                    }
                    if (probeInfo.getCreationMode() != CreationMode.DERIVED) {
                        // skip derived targets as it should be the same as the original target;
                        // sometimes derived targets such as storage browsing has a much
                        // bigger discovery interval, so its status might be stale.
                        targetStatusUpdater.update(assetTarget, targetInfo);
                    }
                } catch (TopologyProcessorException | ApiException | CommunicationException | RuntimeException | JsonProcessingException e) {
                    logger.error(
                            "Error adding or updating status for target {} of probe type {} due to: {}",
                            assetTarget.getMoid(), probeType, e);
                }
            }
        }
    }

    /**
     * Construct a map from the assist MOID to the assist device MOID.  The latter is available
     * on the assist which will be used in IWO probe registration, and thus is the
     * "communicationChannelBinding" we need to match when creating a target in topology
     * processor.  When creating a target, we only have the {@link AssetTarget} object, which
     * contains the assist MOID but not the assist device MOID.  This map helps look up the assist
     *
     * @param assetTargets the list of {@link AssetTarget}s
     * @return the map from assist MOID to device MOID
     */
    @Nonnull
    private Map<String, Optional<String>> getAssistToDeviceMap(@Nonnull List<AssetTarget> assetTargets) {
        return Objects.requireNonNull(assetTargets).stream()
                .filter(t -> TargetTypeEnum.INTERSIGHTASSIST.equals(t.getTargetType()))
                .collect(Collectors.toMap(t -> t.getMoid(),
                        t -> Optional.ofNullable(t).map(AssetTarget::getParent).map(MoMoRef::getMoid)));
    }

    /**
     * Retrieve the assist device MOID given the {@link AssetTarget} and the previously
     * constructed assist-target-id-to-device-id map.
     *
     * @param assetTarget the input {@link AssetTarget}
     * @param assistToDeviceMap the previously constructed assist-target-id-to-device-id map
     * @return the found assist device MOID in {@link Optional} form
     */
    private Optional<String> getAssistDeviceMoid(@Nonnull AssetTarget assetTarget,
            @Nonnull Map<String, Optional<String>> assistToDeviceMap) {
        return Optional.ofNullable(assetTarget).map(AssetTarget::getAssist).map(MoMoRef::getMoid)
                .flatMap(assistToDeviceMap::get);
    }

    /**
     * Find the {@link TargetInfo} for the given {@link AssetTarget} from Intersight.
     *
     * @param assetTarget the input asset target from Intersight for which the corresponding
     *                    target info to be found
     * @param probeInfo   the probe info associated with the target
     * @return the found {@link TargetInfo} or null if not found
     */
    @Nullable
    private TargetInfo findTargetInfo(@Nonnull final AssetTarget assetTarget,
            @Nonnull final ProbeInfo probeInfo) {
        final List<String> targetIdFields = probeInfo.getIdentifyingFields();
        final long probeId = probeInfo.getId();
        final List<TargetInfo> targets = targetsByProbeId.get(probeId);
        if (targets == null) {
            return null;
        }
        for (final TargetInfo target : targets) {
            if (target.getAccountData().stream()
                    .filter(ac -> targetIdFields.contains(ac.getName()))
                    .anyMatch(ac -> Objects.equals(assetTarget.getMoid(), ac.getStringValue()))) {
                return target;
            }
        }
        return null;
    }

    /**
     * Call topology processor to update the target with the new assist id if changed from
     * Intersight.
     *
     * @param assetTarget the target in Intersight {@link AssetTarget} data structure
     * @param targetInfo the target info in topology processor API data structure
     * @param probeInfo the probe info in topology processor API data structure
     * @param assistId the assist device MOID in {@link Optional} form or empty if no assist
     *                 associated with this target
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws TopologyProcessorException when topology processor sends back an exception
     */
    private void updateAssistIdIfNeeded(@Nonnull final AssetTarget assetTarget,
            @Nonnull final TargetInfo targetInfo, @Nonnull final ProbeInfo probeInfo,
            @Nonnull Optional<String> assistId)
            throws CommunicationException, TopologyProcessorException {
        Objects.requireNonNull(assetTarget);
        Objects.requireNonNull(targetInfo);
        Objects.requireNonNull(probeInfo);
        Objects.requireNonNull(assistId);

        final Optional<String> channel = targetInfo.getCommunicationBindingChannel();
        if (!Objects.equals(assistId, channel)) {
            // Update associated assist id in the target since it's been changed;
            // convert Optional.empty() to Optional.of("") which is the way to wipe it out
            final TargetInputFields targetInputFields = IntersightTargetConverter.inputFields(
                    assetTarget, Optional.of(assistId.orElse("")), probeInfo);
            topologyProcessor.modifyTarget(targetInfo.getId(), targetInputFields);
            logger.info("Updated {} target {} with {}", probeInfo.getType(), assetTarget.getMoid(),
                    assistId.map(id -> "new assist " + id).orElse("no assist"));
        }
    }

    /**
     * Remove all stale targets that are no longer discovered from Intersight.
     *
     * @throws CommunicationException when having problems communicating with the topology processor
     */
    protected void removeStaleTargets() throws CommunicationException {
        for (final TargetInfo target : staleTargets) {
            try {
                topologyProcessor.removeTarget(target.getId());
                logger.info("Removed target {}", target.getDisplayName());
            } catch (TopologyProcessorException e) {
                logger.error("Error removing target {} due to: {}", target, e.getMessage());
            }
        }
    }
}
