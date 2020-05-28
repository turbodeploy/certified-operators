package com.vmturbo.integrations.intersight.targetsync;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.api.AssetApi;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetTarget.TargetTypeEnum;
import com.cisco.intersight.client.model.AssetTargetList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.mediation.connector.intersight.IntersightDefaultQueryParameters;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;

/**
 * {@link IntersightTargetSyncService} checks on the specified Intersight instance and
 * synchronize target infos between Intersight and topology processor.
 */
public class IntersightTargetSyncService implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private static final String INTERSIGHT_TRACEID =
            "x-starship-traceid";

    /**
     * The connection to access the Intersight instance.
     */
    private final IntersightConnection intersightConnection;

    /**
     * The entry point to access topology processor APIs.
     */
    private final TopologyProcessor topologyProcessor;

    /**
     * How long in seconds to hold off target status update since the target is created/modified
     * in Intersight.
     */
    private final long noUpdateOnChangePeriodSeconds;

    /**
     * Create a {@link IntersightTargetSyncService} object that will check on the specified Intersight instance
     * and synchronize targets between Intersight and our topology processor.
     *
     * @param intersightConnection provides connection to the Intersight instance
     * @param topologyProcessor    the entry point to access topology processor APIs
     * @param noUpdateOnChangePeriodSeconds how long to hold off target status update upon target
     *                                      creation/modification
     */
    public IntersightTargetSyncService(@Nonnull IntersightConnection intersightConnection,
                                       @Nonnull TopologyProcessor topologyProcessor,
                                       long noUpdateOnChangePeriodSeconds) {
        this.intersightConnection = Objects.requireNonNull(intersightConnection);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.noUpdateOnChangePeriodSeconds = noUpdateOnChangePeriodSeconds;
    }

    @Override
    public void run() {
        try {
            syncTargets();
        } catch (Throwable t) {
            logger.error("Intersight target sync did not complete due to error: ", t);
        }
    }

    /**
     * Discover targets from Intersight and sync them to what's in the topology processor.
     *
     * @throws IOException when having problems acquiring an auth token from the Intersight instance
     * @throws ApiException when having problems fetching the list of targets from Intersight
     * @throws CommunicationException when having problems communicating with the topology processor
     * @throws InterruptedException when interrupted while waiting for topology processor to respond
     */
    private void syncTargets() throws IOException, CommunicationException, ApiException,
            InterruptedException {
        // For looking up the probe below to add targets which requires the probe id
        final Map<String, ProbeInfo> probesByType = topologyProcessor.getAllProbes().stream()
                .collect(Collectors.toMap(ProbeInfo::getType, Function.identity(), (p1, p2) -> p1));

        final Set<TargetInfo> targets = topologyProcessor.getAllTargets().stream()
                .filter(t -> !t.isHidden()).collect(Collectors.toSet());
        final Set<TargetInfo> staleTargets = new HashSet<>(targets);
        final Map<Long, List<TargetInfo>> targetsByProbeId = targets.stream()
                .collect(Collectors.groupingBy(TargetInfo::getProbeId));

        final ApiClient apiClient = intersightConnection.getApiClient();
        final AssetApi assetApi = new AssetApi(apiClient);
        // Technically we only need Moid and TargetType.  We are getting CreateTime and ModTime
        // too to carve out appropriate actions correspondingly to achieve better user experience.
        // We are also getting "Services" and "Status" because:
        // "Services": updating status requires passing back the entire Services portion;
        //             get it and retain the other parts unchanged
        // "Status": this is a top-level status field and is an enum and read-only;
        //           To ensure accepted by the server, this has to be passed back unchanged.
        final String select = "$select=Moid,TargetType,Services,Status,CreateTime,ModTime";
        AssetTargetList assetTargetList =  null;
        try {
            assetTargetList = assetApi.getAssetTargetList(
                    IntersightDefaultQueryParameters.$filter,
                    IntersightDefaultQueryParameters.$orderby,
                    IntersightDefaultQueryParameters.$top,
                    IntersightDefaultQueryParameters.$skip,
                    select,
                    IntersightDefaultQueryParameters.$expand,
                    IntersightDefaultQueryParameters.$apply,
                    IntersightDefaultQueryParameters.$count,
                    IntersightDefaultQueryParameters.$inlinecount,
                    IntersightDefaultQueryParameters.$at,
                    IntersightDefaultQueryParameters.$tags);
        } catch (ApiException e) {
            logger.error("Error Getting Targets using Intersight API. Query TraceId {} ",
                    e.getResponseHeaders() != null ?
                            e.getResponseHeaders().get(INTERSIGHT_TRACEID) : "Unknown");
            throw e;
        }

        if (assetTargetList != null && assetTargetList.getResults() != null) {
            final IntersightTargetUpdater targetUpdater = new IntersightTargetUpdater(apiClient,
                    topologyProcessor, noUpdateOnChangePeriodSeconds);
            for (final AssetTarget assetTarget : assetTargetList.getResults()) {
                final SDKProbeType probeType = findProbeType(assetTarget);
                if (probeType == null) {
                    continue;
                }
                final ProbeInfo probeInfo = probesByType.get(probeType.getProbeType());
                if (probeInfo == null) {
                    logger.info("No probe found for probe type {}; can't process asset target {}",
                            probeType, assetTarget.getMoid());
                    continue;
                }
                try {
                    TargetInfo targetInfo = findTargetInfo(assetTarget, probeInfo, targetsByProbeId);
                    if (targetInfo == null) {
                        final TargetData targetData = new AssetTargetData(assetTarget, probeInfo);
                        final long targetId = topologyProcessor.addTarget(probeInfo.getId(), targetData);
                        logger.info("Added {} target {}", probeType, assetTarget.getMoid());
                        targetInfo = topologyProcessor.getTarget(targetId);
                    } else {
                        staleTargets.remove(targetInfo);
                    }
                    targetUpdater.update(assetTarget, targetInfo);
                } catch (TopologyProcessorException | ApiException | RuntimeException e) {
                    logger.error("Error adding or updating status for target {} of probe type {} due to: {}",
                            assetTarget.getMoid(), probeType, e);
                }
            }
        }

        // remove targets no longer discovered
        for (final TargetInfo target : staleTargets) {
            try {
                topologyProcessor.removeTarget(target.getId());
                logger.info("Removed target {}", target.getDisplayName());
            } catch (TopologyProcessorException e) {
                logger.error("Error removing target {} due to: {}", target, e.getMessage());
            }
        }
    }

    /**
     * Find the {@link TargetInfo} for the given {@link AssetTarget} from Intersight.
     *
     * @param assetTarget the input asset target from Intersight for which the corresponding
     * target info to be found
     * @param probeInfo the probe info associated with the target
     * @param targetsByProbeId the targets organized by probe id
     * @return the found {@link TargetInfo} or null if not found
     */
    @Nullable
    private static TargetInfo findTargetInfo(@Nonnull final AssetTarget assetTarget,
                                             @Nonnull final ProbeInfo probeInfo,
                                             @Nonnull final Map<Long, List<TargetInfo>> targetsByProbeId) {
        final List<String> targetIdFields = probeInfo.getIdentifyingFields();
        final long probeId = probeInfo.getId();
        final List<TargetInfo> targets = targetsByProbeId.get(probeId);
        if (targets == null) {
            return null;
        }
        for (final TargetInfo target : targets) {
            if (target.getAccountData().stream().filter(ac -> targetIdFields.contains(ac.getName()))
                    .anyMatch(ac -> Objects.equals(assetTarget.getMoid(), ac.getStringValue()))) {
                return target;
            }
        }
        return null;
    }

    /**
     * Find the corresponding {@link SDKProbeType} for the given {@link AssetTarget} from
     * Intersight.  Maybe should make this a config map?
     *
     * @param assetTarget the {@link AssetTarget} from Intersight
     * @return the corresponding {@link SDKProbeType} or null if no corresponding probe found
     */
    @Nullable
    private static SDKProbeType findProbeType(@Nonnull final AssetTarget assetTarget) {
        final TargetTypeEnum targetType = Objects.requireNonNull(assetTarget).getTargetType();
        if (targetType == null) {
            logger.warn("Null Intersight target type in asset.Target {}", assetTarget.getMoid());
            return null;
        }
        switch (targetType) {
            case VMWAREVCENTER:
                return SDKProbeType.VCENTER;
            case APPDYNAMICS:
                return SDKProbeType.APPDYNAMICS;
            case PURESTORAGEFLASHARRAY:
                return SDKProbeType.PURE;
            case NETAPPONTAP:
                return SDKProbeType.NETAPP;
            case EMCSCALEIO:
                return SDKProbeType.SCALEIO;
            case EMCVMAX:
                return SDKProbeType.VMAX;
            case EMCVPLEX:
                return SDKProbeType.VPLEX;
            case MICROSOFTSQLSERVER:
                return SDKProbeType.MSSQL;
            case MICROSOFTAZURE:
                return SDKProbeType.AZURE;
            case MICROSOFTHYPERV:
                return SDKProbeType.HYPERV;
            case DYNATRACE:
                return SDKProbeType.DYNATRACE;
            default:
                logger.warn("Unsupported Intersight target type {} in asset.Target {}",
                        assetTarget.getTargetType(), assetTarget.getMoid());
                return null;
        }
    }

    /**
     * {@link AssetTargetData} implements {@link TargetData} by converting an input
     * {@link AssetTarget} into a set of {@link AccountValue}s.
     */
    private static class AssetTargetData implements TargetData {
        private final AssetTarget assetTarget;
        private final ProbeInfo probeInfo;

        /**
         * Construct an {@link AssetTargetData} given the {@link AssetTarget}.
         *
         * @param assetTarget the input {@link AssetTarget}
         * @param probeInfo the corresponding {@link ProbeInfo}
         */
        AssetTargetData(@Nonnull final AssetTarget assetTarget, @Nonnull final ProbeInfo probeInfo) {
            this.assetTarget = Objects.requireNonNull(assetTarget);
            this.probeInfo = Objects.requireNonNull(probeInfo);
        }

        @Nonnull
        @Override
        public Set<AccountValue> getAccountData() {
            return probeInfo.getAccountDefinitions().stream()
                    // filter all proxy fields as Intersight has its own http proxy
                    .filter(accountDefEntry -> !accountDefEntry.getName().startsWith("proxy"))
                    .map(accountDefEntry -> {
                        final String name = accountDefEntry.getName();
                        final String value;
                        // replace all string id fields with the target moid, assuming there
                        // is at least one such field; for all other fields, try to fill in
                        // something legal
                        if (probeInfo.getIdentifyingFields().contains(name)
                                && accountDefEntry.getValueType() == AccountFieldValueType.STRING) {
                            value = assetTarget.getMoid();
                        } else if (accountDefEntry.getDefaultValue() != null) {
                            value = accountDefEntry.getDefaultValue();
                        } else if (accountDefEntry.getAllowedValues() != null &&
                                accountDefEntry.getAllowedValues().size() > 0) {
                            value = accountDefEntry.getAllowedValues().get(0);
                        } else {
                            switch (accountDefEntry.getValueType()) {
                                case BOOLEAN:
                                    value = "false";
                                    break;
                                case NUMERIC:
                                    value = "0";
                                    break;
                                default:
                                    value = "";
                                    break;
                            }
                        }
                        return new InputField(name, Objects.toString(value, ""), Optional.empty());
                    }).collect(Collectors.toSet());
        }
    }
}
