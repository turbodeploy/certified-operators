package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.translation.batch.translator.CloudMoveBatchTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

/**
 * A cache inside the action orchestrator that keepts the action capability objects for all
 * probes registered with the topology processor.
 * <p>
 * We need easy access to action capability information when resolving support levels for actions
 * as they come in to the market - and for determining the target to execute an action against.
 * This {@link ProbeCapabilityCache} ensures that we avoid making multiple RPC calls to the
 * topology processor. It also provides utility methods for working with action capabilities
 * (see: {@link CachedCapabilities}.
 */
public class ProbeCapabilityCache implements ProbeListener {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    private final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesBlockingStub;

    private final CachedCapabilitiesFactory cachedCapabilitiesFactory;

    @GuardedBy("cacheLock")
    private Map<Long, Long> targetToProbe = new HashMap<>();

    @GuardedBy("cacheLock")
    private Map<Long, ProbeCategory> probeInfosById = new HashMap<>();

    @GuardedBy("cacheLock")
    private Map<Long, List<ProbeActionCapability>> capabilitiesByProbeId = new HashMap<>();

    @GuardedBy("cacheLock")
    private CachedCapabilities cachedCapabilities = null;

    private final Object cacheLock = new Object();

    ProbeCapabilityCache(@Nonnull final TopologyProcessor topologyProcessor,
                         @Nonnull final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesBlockingStub) {
        this(topologyProcessor,
            actionCapabilitiesBlockingStub,
            new CachedCapabilitiesFactory(new CapabilityMatcher()));
    }

    @VisibleForTesting
    ProbeCapabilityCache(@Nonnull final TopologyProcessor topologyProcessor,
                         @Nonnull final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesBlockingStub,
                         @Nonnull final CachedCapabilitiesFactory cachedCapabilitiesFactory) {
        this.topologyProcessor = topologyProcessor;
        this.actionCapabilitiesBlockingStub = actionCapabilitiesBlockingStub;
        this.cachedCapabilitiesFactory = cachedCapabilitiesFactory;

        topologyProcessor.addProbeListener(this);
    }

    /**
     * Get the current {@link CachedCapabilities}. If the cache is not initialized yet, this call
     * will do a full refresh (involving RPC calls to the TP). Otherwise this call will be very quick.
     *
     * @return The {@link CachedCapabilities}.
     */
    @Nonnull
    public CachedCapabilities getCachedCapabilities() {
        synchronized (cacheLock) {
            if (this.cachedCapabilities == null) {
                return fullRefresh();
            } else {
                return cachedCapabilities;
            }
        }
    }

    /**
     * Do a full refresh of the cache. This will do the necessary remote calls to the topology
     * processor, and return the most up-do-date {@link CachedCapabilities}.
     *
     * @return The refreshed {@link CachedCapabilities}. If there is an error communicating with
     *         the topology processor, returns the previous {@link CachedCapabilities}.
     */
    @Nonnull
    public CachedCapabilities fullRefresh() {
        // It's possible - though not likely - for two threads to simultaneously call a refresh.
        // We could have an extra lock to ensure only one active refresh is happening at a time,
        // and to avoid a re-refresh, but refreshes should be fast, so it's okay to do two
        // of them consecutively as long as the internal state (guarded by the cache lock) is
        // always valid.
        try (DataMetricTimer timer = Metrics.UPDATE_TIME.startTimer()) {
            final Map<Long, ProbeCategory> newProbeCategoriesById =
                topologyProcessor.getAllProbes().stream()
                    .collect(Collectors.toMap(ProbeInfo::getId,
                        probeInfo -> ProbeCategory.create(probeInfo.getCategory())));
            final Map<Long, List<ProbeActionCapability>> newCapabilitiesByProbeId = new HashMap<>(newProbeCategoriesById.size());
            actionCapabilitiesBlockingStub.listProbeActionCapabilities(
                ListProbeActionCapabilitiesRequest.getDefaultInstance()).forEachRemaining(probeActionCapabilities -> {
                newCapabilitiesByProbeId.put(probeActionCapabilities.getProbeId(), probeActionCapabilities.getActionCapabilitiesList());
            });
            final Map<Long, Long> newTargetToProbeId =
                topologyProcessor.getAllTargets().stream()
                    .collect(Collectors.toMap(TargetInfo::getId, TargetInfo::getProbeId));
            // Synchronize AFTER doing all the remote calls, so concurrent users can still
            // access the cache while we're refreshing.
            synchronized (cacheLock) {
                this.probeInfosById = newProbeCategoriesById;
                this.capabilitiesByProbeId = newCapabilitiesByProbeId;
                this.targetToProbe = newTargetToProbeId;

                this.cachedCapabilities = rebuildCapabilities();
            }

            return this.cachedCapabilities;
        } catch (CommunicationException | StatusRuntimeException e) {
            logger.error("Failed to refresh capability cache due to exception: {}",
                e.getMessage());
            // If this happens, we most likely couldn't reach the topology processor.
            if (cachedCapabilities == null) {
                return cachedCapabilitiesFactory.newCapabilities(
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    Collections.emptyMap());
            } else {
                return cachedCapabilities;
            }
        }
    }

    @Nonnull
    private CachedCapabilities rebuildCapabilities() {
        return cachedCapabilitiesFactory.newCapabilities(probeInfosById,
            capabilitiesByProbeId, targetToProbe);
    }

    /**
     * Factory class for {@link CachedCapabilities}, used mainly for unit testing.
     */
    @VisibleForTesting
    static class CachedCapabilitiesFactory {
        private final CapabilityMatcher capabilityMatcher;

        CachedCapabilitiesFactory(@Nonnull final CapabilityMatcher capabilityMatcher) {
            this.capabilityMatcher = capabilityMatcher;
        }

        @Nonnull
        CachedCapabilities newCapabilities(
                @Nonnull final Map<Long, ProbeCategory> probeCategoriesById,
                @Nonnull final Map<Long, List<ProbeActionCapability>> capabilitiesByProbeId,
                @Nonnull final Map<Long, Long> targetToProbe) {
            final Map<Long, ProbeCapabilities> capabilitiesById = new HashMap<>();
            probeCategoriesById.forEach((probeId, probeCategory) -> {
                final List<ProbeActionCapability> capabilities = capabilitiesByProbeId.get(probeId);
                if (capabilities == null) {
                    logger.error("Action capabilities for probe {} " +
                        "not found in cache.", probeId);
                } else {
                    capabilitiesById.put(probeId, ImmutableProbeCapabilities.builder()
                        .probeCategory(probeCategory)
                        .capabilities(capabilities)
                        .build());
                }
            });
            return new CachedCapabilities(capabilitiesById, capabilityMatcher, targetToProbe);
        }
    }

    @Override
    public void onProbeRegistered(@Nonnull TopologyProcessorDTO.ProbeInfo probe) {
        synchronized (cacheLock) {
            if (this.cachedCapabilities == null) {
                fullRefresh();
                return;
            }
        }

        try {
            final List<ProbeActionCapability> capabilitiesForProbe =
                actionCapabilitiesBlockingStub.getProbeActionCapabilities(
                    GetProbeActionCapabilitiesRequest.newBuilder()
                        .setProbeId(probe.getId())
                        .build()).getActionCapabilitiesList();
            synchronized (cacheLock) {
                probeInfosById.put(probe.getId(), ProbeCategory.create(probe.getCategory()));
                capabilitiesByProbeId.put(probe.getId(), capabilitiesForProbe);
                this.cachedCapabilities = rebuildCapabilities();
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get action capabilities of newly registered probe {}." +
                "Error: {}", probe.getId(), e.getMessage());
        }
    }

    /**
     * Encapsulates all action-capability-related information for a particular probe.
     */
    @Value.Immutable
    interface ProbeCapabilities {
        /**
         * The probe category of the probe. We need this because we use the category to resolve
         * conflicts when multiple probes can execute an action.
         */
        ProbeCategory probeCategory();

        /**
         * The action capabilities sent by the probe.
         */
        List<ProbeActionCapability> capabilities();
    }

    /**
     * Represents the cached per-target action capabilities retrieved from the topology processor.
     */
    @Immutable
    @ThreadSafe
    public static class CachedCapabilities {
        private final Map<Long, ProbeCapabilities> capabilitiesByProbeId;

        private final CapabilityMatcher capabilityMatcher;

        private final ActionCapabilityMerger actionCapabilityMerger = new ActionCapabilityMerger();

        private final Map<Long, Long> targetToProbe;

        private CachedCapabilities(@Nonnull final Map<Long, ProbeCapabilities> capabilitiesByProbeId,
                                  @Nonnull final CapabilityMatcher capabilityMatcher,
                                   @Nonnull final Map<Long, Long> targetToProbe) {
            this.capabilitiesByProbeId = capabilitiesByProbeId;
            this.capabilityMatcher = capabilityMatcher;
            this.targetToProbe = targetToProbe;
        }

        /**
         * Calculate merged action capability, given the executant entity and the target that will
         * execute the action. Merged action capability is calculated based on capability elements
         * provided by the probe. Elements with scope field populated have priority over elements
         * that are defined without scope.
         *
         * @param action The {@link ActionDTO.Action} generated by the market.
         * @param executantEntity The entity that will "execute" the action. This will be one of
         *                        the entities involved in the action. For example, in a
         *                        vMotion, the VM is the executant entity.
         * @param probeId The ID of the probe that will execute the action. This will be one of
         *                 the probes that discovered the executant entity.
         * @return The {@link SupportLevel} for this (action, executant entity, targetId) tuple.
         */
        @Nonnull
        public MergedActionCapability getMergedActionCapability(
                @Nonnull final ActionDTO.Action action,
                @Nonnull final ActionEntity executantEntity,
                final long probeId) {
            final ProbeCapabilities capabilities = capabilitiesByProbeId.get(probeId);
            if (capabilities == null) {
                return MergedActionCapability.createNotSupported();
            }

            final Collection<ActionCapabilityElement> activeCapabilities =
                    capabilityMatcher.getApplicableCapabilities(
                            action,
                            executantEntity,
                            capabilities.capabilities());

            return actionCapabilityMerger.merge(activeCapabilities);
        }

        /**
         * Get the probe category of a probe.
         *
         * @param probeId The id of the probe.
         * @return The {@link ProbeCategory} of the probe, or an empty optional if there is no
         *         probe for the target in the cache.
         */
        public Optional<ProbeCategory> getProbeCategory(@Nullable final Long probeId) {
            return Optional.ofNullable(capabilitiesByProbeId.get(probeId))
                .map(ProbeCapabilities::probeCategory);
        }

        /**
         * Get the probe id from target id.
         *
         * @param targetId The id of the target.
         * @return The probe id of the probe.
         */
        public Optional<Long> getProbeFromTarget(long targetId) {
            return Optional.ofNullable(this.targetToProbe.get(targetId));
        }
    }

    /**
     * Abstract capability matcher, holding most of the matcher logic to determine whether
     * an {@link ActionCapabilityElement} applies to a particular {@link ActionDTO.Action}.
     */
    @VisibleForTesting
    static class CapabilityMatcher {

        /**
         * Get the {@link ActionCapabilityElement} from a collection of {@link ProbeActionCapability}s
         * that apply to a particular action.
         *
         * @param action The {@link ActionDTO.Action} describing the action recommended by the market.
         * @param executantEntity The entity that is considered to "execute" the action. This should
         *                        be one of the entities involved in the action. It's injected
         *                        because there is specialized logic elsewhere to determine this
         *                        entity.
         * @param actionCapabilities The list of {@link ProbeActionCapability} from the probe that
         *                           will be (potentially) executing this action.
         * @return The list of {@link ActionCapabilityElement}s that apply to this action/entity.
         *         In most cases it will just be one element. However, in, say, a compound move
         *         with a storage and host move, there may be one element for the storage
         *         and one element for the host.
         */
        @Nonnull
        public Collection<ActionCapabilityElement> getApplicableCapabilities(
                    @Nonnull final ActionDTO.Action action,
                    @Nonnull final ActionEntity executantEntity,
                    @Nonnull final Collection<ProbeActionCapability> actionCapabilities) {
            final int entityType = executantEntity.getType();
            final Optional<ProbeActionCapability> capability = actionCapabilities.stream()
                .filter(cpb -> cpb.getEntityType() == entityType)
                .findAny();
            return capability.map(cpb -> cpb.getCapabilityElementList()
                .stream()
                .filter(cpbElement -> capabilityAppliesToActions(action, cpbElement))
                .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }

        /**
         * Returns whether an action specified matches the action capability specified.
         *
         * @param action action to test
         * @param actionCapabilityElement action capability to test
         * @return whether the action and the capability match each other
         */
        private boolean capabilityAppliesToActions(@Nonnull ActionDTO.Action action,
                                                   @Nonnull ActionCapabilityElement actionCapabilityElement) {
            // Action is not yet translated at this point. Therefore Cloud Move action has MOVE
            // type and we need to change it to SCALE.
            final ActionType actionType = CloudMoveBatchTranslator.translateCloudMoveAction(action)
                    ? ActionType.SCALE
                    : ActionDTOUtil.getActionInfoActionType(action);
            boolean match = actionType == actionCapabilityElement.getActionType();

            // For a Move action, we need to check that the destination type is supported by the
            // probe
            if (match && actionType == ActionType.MOVE) {
                if (!(actionCapabilityElement.hasMove() && action.getInfo().hasMove())) {
                    match = false;
                } else {
                    final Set<Integer> actionTargetEntityTypes = action.getInfo()
                        .getMove()
                        .getChangesList()
                        .stream()
                        .map(cp -> cp.getDestination().getType())
                        .collect(Collectors.toSet());
                    match = actionCapabilityElement.getMove()
                        .getTargetEntityTypeList()
                        .stream()
                        .anyMatch(actionTargetEntityTypes::contains);
                }
            }

            if (match) {
                // Check provider scope for Move/Scale actions
                if (actionCapabilityElement.hasProviderScope()) {
                    final int providerType = actionCapabilityElement.getProviderScope()
                            .getProviderType().getNumber();
                    match = ActionDTOUtil.getChangeProviderList(action).stream()
                            .map(ChangeProvider::getDestination)
                            .map(ActionEntity::getType)
                            .anyMatch(type -> type == providerType);
                }

                // Check commodity scope for Resize/Scale actions
                if (actionCapabilityElement.hasCommodityScope()) {
                    final int commodityType = actionCapabilityElement.getCommodityScope()
                            .getCommodityType().getNumber();
                    if (action.getInfo().hasResize()) {
                        // Resize
                        match = action.getInfo().getResize().getCommodityType().getType()
                                == commodityType;
                    } else if (action.getInfo().hasScale()) {
                        // Scale
                        match = action.getInfo().getScale()
                                .getCommodityResizesList().stream()
                                .map(ResizeInfo::getCommodityType)
                                .map(CommodityType::getType)
                                .anyMatch(type -> type == commodityType);
                    }
                }
            }

            return match;
        }
    }

    private static class Metrics {

        private static final DataMetricSummary UPDATE_TIME = DataMetricSummary.builder()
            .withName("ao_probe_capability_cache_update_time_seconds")
            .withHelp("Information about how long it took to update the probe capability cache.")
            .build()
            .register();
    }
}
