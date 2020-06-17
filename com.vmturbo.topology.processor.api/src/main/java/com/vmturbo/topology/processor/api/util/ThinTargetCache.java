package com.vmturbo.topology.processor.api.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * A cache for simple target information, used to avoid going to the topology processor every
 * time we need to know (for example) the name of a target for an RPC.
 *
 * The cache is lazily initialized, but existing entries never go out of date because it listens to
 * removal/addition/update notifications
 */
public class ThinTargetCache implements TargetListener {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    /**
     * Cached probe information, arranged by probe ID.
     * The value is a {@link SetOnce} for concurrency - so multiple threads looking for the same
     * probe result in only one RPC.
     */
    private final Map<Long, SetOnce<ThinProbeInfo>> probeById =
        Collections.synchronizedMap(new HashMap<>());

    /**
     * Cachet target information, arranged by target ID.
     * The value is a {@link SetOnce} for concurrency - so multiple threads looking for the same
     * target result in only one RPC.
     */
    private final Map<Long, SetOnce<ThinTargetInfo>> targetsById =
        Collections.synchronizedMap(new HashMap<>());

    /**
     * Lazily set to true IFF we retrieve all targets (via the {@link ThinTargetCache#getAllTargets()}
     * method). After this is set, we know that subsequent calls to
     * {@link ThinTargetCache#getAllTargets()} won't need to make RPC calls, since the cache
     * will record new/updated/removed targets.
     */
    private final SetOnce<Boolean> allTargetsRetrieved = new SetOnce<>();

    public ThinTargetCache(@Nonnull final TopologyProcessor topologyProcessor) {
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        topologyProcessor.addTargetListener(this);
    }

    /**
     * Get the {@link ThinTargetInfo} associated with the target ID. This may result in a call
     * to the topology processor. Concurrent calls to this method for the same target ID will
     * only result in one topology processor RPC.
     *
     * @param targetId The target ID.
     * @return An {@link Optional} containing a {@link ThinTargetInfo}. Note - it's a thin
     *         {@link ThinTargetInfo}, containing only high-level display information (e.g. name).
     */
    @Nonnull
    public Optional<ThinTargetInfo> getTargetInfo(final long targetId) {
        final SetOnce<ThinTargetInfo> target = targetsById.computeIfAbsent(targetId, k -> new SetOnce<>());
        return Optional.ofNullable(target.ensureSet(() -> fetchThinTargetInfo(targetId).orElse(null)));
    }

    /**
     * Get all targets registered with the topology processor. This may result in a call to the
     * topology processor if this is the first time the method got called.
     *
     * @return A {@link ThinTargetInfo} for every registered target.
     */
    @Nonnull
    public List<ThinTargetInfo> getAllTargets() {
        allTargetsRetrieved.ensureSet(() -> {
            final Set<TargetInfo> targetInfoSet;
            final Set<ProbeInfo> probeInfos;
            try {
                targetInfoSet = topologyProcessor.getAllTargets();
                probeInfos = topologyProcessor.getAllProbes();
            } catch (CommunicationException e) {
                // Leave "initialized" unset.
                return null;
            }

            // Since we got all the probe infos, replace whatever existing probe information is
            // in the cache with the new ones.
            synchronized (probeById) {
                probeById.clear();
                probeInfos.stream()
                    .map(this::toThinProbeInfo)
                    .forEach(probeInfo -> {
                        final SetOnce<ThinProbeInfo> setOnce = new SetOnce<>();
                        setOnce.trySetValue(probeInfo);
                        probeById.put(probeInfo.oid(), setOnce);
                    });
            }

            // Since we got all the target infos, replace whatever is in the cache.
            synchronized (targetsById) {
                targetsById.clear();
                targetInfoSet.stream()
                    .filter(targetInfo -> probeById.containsKey(targetInfo.getProbeId()))
                    .map(targetInfo -> getProbeInfo(targetInfo.getProbeId())
                        .map(probe -> toThinTargetInfo(targetInfo, probe)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(targetInfo -> {
                        final SetOnce<ThinTargetInfo> setOnce = new SetOnce<>();
                        setOnce.trySetValue(targetInfo);
                        targetsById.put(targetInfo.oid(), setOnce);
                    });
            }
            return true;
        });

        synchronized (targetsById) {
            return targetsById.values().stream()
                .map(SetOnce::getValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        }
    }

    @Override
    public void onTargetAdded(@Nonnull final TargetInfo targetInfo) {
        getProbeInfo(targetInfo.getProbeId())
            .map(probe -> toThinTargetInfo(targetInfo, probe))
            .ifPresent(thinTargetInfo -> {
                final SetOnce<ThinTargetInfo> setOnceTI = new SetOnce<>();
                setOnceTI.trySetValue(thinTargetInfo);
                targetsById.put(targetInfo.getId(), setOnceTI);
            });
    }

    @Override
    public void onTargetRemoved(final long targetId) {
        logger.info("Target {} removed. Clearing it from the cache.", targetId);
        targetsById.remove(targetId);
    }

    @Override
    public void onTargetChanged(@Nonnull final TargetInfo newInfo) {
        logger.info("Target {} modified. Updating it in the cache.", newInfo.getId());
        getProbeInfo(newInfo.getProbeId())
            .map(probe -> toThinTargetInfo(newInfo, probe))
            .ifPresent(thinTargetInfo -> {
                final SetOnce<ThinTargetInfo> setOnceTI = new SetOnce<>();
                setOnceTI.trySetValue(thinTargetInfo);
                targetsById.put(newInfo.getId(), setOnceTI);
            });
    }

    @Nonnull
    private ThinTargetInfo toThinTargetInfo(@Nonnull final TargetInfo targetInfo,
                                            @Nonnull final ThinProbeInfo probeInfo) {
        final ImmutableThinTargetInfo.Builder resultBldr = ImmutableThinTargetInfo.builder()
            .oid(targetInfo.getId()).isHidden(targetInfo.isHidden());
        resultBldr.displayName(targetInfo.getDisplayName());

        // fetch information about the probe, and store the probe type in the result
        resultBldr.probeInfo(probeInfo);
        return resultBldr.build();
    }

    @Nonnull
    private Optional<ThinTargetInfo> fetchThinTargetInfo(long targetId) {
        logger.info("Loading target {} into cache.", targetId);

        try {
            // get target info from the topology processor
            final TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
            return getProbeInfo(targetInfo.getProbeId())
                .map(probe -> toThinTargetInfo(targetInfo, probe));
        } catch (CommunicationException e) {
            logger.warn("Error communicating with the topology processor: {}", e.getMessage());
            return Optional.empty();
        } catch (TopologyProcessorException e) {
            // Not found.
            targetsById.remove(targetId);
            return Optional.empty();
        }
    }

    private ThinProbeInfo toThinProbeInfo(@Nonnull final ProbeInfo pInfo) {
        return ImmutableThinProbeInfo.builder()
            .oid(pInfo.getId())
            .type(pInfo.getType())
            .category(pInfo.getCategory())
            .uiCategory(pInfo.getUICategory())
            .build();
    }

    private Optional<ThinProbeInfo> getProbeInfo(final long probeId) {
        final SetOnce<ThinProbeInfo> probeInfo = probeById.computeIfAbsent(probeId, k -> new SetOnce<>());
        return Optional.ofNullable(probeInfo.ensureSet(() -> {
            try {
                return toThinProbeInfo(topologyProcessor.getProbe(probeId));
            } catch (CommunicationException | TopologyProcessorException e) {
                return null;
            }
        }));
    }

    @Value.Immutable
    public interface ThinProbeInfo {
        long oid();
        String type();
        String category();
        String uiCategory();
    }

    @Value.Immutable
    public interface ThinTargetInfo {

        long oid();

        String displayName();

        ThinProbeInfo probeInfo();

        /**
         * Whether the target is not visible.
         *
         * @return true when hidden
         */
        boolean isHidden();
    }
}
