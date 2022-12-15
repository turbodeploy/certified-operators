package com.vmturbo.topology.processor.scheduling;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule.TargetDiscoveryScheduleData;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Maintains and runs scheduled events such as target discovery and topology broadcast.
 *
 * TARGET DISCOVERY NOTES: a target is mapped to a single discovery schedule.
 * Setting the discovery schedule for a target that already has one overrides
 * the existing schedule.
 *
 * When a scheduled discovery is attempted for a target that already has an ongoing discovery, a
 * pending discovery is instead added for that target. See
 * {@link OperationManager#addPendingDiscovery(long, DiscoveryType)} for further details on how this works.
 *
 * TOPOLOGY BROADCAST NOTES: There is a single schedule for topology broadcasting.
 * Setting the discovery schedule for a topology broadcast overrides the previously set schedule.
 *
 * All schedule operations that write to persistent storage will fail if the associated change
 * cannot be written to persistent storage.
 *
 * THREADING MODEL NOTES:
 * The discovery scheduler is made thread-safe via synchronizing all publicly visible methods.
 * Clients should not synchronize on references they hold to a Scheduler object.
 * The {@link Scheduler} should make no external calls through its public interface
 * that potentially block on another call to the {@link Scheduler} made from a
 * separate thread.
 */
@ThreadSafe
public class Scheduler implements TargetStoreListener, ProbeStoreListener, RequiresDataInitialization {
    private final Logger logger = LogManager.getLogger();

    // suffix after probe id that we attach to the full discovery executor service for a probe type
    private static final String FULL_DISCOVERY_SCHEDULER_SUFFIX =
            "-target-full-discovery-scheduler";

    // suffix after probe id that we attach to the incremental discovery executor service for a
    // probe type
    private static final String INCREMENTAL_DISCOVERY_SCHEDULER_SUFFIX =
            "-target-incremental-discovery-scheduler";

    // Factory that takes a name and creates a singled threaded executor with that name.  One such
    // executor will be created for each probe type that has a target.
    private final Function<String, ScheduledExecutorService> fullDiscoveryScheduleExecutorFactory;

    // Factory that takes a name and creates a singled threaded executor with that name.  One such
    // executor will be created for each probe type that supports incremental discovery and
    // has a target.
    private final Function<String, ScheduledExecutorService>
            incrementalDiscoveryScheduleExecutorFactory;

    // thread pool for scheduling the realtime broadcast.
    private final ScheduledExecutorService broadcastExecutor;
    // thread pool for expiring long-running operations with.
    private final ScheduledExecutorService expiredOperationExecutor;
    // mapping from target id to mapping of schedule by discovery type
    private final Map<Long, Map<DiscoveryType, TargetDiscoverySchedule>> discoveryTasks;
    // mapping from probe type to executor to run full discoveries of that probe type.
    // we have one single threaded executor per probe type so that blocking while waiting for a
    // permit on one target does not block discovery for targets of probe types that have permits
    // available.
    private final Map<String, ScheduledExecutorService> probeTypeToFullDiscoveryScheduleExecutor;
    // mapping from probe type to executor to run incremental discoveries of that probe type.
    private final Map<String, ScheduledExecutorService>
            probeTypeToIncrementalDiscoveryScheduleExecutor;

    /**
     * In-memory map storing the TargetDiscoveryScheduleData for each target, backed by consul.
     */
    private final Map<Long, TargetDiscoveryScheduleData> targetDiscoveryScheduleData = new HashMap<>();

    private final IOperationManager operationManager;
    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final TopologyHandler topologyHandler;
    private final KeyValueStore scheduleStore;
    private final StitchingJournalFactory journalFactory;
    private final Gson gson = new Gson();

    private Optional<TopologyBroadcastSchedule> broadcastSchedule;
    private Schedule operationExpirationSchedule;

    private final long initialBroadcastIntervalMinutes;

    private final int numDiscoveryIntervalsMissedBeforeLogging;

    public static final long FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES = 10;
    public static final String BROADCAST_SCHEDULE_KEY = "broadcast";
    public static final String SCHEDULE_KEY_OFFSET = "schedule/";

    /**
     * Create a {@link Scheduler} for scheduling discoveries on a per-target basis.
     *
     * @param operationManager The {@link OperationManager} for performing scheduled discoveries.
     * @param targetStore The {@link TargetStore} to use to find targets.
     * @param probeStore The {@link ProbeStore} to use for finding probe info.
     * @param topologyHandler The {@link TopologyHandler} to use for broadcasting topologies to other services.
     * @param scheduleStore The store used for saving and loading data data from/to persistent storage.
     * @param journalFactory The factory for constructing stitching journals to be used in tracing changes
     *                       made during topology broadcasts initiated by the scheduler.
     * @param fullDiscoveryExecutor The executor to be used for scheduling full discoveries.
     * @param incrementalDiscoveryExecutor The executor to be used for scheduling incremental discoveries.
     * @param broadcastExecutor The executor to be used for scheduling realtime broadcasts.
     * @param expirationExecutor The executor to be used for scheduling pending operation expiration checks.
     * @param initialBroadcastIntervalMinutes The initial broadcast interval specified in minutes.
     * @param numDiscoveryIntervalsMissedBeforeLogging The number of discovery intervals to wait
     * before logging a discovery as late.
     */
    public Scheduler(@Nonnull final IOperationManager operationManager,
                     @Nonnull final TargetStore targetStore,
                     @Nonnull final ProbeStore probeStore,
                     @Nonnull final TopologyHandler topologyHandler,
                     @Nonnull final KeyValueStore scheduleStore,
                     @Nonnull final StitchingJournalFactory journalFactory,
                     @Nonnull final Function<String, ScheduledExecutorService> fullDiscoveryExecutor,
                     @Nonnull final Function<String, ScheduledExecutorService> incrementalDiscoveryExecutor,
                     @Nonnull final ScheduledExecutorService broadcastExecutor,
                     @Nonnull final ScheduledExecutorService expirationExecutor,
                     final long initialBroadcastIntervalMinutes,
                     final int numDiscoveryIntervalsMissedBeforeLogging) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.journalFactory = Objects.requireNonNull(journalFactory);
        this.fullDiscoveryScheduleExecutorFactory = Objects.requireNonNull(fullDiscoveryExecutor);
        this.incrementalDiscoveryScheduleExecutorFactory = Objects.requireNonNull(incrementalDiscoveryExecutor);
        this.broadcastExecutor = Objects.requireNonNull(broadcastExecutor);
        this.expiredOperationExecutor = Objects.requireNonNull(expirationExecutor);
        this.scheduleStore = Objects.requireNonNull(scheduleStore);
        this.initialBroadcastIntervalMinutes = initialBroadcastIntervalMinutes;
        this.numDiscoveryIntervalsMissedBeforeLogging = numDiscoveryIntervalsMissedBeforeLogging;

        discoveryTasks = new HashMap<>();
        broadcastSchedule = Optional.empty();
        probeTypeToFullDiscoveryScheduleExecutor = new HashMap<>();
        probeTypeToIncrementalDiscoveryScheduleExecutor = new HashMap<>();

        probeStore.addListener(this);
        targetStore.addListener(this);
    }

    /**
     * Set the discovery schedule for a target.
     *
     * If the target did not have a previous discovery schedule, a discovery will be scheduled
     * immediately and then a subsequent discovery will be set up for each discoveryInterval
     * thereafter.
     *
     * If a schedule was previously set for a target, that schedule will be overridden.
     * When overriding an existing schedule, time elapsed against the previously set schedule
     * will be counted against the new schedule. For example, if a target had a 10 minute schedule
     * and 3 minutes had elapsed since the last discovery, then a new 5 minute schedule is set,
     * those 3 elapsed minutes will count against the new discovery (ie it will be 2 minutes until
     * the next discovery, and then discoveries will be requested at every 5 minute interval after that.
     *
     * @param targetId The ID of the target to discover.
     * @param discoveryType type of the discovery to set schedule for.
     * @param discoveryInterval The interval at which to discover the target.
     * @param unit The time units to apply to the discovery interval.
     * @param syncToBroadcastSchedule whether or not to sync with broadcast schedule.
     * @throws TargetNotFoundException when the targetId is not associated with any known target.
     * @throws UnsupportedDiscoveryTypeException when the discovery type is not supported by probe.
     * @throws IllegalArgumentException when the discoveryInterval is less than or equal to zero.
     * @return The discovery schedule for the input target.
     */
    @Nonnull
    public synchronized TargetDiscoverySchedule setDiscoverySchedule(final long targetId,
            @Nonnull final DiscoveryType discoveryType, final long discoveryInterval,
            @Nonnull final TimeUnit unit, final boolean syncToBroadcastSchedule)
            throws TargetNotFoundException, UnsupportedDiscoveryTypeException {
        if (discoveryInterval <= 0) {
            throw new IllegalArgumentException("Illegal discovery interval: " + discoveryInterval);
        }

        Target target = targetStore.getTarget(targetId).orElseThrow(
            () -> new TargetNotFoundException(targetId));

        // get the probe info so we can find the probe's preferred discovery interval.
        final ProbeInfo probeInfo = probeStore.getProbe(target.getProbeId()).orElseThrow(
            () -> new IllegalStateException("Probe id " + target.getProbeId() + " not found in probe store."));

        // check if the given discovery type is supported by probe
        getDiscoveryInterval(probeInfo, discoveryType, syncToBroadcastSchedule).orElseThrow(
            () -> new UnsupportedDiscoveryTypeException(discoveryType, probeInfo.getProbeType()));

        final long discoveryIntervalMillis = TimeUnit.MILLISECONDS.convert(discoveryInterval, unit);
        saveTargetScheduleData(targetId, discoveryType, discoveryIntervalMillis, syncToBroadcastSchedule);

        final Optional<TargetDiscoverySchedule> existingSchedule = stopAndRemoveDiscoverySchedule(
            targetId, discoveryType);
        final long initialDelayMillis = calculateInitialDelayMillis(discoveryIntervalMillis, existingSchedule);

        return scheduleDiscovery(targetId, probeInfo.getProbeType(), initialDelayMillis,
                discoveryIntervalMillis, discoveryType, syncToBroadcastSchedule);
    }

    /**
     * Set the discovery schedule for a target. If syncToBroadcastSchedule is true, the schedule
     * interval will be set to match the broadcast schedule interval. If the broadcast schedule
     * interval is updated, the synched discovery schedule will also be updated.
     *
     * If a schedule was previously set for a target, that schedule will be overridden.
     * When overriding an existing schedule, time elapsed against the previously set schedule will
     * be counted against the new schedule. For example, if a target had a 10 minute schedule and
     * 3 minutes had elapsed since the last discovery, then a new 5 minute schedule is set,
     * those 3 elapsed minutes will count against the new discovery (ie it will be 2 minutes until
     * the next discovery, and then discoveries will be requested at every 5 minute interval after that.
     *
     * UPDATE: As of OM-39776, the discovery schedule is based primarily on the probe configuration.
     * They are no longer synched to the broadcast as closely as this method header describes. The
     * broadcast interval is still used as a fallback setting, but if a probe configuration specifies
     * a discovery interval that is greater (i.e. longer delay between discovery events) than the
     * broadcast cycle, then the probe discovery interval setting will be used to schedule the
     * discovery instead of the broadcast interval. This was done to support the probe types that
     * discover less frequently and retrieve more data (e.g. the AWS cost probe) than regular
     * probe discoveries do.
     *
     * @param targetId The ID of the target to discover.
     * @param discoveryType type of the discovery.
     * @param syncToBroadcastSchedule whether or not to sync with broadcast schedule.
     * @throws TargetNotFoundException when the targetId is not associated with any known target.
     * @throws UnsupportedDiscoveryTypeException when the type of discovery is not supported by platform
     * @return The discovery schedule for the input target.
     */
    @Nonnull
    public synchronized TargetDiscoverySchedule setDiscoverySchedule(final long targetId,
                                                                     DiscoveryType discoveryType,
                                                                     boolean syncToBroadcastSchedule)
            throws TargetNotFoundException, UnsupportedDiscoveryTypeException {
        Target target = targetStore.getTarget(targetId).orElseThrow(
            () -> new TargetNotFoundException(targetId));

        // get the probe info so we can find the probe's preferred discovery interval.
        ProbeInfo probeInfo = probeStore.getProbe(target.getProbeId()).orElseThrow(
            () -> new IllegalStateException("Probe id " + target.getProbeId() + " not found in probe store."));

        // check if the given discovery type is supported by probe
        long probeDiscoveryIntervalMillis = getDiscoveryInterval(probeInfo, discoveryType,
            syncToBroadcastSchedule).orElseThrow(
                () -> new UnsupportedDiscoveryTypeException(discoveryType, probeInfo.getProbeType()));

        return setDiscoverySchedule(targetId, discoveryType, probeDiscoveryIntervalMillis,
            TimeUnit.MILLISECONDS, syncToBroadcastSchedule);
    }

    /**
     * Set the discovery schedules for all supported discovery types of the given target.
     *
     * @param targetId id of the target to set discovery schedule for
     * @param syncToBroadcastSchedule whether or not to sync with broadcast schedule
     * @return map of schedule for each supported discovery type
     * @throws TargetNotFoundException when the targetId is not associated with any known target.
     * @throws UnsupportedDiscoveryTypeException when the discovery type is not supported by the given target.
     */
    @Nonnull
    public synchronized Map<DiscoveryType, TargetDiscoverySchedule> setDiscoverySchedules(
            final long targetId, final boolean syncToBroadcastSchedule)
            throws TargetNotFoundException, UnsupportedDiscoveryTypeException {
        Target target = targetStore.getTarget(targetId).orElseThrow(
            () -> new TargetNotFoundException(targetId));

        // get the probe info so we can find the probe's preferred discovery interval.
        ProbeInfo probeInfo = probeStore.getProbe(target.getProbeId()).orElseThrow(
            () -> new IllegalStateException("Probe id " + target.getProbeId() + " not found in probe store."));

        final Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap = new HashMap<>();

        scheduleMap.put(DiscoveryType.FULL,
            setDiscoverySchedule(targetId, DiscoveryType.FULL, syncToBroadcastSchedule));

        Optional<Long> incrementalDiscoveryInterval = getIncrementalDiscoveryInterval(probeInfo);
        if (incrementalDiscoveryInterval.isPresent()) {
            scheduleMap.put(DiscoveryType.INCREMENTAL,
                setDiscoverySchedule(targetId, DiscoveryType.INCREMENTAL, syncToBroadcastSchedule));
        }
        return scheduleMap;
    }

    /**
     * Get the discovery interval to use for the given probe and discovery type. It will also try
     * to sync with broadcast schedule if needed.
     *
     * @param probeInfo probe info containing default discovery intervals for the probe
     * @param discoveryType type of the discovery
     * @param syncToBroadcastSchedule whether or not to sync with broadcast schedule
     * @return optional discovery interval to use
     */
    private Optional<Long> getDiscoveryInterval(ProbeInfo probeInfo,
            DiscoveryType discoveryType, boolean syncToBroadcastSchedule) {
        switch (discoveryType) {
            case FULL:
                return Optional.of(getFullDiscoveryInterval(probeInfo, syncToBroadcastSchedule));
            case INCREMENTAL:
                return getIncrementalDiscoveryInterval(probeInfo);
            default:
                return Optional.empty();
        }
    }

    /**
     * Get the FULL discovery interval to use for this probe, respecting the broadcast schedule
     * if needed.
     *
     * <p>If syncToBroadcastSchedule is false, we will use the full rediscovery interval
     * as our rediscovery interval.
     *
     * <p>If syncToBroadcastSchedule is true, we will also only accept probe discovery intervals that
     * are greater than the broadcast interval. If a probe configures discovery at a higher
     * frequency than the broadcast cycle, we will fall back to the broadcast time. There isn't
     * much point to discovering more frequently than the broadcast time, since any results
     * wouldn't be shown until the next broadcast cycle anyways.
     *
     * @param probeInfo The {@link ProbeInfo} to read the discovery interval properties from.
     * @param syncToBroadcastSchedule whether or not to sync with broadcast schedule.
     * @return The discovery interval to use (in milliseconds) for full discovery
     */
    public long getFullDiscoveryInterval(@Nonnull ProbeInfo probeInfo, boolean syncToBroadcastSchedule) {
        if (probeInfo.getFullRediscoveryIntervalSeconds() > 0) {
            final long fullDiscoveryIntervalMillis = TimeUnit.SECONDS.toMillis(probeInfo.getFullRediscoveryIntervalSeconds());
            // if needs to sync with broadcast schedule, the final discovery interval should be
            // at least as long as the broadcast cycle
            return syncToBroadcastSchedule
                ? Math.max(getBroadcastIntervalMillis(), fullDiscoveryIntervalMillis)
                : fullDiscoveryIntervalMillis;
        }
        // if no full interval defined in probe, set the discovery interval to
        // the broadcast interval.
        return getBroadcastIntervalMillis();
    }

    /**
     * Get the optional INCREMENTAL discovery interval to use for this probe.
     *
     * @param probeInfo The {@link ProbeInfo} to read the discovery interval properties from.
     * @return The discovery interval to use (in milliseconds) for incremental discovery
     */
    public Optional<Long> getIncrementalDiscoveryInterval(@Nonnull ProbeInfo probeInfo) {
        return probeInfo.hasIncrementalRediscoveryIntervalSeconds()
            ? Optional.of(TimeUnit.SECONDS.toMillis(
                probeInfo.getIncrementalRediscoveryIntervalSeconds()))
            : Optional.empty();
    }

    /**
     * Get the discovery schedule for a target, organized by discovery type.
     *
     * @param targetId The ID of the target whose schedule should be retrieved.
     * @return An map containing the target's discovery schedule for each supported discovery type
     */
    public synchronized Map<DiscoveryType, TargetDiscoverySchedule> getDiscoverySchedule(final long targetId) {
        return Optional.ofNullable(discoveryTasks.get(targetId)).orElse(Collections.emptyMap());
    }

    /**
     * Get the discovery schedule for a target and discovery type.
     *
     * @param targetId The ID of the target whose schedule should be retrieved.
     * @param discoveryType the type of discovery to get schedule for.
     * @return An {@link Optional} containing the target's discovery schedule, or
     *         {@link Optional#empty()} if no schedule exists for the target.
     */
    public synchronized Optional<TargetDiscoverySchedule> getDiscoverySchedule(
            final long targetId, DiscoveryType discoveryType) {
        return Optional.ofNullable(discoveryTasks.get(targetId))
            .map(scheduleMap -> scheduleMap.get(discoveryType));
    }

    /**
     * Cancel all the discovery schedules for a target, which may include full and incremental
     * discoveries. Cancelling the discovery schedule will close the regular discovery of the
     * target at fixed intervals.
     *
     * @param targetId The ID of the target whose schedule should be cancelled.
     */
    public synchronized void disableDiscoverySchedule(final long targetId) {
        deleteScheduleData(targetId);
        stopAndRemoveDiscoverySchedule(targetId, DiscoveryType.FULL);
        stopAndRemoveDiscoverySchedule(targetId, DiscoveryType.INCREMENTAL);
    }

    /**
     * Cancel the discovery schedule for a target and discovery type. Cancelling the discovery
     * schedule will close the regular discovery of the target at fixed intervals.
     *
     * @param targetId The ID of the target whose schedule should be cancelled.
     * @param discoveryType The type of discovery to disable discovery schedule for.
     * @return An {@link Optional} containing the target's cancelled discovery schedule
     *         if it was successfully cancelled, or {@link Optional#empty()} if the
     *         data could not be cancelled.
     */
    public synchronized Optional<TargetDiscoverySchedule> disableDiscoverySchedule(
            final long targetId, final DiscoveryType discoveryType) {
        logger.info("Disabling {} discovery for target {}", discoveryType, targetId);
        saveTargetScheduleData(targetId, discoveryType, -1, false);
        return stopAndRemoveDiscoverySchedule(targetId, discoveryType);
    }

    /**
     * Reset the discovery schedule for a target. Resetting the discovery schedule for a
     * target will cause the full scheduled discovery interval to elapse from the time
     * of the reset before the next scheduled discovery.
     *
     * <p>If there is no existing discovery schedule for the target, returns
     * {@link Optional#empty()}, otherwise returns an {@link Optional} containing
     * the new TargetDiscoverySchedule describing the scheduled discovery.
     *
     * @param targetId the ID of the target whose discovery schedule should be reset.
     * @param discoveryType the type of discovery to reset schedule for
     * @return If a discovery schedule exists for the target and the target exists in the
     * TargetStore, an {@link Optional} containing the new TargetDiscoverySchedule describing the
     * scheduled discovery. Otherwise returns {@link Optional#empty()}.
     */
    public synchronized Optional<TargetDiscoverySchedule> resetDiscoverySchedule(long targetId,
            DiscoveryType discoveryType) {
        // Cancel the ongoing task and reschedule it.
        final Optional<TargetDiscoverySchedule> discoveryTask =
            stopAndRemoveDiscoverySchedule(targetId, discoveryType);

        Optional<Target> optTarget = targetStore.getTarget(targetId);
        if (!optTarget.isPresent()) {
            return Optional.empty();
        }

        return discoveryTask.map(task -> scheduleDiscovery(
            task.getTargetId(),
            optTarget.get().getProbeInfo().getProbeType(),
            task.getScheduleInterval(TimeUnit.MILLISECONDS),
            task.getScheduleInterval(TimeUnit.MILLISECONDS),
            discoveryType,
            task.isSynchedToBroadcast()
        ));
    }

    /**
     * Set the broadcast data for the topology processor. If a data was previously set,
     * that data will be overridden. When overriding an existing data, time elapsed against
     * the previously set data will be counted against the new data. For example, if a
     * there was a 10 minute data and 3 minutes had elapsed since the last broadcast, then a
     * new 5 minute data is set, those 3 elapsed minutes will count against the new broadcast
     * (ie it will be 2 minutes until the next broadcast, and then broadcasts will be requested
     * at every 5 minute interval after that.
     *
     * @param broadcastInterval The interval at which to broadcast the topology.
     * @param unit The time units to apply to the broadcast interval.
     * @throws IllegalArgumentException when the broadcastInterval is less than or equal to zero.
     * @return The broadcast data for the input target.
     */
    @Nonnull
    public synchronized TopologyBroadcastSchedule setBroadcastSchedule(final long broadcastInterval,
                                                                       @Nonnull final TimeUnit unit) {
        if (broadcastInterval <= 0) {
            throw new IllegalArgumentException("Illegal broadcast interval: " + broadcastInterval);
        }

        final long broadcastIntervalMillis = TimeUnit.MILLISECONDS.convert(broadcastInterval, unit);

        final Optional<TopologyBroadcastSchedule> existingSchedule = stopAndRemoveBroadcastSchedule();
        final long initialDelayMillis = calculateInitialDelayMillis(broadcastIntervalMillis, existingSchedule);

        return scheduleBroadcast(initialDelayMillis, broadcastIntervalMillis);
    }

    /**
     * Get the broadcast data for the topology processor.
     *
     * @return An {@link Optional} containing the topology processor's broadcast data, or
     *         {@link Optional#empty()} if no data exists.
     */
    public synchronized Optional<TopologyBroadcastSchedule> getBroadcastSchedule() {
        return broadcastSchedule;
    }

    /**
     * Reset the broadcast data. Resetting the broadcast data for a
     * target will cause the full scheduled broadcast interval to elapse from the time
     * of the reset before the next scheduled broadcast.
     *
     * If there is no existing broadcast data , returns
     * {@link Optional#empty()}, otherwise returns an {@link Optional} containing
     * the new {@link TopologyBroadcastSchedule} describing the broadcast data.
     *
     * @return If a broadcast data exists, an {@link Optional} containing
     * the new {@link TopologyBroadcastSchedule} describing the broadcast data. Otherwise
     * returns {@link Optional#empty()}.
     */
    public synchronized Optional<TopologyBroadcastSchedule> resetBroadcastSchedule() {
        // Cancel the ongoing task and reschedule it.
        final Optional<TopologyBroadcastSchedule> broadcastTask = stopAndRemoveBroadcastSchedule();
        return broadcastTask.map(task -> scheduleBroadcast(
            task.getScheduleInterval(TimeUnit.MILLISECONDS),
            task.getScheduleInterval(TimeUnit.MILLISECONDS)
        ));
    }

    /**
     * When a new ProbeInfo is registered, if it supports incremental discovery, but existing
     * ProbeInfo doesn't, then schedule incremental discovery for all targets of same probe type.
     * If it doesn't support incremental discovery, but existing one supports, then jsut stop and
     * remove incremental discovery for all targets of same probe type.
     *
     * <p>This is usually used when PT does some testing like switching between two different
     * version of vc probes, one with incremental discovery supported, the other one without.
     *
     * @param probeId The ID of the probe that was registered.
     * @param newProbeInfo The info for the probe that was registered with the {@link ProbeStore}.
     */
    @Override
    public void onProbeRegistered(long probeId, ProbeInfo newProbeInfo) {
        final boolean requireSavedScheduleData = scheduleStore.containsKey(SCHEDULE_KEY_OFFSET);
        targetStore.getProbeTargets(probeId).forEach(target -> {
            // Schedule full discovery if the probe supports it. If consul was unreachable when
            // the target was added, we may have missed creating a full discovery schedule at
            // that time.  If we don't create it now, we'll end up persisting a schedule with 0
            // full discovery interval, and that causes problems on TP restart.
            // @TODO OM-94056 come up with a better way to handle consul unavailability
            if (newProbeInfo.hasFullRediscoveryIntervalSeconds()
                    && !getScheduleData(target.getId(), requireSavedScheduleData).isPresent()) {
                try {
                    setDiscoverySchedule(target.getId(), DiscoveryType.FULL, true);
                } catch (TargetNotFoundException | UnsupportedDiscoveryTypeException e) {
                    logger.error("Could not set full discovery for target {}.",
                            target.getId(), e);
                }
            }
            // try to stop and remove existing incremental discovery schedule if any
            stopAndRemoveDiscoverySchedule(target.getId(), DiscoveryType.INCREMENTAL);
            // try to schedule new incremental discovery if probe supports
            initializeIncrementalDiscoverySchedule(target.getId(), newProbeInfo, requireSavedScheduleData);
        });
    }

    /**
     * When a target is added, create a default discovery schedule for the target that is synched to
     * match the interval of the topology broadcast schedule.
     *
     * @param target The target that was added to the {@TargetStore}.
     */
    @Override
    public void onTargetAdded(final @Nonnull Target target) {
        Objects.requireNonNull(target);

        try {
            final ProbeInfo probeInfo = target.getProbeInfo();
            if (probeInfo.hasFullRediscoveryIntervalSeconds()) {
                setDiscoverySchedule(target.getId(), DiscoveryType.FULL, true);
            }
            // schedule incremental discovery if the probe supports it
            if (target.getProbeInfo().hasIncrementalRediscoveryIntervalSeconds()) {
                setDiscoverySchedule(target.getId(), DiscoveryType.INCREMENTAL, false);
            }
        } catch (TargetNotFoundException | UnsupportedDiscoveryTypeException e) {
            logger.error("Unable to add default data for target '{}' ({})",
                target.getDisplayName(), target.getId());
        }
    }

    /**
     * When a target has been removed it is required to remove it from discovery schedule.
     *
     * @param target The target that was removed from the {@TargetStore}.
     */
    @Override
    public void onTargetRemoved(final @Nonnull Target target) {
        Objects.requireNonNull(target);
        disableDiscoverySchedule(target.getId());
    }

    /**
     * When a target is updated, there is no need to reschedule target discovery.
     *
     * @param target The target that was updated to the {@TargetStore}.
     */
    @Override
    public void onTargetUpdated(Target target) {
        // Do nothing
    }

    /**
     * Calculate the initial delay for a scheduled task given the interval and an optionally existing
     * data.
     *
     * @param intervalMillis The interval for the scheduled task in milliseconds.
     * @param existingSchedule The data that was in place prior to overriding it. Pass in
     *                         {@link Optional#empty()} if there was no existing data.
     * @param <T> The type of the scheduled task.
     * @return The calculated initial delay in milliseconds for the new data to be put in place.
     */
    private <T extends Schedule> long calculateInitialDelayMillis(long intervalMillis,
                                                                  Optional<T> existingSchedule) {
        return existingSchedule
            .map(schedule -> Math.max(0L, intervalMillis - schedule.getElapsedTimeMillis()))
            .orElse(0L);
    }

    /**
     * Create and put a discovery schedule into the map of discovery tasks.
     *
     * @param targetId The ID of the target whose schedule should be put in the tasks.
     * @param probeType The probeType of the target.  This is used to access the executor for the
     * target.
     * @param delayMillis The initial delay before running the first discovery in the schedule.
     * @param discoveryIntervalMillis The interval between repeated discoveries for the target.
     * @param discoveryType type of the discovery to schedule
     * @param synchedToBroadcastSchedule Whether the discovery should be synched to the schedule
     *                                   for broadcasting the topology.
     * @return The discovery schedule for the target.
     */
    private TargetDiscoverySchedule scheduleDiscovery(final long targetId,
            @Nonnull final String probeType, final long delayMillis,
            final long discoveryIntervalMillis, final DiscoveryType discoveryType,
            boolean synchedToBroadcastSchedule) {
        final ScheduledExecutorService discoveryScheduleExecutor;
        Objects.requireNonNull(probeType);
        switch (discoveryType) {
            case FULL:
                discoveryScheduleExecutor = probeTypeToFullDiscoveryScheduleExecutor
                        .computeIfAbsent(probeType, key ->
                                fullDiscoveryScheduleExecutorFactory
                                        .apply(key + FULL_DISCOVERY_SCHEDULER_SUFFIX));
                break;
            case INCREMENTAL:
                discoveryScheduleExecutor = probeTypeToIncrementalDiscoveryScheduleExecutor
                        .computeIfAbsent(probeType, key ->
                                incrementalDiscoveryScheduleExecutorFactory.apply(key
                                        + INCREMENTAL_DISCOVERY_SCHEDULER_SUFFIX));
                break;
            default:
                throw new IllegalStateException("No thread pool available for discovery type: "
                    + discoveryType);
        }

        final ScheduledFuture<?> scheduledTask = discoveryScheduleExecutor.scheduleAtFixedRate(
            () -> executeScheduledDiscovery(targetId, discoveryType),
            delayMillis,
            discoveryIntervalMillis,
            TimeUnit.MILLISECONDS
        );

        final TargetDiscoverySchedule discoveryTask = new TargetDiscoverySchedule(scheduledTask,
            targetId, discoveryIntervalMillis, synchedToBroadcastSchedule);

        discoveryTasks.computeIfAbsent(targetId, k -> new HashMap<>())
            .put(discoveryType, discoveryTask);

        logger.info("Set {} {}", discoveryType, discoveryTask);

        return discoveryTask;
    }

    /**
     * Create a data for broadcasting.
     *
     * @param delayMillis The initial delay before running the first broadcast in the data.
     * @param broadcastIntervalMillis The interval between repeated broadcasts.
     * @return The broadcast data for the topology processor.
     */
    private TopologyBroadcastSchedule scheduleBroadcast(final long delayMillis,
                                                        final long broadcastIntervalMillis) {
        final ScheduledFuture<?> scheduledTask = broadcastExecutor.scheduleAtFixedRate(
            this::executeTopologyBroadcast,
            delayMillis,
            broadcastIntervalMillis,
            TimeUnit.MILLISECONDS
        );

        TopologyBroadcastSchedule schedule =
            new TopologyBroadcastSchedule(scheduledTask, broadcastIntervalMillis);
        broadcastSchedule = Optional.of(schedule);
        updateBroadcastSynchedDiscoverySchedules();
        logger.info("Set {}", schedule);

        return schedule;
    }

    /**
     * Attempt to update the interval for all the target discovery schedules which are synched to
     * the topology broadcast data. Only FULL discovery schedule may be synched.
     */
    private void updateBroadcastSynchedDiscoverySchedules() {
        List<Long> synchedDiscoverySchedules = discoveryTasks.entrySet().stream()
            .filter(entry -> Optional.ofNullable(entry.getValue().get(DiscoveryType.FULL))
                .map(TargetDiscoverySchedule::isSynchedToBroadcast).orElse(false))
            .map(Entry::getKey)
            .collect(Collectors.toList());

        synchedDiscoverySchedules.forEach(targetId -> {
            try {
                setDiscoverySchedule(targetId, DiscoveryType.FULL, true);
            } catch (TargetNotFoundException | IllegalStateException | UnsupportedDiscoveryTypeException e) {
                logger.error("Unable to update synched {} discovery schedule for target {}",
                    DiscoveryType.FULL, targetId, e);
            }
        });
    }

    /**
     * Cancel a target's discovery schedule and remove it from the internal map.
     *
     * @param targetId The ID of the target whose discovery schedule should be stopped and removed.
     * @param discoveryType type of the discovery type
     * @return The stopped discovery schedule if one existed for the target, or empty if not.
     */
    private synchronized Optional<TargetDiscoverySchedule> stopAndRemoveDiscoverySchedule(
            final long targetId, @Nonnull DiscoveryType discoveryType) {
        Map<DiscoveryType, TargetDiscoverySchedule> targetDiscoveryScheduleMap =
            discoveryTasks.get(targetId);
        if (targetDiscoveryScheduleMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(targetDiscoveryScheduleMap.remove(discoveryType))
            .map(schedule -> {
                logger.info("Removing {} discovery schedule for target {}", discoveryType, targetId);
                schedule.cancel();
                return schedule;
            });
    }

    /**
     * Cancel the broadcast schedule and remove it.
     *
     * @return The broadcast schedule that was stopped, or empty if no broadcast schedule was present.
     */
    private Optional<TopologyBroadcastSchedule> stopAndRemoveBroadcastSchedule() {
        final Optional<TopologyBroadcastSchedule> task = broadcastSchedule;
        broadcastSchedule = Optional.empty();
        task.ifPresent(Schedule::cancel);

        return task;
    }

    /**
     * Get the broadcast interval in milliseconds.
     *
     * @return Get the broadcast interval in milliseconds. If no broadcast data is present, the
     *         failover value in milliseconds will be used.
     */
    private long getBroadcastIntervalMillis() {
        return broadcastSchedule
            .map(schedule -> schedule.getScheduleInterval(TimeUnit.MILLISECONDS))
            .orElse(TimeUnit.MILLISECONDS.convert(FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES, TimeUnit.MINUTES));
    }

    /**
     * A helper method that executes scheduled discoveries.
     * Passed via lambda to the scheduleExecutor.
     *
     * @param targetId The target where the discovery should be executed.
     * @param discoveryType type of the discovery type
     */
    private void executeScheduledDiscovery(final long targetId, DiscoveryType discoveryType) {
        if (!operationManager.getLastDiscoveryForTarget(targetId, discoveryType).isPresent()) {
            logger.info("Executing initial discovery for target {}({})", targetId,
                    discoveryType);
        }
        try {
            operationManager.addPendingDiscovery(targetId, discoveryType);
        } catch (TargetNotFoundException | InterruptedException e) {
            // When a target is not found or operation is interrupted, it is a
            // non-recoverable error and it does not make sense to continue
            // executing discoveries with this task, so close.
            logger.error("Unrecoverable error during {} discovery execution. " +
                "Cancelling discovery schedule for the target.", discoveryType, e);

            disableDiscoverySchedule(targetId);
        } catch (CommunicationException | RuntimeException e) {
            // Communication exceptions are correctable errors, so continue
            // to run future discoveries. Also continue to execute future discoveries
            // if a generic runtime exception occurred.
            logger.error("Exception when executing scheduled {} discovery.", discoveryType, e);
        }
    }

    private void addStaleTargetsForProbe(long probeId, @Nonnull DiscoveryType discoveryType,
            long rediscoveryIntervalSeconds, @Nonnull Map<Long, LocalDateTime> staleTargetMap,
            @Nonnull Set<Long> undiscoveredTargets) {
        final long staleSeconds = rediscoveryIntervalSeconds
                * numDiscoveryIntervalsMissedBeforeLogging;
        targetStore.getProbeTargets(probeId).forEach(target -> {
            final Optional<Discovery> discovery =
                    operationManager.getLastDiscoveryForTarget(target.getId(), discoveryType);
            if (discovery.isPresent() && discovery.get().getCompletionTime() != null) {
                if (LocalDateTime.now().minusSeconds(staleSeconds)
                        .isAfter(discovery.get().getCompletionTime())) {
                    staleTargetMap.put(target.getId(), discovery.get().getCompletionTime());
                }
            } else {
                undiscoveredTargets.add(target.getId());
            }
        });
    }

    @VisibleForTesting
    void logLaggingDiscoveries() {
        final Map<Long, LocalDateTime> staleFullDiscoveryTargetMap = Maps.newHashMap();
        final Map<Long, LocalDateTime> staleIncrementalDiscoveryTargetMap = Maps.newHashMap();
        final Set<Long> fullDiscoveryUndiscovered = Sets.newHashSet();
        final Set<Long> incrementalDiscoveryUndiscovered = Sets.newHashSet();

        probeStore.getProbes().forEach((probeId, probeInfo) -> {
            getDiscoveryInterval(probeInfo, DiscoveryType.FULL, true)
                    .ifPresent(rediscoveryInterval -> addStaleTargetsForProbe(probeId,
                            DiscoveryType.FULL, TimeUnit.MILLISECONDS.toSeconds(rediscoveryInterval),
                            staleFullDiscoveryTargetMap, fullDiscoveryUndiscovered));

            getDiscoveryInterval(probeInfo, DiscoveryType.INCREMENTAL, false)
                    .ifPresent(rediscoveryInterval -> addStaleTargetsForProbe(probeId,
                            DiscoveryType.INCREMENTAL,
                            TimeUnit.MILLISECONDS.toSeconds(rediscoveryInterval),
                            staleIncrementalDiscoveryTargetMap, incrementalDiscoveryUndiscovered));
        });

        if (!staleFullDiscoveryTargetMap.isEmpty()) {
            logger.warn("The following targets have not been discovered in more "
                    + "than {} discovery intervals:", numDiscoveryIntervalsMissedBeforeLogging);
            staleFullDiscoveryTargetMap.forEach(
                    (targetId, lastDiscoveryDate) -> logger.warn("Target {} last discovered {}.",
                            targetId, lastDiscoveryDate));
        }
        if (!fullDiscoveryUndiscovered.isEmpty()) {
            logger.warn("The following targets have not yet been discovered: {}",
                    fullDiscoveryUndiscovered.stream()
                            .map(String::valueOf)
                            .collect(Collectors.joining(", ")));
        }
        if (!staleIncrementalDiscoveryTargetMap.isEmpty()) {
            logger.warn("The following targets have not had an incremental discovery completed"
                            + " in more than {} incremental discovery intervals:",
                    numDiscoveryIntervalsMissedBeforeLogging);
            staleIncrementalDiscoveryTargetMap.forEach((targetId, lastDiscoveryTime) -> logger.warn(
                    "Target {} had its last incremental discovery at {}.", targetId,
                    lastDiscoveryTime));
        }
        if (!incrementalDiscoveryUndiscovered.isEmpty()) {
            logger.warn("No incremental discoveries have completed yet on the following targets:"
                    + " {}", incrementalDiscoveryUndiscovered.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(", ")));
        }
    }

    /**
     * A helper method that executes scheduled topology broadcasts.
     */
    private void executeTopologyBroadcast() {
        try {
            topologyHandler.broadcastLatestTopology(journalFactory);
        } catch (RuntimeException | PipelineException e) {
            // Continue to execute future broadcasts if a generic runtime exception occurred.
            logger.error("Unexpected runtime exception when executing scheduled broadcast.", e);
        } catch (InterruptedException e) {
            logger.error("Interruption during broadcast of latest topology.");
            // Propagate the interrupt status.
            Thread.currentThread().interrupt();
        }
        // we need to catch RuntimeException because if the logging call throws one, future
        // topology broadcasts will not be scheduled
        try {
            // log information about lagging discoveries
            logLaggingDiscoveries();
        } catch (RuntimeException e) {
            logger.warn("Error while logging discovery information.", e);
        }
    }

    /**
     * Initialize the broadcast data, with the broadcast interval set by the injected
     * initialization value in minutes.
     *
     * If an error occurs when attempting to create the broadcast data, resort to creating the data
     * using a hardcoded default.
     *
     * @param initialBroadcastIntervalMinutes The initial broadcast data, specified in minutes. This
     *                                        number can be modified at run-time by calling
     *                                        {@link #setBroadcastSchedule(long, TimeUnit)}
     * @param requireSavedScheduleData If true, requires that saved schedule data be present for the schedule
     *                                 and logs an error if it is not.
     */
    private void initializeBroadcastSchedule(final long initialBroadcastIntervalMinutes, boolean requireSavedScheduleData) {
        long broadcastIntervalMillis = TimeUnit.MILLISECONDS.convert(initialBroadcastIntervalMinutes, TimeUnit.MINUTES);

        try {
            broadcastSchedule = Optional.of(setBroadcastSchedule(broadcastIntervalMillis, TimeUnit.MILLISECONDS));
        } catch (IllegalArgumentException e) {
            logger.error("Failed to initialize broadcast interval. Instead initializing with default value of: " +
                FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES + " minutes.", e);
            broadcastSchedule = Optional.of(
                setBroadcastSchedule(FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES, TimeUnit.MINUTES)
            );
        }
    }

    /**
     * Initialize discovery schedules for all targets initially in the target store.
     *
     * First attempt to restore any prior persisted schedules. For any target that does not
     * have prior persisted data, synch its discovery schedule with the broadcast schedule.
     *
     * @param requireSavedScheduleData If true, requires that saved schedule data be present for the schedule
     *                                 and logs an error if it is not.
     */
    private synchronized void initializeDiscoverySchedules(boolean requireSavedScheduleData) {
        // read all saved discovery schedules from consul into memory
        final Map<String, String> allDiscoverySchedules = scheduleStore.getByPrefix(SCHEDULE_KEY_OFFSET);
        allDiscoverySchedules.forEach((key, value) -> {
            int targetIdIndex = key.indexOf(SCHEDULE_KEY_OFFSET);
            if (targetIdIndex != -1) {
                String idStr = key.substring(SCHEDULE_KEY_OFFSET.length());
                try {
                    targetDiscoveryScheduleData.put(Long.valueOf(idStr),
                            gson.fromJson(value, TargetDiscoveryScheduleData.class));
                } catch (NumberFormatException e) {
                    logger.error("Invalid target id {}", idStr);
                }
            }
        });

        targetStore.getAll().forEach(target -> {
            final long targetId = target.getId();
            final ProbeInfo probeInfo = target.getProbeInfo();
            if (probeInfo.hasFullRediscoveryIntervalSeconds()) {
                initializeFullDiscoverySchedule(targetId, probeInfo.getProbeType(),
                        requireSavedScheduleData);
            }
            if (probeInfo.hasIncrementalRediscoveryIntervalSeconds()) {
                initializeIncrementalDiscoverySchedule(targetId, probeInfo,
                        requireSavedScheduleData);
            }
        });
    }

    /**
     * Initialize the full discovery schedule for a specific target, first attempting to restore it
     * from persistent storage, then attempting to use the default interval from probe, and try to
     * sync with broadcast discovery, if required.
     *
     * @param targetId id of the target whose schedule should be loaded.
     * @param probeType probe type of the target.
     * @param requireSavedScheduleData If true, requires that saved schedule data be present for the schedule
     *                                 and logs an error if it is not.
     */
    private synchronized void initializeFullDiscoverySchedule(long targetId,
            @Nonnull String probeType, boolean requireSavedScheduleData) {
        final Optional<TargetDiscoveryScheduleData> optTargetDiscoveryScheduleData =
                getScheduleData(targetId, requireSavedScheduleData);

        if (optTargetDiscoveryScheduleData.isPresent()) {
            TargetDiscoveryScheduleData targetDiscoveryScheduleData =
                optTargetDiscoveryScheduleData.get();
            scheduleDiscovery(targetId, Objects.requireNonNull(probeType), 0, targetDiscoveryScheduleData.getFullIntervalMillis(),
                DiscoveryType.FULL, targetDiscoveryScheduleData.isSynchedToBroadcast());
        } else {
            try {
                setDiscoverySchedule(targetId, DiscoveryType.FULL, true);
            } catch (TargetNotFoundException | UnsupportedDiscoveryTypeException e) {
                logger.error("Failed to initialize FULL discovery schedule for target " + targetId);
            }
        }
    }

    /**
     * Initialize the incremental discovery schedule for a specific target, first attempting to
     * restore it from persistent storage, then attempting to use the default interval from probe.
     *
     * @param targetId id of the target whose schedule should be loaded.
     * @param probeInfo probe info
     * @param requireSavedScheduleData If true, requires that saved schedule data be present for
     *                                 the schedule and logs an error if it is not.
     */
    private synchronized void initializeIncrementalDiscoverySchedule(
            long targetId, @Nonnull ProbeInfo probeInfo, boolean requireSavedScheduleData) {
        final Optional<TargetDiscoveryScheduleData> optTargetDiscoveryScheduleData =
                getScheduleData(targetId, requireSavedScheduleData);

        if (optTargetDiscoveryScheduleData.isPresent()) {
            TargetDiscoveryScheduleData targetDiscoveryScheduleData =
                optTargetDiscoveryScheduleData.get();

            final Optional<Long> incrementalDiscoveryInterval;
            if (targetDiscoveryScheduleData.hasIncrementalIntervalMillis()) {
                if (probeInfo.hasIncrementalRediscoveryIntervalSeconds()) {
                    // use persisted incremental interval
                    incrementalDiscoveryInterval = Optional.of(
                        targetDiscoveryScheduleData.getIncrementalIntervalMillis());
                } else {
                    incrementalDiscoveryInterval = Optional.empty();
                    // if target doesn't support incremental any more, update schedule data
                    TargetDiscoveryScheduleData newTargetDiscoveryScheduleData =
                        TargetDiscoveryScheduleData.newBuilder(targetDiscoveryScheduleData)
                            .clearIncrementalIntervalMillis()
                            .build();
                    saveScheduleData(newTargetDiscoveryScheduleData, targetId);
                }
            } else {
                if (targetDiscoveryScheduleData.isIncrementalDiscoveryDisabled()) {
                    // log a warning, since user may forgot to change it back
                    logger.warn("Incremental discovery was disabled for target {}", targetId);
                    incrementalDiscoveryInterval = Optional.empty();
                } else {
                    // if target previously doesn't support incremental, schedule data will not
                    // include incremental interval we should check if latest ProbeInfo contains
                    // incremental and schedule it
                    incrementalDiscoveryInterval = getIncrementalDiscoveryInterval(probeInfo);
                    if (incrementalDiscoveryInterval.isPresent()) {
                        // update persisted schedule data
                        TargetDiscoveryScheduleData newTargetDiscoveryScheduleData =
                            TargetDiscoveryScheduleData.newBuilder(targetDiscoveryScheduleData)
                                .setIncrementalIntervalMillis(incrementalDiscoveryInterval.get())
                                .build();
                        saveScheduleData(newTargetDiscoveryScheduleData, targetId);
                    }
                }
            }

            // try to schedule incremental discovery if it's available
            incrementalDiscoveryInterval.ifPresent(interval ->
                scheduleDiscovery(targetId, probeInfo.getProbeType(), 0, interval, DiscoveryType.INCREMENTAL, false));
        } else {
            try {
                // schedule incremental discovery if the probe supports it
                if (probeInfo.hasIncrementalRediscoveryIntervalSeconds()) {
                    setDiscoverySchedule(targetId, DiscoveryType.INCREMENTAL, false);
                }
            } catch (TargetNotFoundException | UnsupportedDiscoveryTypeException e) {
                logger.error("Failed to initialize incremental discovery schedule for target "
                        + targetId, e);
            }
        }
    }

    /**
     * Check for expired operations at some fixed interval.
     *
     * @return The schedule for operation expiration checks.
     */
    private Schedule setupOperationExpirationCheck() {
        final long operationTimeoutMs = Math.min(operationManager.getDiscoveryTimeoutMs(),
            Math.min(operationManager.getValidationTimeoutMs(), operationManager.getActionTimeoutMs()));

        final ScheduledFuture<?> scheduledTask = expiredOperationExecutor.scheduleAtFixedRate(
            operationManager::checkForExpiredOperations,
            operationTimeoutMs,
            operationTimeoutMs,
            TimeUnit.MILLISECONDS
        );

        Schedule schedule = new Schedule(scheduledTask, operationTimeoutMs) {
            @Override
            public String toString() {
                return "OperationExpiration check every " + scheduleIntervalMillis + " ms";
            }
        };
        logger.info("Scheduled operation timeout check " + schedule);

        return schedule;
    }

    /**
     * Persist the schedule data for a given target. It does a partial update based on the
     * parameters provided.
     *
     * @param targetId id of the target
     * @param discoveryType type of the discovery
     * @param discoveryIntervalMillis new interval to overwrite existing one, -1 means the given
     *                                type of discovery should be disabled
     * @param synchedToBroadcast whether or not it's synched to broadcast schedule
     */
    private void saveTargetScheduleData(long targetId, @Nonnull DiscoveryType discoveryType,
            long discoveryIntervalMillis, boolean synchedToBroadcast) {
        Optional<TargetDiscoveryScheduleData> optTargetDiscoveryScheduleData =
                getScheduleData(targetId, false);

        TargetDiscoveryScheduleData.Builder builder = optTargetDiscoveryScheduleData
            .map(TargetDiscoveryScheduleData::newBuilder)
            .orElse(TargetDiscoveryScheduleData.newBuilder());

        switch (discoveryType) {
            case FULL:
                builder.setFullIntervalMillis(discoveryIntervalMillis);
                if (discoveryIntervalMillis == -1) {
                    builder.setSynchedToBroadcast(false);
                } else {
                    builder.setSynchedToBroadcast(synchedToBroadcast);
                }
                break;
            case INCREMENTAL:
                builder.setIncrementalIntervalMillis(discoveryIntervalMillis);
                break;
            default:
                logger.warn("Unsupported discovery type {}, nothing to save", discoveryType);
                return;
        }

        saveScheduleData(builder.build(), targetId);
    }

    /**
     *
     * Save the schedule data to persistent storage.
     *
     * @param scheduleData The broadcast data to save.
     * @param targetId The id of the target to save data for.
     */
    private void saveScheduleData(@Nonnull TargetDiscoveryScheduleData scheduleData, long targetId) {
        scheduleStore.put(scheduleKey(Long.toString(targetId)), gson.toJson(scheduleData, scheduleData.getClass()));
        targetDiscoveryScheduleData.put(targetId, scheduleData);
    }

    /**
     * Delete the data for a schedule from persistent storage.
     * Has no effect if no schedule exists at the key offset.
     *
     * @param targetId The id of the target to delete schedule in persistent storage.
     */
    private void deleteScheduleData(long targetId) {
        scheduleStore.removeKeysWithPrefix(scheduleKey(Long.toString(targetId)));
        targetDiscoveryScheduleData.remove(targetId);
    }

    /**
     * Load the data for a schedule.
     *
     * @param targetId The id of the target to get schedule data for
     * @param requireData If true, the absence of schedule data for the key in the store
     *                    counts as an error.
     * @return A schedule if it was successfully loaded at the key offset, empty otherwise.
     */
    private Optional<TargetDiscoveryScheduleData> getScheduleData(long targetId, boolean requireData) {
        Optional<TargetDiscoveryScheduleData> scheduleData = Optional.ofNullable(
                targetDiscoveryScheduleData.get(targetId));

        if (requireData && !scheduleData.isPresent()) {
            logger.error("Required schedule data for " + targetId + " not found.");
        }
        return scheduleData;
    }

    private String scheduleKey(@Nonnull String key) {
        return SCHEDULE_KEY_OFFSET + key;
    }

    @Override
    public void initialize() {
        boolean requireSavedScheduleData = scheduleStore.containsKey(SCHEDULE_KEY_OFFSET);

        initializeBroadcastSchedule(initialBroadcastIntervalMinutes, requireSavedScheduleData);
        initializeDiscoverySchedules(requireSavedScheduleData);
        operationExpirationSchedule = setupOperationExpirationCheck();
    }

    @Override
    public int priority() {
        return Math.min(targetStore.priority(), probeStore.priority()) - 1;
    }
}
