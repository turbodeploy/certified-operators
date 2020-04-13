package com.vmturbo.topology.processor.scheduling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Schedule.ScheduleData;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule.TargetDiscoveryScheduleData;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

/**
 * Maintains and runs scheduled events such as target discovery and topology broadcast.
 *
 * TARGET DISCOVERY NOTES: a target is mapped to a single discovery schedule.
 * Setting the discovery schedule for a target that already has one overrides
 * the existing schedule.
 *
 * When a scheduled discovery is attempted for a target that already has an ongoing discovery, a
 * pending discovery is instead added for that target. See
 * {@link OperationManager#addPendingDiscovery(long)} for further details on how this works.
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
public class Scheduler implements TargetStoreListener {
    private final Logger logger = LogManager.getLogger();
    // thread pool for scheduling discoveries on.
    private final ScheduledExecutorService discoveryScheduleExecutor;
    // thread pool for scheduling the realtime broadcast.
    private final ScheduledExecutorService broadcastExecutor;
    // thread pool for expiring long-running operations with.
    private final ScheduledExecutorService expiredOperationExecutor;
    private final Map<Long, TargetDiscoverySchedule> discoveryTasks;
    private final IOperationManager operationManager;
    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final TopologyHandler topologyHandler;
    private final KeyValueStore scheduleStore;
    private final StitchingJournalFactory journalFactory;
    private final Gson gson = new Gson();

    private Optional<TopologyBroadcastSchedule> broadcastSchedule;
    private final Schedule operationExpirationSchedule;

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
     * @param discoveryExecutor The executor to be used for scheduling discoveries.
     * @param broadcastExecutor The executor to be used for scheduling realtime broadcasts.
     * @param expirationExecutor The executor to be used for scheduling pending operation expiration checks.
     * @param initialBroadcastIntervalMinutes The initial broadcast interval specified in minutes.
     */
    public Scheduler(@Nonnull final IOperationManager operationManager,
                     @Nonnull final TargetStore targetStore,
                     @Nonnull final ProbeStore probeStore,
                     @Nonnull final TopologyHandler topologyHandler,
                     @Nonnull final KeyValueStore scheduleStore,
                     @Nonnull final StitchingJournalFactory journalFactory,
                     @Nonnull final ScheduledExecutorService discoveryExecutor,
                     @Nonnull final ScheduledExecutorService broadcastExecutor,
                     @Nonnull final ScheduledExecutorService expirationExecutor,
                     final long initialBroadcastIntervalMinutes) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.journalFactory = Objects.requireNonNull(journalFactory);
        this.discoveryScheduleExecutor = Objects.requireNonNull(discoveryExecutor);
        this.broadcastExecutor = Objects.requireNonNull(broadcastExecutor);
        this.expiredOperationExecutor = Objects.requireNonNull(expirationExecutor);
        this.scheduleStore = Objects.requireNonNull(scheduleStore);

        discoveryTasks = new HashMap<>();
        broadcastSchedule = Optional.empty();
        targetStore.addListener(this);

        boolean requireSavedScheduleData = scheduleStore.containsKey(SCHEDULE_KEY_OFFSET);

        initializeBroadcastSchedule(initialBroadcastIntervalMinutes, requireSavedScheduleData);
        initializeDiscoverySchedules(requireSavedScheduleData);
        operationExpirationSchedule = setupOperationExpirationCheck();
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
     * @param discoveryInterval The interval at which to discover the target.
     * @param unit The time units to apply to the discovery interval.
     * @throws TargetNotFoundException when the targetId is not associated with any known target.
     * @throws IllegalArgumentException when the discoveryInterval is less than or equal to zero.
     * @return The discovery schedule for the input target.
     */
    @Nonnull
    public synchronized TargetDiscoverySchedule setDiscoverySchedule(final long targetId,
                                                                     final long discoveryInterval,
                                                                     @Nonnull final TimeUnit unit)
        throws TargetNotFoundException {
        targetStore.getTarget(targetId).orElseThrow(
            () -> new TargetNotFoundException(targetId));
        if (discoveryInterval <= 0) {
            throw new IllegalArgumentException("Illegal discovery interval: " + discoveryInterval);
        }

        final long discoveryIntervalMillis = TimeUnit.MILLISECONDS.convert(discoveryInterval, unit);
        saveScheduleData(new TargetDiscoveryScheduleData(discoveryIntervalMillis, false), Long.toString(targetId));

        final Optional<TargetDiscoverySchedule> existingSchedule = stopAndRemoveDiscoverySchedule(targetId);
        final long initialDelayMillis = calculateInitialDelayMillis(discoveryIntervalMillis, existingSchedule);

        return scheduleDiscovery(targetId, initialDelayMillis, discoveryIntervalMillis, false);
    }

    /**
     * Set the discovery schedule for a target, synched to the broadcast schedule. The schedule interval
     * will be set to match the broadcast schedule interval. If the broadcast schedule interval is updated,
     * the synched discovery schedule will also be updated.
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
     * @throws TargetNotFoundException when the targetId is not associated with any known target.
     * @return The discovery schedule for the input target.
     */
    @Nonnull
    public synchronized TargetDiscoverySchedule setBroadcastSynchedDiscoverySchedule(final long targetId)
        throws TargetNotFoundException {
        Target target = targetStore.getTarget(targetId).orElseThrow(
            () -> new TargetNotFoundException(targetId));

        // get the probe info so we can find the probe's preferred discovery interval.
        ProbeInfo probeInfo = probeStore.getProbe(target.getProbeId()).orElseThrow(
            () -> new IllegalStateException("Probe id "+ target.getProbeId() +" not found in probe store."));

        long discoveryIntervalMillis = getProbeDiscoveryInterval(probeInfo);

        saveScheduleData(new TargetDiscoveryScheduleData(discoveryIntervalMillis, true), Long.toString(targetId));

        final Optional<TargetDiscoverySchedule> existingSchedule = stopAndRemoveDiscoverySchedule(targetId);
        final long initialDelayMillis = calculateInitialDelayMillis(discoveryIntervalMillis, existingSchedule);

        return scheduleDiscovery(targetId, initialDelayMillis, discoveryIntervalMillis, true);
    }

    /**
     * Get the discovery interval to use for this probe.
     *
     * We will use the smaller of either the full rediscovery interval or performance rediscovery
     * interval as our rediscovery interval. This is because XL does not support incremental
     * discovery, which some probes rely on in classic.
     *
     * For example the VC Probe configures full discovery every hour, with the expectation that
     * incremental discoveries on 30 second intervals will fill the gaps between full discoveries.
     * It also specifies "performance" discoveries on 10 minute intervals that ensure updates to the
     * stats on the usual broadcast cycle.
     *
     * XL only supports "full" rediscoveries, so our logic to determine this full discovery interval
     * will be based on the smaller of the full discovery and performance discovery intervals, if
     * available. This should handle the VC configuration (and any other probe using the same full +
     * incremental discovery config), while also respecting the full discovery interval values
     * configured in the probes.
     *
     * In this method, we will also only accept probe discovery intervals that are greater than the
     * broadcast interval. If a probe configures discovery at a higher frequency than the broadcast
     * cycle, we will fall back to the broadcast time. There isn't much point to discovering more
     * frequently than the broadcast time, since any results wouldn't be shown until the next
     * broadcast cycle anyways.
     *
     * @param probeInfo The {@link ProbeInfo} to read the discovery interval properties frrom.
     * @return The discovery interval to use (in milliseconds)
     */
    public long getProbeDiscoveryInterval(ProbeInfo probeInfo) {
        // set the default discovery interval to the broadcast interval.
        long broadcastIntervalMillis = getBroadcastIntervalMillis();
        long discoveryIntervalMillis = broadcastIntervalMillis;

        // as per discussion with Ron, use the smaller of the performance discovery interval or
        // full discovery interval, if both are specified.
        if (probeInfo.getFullRediscoveryIntervalSeconds() > 0) {
            discoveryIntervalMillis = TimeUnit.SECONDS.toMillis(probeInfo.getFullRediscoveryIntervalSeconds());
        }
        // if there is a performance discovery setting, use that if it's less than the current
        // discovery interval.
        if (probeInfo.getPerformanceRediscoveryIntervalSeconds() > 0) {
            long performanceRediscoveryMillis = TimeUnit.SECONDS.toMillis(probeInfo.getPerformanceRediscoveryIntervalSeconds());
            if (performanceRediscoveryMillis < discoveryIntervalMillis) {
                // we're falling back to the performance discovery interval -- log it.
                logger.info("Setting discovery interval based on performance discovery interval"
                        +" of {} ms for probe type {}", performanceRediscoveryMillis, probeInfo.getProbeType());
                discoveryIntervalMillis = performanceRediscoveryMillis;
            }
        }

        // the final discovery interval should be at least as long as the broadcast cycle
        return Math.max(discoveryIntervalMillis, broadcastIntervalMillis);
    }

    /**
     * Get the discovery schedule for a target.
     *
     * @param targetId The ID of the target whose schedule should be retrieved.
     * @return An {@link Optional} containing the target's discovery schedule, or
     *         {@link Optional#empty()} if no schedule exists for the target.
     */
    public synchronized Optional<TargetDiscoverySchedule> getDiscoverySchedule(final long targetId) {
        return Optional.ofNullable(discoveryTasks.get(targetId));
    }

    /**
     * Cancel the discovery schedule for a target. Cancelling the discovery schedule
     * will close the regular discovery of the target at fixed intervals.
     *
     * @param targetId The ID of the target whose schedule should be cancelled.
     * @return An {@link Optional} containing the target's cancelled discovery schedule
     *         if it was successfully cancelled, or {@link Optional#empty()} if the
     *         data could not be cancelled.
     */
    public synchronized Optional<TargetDiscoverySchedule> cancelDiscoverySchedule(final long targetId) {
        if (getDiscoverySchedule(targetId).isPresent()) {
            deleteScheduleData(Long.toString(targetId));
        }

        return stopAndRemoveDiscoverySchedule(targetId);
    }

    /**
     * Reset the discovery schedule for a target. Resetting the discovery schedule for a
     * target will cause the full scheduled discovery interval to elapse from the time
     * of the reset before the next scheduled discovery.
     *
     * If there is no existing discovery schedule for the target, returns
     * {@link Optional#empty()}, otherwise returns an {@link Optional} containing
     * the new TargetDiscoverySchedule describing the scheduled discovery.
     *
     * @param targetId the ID of the target whose discovery schedule should be reset.
     * @return If a discovery schedule exists for the target, an {@link Optional} containing
     * the new TargetDiscoverySchedule describing the scheduled discovery. Otherwise
     * returns {@link Optional#empty()}.
     */
    public synchronized Optional<TargetDiscoverySchedule> resetDiscoverySchedule(final long targetId) {
        // Cancel the ongoing task and reschedule it.
        final Optional<TargetDiscoverySchedule> discoveryTask = stopAndRemoveDiscoverySchedule(targetId);
        return discoveryTask.map(task -> scheduleDiscovery(
            task.getTargetId(),
            task.getScheduleInterval(TimeUnit.MILLISECONDS),
            task.getScheduleInterval(TimeUnit.MILLISECONDS),
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
        saveScheduleData(new ScheduleData(broadcastIntervalMillis), BROADCAST_SCHEDULE_KEY);

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
     * Cancel the topology broadcast data. Cancelling the topology broadcast data
     * will close the regular topology broadcast at fixed intervals.
     *
     * @return An {@link Optional} containing the cancelled broadcast data
     *         if it was successfully cancelled, or {@link Optional#empty()} if the
     *         data could not be cancelled.
     */
    public synchronized Optional<TopologyBroadcastSchedule> cancelBroadcastSchedule() {
        deleteScheduleData(BROADCAST_SCHEDULE_KEY);
        return stopAndRemoveBroadcastSchedule();
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
     * When a target is added, create a default discovery schedule for the target that is synched to
     * match the interval of the topology broadcast schedule.
     *
     * @param target The target that was added to the {@TargetStore}.
     */
    @Override
    public void onTargetAdded(final @Nonnull Target target) {
        Objects.requireNonNull(target);

        try {
            setBroadcastSynchedDiscoverySchedule(target.getId());
        } catch (TargetNotFoundException e) {
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
        cancelDiscoverySchedule(target.getId());
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
     * @param delayMillis The initial delay before running the first discovery in the schedule.
     * @param discoveryIntervalMillis The interval between repeated discoveries for the target.
     * @param synchedToBroadcastSchedule Whether the discovery should be synched to the schedule
     *                                   for broadcasting the topology.
     * @return The discovery schedule for the target.
     */
    private TargetDiscoverySchedule scheduleDiscovery(final long targetId,
                                                      final long delayMillis,
                                                      final long discoveryIntervalMillis,
                                                      boolean synchedToBroadcastSchedule) {
        final ScheduledFuture<?> scheduledTask = discoveryScheduleExecutor.scheduleAtFixedRate(
            () -> executeScheduledDiscovery(targetId),
            delayMillis,
            discoveryIntervalMillis,
            TimeUnit.MILLISECONDS
        );

        final TargetDiscoverySchedule discoveryTask =
            new TargetDiscoverySchedule(scheduledTask, targetId, discoveryIntervalMillis, synchedToBroadcastSchedule);
        discoveryTasks.put(targetId, discoveryTask);
        logger.info("Set {}", discoveryTask);

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
     * the topology broadcast data.
     */
    private void updateBroadcastSynchedDiscoverySchedules() {
        List<Long> synchedDiscoverySchedules = discoveryTasks.entrySet().stream()
            .filter(entry -> entry.getValue().isSynchedToBroadcast())
            .map(Entry::getKey)
            .collect(Collectors.toList());

        synchedDiscoverySchedules.stream().forEach(targetId -> {
            try {
                setBroadcastSynchedDiscoverySchedule(targetId);
            } catch (TargetNotFoundException|IllegalStateException e) {
                logger.error("Unable to update synched discovery schedule for target " + targetId, e);
            }
        });
    }

    /**
     * Cancel a target's discovery schedule and remove it from the internal map.
     *
     * @param targetId The ID of the target whose discovery schedule should be stopped and removed.
     * @return The stopped discovery schedule if one existed for the target, or empty if not.
     */
    private Optional<TargetDiscoverySchedule> stopAndRemoveDiscoverySchedule(final long targetId) {
        final Optional<TargetDiscoverySchedule> task = Optional.ofNullable(discoveryTasks.remove(targetId));
        task.ifPresent(TargetDiscoverySchedule::cancel);

        return task;
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
     */
    private void executeScheduledDiscovery(final long targetId) {
        try {
            operationManager.addPendingDiscovery(targetId);
        } catch (TargetNotFoundException | InterruptedException e) {
            // When a target is not found or operation is interrupted, it is a
            // non-recoverable error and it does not make sense to continue
            // executing discoveries with this task, so close.
            logger.error("Unrecoverable error during discovery execution. Cancelling discovery schedule for the target.", e);

            cancelDiscoverySchedule(targetId);
        } catch (CommunicationException | RuntimeException e) {
            // Communication exceptions are correctable errors, so continue
            // to run future discoveries. Also continue to execute future discoveries
            // if a generic runtime exception occurred.
            logger.error("Exception when executing scheduled discovery.", e);
        }
    }

    /**
     * A helper method that executes scheduled topology broadcasts.
     */
    private void executeTopologyBroadcast() {
        try {
            topologyHandler.broadcastLatestTopology(journalFactory);
        } catch (RuntimeException | TopologyPipelineException e) {
            // Continue to execute future broadcasts if a generic runtime exception occurred.
            logger.error("Unexpected runtime exception when executing scheduled broadcast.", e);
        } catch (InterruptedException e) {
            logger.error("Interruption during broadcast of latest topology.");
            // Propagate the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Initialize the broadcast data, with the broadcast interval set by a value specified in minutes.
     *
     * First attempt to restore the broadcast data from its persisted location. If no data can be
     * restored, attempt to use the injected initialization value.
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
        long broadcastIntervalMillis = loadScheduleData(BROADCAST_SCHEDULE_KEY, ScheduleData.class, requireSavedScheduleData)
            .map(ScheduleData::getScheduleIntervalMillis)
            .orElse(TimeUnit.MILLISECONDS.convert(initialBroadcastIntervalMinutes, TimeUnit.MINUTES));

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
    private void initializeDiscoverySchedules(boolean requireSavedScheduleData) {
        targetStore.getAll().stream()
            .map(Target::getId)
            .forEach(targetId -> initializeDiscoverySchedule(targetId, requireSavedScheduleData));
    }

    /**
     * Initialize the discovery schedule for a specific target, first attempting to restore it
     * from persistent storage, then attempting to use a default broadcast-synched discovery
     * if no schedule can be loaded.
     *
     * @param targetId The ID of the target whose schedule should be loaded.
     * @param requireSavedScheduleData If true, requires that saved schedule data be present for the schedule
     *                                 and logs an error if it is not.
     */
    private void initializeDiscoverySchedule(long targetId, boolean requireSavedScheduleData) {
        loadScheduleData(Long.toString(targetId), TargetDiscoveryScheduleData.class, requireSavedScheduleData)
            .map(data -> scheduleDiscovery(targetId, 0, data.getScheduleIntervalMillis(), data.isSynchedToBroadcast()))
            .orElseGet(() -> {
                try {
                    return setBroadcastSynchedDiscoverySchedule(targetId);
                } catch (TargetNotFoundException e) {
                    logger.error("Failed to initialize discovery schedule for target " + targetId);
                    return null;
                }
            });
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
     *
     * Save the schedule data to persistent storage.
     *
     * @param scheduleData The broadcast data to save.
     * @param key The key to save the data at.
     * @param <S> The type of {@link ScheduleData} to save.
     */
    private <S extends ScheduleData> void saveScheduleData(@Nonnull S scheduleData, @Nonnull String key) {
        scheduleStore.put(scheduleKey(key), gson.toJson(scheduleData, scheduleData.getClass()));
    }

    /**
     * Delete the data for a schedule from persistent storage.
     * Has no effect if no schedule exists at the key offset.
     *
     * @param key The key for the schedule in persistent storage.
     */
    private void deleteScheduleData(@Nonnull String key) {
        scheduleStore.removeKeysWithPrefix(scheduleKey(key));
    }

    /**
     * Load the data for a schedule from persistent storage.
     *
     * @param key The key at which to look for the data in persistent storage.
     * @param scheduleDataClass The class of data to retrieve from persistent storage.
     * @param requireData If true, the absence of schedule data for the key in the store
     *                    counts as an error.
     * @param <S> The class of the data for the schedule.
     * @return A schedule if it was successfully loaded at the key offset, empty otherwise.
     */
    private <S extends ScheduleData> Optional<S> loadScheduleData(@Nonnull String key,
                                                                  @Nonnull Class<S> scheduleDataClass,
                                                                  boolean requireData) {
        Optional<S> scheduleData = scheduleStore.get(scheduleKey(key))
            .map(schedule -> Optional.of(gson.fromJson(schedule, scheduleDataClass)))
            .orElse(Optional.empty());

        if (requireData && !scheduleData.isPresent()) {
            logger.error("Required schedule data for " + key + " not found.");
        }
        return scheduleData;
    }

    private String scheduleKey(@Nonnull String key) {
        return SCHEDULE_KEY_OFFSET + key;
    }
}
