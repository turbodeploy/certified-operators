package com.vmturbo.voltron.extensions.tp;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * A scheduler implementation which does not automatically create a discovery or broadcast schedule.
 */
public class CacheOnlyScheduler extends Scheduler {

    private final Logger logger = LogManager.getLogger();

    /**
     * Create a {@link CacheOnlyScheduler} for preventing scheduled discoveries.
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
    public CacheOnlyScheduler(@Nonnull final IOperationManager operationManager,
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
        super(operationManager, targetStore, probeStore, topologyHandler, scheduleStore,
                journalFactory, fullDiscoveryExecutor, incrementalDiscoveryExecutor, broadcastExecutor,
                expirationExecutor, initialBroadcastIntervalMinutes, numDiscoveryIntervalsMissedBeforeLogging);
    }

    @Override
    public void initialize() {
        logger.info("Not scheduling automatic discoveries or broadcasts in cache only mode!");
    }
}
