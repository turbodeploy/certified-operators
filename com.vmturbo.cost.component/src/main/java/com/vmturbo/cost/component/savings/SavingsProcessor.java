package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupTimesStore;

/**
 * Called periodically (daily) by ExecutorService and triggers savings processing when called.
 */
public class SavingsProcessor {
    private final Logger logger = LogManager.getLogger();

    /**
     * UTC clock.
     */
    private final Clock clock;

    /**
     * Store for timing related to when last processing was done.
     */
    private final RollupTimesStore rollupTimesStore;

    /**
     * Store for rollup processing.
     */
    private final RollupSavingsProcessor rollupProcessor;

    /**
     * State tracking store.
     */
    private final StateStore stateStore;

    /**
     * Processing chunk size, typically 100 or so entities at a time.
     */
    private final int chunkSize;

    /**
     * Tracker instance.
     */
    private final SavingsTracker savingsTracker;

    /**
     * For old data cleanup.
     */
    private final DataRetentionProcessor dataRetentionProcessor;

    /**
     * Creates a new savings processor instance.
     *
     * @param clock Clock for time format conversion.
     * @param chunkSize Query chunk size.
     * @param rollupTimesStore Store for reading last saved times.
     * @param rollupProcessor Store for triggering rollups to monthly.
     * @param stateStore Store for reading states.
     * @param savingsTracker Tracker instance.
     * @param dataRetentionProcessor For cleanup of old data.
     */
    public SavingsProcessor(@Nonnull final Clock clock,
            int chunkSize,
            @Nonnull final RollupTimesStore rollupTimesStore,
            @Nonnull RollupSavingsProcessor rollupProcessor,
            @Nonnull final StateStore stateStore,
            @Nonnull final SavingsTracker savingsTracker,
            @Nonnull final DataRetentionProcessor dataRetentionProcessor) {
        this.clock = clock;
        this.chunkSize = chunkSize;
        this.rollupTimesStore = rollupTimesStore;
        this.rollupProcessor = rollupProcessor;
        this.stateStore = stateStore;
        this.savingsTracker = savingsTracker;
        this.dataRetentionProcessor = dataRetentionProcessor;
    }

    /**
     * Called periodically (daily), calls the savings tracker to process stats for the time periods
     * required. Manages rollup details of saved stats.
     */
    public void execute() {
        try {
            final SavingsTimes savingsTimes = new SavingsTimes(rollupTimesStore.getLastRollupTimes(),
                    clock);
            logger.info("Start billing savings processing with times: {}.", savingsTimes);

            // Total count of chunks.
            final AtomicInteger chunkCounter = new AtomicInteger();
            // Total count of entities.
            final AtomicInteger entityCounter = new AtomicInteger();
            final List<EntityState> stateChunk = new ArrayList<>();

            // Process a chunk of entity states at a time.
            final AtomicBoolean successfullyProcessed = new AtomicBoolean(true);
            stateStore.getEntityStates(state -> {
                if (!successfullyProcessed.get()) {
                    // If any chunk processing fails, we skip the rest.
                    return;
                }
                entityCounter.incrementAndGet();
                if (stateChunk.size() < chunkSize) {
                    stateChunk.add(state);
                    // We are still filling up the current chunk, so return here.
                    return;
                }
                // One chunk is now filled up, ready for processing. This also clears the chunk list.
                if (!processStateChunk(stateChunk, savingsTimes, chunkCounter)) {
                    successfullyProcessed.set(false);
                }
            });
            // Process any leftover chunk items, if we are able to process the previous ones.
            if (!stateChunk.isEmpty() && !successfullyProcessed.get()) {
                if (!processStateChunk(stateChunk, savingsTimes, chunkCounter)) {
                    successfullyProcessed.set(false);
                }
            }
            // Update rollup time if successfully processed all chunks.
            if (entityCounter.get() > 0 && successfullyProcessed.get()) {
                updateRollup(savingsTimes);
            }
            dataRetentionProcessor.process(false);
            logger.info("End billing savings processing ({}) for {} entities in {} chunks.",
                    (successfullyProcessed.get() ? "Success" : "Failed"), entityCounter, chunkCounter);
        } catch (Exception e) {
            logger.error("Unable to process billing savings/investments.", e);
        }
    }

    /**
     * Processes a chunk of entity states.
     *
     * @param stateChunk Chunk (batch) of entities, e.g. a set of 100, to process at a time.
     * @param savingsTimes Contains timing related info used for query, stores responses as well.
     * @param chunkCounter Total chunk counter.
     * @return True if this chunk was successfully processed, false if an error during processing.
     */
    private boolean processStateChunk(@Nonnull final List<EntityState> stateChunk,
            @Nonnull final SavingsTimes savingsTimes, @Nonnull final AtomicInteger chunkCounter) {
        boolean processed = true;
        try {
            savingsTracker.processStates(stateChunk, savingsTimes, chunkCounter);
            // Once we process this chunk, we clear the chunk states list, in preparation
            // for it to be filled with the next chunk of states.
            stateChunk.clear();
            chunkCounter.incrementAndGet();
        } catch (EntitySavingsException ese) {
            logger.warn("Unable to process state chunk # {} of size {}", chunkCounter,
                    stateChunk.size(), ese);
            logger.trace("For chunk # {}, states: {}", chunkCounter, stateChunk);
            processed = false;
        }
        return processed;
    }

    /**
     * Called once stats have been saved for a time period. Updates the daily rollup timestamp,
     * so that we can continue from there next time we get called. Also triggers daily -> monthly
     * rollup.
     * savingsTimes Contains timing related info used for query, stores responses as well.
     */
    private void updateRollup(@Nonnull final SavingsTimes savingsTimes) {
        final List<Long> dailyTimes = savingsTimes.getSortedDailyStatsTimes();
        savingsTimes.updateLastRollupTimes(dailyTimes);
        final LastRollupTimes updatedRollupTimes = savingsTimes.getLastRollupTimes();
        rollupTimesStore.setLastRollupTimes(updatedRollupTimes);

        logger.info("Updated last times to: {}. Total {} daily times.", updatedRollupTimes,
                dailyTimes.size());
        logger.trace("Daily written times: {}", () -> dailyTimes);

        // Get the start times for all periods, those are the ones to rollup to monthly.
        rollupProcessor.process(RollupDurationType.DAILY, new ArrayList<>(dailyTimes));
    }
}
