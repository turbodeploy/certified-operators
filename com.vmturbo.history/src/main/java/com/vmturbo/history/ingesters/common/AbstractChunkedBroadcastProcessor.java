package com.vmturbo.history.ingesters.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.history.ingesters.common.IChunkProcessor.ChunkDisposition;

/**
 * This class manages the processing of data broadcasts, each consisting of a sequence of objects
 * of a given type.
 *
 * <p>A broadcast is delivered as a sequence of chunks, each of which contains the next
 * subsequence of the objects appearing in the overall broadcast.</p>
 *
 * <p>Processing is performed one chunk at a time, but each chunk may be processed by several
 * chunk processors, and these are invoked in parallel. All processing tasks initiated for a given
 * chunk must complete before processing begins for the next chunk.</p>
 *
 * @param <T>       the type of the objects that make up the broadcast
 * @param <InfoT>   type of metadata about the broadcast as a whole
 * @param <StateT>  type of a shared state object provided to all chunk processors
 * @param <ResultT> type of data returned after processing a broadcast
 */
public abstract class AbstractChunkedBroadcastProcessor<T, InfoT, StateT, ResultT> {

    private final Logger logger;
    protected final Collection<? extends IChunkProcessorFactory<T, InfoT, StateT>> chunkProcessorFactories;
    private final ChunkedBroadcastProcessorConfig config;

    /**
     * Creates an {@link AbstractChunkedBroadcastProcessor} instance, which can be used to process
     * multiple broadcasts.
     *
     * <p>Each time a new broadcast is presented for processing, the supplied factories will be
     * invoked to create the chunk processors that will be invoked for each chunk of that broadcast.
     * </p>
     *
     * @param chunkProcessorFactories collection of factories that will create the chunk processors
     * @param config                  config information for the processor
     */
    public AbstractChunkedBroadcastProcessor(
            @Nonnull Collection<? extends IChunkProcessorFactory<T, InfoT, StateT>> chunkProcessorFactories,
            @Nonnull ChunkedBroadcastProcessorConfig config) {
        this.chunkProcessorFactories = chunkProcessorFactories;
        this.config = config;
        this.logger = LogManager.getLogger(getClass());
    }

    /**
     * Create a short summary of key metadata in the broadcast info, primarily for use in logging.
     *
     * @param info the broadcast info metadata object
     * @return the info summary
     */
    protected abstract String summarizeInfo(InfoT info);

    /**
     * Given a topology chunk, return the number of objects processed from the chunk.
     *
     * <p>The default implementation is just the chunk size, but with the new topology
     * extensions capabilities, an ingester can override this in order to count only
     * entities, and not extensions, which may be intermingled in any given topology
     * chunk.</p>
     *
     * @param chunk chunk to be counted
     * @return number of objects processed from the chunk
     */
    protected int getChunkObjectCount(Collection<T> chunk) {
        return chunk.size();
    }

    /**
     * Process a data broadcast.
     *
     * <p>Processing continues chunk-by-chunk as long as active chunk processors remain, by
     * asking all active processors to process the current chunk in parallel. This is followed
     * by asking each chunk processor to perform its finish processing.</p>
     *
     * <p>A processor becomes inactive by returning a {@link ChunkDisposition#DISCONTINUE} value for
     * any single chunk. In addition, if any chunk processor ever returns
     * {@link ChunkDisposition#TERMINATE}, all chunk processors will be immediately deactivated.</p>
     *
     * <p>Finish processing is performed in all chunk processors, even those that are no longer
     * active by the time all chunks have been processed.</p>
     *
     * <p>Supplying a shared state is not normally necessary, unless the invoking code will want
     * access to it after broadcast has been fully processed. If one is not supplied, the subclass
     * will be asked to create an instance.</p>
     *
     * @param info          the metadata info object for this broadcast
     * @param chunkIterator provider of broadcast chunks
     * @param sharedState   shared state to be used by all writers, or null for this to be created
     *                      by the ingester.
     * @return processing results
     */
    public ResultT processBroadcast(@Nonnull final InfoT info,
            @Nonnull final RemoteIterator<T> chunkIterator,
            StateT sharedState) {
        if (sharedState == null) {
            sharedState = getSharedState(info);
        }
        return new ChunksProcessor(chunkIterator, info, sharedState).process();
    }

    /**
     * Process a broadcast, using a shared state instance provided by the subclass.
     *
     * <p>See {@link #processBroadcast(Object, RemoteIterator)} for details.</p>
     *
     * @param info          the metadata info object for this broadcast
     * @param chunkIterator provider of broadcast chunks
     * @return processing results
     */
    public ResultT processBroadcast(@Nonnull final InfoT info,
            @Nonnull final RemoteIterator<T> chunkIterator) {
        return processBroadcast(info, chunkIterator, null);
    }

    /**
     * Return the shared state object to be used for a broadcast.
     *
     * <p>This method will be called exactly once for each broadcast that is processed.</p>
     *
     * @param info info object for the broadcast
     * @return the shared state for the broadcast
     */
    protected StateT getSharedState(InfoT info) {
        return null;
    }

    // processing hooks, to be overridden as needed by subclasses

    /**
     * Hook invoked prior to receipt of first chunk.
     *
     * @param info  info object for this broadcast
     * @param state shared state for this broadcast
     * @param timer timer for this broadcast
     */
    protected void startBroadcastHook(InfoT info, StateT state, MultiStageTimer timer) {
    }

    /**
     * Hook invoked prior to processing each chunk.
     *
     * @param chunk   the chunk to be processed
     * @param chunkNo the chunk number (starting with 1) of this chunk in the overall broadcast
     * @param info    info object for this broadcast
     * @param state   shared state for this broadcast
     * @param timer   timer for this broadcast
     */
    protected void beforeChunkHook(
            Collection<T> chunk, int chunkNo, InfoT info, StateT state, MultiStageTimer timer) {
    }

    /**
     * Hook invoked after processing of each chunk.
     *
     * @param chunk   the chunk that was just processed
     * @param chunkNo the chunk number (starting with 1) of this chunk in the overall broadcast
     * @param info    info object for this broadcast
     * @param state   shared state for this broadcast
     * @param timer   timer for this broadcast
     */
    protected void afterChunkHook(
            Collection<T> chunk, int chunkNo, InfoT info, StateT state, MultiStageTimer timer) {
    }

    /**
     * Hook invoked after all chunks have been processed, before finish processing.
     *
     * @param info  info for this broadcast
     * @param state shared state for this broadcast
     * @param timer timer for this broadcast
     */
    protected void beforeFinishHook(InfoT info, StateT state, MultiStageTimer timer) {
    }

    /**
     * Hook invoked after finish processing.
     *
     * @param info        info object for this broadcast
     * @param state       shared state for this broadcast
     * @param objectCount number of objects processed from this object
     * @param timer       timer object for this broadcast
     */
    protected void afterFinishHook(InfoT info, StateT state, int objectCount, MultiStageTimer timer) {
    }


    /**
     * Get the result of processing a broadcast.
     *
     * @param info  the info object for the broadcast
     * @param state the shared state of the broadcast
     * @return processing result
     */
    protected abstract ResultT getProcessingResult(InfoT info, StateT state);

    /**
     * Nested class to perform processing of the chunks comprising a broadcast.
     */
    private class ChunksProcessor {

        public static final String RECEIVE_CHUNKS_TIMER_STAGE = "Receive Chunks";
        private final RemoteIterator<T> chunkIterator;
        private final List<IChunkProcessor<T>> allProcessors;
        private final List<IChunkProcessor<T>> activeProcessors;
        private final InfoT info;
        private final StateT state;
        private final String infoSummary;
        private final MultiStageTimer timer;
        private int objectCount = 0;
        private long totalChunkTime = 0;

        /**
         * Create a new instance, responsible for processing all the chunks produced by the given
         * chunk iterator.
         *
         * @param chunkIterator iterator that will yield chunks in order
         * @param infoT         info object for this broadcast
         * @param state         shared state for this broadcast
         */
        ChunksProcessor(RemoteIterator<T> chunkIterator, InfoT infoT, StateT state) {
            this.chunkIterator = chunkIterator;
            this.info = infoT;
            this.state = state;
            // create all the chunk processors
            this.allProcessors = chunkProcessorFactories.stream()
                    .map(fac -> fac.getChunkProcessor(infoT, state))
                    // individual processor factories may opt out of a given broadcast
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            // separately keep track of active processors, which may shrink before we finish
            this.activeProcessors = new ArrayList<>(allProcessors);
            this.infoSummary = summarizeInfo(infoT);
            // set up for a concise summary log at end of processing
            this.timer = new MultiStageTimer(logger);
            // seed stage names in order of processors
            timer.start(RECEIVE_CHUNKS_TIMER_STAGE);
            allProcessors.forEach(p -> timer.start(p.getLabel()));
            timer.stopAll();
            logger.debug("Created new ChunksProcesssor for {}", infoSummary);
        }

        /**
         * Process the chunks, one by one, and return the number of objects processed.
         *
         * <p>If we're interrupted during processing we attempt to minimize remaining work that
         * would normally be performed for this broadcast.</p>
         *
         * @return total number of objects processed
         */
        public ResultT process() {
            try (AsyncTimer elapsed = timer.async("Total Elapsed")) {
                logger.info("Processing {}", infoSummary);
                int chunkNo = 1;
                // if we don't flip this by the time we're done, we'll log a warning
                boolean completelyProcessed = false;
                logger.debug("Beginning ingestion of toplogy {}", infoSummary);
                try {
                    startBroadcastHook(info, state, timer);
                    for (Optional<Collection<T>> chunk = getNextChunk();
                         chunk.isPresent();
                         chunk = getNextChunk()) {
                        beforeChunkHook(chunk.get(), chunkNo, info, state, timer);
                        logger.debug("Procesing chunk #{} of topology {}", chunkNo, infoSummary);
                        processChunk(chunk.get(), chunkNo);
                        afterChunkHook(chunk.get(), chunkNo, info, state, timer);
                        chunkNo += 1;
                        objectCount += getChunkObjectCount(chunk.get());
                    }
                    // if we get here and no processor got deactivated, then we are complete
                    completelyProcessed = activeProcessors.size() == allProcessors.size();
                } catch (InterruptedException e) {
                    logger.error("Interrupted while processing chunk #{} from {}; "
                                    + "abandoning remainder of broadcast",
                            chunkNo, infoSummary, e);
                    // don't hide the fact that we got interrupted
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.error(
                            "Failed to obtain chunk #{} from {}; abandoning remainder of broadcast",
                            chunkNo, infoSummary, e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    logger.warn(
                            "Skipping finish processing for {} because we were interrupted",
                            infoSummary);
                } else {
                    beforeFinishHook(info, state, timer);
                    finishProcessing(allProcessors);
                    afterFinishHook(info, state, objectCount, timer);
                }
                if (!completelyProcessed) {
                    logger.warn("{} was not processed in its entirety", infoSummary);
                }
            }
            timer.stopAll().info(
                    String.format("Processed %d entities for %s in ", objectCount, infoSummary),
                    Detail.STAGE_SUMMARY);
            return getProcessingResult(info, state);
        }

        private Optional<Collection<T>> getNextChunk()
                throws InterruptedException, TimeoutException, CommunicationException {

            timer.start(RECEIVE_CHUNKS_TIMER_STAGE);
            try {
                if (chunkIterator.hasNext()) {
                    return Optional.of(chunkIterator.nextChunk());
                } else {
                    return Optional.empty();
                }
            } finally {
                timer.stop();
            }
        }

        // futures for tasks submitted for active chunk processors for current chunk
        private Map<IChunkProcessor<T>, Future<ChunkDisposition>> futures = new HashMap<>();
        // dispositions returned by active chunk processors for current chunk
        private Map<IChunkProcessor<T>, ChunkDisposition> dispositions = new HashMap<>();

        private void processChunk(Collection<T> chunk, int chunkNo) throws InterruptedException {
            final Stopwatch chunkTimer = Stopwatch.createStarted();
            // reset per-chunk state
            futures.clear();
            dispositions.clear();
            submitProcessors(chunk, chunkNo);
            collectResults(chunkNo);
            performDeactivations();
            totalChunkTime += chunkTimer.stop().elapsed().toMillis();
            String rate = String.format("%.2f", chunkNo / (totalChunkTime / 60000.0));
            logger.debug("Processed chunk {} in {}; avg rate {}/min",
                    chunkNo, chunkTimer, rate);
        }

        /**
         * Submit a task for each of our still-active chunk processors to process the current chunk.
         *
         * @param chunk   the current chunk
         * @param chunkNo the chunk number, for use in logging
         */
        private void submitProcessors(Collection<T> chunk, int chunkNo) {
            for (IChunkProcessor<T> processor : activeProcessors) {
                try {
                    futures.put(processor, submitChunkProcessor(processor, chunk, timer));
                } catch (RejectedExecutionException e) {
                    logger.error(
                            "Chunk processor {} could not be scheduled for chunk #{} of {}",
                            processor.getLabel(), chunkNo, infoSummary, e);
                    // if we couldn't schedule a processor, apply its exception disposition
                    dispositions.put(processor, processor.getDispositionOnException());
                }
            }
        }

        /**
         * Collect the dispositions of all the tasks we submitted for processing this chunk.
         *
         * @param chunkNo the chunk number, for use in logging
         * @throws InterruptedException if we're interrupted while awaiting a result
         */
        private void collectResults(int chunkNo) throws InterruptedException {
            // keep track of whether we're interrupted
            InterruptedException interrupted = null;
            for (Entry<IChunkProcessor<T>, Future<ChunkDisposition>> entry : futures.entrySet()) {
                final IChunkProcessor<T> processor = entry.getKey();
                try {
                    if (interrupted == null) {
                        // collect this processor's result
                        long timeLimit = getChunkTimeLimit(processor);
                        dispositions.put(processor, entry.getValue()
                                .get(timeLimit, TimeUnit.MILLISECONDS));
                    } else {
                        // if we've been interrupted, try to stop ongoing work
                        logger.error("Ingester interrupted while procesing chunk #{} of topology {}; "
                                + "attempting to cancel current procesing", chunkNo, infoSummary);
                        entry.getValue().cancel(true);
                    }
                } catch (ExecutionException e) {
                    logger.error(
                            "Chunk processor {} failed to process chunk #{} of {}",
                            processor.getLabel(), chunkNo, infoSummary, e);
                    // for a failed processor, use its exception disposition as a result
                    dispositions.put(processor, processor.getDispositionOnException());
                } catch (TimeoutException e) {
                    // attempt to cancel the in-progress execution, and free up the thread for
                    // another task
                    entry.getValue().cancel(true);
                    logger.error(
                            "Chunk processor {} timed out processing chunk #{} of {}",
                            processor.getLabel(), chunkNo, infoSummary);
                    // for a failed processor, use its exception disposition as a result
                    dispositions.put(processor, processor.getDispositionOnTimeout());
                } catch (InterruptedException e) {
                    logger.warn(
                            "Chunk processor {} was interrupted procesing chunk #{} of {}; "
                                    + "other processors that have not completed will be canceled.",
                            processor.getLabel(), chunkNo, infoSummary);
                    // don't hide the fact that this thread has been interrupted
                    Thread.currentThread().interrupt();
                    // and stash the excption so we can rethrow after we've canceled all the
                    // other processors
                    interrupted = e;
                }
            }
            if (interrupted != null) {
                throw interrupted;
            }
        }

        /**
         * Handle dispositions reported by our chunk prcoessors.
         */
        private void performDeactivations() {
            for (Entry<IChunkProcessor<T>, ChunkDisposition> entry : dispositions.entrySet()) {
                switch (entry.getValue()) {
                    case DISCONTINUE:
                        // this processor doesn't want any more chunks
                        logger.debug("Writer {} has opted out of further chunks for topology {}",
                                entry.getKey().getLabel(), infoSummary);
                        activeProcessors.remove(entry.getKey());
                        break;
                    case TERMINATE:
                        // no more chunks should be processed by this or any other chunk processor
                        logger.debug("Writer {} has terminated topology {}; "
                                        + "no further chunks will be processed",
                                entry.getKey().getLabel(), infoSummary);
                        activeProcessors.clear();
                        // no need to check any other dispositions
                        return;
                    case SUCCESS:
                    case CONTINUE:
                    default:
                        // keep using this chunk processor
                        break;
                }
            }
        }

        /**
         * Get the time limit for processing a chunk by the given processor.
         *
         * @param processor chunk processor
         * @return time limit in msec
         */
        private long getChunkTimeLimit(IChunkProcessor<T> processor) {
            final Long limit = processor.getChunkTimeLimitMsec();
            return limit != null ? limit : config.defaultChunkTimeLimitMsec();
        }

        /**
         * Invoke finish processing on all the chunk processors after final chunk has been
         * processed.
         *
         * <p>This is invoked even for processors that were deactivated during processing.</p>
         *
         * @param chunkProcessors complete initial list of chunk processors
         */
        private void finishProcessing(List<IChunkProcessor<T>> chunkProcessors) {
            // true if we were interrupted during chunk processing, or if a processor is interrupted
            // during finish processing; in either case, remaining processors are asked to
            // expedite their finish processing
            boolean interrupted = Thread.currentThread().isInterrupted();
            for (IChunkProcessor<T> cp : chunkProcessors) {
                try {
                    timer.start(cp.getLabel());
                    cp.finish(objectCount, interrupted, infoSummary);
                } catch (InterruptedException e) {
                    logger.warn(
                            "Chunk processor {} interrupted during finish processing for  {}; "
                                    + "other processors will be advised",
                            cp.getLabel(), infoSummary, e);
                    interrupted = true;
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.warn(
                            "Chunk procesor {} failed during finish processing for {}",
                            cp.getLabel(), infoSummary, e);
                } finally {
                    timer.stop();
                }
            }
        }

        /**
         * Submit a task to execute the given chunk processor on the given chunk.
         *
         * <p>We time each execution so we can get useful summary timings at end of broadcast.</p>
         *
         * @param processor the chunk processor instance to process the chunk
         * @param chunk     the chunk to be processed
         * @param timer     a timer to track processing by various chunk processors
         * @return future that will provide processing disposition
         */
        private Future<ChunkDisposition> submitChunkProcessor(
                IChunkProcessor<T> processor, Collection<T> chunk, MultiStageTimer timer) {

            return config.threadPool().submit(() -> {
                try (AsyncTimer taskTimer = timer.async(processor.getLabel())) {
                    return processor.processChunk(chunk, infoSummary);
                }
            });
        }
    }
}
