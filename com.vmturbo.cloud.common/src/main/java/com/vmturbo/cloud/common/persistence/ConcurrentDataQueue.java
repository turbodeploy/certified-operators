package com.vmturbo.cloud.common.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.SetOnce;

/**
 * An implementation of {@link DataQueue}, providing a thread pool for concurrent processing of data batches.
 * @param <DataTypeT> The data type.
 * @param <DataStatsT> The data statistics type.
 * @param <DataSummaryT> The data statistics summary tupe.
 */
public class ConcurrentDataQueue<DataTypeT, DataStatsT, DataSummaryT> implements DataQueue<DataTypeT, DataSummaryT> {

    private final Logger logger = LogManager.getLogger();

    private final DataSink<DataTypeT, DataStatsT> dataSink;

    private final DataBatcher<DataTypeT> dataBatcher;

    private final DataQueueJournal<DataStatsT, DataSummaryT> queueJournal;

    private final DataQueueConfiguration queueConfiguration;

    private final ExecutorService executorService;

    private final List<DataTypeT> dataQueue = new LinkedList<>();

    private final AtomicLong pendingOperations = new AtomicLong();

    private final Object jobCompletionSignal = new Object();

    private final BlockingQueue<DataOperation> failedJobs = new LinkedBlockingQueue<>();

    private final AtomicLong operationIdProvider = new AtomicLong();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean retryFailedJobs = new AtomicBoolean(true);

    /**
     * Constructs a new {@link ConcurrentDataQueue} instance.
     * @param dataSink The data sink.
     * @param dataBatcher The data batcher.
     * @param queueJournal The queue journal, responsible for collecting stats and logging
     * events.
     * @param queueConfiguration The queue configuration.
     */
    public ConcurrentDataQueue(@Nonnull DataSink<DataTypeT, DataStatsT> dataSink,
                               @Nonnull DataBatcher<DataTypeT> dataBatcher,
                               @Nonnull DataQueueJournal<DataStatsT, DataSummaryT> queueJournal,
                               @Nonnull DataQueueConfiguration queueConfiguration) {

        this.dataSink = Objects.requireNonNull(dataSink);
        this.dataBatcher = Objects.requireNonNull(dataBatcher);
        this.queueJournal = Objects.requireNonNull(queueJournal);
        this.queueConfiguration = Objects.requireNonNull(queueConfiguration);

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(queueConfiguration.queueName() + "-%d")
                .build();
        this.executorService = Executors.newFixedThreadPool(
                Math.max(queueConfiguration.concurrency(), 1), threadFactory);
    }

    @Override
    public void addData(DataTypeT data) {

        synchronized (dataQueue) {

            checkClosed();

            dataBatcher.splitBatches(data).forEach(dataBatch -> {

                dataQueue.add(dataBatch);

                if (dataBatcher.isFullBatch(dataQueue)) {
                    flush();
                }
            });
        }
    }

    @Override
    public void flush() {

        // Make sure nothing attempts to close the executor service until
        // data currently in the queue has been submitted.
        synchronized (dataQueue) {

            checkClosed();

            final List<DataTypeT> dataBatch = ImmutableList.copyOf(dataQueue);

            if (!dataBatch.isEmpty()) {

                final DataOperation operation = new DataOperation(dataBatch);
                queueJournal.recordOperationCreation(operation);

                CompletableFuture.runAsync(operation, executorService).whenComplete((r, t) -> onOperationCompletion(operation, t));
                pendingOperations.incrementAndGet();
            }

            dataQueue.clear();
        }
    }

    @Override
    public DataQueueStats<DataSummaryT> drainAndClose(@Nonnull Duration timeout) throws Exception {

        // block any further submissions of data
        synchronized (dataQueue) {
            if (!closed.get()) {
                flush();
                close();
            }
        }

        final Instant timeoutBarrier = Instant.now().plus(timeout);
        while (pendingOperations.get() > 0) {

            final Duration waitTime = Duration.between(Instant.now(), timeoutBarrier);
            if (!waitTime.isNegative()) {
                synchronized (jobCompletionSignal) {
                    jobCompletionSignal.wait(waitTime.toMillis());
                }
            } else {
                forceClose();
                throw new TimeoutException("Timed out waiting for data queue operation completion");
            }
        }

        // Any secondary invocations reaching this point should block until
        // retryFailedJobsSequentially completes
        synchronized (retryFailedJobs) {

            if (retryFailedJobs.compareAndSet(true, false)) {
                retryFailedJobsSequentially(timeoutBarrier);
            }
        }

        return queueJournal.getQueueStats();
    }

    @Override
    public void forceClose() {
        closed.set(true);
        executorService.shutdownNow();
    }

    private void onOperationCompletion(@Nonnull DataOperation dataOperation,
                                       @Nullable Throwable t) {

        try {

            if (t != null) {
                // Queue the operation to possibly retry during queue shutdown
                failedJobs.add(dataOperation);
                queueJournal.recordFailedOperation(dataOperation, t);
            } else {
                queueJournal.recordSuccessfulOperation(dataOperation);
            }

        } catch (Exception e) {
            logger.error("Error during job completion callback", e);
        } finally {
            pendingOperations.decrementAndGet();
            synchronized (jobCompletionSignal) {
                jobCompletionSignal.notifyAll();
            }
        }
    }

    private void checkClosed() {

        if (closed.get()) {
            throw new IllegalStateException("This data queue is closed");
        }
    }

    private void close() {

        if (closed.compareAndSet(false, true)) {
            executorService.shutdown();
        }
    }

    private void retryFailedJobsSequentially(final Instant timeBarrier) throws Exception {

        final List<DataOperation> retryList = new ArrayList<>();
        failedJobs.drainTo(retryList);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AutoCloseable executorShutdown = executor::shutdownNow) {
            for (DataOperation operationRetry : retryList) {

                final CompletableFuture<?> retry = CompletableFuture.runAsync(operationRetry, executorService)
                        .whenComplete((r, t) -> onOperationCompletion(operationRetry, t));
                pendingOperations.incrementAndGet();

                final Duration waitTime = Duration.between(Instant.now(), timeBarrier);
                retry.get(waitTime.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * An invocation of the data sink to process a batch of data.
     */
    protected class DataOperation implements Runnable {

        private final List<DataTypeT> dataBatch;

        private final Stopwatch runtime = Stopwatch.createUnstarted();

        private final SetOnce<DataStatsT> operationStats = new SetOnce<>();



        private final String operationId;

        private DataOperation(@Nonnull List<DataTypeT> batch) {

            this.dataBatch = ImmutableList.copyOf(Objects.requireNonNull(batch));
            this.operationId = String.format("%s:%s", queueConfiguration.queueName(), operationIdProvider.getAndIncrement());
        }

        @Override
        public void run() {

            final RetryPolicy<DataStatsT> retryPolicy = RetryPolicy.<DataStatsT>builder()
                    .withMaxRetries(queueConfiguration.retryPolicy().retryAttempts())
                    .withDelay(queueConfiguration.retryPolicy().minRetryDelay(),
                            queueConfiguration.retryPolicy().maxRetryDelay())
                    .build();

            operationStats.trySetValue(() -> {

                try {
                    if (queueConfiguration.hasDataOperationTimeout()) {

                        final Timeout<DataStatsT> timeoutPolicy = Timeout.<DataStatsT>builder(queueConfiguration.dataOperationTimeout())
                                .withInterrupt()
                                .build();
                        return Failsafe.with(retryPolicy).compose(timeoutPolicy).get(this::callDataSink);
                    } else {
                        return Failsafe.with(retryPolicy).get(this::callDataSink);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public Optional<DataStatsT> jobStats() {
            return operationStats.getValue();
        }

        public Duration runtime() {
            return runtime.elapsed();
        }

        public String operationId() {
            return operationId;
        }

        private DataStatsT callDataSink() throws Exception {
            runtime.start();
            try {
                return dataSink.apply(dataBatch);
            } finally {
                runtime.stop();
            }
        }
    }
}
