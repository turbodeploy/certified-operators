package com.vmturbo.cloud.common.persistence;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.cloud.common.persistence.DataBatcher.DirectDataBatcher;
import com.vmturbo.cloud.common.persistence.DataQueueFactory.DefaultDataQueueFactory;
import com.vmturbo.cloud.common.persistence.DataQueueStats.NullStatsCollector;

public class ConcurrentDataQueueTest {

    private final DataQueueFactory queueFactory = new DefaultDataQueueFactory();

    @Test
    public void testInvocationOnDrain() throws Exception {

        final DataBatcher<TestData> dataBatcher = new DataBatcher<TestData>() {
            @Override
            public boolean isFullBatch(@NotNull List<TestData> dataList) {
                return false;
            }

            @NotNull
            @Override
            public List<TestData> splitBatches(@NotNull TestData data) {
                return ImmutableList.of(data);
            }
        };

        final AtomicBoolean sinkCalled = new AtomicBoolean(false);
        final DataSink<TestData, Void> dataSink = (testData) -> {
            sinkCalled.set(true);
            return null;
        };

        final DataQueueConfiguration queueConfiguration = DataQueueConfiguration.builder()
                .queueName("TestQueue")
                .concurrency(1)
                .build();
        final DataQueue<TestData, Void> dataQueue = queueFactory.createQueue(
                dataBatcher, dataSink, NullStatsCollector.create(), queueConfiguration);

        dataQueue.addData(new TestData());

        // Check that the data was queued (data sink was not invoked)
        assertFalse(sinkCalled.get());

        // drain the queue
        final DataQueueStats<Void> queueStats = dataQueue.drainAndClose(Duration.ofSeconds(10));

        // check that the data sink was invoked
        assertTrue(sinkCalled.get());
        assertThat(queueStats, not(nullValue()));
        assertThat(queueStats.successfulOperations(), equalTo(1L));
    }

    @Test
    public void testInvocationOnSubmission() throws Exception {

        final AtomicBoolean sinkCalled = new AtomicBoolean(false);
        final DataSink<TestData, Void> dataSink = (testData) -> {
            sinkCalled.set(true);
            return null;
        };

        final DataQueueConfiguration queueConfiguration = DataQueueConfiguration.builder()
                .queueName("TestQueue")
                .concurrency(1)
                .build();
        final DataQueue<TestData, Void> dataQueue = queueFactory.createQueue(
                DirectDataBatcher.create(), dataSink, NullStatsCollector.create(), queueConfiguration);

        dataQueue.addData(new TestData());

        // drain the queue
        final DataQueueStats<Void> queueStats = dataQueue.drainAndClose(Duration.ofSeconds(10));

        // check that the data sink was invoked
        assertTrue(sinkCalled.get());
    }

    @Test
    public void testRetryFailedJobsUseLocalExecutor() throws Exception {

        final AtomicBoolean sinkCalled = new AtomicBoolean(false);
        final AtomicBoolean sinkCalledInRetry = new AtomicBoolean(false);
        // first sink call fails, re-try sink call succeeds
        final DataSink<TestData, Void> dataSink = (testData) -> {
            if (!sinkCalled.getAndSet(true)) {
                throw new Exception();
            } else {
                sinkCalledInRetry.set(true);
                return null;
            }
        };
        final DataQueueConfiguration queueConfiguration = DataQueueConfiguration.builder()
            .queueName("TestQueue")
            .concurrency(1)
            .build();
        final DataQueue<TestData, Void> dataQueue = queueFactory.createQueue(
            DirectDataBatcher.create(), dataSink, NullStatsCollector.create(), queueConfiguration);

        dataQueue.addData(new TestData());
        // drain the queue
        dataQueue.drainAndClose(Duration.ofSeconds(30));

        // check that the data sink was invoked twice
        assertTrue(sinkCalled.get());
        assertTrue(sinkCalledInRetry.get());
    }


    private static class TestData {}
}
