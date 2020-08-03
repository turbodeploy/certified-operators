package com.vmturbo.cost.component.reserved.instance;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;

public class BuyRIAnalysisSchedulerTest {

    private final long initialIntervalHours = 1;

    private final long normalIntervalHours = 2;

    private final ScheduledExecutorService scheduledExecutorSpy = Mockito.spy(new DelegationExecutor());

    private final ComputeTierDemandStatsStore computeTierDemandStatsStoreMock = Mockito.mock(
            ComputeTierDemandStatsStore.class);

    @Test
    public void testCreateNormalScheduleTask() {
        final ScheduledFuture<?> initialMockFuture = Mockito.mock(ScheduledFuture.class);
        final long initialIntervalMillis = TimeUnit.MILLISECONDS.convert(initialIntervalHours, TimeUnit.HOURS);
        final long normalIntervalMillis = TimeUnit.MILLISECONDS.convert(normalIntervalHours, TimeUnit.HOURS);
        Mockito.doReturn(initialMockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
                any(), Mockito.anyLong(), eq(initialIntervalMillis), any()
        );
        ReservedInstanceAnalysisInvoker invoker = Mockito.mock(ReservedInstanceAnalysisInvoker.class);
        final BuyRIAnalysisScheduler buyRIAnalysisScheduler = new BuyRIAnalysisScheduler(
                scheduledExecutorSpy, invoker, normalIntervalHours);
        buyRIAnalysisScheduler.normalTriggerBuyRIAnalysis();
        final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = invoker.getStartBuyRIAnalysisRequest();
        verify(invoker).invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
    }

    /**
     * A wrapper class for mocking ScheduledExecutorService.
     */
    public class DelegationExecutor implements ScheduledExecutorService {
        private final ScheduledExecutorService innerExecutor = Executors.newSingleThreadScheduledExecutor(
                new BasicThreadFactory.Builder().namingPattern("DelegationExecutor-%d").build()
        );

        @Override
        public void shutdown() {
            innerExecutor.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return innerExecutor.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return innerExecutor.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return innerExecutor.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return innerExecutor.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return innerExecutor.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return innerExecutor.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return innerExecutor.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            return innerExecutor.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return innerExecutor.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return innerExecutor.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return innerExecutor.invokeAny(tasks, timeout, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return innerExecutor.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return innerExecutor.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                      long period, TimeUnit unit) {
            return innerExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            return innerExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void execute(Runnable command) {
            innerExecutor.execute(command);
        }
    }
}
