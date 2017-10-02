package com.vmturbo.grpc.extensions;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import io.grpc.Status;
import io.grpc.internal.ClientTransport.PingCallback;
import io.grpc.internal.ManagedClientTransport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PingRunnerTest {

    private final ManagedClientTransport transport = mock(ManagedClientTransport.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final long pingDelayNanos = TimeUnit.SECONDS.toNanos(30);
    private final PingRunner runner = new PingRunner(transport, scheduler, pingDelayNanos);

    final ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
    final ArgumentCaptor<Runnable> sendPingCaptor = ArgumentCaptor.forClass(Runnable.class);
    final ArgumentCaptor<PingCallback> pingCallbackCaptor = ArgumentCaptor.forClass(PingCallback.class);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStartSchedulesPing() throws Exception {
        runner.start();
        verify(scheduler, times(1)).schedule(sendPingCaptor.capture(), delayCaptor.capture(),
            isA(TimeUnit.class));

        final Runnable sendPing = sendPingCaptor.getValue();
        final Long delay = delayCaptor.getValue();

        assertEquals(pingDelayNanos, delay.longValue());
        sendPing.run();

        verify(transport).ping(any(PingCallback.class), any(Executor.class));
    }

    @Test
    public void testPingRescheduledOnSuccess() {
        runner.start();
        verify(scheduler, times(1)).schedule(sendPingCaptor.capture(), delayCaptor.capture(),
            isA(TimeUnit.class));

        final Runnable sendPing = sendPingCaptor.getValue();
        sendPing.run();

        verify(transport).ping(pingCallbackCaptor.capture(), any(Executor.class));
        pingCallbackCaptor.getValue().onSuccess(100);

        // Verify a subsequent ping is scheduled.
        verify(scheduler, times(2)).schedule(sendPingCaptor.capture(), delayCaptor.capture(),
            isA(TimeUnit.class));
    }

    @Test
    public void testPingRescheduledOnFailure() {
        runner.start();
        verify(scheduler, times(1)).schedule(sendPingCaptor.capture(), delayCaptor.capture(),
            isA(TimeUnit.class));

        final Runnable sendPing = sendPingCaptor.getValue();
        sendPing.run();

        verify(transport).ping(pingCallbackCaptor.capture(), any(Executor.class));
        pingCallbackCaptor.getValue().onFailure(mock(Throwable.class));

        // Verify the transport was shut down.
        verify(transport).shutdownNow(any(Status.class));
    }

    @Test
    public void testPingCancelledOnTransportShutdown() throws Exception {
        ScheduledFuture pingFuture = mock(ScheduledFuture.class);
        when(scheduler.schedule(any(Runnable.class), eq(pingDelayNanos), eq(TimeUnit.NANOSECONDS)))
            .thenReturn(pingFuture);

        runner.start();
        runner.onTransportShutdown();

        verify(pingFuture).cancel(eq(false));
    }

    @Test
    public void testPingRunnerDoesNotPermitTooShortIntervals() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        new PingRunner(transport, scheduler, PingRunner.MIN_PING_DELAY_NANOS - 1000);
    }
}