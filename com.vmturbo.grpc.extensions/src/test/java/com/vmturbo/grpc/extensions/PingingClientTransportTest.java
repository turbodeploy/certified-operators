package com.vmturbo.grpc.extensions;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Status;
import io.grpc.internal.ClientTransport.PingCallback;
import io.grpc.internal.ConnectionClientTransport;
import io.grpc.internal.ManagedClientTransport.Listener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PingingClientTransportTest {

    private final ConnectionClientTransport delegateTransport = mock(ConnectionClientTransport.class);
    private final PingingClientTransport pingingTransport = new PingingClientTransport(
        delegateTransport, TimeUnit.MINUTES.toNanos(1)
    );

    @Test
    public void testGetAttrsDelegates() throws Exception {
        pingingTransport.getAttrs();

        verify(delegateTransport).getAttrs();
    }

    @Test
    public void testStartDelegates() throws Exception {
        final Listener listener = mock(Listener.class);
        pingingTransport.start(listener);

        verify(delegateTransport).start(listener);
    }

    @Test
    public void testShutdownDelegates() throws Exception {
        pingingTransport.start(Mockito.mock(Listener.class));
        pingingTransport.shutdown();

        verify(delegateTransport).shutdown();
    }

    @Test
    public void testShutdownNowDelegates() throws Exception {
        pingingTransport.start(Mockito.mock(Listener.class));
        pingingTransport.shutdownNow(Status.DEADLINE_EXCEEDED);

        verify(delegateTransport).shutdownNow(Status.DEADLINE_EXCEEDED);
    }

    @Test
    public void testPingDelegates() throws Exception {
        final PingCallback pingCallback = mock(PingCallback.class);
        final Executor executor = mock(Executor.class);
        pingingTransport.ping(pingCallback, executor);

        verify(delegateTransport).ping(pingCallback, executor);
    }

    @Test
    public void testGetLogIdDelegates() throws Exception {
        when(delegateTransport.getLogId()).thenReturn("logId");
        assertEquals("logId", pingingTransport.getLogId());
    }

    @Test
    public void testGetPingIntervalNanos() throws Exception {
        final PingingClientTransport pingingTransport = new PingingClientTransport(
            delegateTransport, TimeUnit.MINUTES.toNanos(1)
        );
        assertEquals(TimeUnit.MINUTES.toNanos(1), pingingTransport.getPingIntervalNanos());
    }
}