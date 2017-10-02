package com.vmturbo.grpc.extensions;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.ConnectionClientTransport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PingingChannelBuilderTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFailInvalidAuthority() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        PingingChannelBuilder.forAddress(new InetSocketAddress("invalid_authority", 1234));
    }

    @Test
    public void testCreatesPingingClientTransport() throws Exception {
        final SocketAddress mockAddress = mock(InetSocketAddress.class);
        final ClientTransportFactory mockDelegateFactory = mock(ClientTransportFactory.class);
        final long pingIntervalSeconds = 200;
        when(mockDelegateFactory.newClientTransport(eq(mockAddress), eq("authority"), eq("agent")))
            .thenReturn(mock(ConnectionClientTransport.class));

        final ConnectionClientTransport transport = PingingChannelBuilder.forAddress("localhost", 1234)
            .setPingInterval(pingIntervalSeconds, TimeUnit.SECONDS)
            .buildTransportFactory(mockDelegateFactory)
            .newClientTransport(mockAddress, "authority", "agent");

        assertTrue(transport instanceof PingingClientTransport);
        assertEquals(
            TimeUnit.NANOSECONDS.convert(pingIntervalSeconds, TimeUnit.SECONDS),
            ((PingingClientTransport)transport).getPingIntervalNanos()
        );
    }

    @Test
    public void testSetPingInterval() throws Exception {
        PingingChannelBuilder.forAddress("localhost", 443)
            .setPingInterval(60, TimeUnit.SECONDS)
            .build();
    }
}