package com.vmturbo.grpc.extensions;

import org.junit.Test;

import io.grpc.netty.NettyChannelProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PingingChannelProviderTest {
    private final PingingChannelProvider provider = new PingingChannelProvider();

    @Test
    public void testIsAvailable() {
        assertTrue(provider.isAvailable());
    }

    @Test
    public void testPriorityIsHigherThanNetty() {
        assertTrue(provider.priority() > new NettyChannelProvider().priority());
    }

    @Test
    public void testBuilderForAddress() {
        assertEquals(PingingChannelBuilder.class, provider.builderForAddress("localhost", 443).getClass());
    }

    @Test
    public void testBuilderForTarget() {
        assertEquals(PingingChannelBuilder.class, provider.builderForTarget("localhost:443").getClass());
    }
}
