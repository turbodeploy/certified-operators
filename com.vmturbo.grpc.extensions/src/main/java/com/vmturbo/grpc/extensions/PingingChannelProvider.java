package com.vmturbo.grpc.extensions;

import io.grpc.ManagedChannelProvider;
import io.grpc.netty.NettyChannelProvider;

/**
 * Provider for {@link PingingChannelBuilder} instances.
 *
 * Used as a provider for creating {@link PingingClientTransport} instances.
 *
 * Dynamically loaded as a service on the classpath via Service Provider Interface
 * mechanisms (see https://docs.oracle.com/javase/tutorial/ext/basics/spi.html)
 */
public class PingingChannelProvider extends ManagedChannelProvider {
    @Override
    public boolean isAvailable() {
        return true;
    }

    /**
     * A priority, from 0 to 10 that this provider should be used, taking the current environment into
     * consideration. 5 should be considered the default, and then tweaked based on environment
     * detection. A priority of 0 does not imply that the provider wouldn't work; just that it should
     * be last in line.
     */
    @Override
    public int priority() {
        // Ensure we have a higher priority than plain Netty channels.
        return Math.min(new NettyChannelProvider().priority() + 1, 10);
    }

    @Override
    public PingingChannelBuilder builderForAddress(String name, int port) {
        return PingingChannelBuilder.forAddress(name, port);
    }

    @Override
    public PingingChannelBuilder builderForTarget(String target) {
        return PingingChannelBuilder.forTarget(target);
    }
}
