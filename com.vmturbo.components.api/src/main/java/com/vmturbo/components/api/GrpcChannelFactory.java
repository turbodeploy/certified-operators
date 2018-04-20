package com.vmturbo.components.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;

/**
 * A utility class to create pre-configured {@link ManagedChannelBuilder}s.
 */
public class GrpcChannelFactory {

    /**
     * The default interval for keep-alives for the channel.
     *
     * This should not be lower than GRPC_MIN_KEEPALIVE_TIME_MIN in BaseVmtComponent (i.e. the
     * lowest keepalive interval accepted by the server).
     */
    private static final int DEFAULT_KEEP_ALIVE_MIN = 5;

    /**
     * Create a new {@link ManagedChannelBuilder}, pre-configured with the default options
     * used for communication between components.
     *
     * @param host The host to connect to.
     * @param port The port to connect at.
     * @return A {@link ManagedChannelBuilder}. The caller can continue to configure it if desired.
     */
    @Nonnull
    public static ManagedChannelBuilder newChannelBuilder(@Nonnull final String host,
                                                          final int port) {
        Preconditions.checkArgument(!StringUtils.isEmpty(host), "Host must be provided.");
        Preconditions.checkArgument(port > 0, "Port must be a positive integer!");
        return NettyChannelBuilder.forAddress(host, port)
                .keepAliveWithoutCalls(true)
                .keepAliveTime(DEFAULT_KEEP_ALIVE_MIN, TimeUnit.MINUTES)
                .usePlaintext();
    }
}
