package com.vmturbo.grpc.extensions;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.internal.ClientStream;
import io.grpc.internal.ConnectionClientTransport;
import io.grpc.internal.SharedResourceHolder;

import static io.grpc.internal.GrpcUtil.TIMER_SERVICE;

/**
 * Delegates all calls to a delegate transport. Contains a PingRunner that issues periodic pings
 * in order to keep a connection active.
 */
public class PingingClientTransport implements ConnectionClientTransport {
    private final ConnectionClientTransport delegateTransport;

    @GuardedBy("lock")
    private Optional<PingRunner> pingRunner;
    private ScheduledExecutorService scheduler;
    private final long pingIntervalNanos;
    private final Object lock = new Object();

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new {@link PingingClientTransport}.
     * @param delegateTransport The transport to which all activity will be delegated.
     * @param pingIntervalNanos The interval in nanoseconds at which pings will be sent.
     */
    public PingingClientTransport(@Nonnull final ConnectionClientTransport delegateTransport,
                                  final long pingIntervalNanos) {
        logger.debug("Creating PingingClientTransport with delay in nanos=" + pingIntervalNanos);
        this.delegateTransport = Objects.requireNonNull(delegateTransport);
        this.pingIntervalNanos = pingIntervalNanos;
        this.pingRunner = Optional.empty();
    }

    @Override
    public Attributes getAttrs() {
        return delegateTransport.getAttrs();
    }

    @Nullable
    @Override
    public Runnable start(Listener listener) {
        synchronized (lock) {
            if (!pingRunner.isPresent()) {
                scheduler = SharedResourceHolder.get(TIMER_SERVICE);
                final PingRunner runner = new PingRunner(this, scheduler, pingIntervalNanos);
                runner.start();
                pingRunner = Optional.of(runner);
            }
        }

        return delegateTransport.start(listener);
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            pingRunner.ifPresent(PingRunner::onTransportShutdown);
            scheduler = SharedResourceHolder.release(TIMER_SERVICE, scheduler);

            pingRunner = Optional.empty();
        }

        delegateTransport.shutdown();
    }

    @Override
    public void shutdownNow(Status status) {
        shutdown();
        delegateTransport.shutdownNow(status);
    }

    @Override
    public ClientStream newStream(MethodDescriptor<?, ?> methodDescriptor, Metadata metadata, CallOptions callOptions) {
        return delegateTransport.newStream(methodDescriptor, metadata, callOptions);
    }

    @Override
    public ClientStream newStream(MethodDescriptor<?, ?> methodDescriptor, Metadata metadata) {
        return delegateTransport.newStream(methodDescriptor, metadata);
    }

    @Override
    public void ping(PingCallback pingCallback, Executor executor) {
         delegateTransport.ping(pingCallback, executor);
    }

    @Override
    public String getLogId() {
        return delegateTransport.getLogId();
    }

    /**
     * Get the ping interval in nanoseconds.
     *
     * @return The ping interval in nanoseconds.
     */
    public long getPingIntervalNanos() {
        return pingIntervalNanos;
    }

    @Override
    public String toString() {
        return "pinging-client-transport-" + hashCode();
    }
}
