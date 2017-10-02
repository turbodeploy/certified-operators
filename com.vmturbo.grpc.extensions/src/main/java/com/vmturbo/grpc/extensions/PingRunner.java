package com.vmturbo.grpc.extensions;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.Status;
import io.grpc.internal.ClientTransport;
import io.grpc.internal.ManagedClientTransport;

/**
 * Based on KeepAliveManager targeted for release in gRPC 1.2 but does not keep a state machine or attempt to shut
 * down transport on failure. It always pings at the given interval.
 *
 * See https://github.com/grpc/grpc-java/issues/1648#issuecomment-207581337 for how gRPC is thinking of keep-alives.
 * Their solution is insufficient to keep alive our connections because:
 *
 * 1. They are not currently implemented (scheduled for 1.2 milestone when current release is 1.0.3)
 * 2. Even when they are implemented, pings will only be sent for connections with active RPCs. When a channel
 *    goes idle, pings will not be sent, and this is exactly when we need pings to be sent to prevent a silent
 *    drop of our connection by the load balancer (Docker Swarm / AWS / Google Compute load balancers will all
 *    drop inactive connections with a different timeout in seconds).
 *
 * Pings are sent at a regular interval while the connection is open. The PingRunner does only the minimum level
 * of broken connection detection necessary for this solution to be functional as an inactivity-prevention mechanism
 * and so that we do not repeatedly ping on dead connections. gRPC has its own sophisticated broken connection
 * detection and recovery mechanisms that we do not attempt to duplicate.
 */
public class PingRunner {
    /**
     * Units of nanos are used to be consistent with gRPC KeepAliveManager. I'm not sure
     * why they need such high granularity.
     */
    public static final long MIN_PING_DELAY_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final ScheduledExecutorService scheduler;
    private final ManagedClientTransport transport;
    private State state = State.CONNECTED;
    private ScheduledFuture<?> pingFuture;

    private static final Logger logger = LogManager.getLogger();

    private final Runnable sendPing = new Runnable() {
        @Override
        public void run() {
            // Ask the transport to send a ping.
            logger.debug("Sending ping on transport {}...", transport);
            transport.ping(pingCallback, MoreExecutors.directExecutor());
        }
    };
    private final RunnerPingCallback pingCallback = new RunnerPingCallback();
    private long pingDelayInNanos;

    /**
     * The state of the runner which tracks to the underlying transport.
     */
    private enum State {
        /**
         * The transport is connected. We send pings.
         */
        CONNECTED,

        /*
         * The transport has been disconnected. We won't ping any more.
         */
        DISCONNECTED,
    }

    /**
     * Creates a PingRunner.
     *
     * @param transport The transport used for pinging.
     * @param scheduler The scheduler to use for scheduling pings.
     * @param pingDelayInNanos The interval in nanoseconds at which pings will be sent.
     */
    public PingRunner(ManagedClientTransport transport, ScheduledExecutorService scheduler,
                            long pingDelayInNanos) {
        this.transport = Preconditions.checkNotNull(transport, "transport");
        this.scheduler = Preconditions.checkNotNull(scheduler, "scheduler");
        // Set a minimum on ping dealy.
        if (pingDelayInNanos < MIN_PING_DELAY_NANOS) {
            throw new IllegalArgumentException("Ping delay in nanos " + pingDelayInNanos + " less than minimum of " +
                MIN_PING_DELAY_NANOS + ". Using minimum value instead.");
        } else {
            this.pingDelayInNanos = pingDelayInNanos;
        }
    }

    /**
     * Start the PingRunner. Pings will be sent at the configured interval.
     */
    public synchronized void start() {
        logger.debug("Starting ping runner on transport {}", transport);
        pingFuture = scheduler.schedule(sendPing, pingDelayInNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Transport is shutting down. We no longer need to do Pings.
     */
    public synchronized void onTransportShutdown() {
        if (state != State.DISCONNECTED) {
            state = State.DISCONNECTED;
            logger.debug("Transport {} shut down.", transport);

            if (pingFuture != null) {
                pingFuture.cancel(false);
            }
        }
    }

    /**
     * A callback to be called on completion of a ping.
     */
    private class RunnerPingCallback implements ClientTransport.PingCallback {

        @Override
        public void onSuccess(long roundTripTimeNanos) {
            synchronized (this) {
                logger.debug("Ping succeeded on transport {} in {} nanos.", transport, roundTripTimeNanos);
                pingFuture = scheduler.schedule(sendPing, pingDelayInNanos, TimeUnit.NANOSECONDS);
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            // Don't attempt to shut down the transport or detect a broken connection.
            // Allow gRPC logic to handle this for now. When we have more experience using
            // this we may want to revisit this decision.
            logger.info("Ping failed on transport {}. A new transport will be created on next RPC.", transport);
            // These messages are extremely chatty if an RPC failed due to the server being unavailable
            // and the information here will be duplicated there in any case.
            logger.debug("Failure reason: ", cause);

            // Shut down the transport because it is likely dead. Further client attempts to use
            // the channel that this transport serviced will cause the allocation of a new transport
            // with a new ping runner. Recovery is seamless and transparent to clients.
            transport.shutdownNow(Status.UNAVAILABLE.withDescription(
                "Ping failed. The connection is likely gone"));
        }
    }
}
