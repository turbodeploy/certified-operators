package com.vmturbo.components.test.utilities.component;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.State;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

/**
 * Utilities to block execution until a service is ready.
 * Service readiness is decided by periodically checking if the service is healthy
 * until that call reports a healthy status or a timeout occurs.
 */
public abstract class ServiceHealthCheck {

    /**
     * How frequently to poll a component's health.
     */
    private static final long POLL_INTERVAL_MILLIS = 100;

    private final Clock waitClock;

    public ServiceHealthCheck() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    ServiceHealthCheck(@Nonnull final Clock clock) {
        this.waitClock = Objects.requireNonNull(clock);
    }

    /**
     * Check if a service is healthy.
     *
     * Health checks for specific services should override this method. It will be polled at
     * intervals of {@code POLL_INTERVAL_MILLIS} until either the service reports that it is
     * healthy, an exception is thrown, or a timeout occurs.
     *
     * @param container The container hosting the service whose health should be checked.
     * @return True if the service is healthy, false if the service is unhealthy or cannot be reached.
     * @throws ContainerUnreadyException If the container hosting the service is no longer running.
     * @throws InterruptedException If the thread got interrupted while executing the command.
     */
    public abstract SuccessOrFailure isHealthy(@Nonnull final Container container)
            throws ContainerUnreadyException, InterruptedException;

    /**
     * Block execution until the component being hosted in the given container reports itself as healthy or
     * the specified duration elapses.
     *
     * @param container The container whose health should be checked until healthy.
     * @param timeout The maximum amount of time to wait until the container is ready.
     * @throws InterruptedException If the wait is interrupted.
     * @throws ContainerUnreadyException If the timeout elapses before the container becomes ready.
     */
    public void waitUntilReady(@Nonnull final Container container, @Nonnull final Duration timeout)
        throws InterruptedException, ContainerUnreadyException {

        final long waitStartTime = waitClock.millis();
        while (isHealthy(container).failed()) {
            Thread.sleep(POLL_INTERVAL_MILLIS);
            final Duration elapsed = Duration.ofMillis(waitClock.millis() - waitStartTime);

            if (elapsed.compareTo(timeout) > 0) {
                throw new ContainerUnreadyException("Container " + container.getContainerName() +
                    " unready after " + timeout);
            }
        }
    }

    /**
     * Check the status of a container. If the container status is {@code State.DOWN}, throws a
     * ContainerUnreadyException.
     *
     * @param container The container whose status to check.
     * @throws ContainerUnreadyException If the container status is {@code State.DOWN}.
     * @throws InterruptedException If the thread got interrupted while executing the command.
     */
    protected void checkContainerStatus(@Nonnull final Container container)
        throws ContainerUnreadyException, InterruptedException {
        try {
            if (container.state().equals(State.DOWN)) {
                throw new ContainerUnreadyException("Container " + container.getContainerName() +
                    " is down!");
            }
        } catch (IOException e) {
            // Failure to execute the command to check the component state doesn't necessarily
            // mean the component is unhealthy.
        }
    }

    /**
     * An exception thrown when a component does not become healthy in a duration
     * specified by clients.
     */
    public static class ContainerUnreadyException extends RuntimeException {
        public ContainerUnreadyException(final String message) {
            super(message);
        }
    }

    /**
     * A basic service health check only checks if the container hosting the service is down.
     * If the container is down, it throws a {@link ContainerUnreadyException}.
     *
     * If it is not down, the health check reports success.
     */
    public static class BasicServiceHealthCheck extends ServiceHealthCheck {

        /**
         * Create a new {@link BasicServiceHealthCheck}.
         */
        public BasicServiceHealthCheck() {

        }

        /**
         * Create a new {@link BasicServiceHealthCheck} using the given clock for timing out failed health checks.
         */
        @VisibleForTesting
        BasicServiceHealthCheck(@Nonnull final Clock waitClock) {
            super(waitClock);
        }

        @Override
        public SuccessOrFailure isHealthy(@Nonnull Container container)
            throws ContainerUnreadyException, InterruptedException {
            checkContainerStatus(container);

            return SuccessOrFailure.success();
        }
    }
}
