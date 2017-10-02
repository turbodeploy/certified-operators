package com.vmturbo.components.test.utilities.component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;

/**
 * Capture logs from an individual container.
 *
 * For capturing all logs to a file, see
 * {@link com.palantir.docker.compose.DockerComposeRule.Builder#saveLogsTo(String)}.
 */
@ThreadSafe
public class ServiceLogger implements AutoCloseable {

    /**
     * The {@link DockerComposeRule} orchestrating the test.
     */
    @Nonnull
    private final DockerComposeRule docker;

    /**
     * A map of the containers being logged. They can be stopped at any time.
     */
    @Nonnull
    private final Map<Container, ExecutorService> logExecutors = new HashMap<>();

    /**
     * Create a new {@link ServiceLogger} for capturing individual container logs.
     *
     * @param docker The {@link DockerComposeRule} orchestrating the test.
     */
    public ServiceLogger(@Nonnull final DockerComposeRule docker) {
        this.docker = docker;
    }

    /**
     * Capture the logs for a container and print them to a file.
     *
     * Useful for debugging of tests when you are only interested in an individual container's logs.
     * Log collection will stop either when stopCollecting() is called for the same container or when
     * the test suite ends.
     *
     * It is not currently permitted to collect a container's log multiple times.
     *
     * IMPORTANT NOTE: Start collecting logs for a service AFTER starting that service up.
     * If you begin collecting logs prior to starting the service, log collection will
     * stop immediately when the service actually starts up (because of the way the
     * 'docker-compose logs --follow' command works).
     *
     * @param container The container whose logs should be collected.
     * @param fileName The name of a file where the logs should be collected.
     */
    public synchronized void startCollecting(@Nonnull final Container container,
                                             @Nonnull final String fileName) throws FileNotFoundException {
        File logFile = new File(fileName);
        startCollecting(container, new FileOutputStream(logFile));
    }

    /**
     * Capture the logs for a container and print them to the provided output stream.
     *
     * Useful for debugging of tests when you are only interested in an individual container's logs.
     * Log collection will stop either when stopCollecting() is called for the same container or when
     * the test suite ends.
     *
     * It is not currently permitted to collect a container's log multiple times.
     *
     * IMPORTANT NOTE: Start collecting logs for a service AFTER starting that service up.
     * If you begin collecting logs prior to starting the service, log collection will
     * stop immediately when the service actually starts up (because of the way the
     * 'docker-compose logs --follow' command works).
     *
     * @param container The container whose logs should be collected.
     * @param outputStream The stream where the logs should be sent.
     */
    public synchronized void startCollecting(@Nonnull final Container container,
                                             @Nonnull final OutputStream outputStream) {
        Objects.requireNonNull(outputStream);
        if (isCollecting(container)) {
            throw new IllegalStateException("Already collecting logs for container " + container.getContainerName());
        }

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        logExecutors.put(container, executorService);
        executorService.execute(() -> {
            try {
                docker.dockerCompose().writeLogs(container.getContainerName(), outputStream);
            } catch (IOException e) {
                throw new RuntimeException("Error reading log", e);
            }
        });
    }

    /**
     * Check whether logs are being collected for a particular container.
     *
     * @param container The container to check.
     * @return True if the container's logs are being collected, false otherwise.
     */
    public synchronized boolean isCollecting(@Nonnull final Container container) {
        return logExecutors.get(container) != null;
    }

    /**
     * Capture the logs for a container and print them to stdout.
     * Useful for debugging of tests when you are only interested in an individual container's logs.
     *
     * @param container The container whose logs should be printed to stdout.
     */
    public synchronized boolean stopCollecting(@Nonnull final Container container) {
        ExecutorService executorService = logExecutors.remove(container);
        if (executorService != null) {
            executorService.shutdownNow();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Closes the logger and stops collecting all service logs.
     * Clears references to all currently collecting loggers.
     */
    @Override
    public void close() {
        logExecutors.values().forEach(ExecutorService::shutdownNow);
        logExecutors.clear();
    }
}
