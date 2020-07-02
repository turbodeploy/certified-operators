package com.vmturbo.topology.processor.operation;

import static junit.framework.TestCase.fail;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.apache.http.annotation.GuardedBy;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;

/**
 * Static functions for the operation tests.
 */
public class OperationTestUtilities {
    /**
     * Timeout used when waiting for operation.
     */
    public static final long DISCOVERY_PROCESSING_TIMEOUT_SECONDS = 30;

    private OperationTestUtilities() {}

    /**
     * Notify and wait for the operation manager to complete a discovery.
     *
     * @param operationManager The operation manager running the discovery.
     * @param discovery The discovery that should complete.
     * @param discoveryResponse discovery response
     * @throws Exception If anything goes wrong.
     */
    public static void notifyAndWaitForDiscovery(@Nonnull final OperationManager operationManager,
            @Nonnull final Discovery discovery,
            @Nonnull final DiscoveryResponse discoveryResponse) throws Exception {
        operationManager.notifyDiscoveryResult(discovery, discoveryResponse).get(
            DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Notify and wait for the operation manager to complete a validation.
     *
     * @param operationManager The operation manager running the validation.
     * @param validation The validation that should complete.
     * @param validationResponse validation response
     * @throws Exception If anything goes wrong.
     */
    public static void notifyAndWaitForValidation(@Nonnull final OperationManager operationManager,
            @Nonnull final Validation validation,
            @Nonnull final ValidationResponse validationResponse) throws Exception {
        operationManager.notifyValidationResult(validation, validationResponse).get(
            DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Wait for the operation manager to complete an action.
     *
     * @param operationManager The operation manager running the discovery.
     * @param action The action that should complete.
     * @throws Exception If anything goes wrong.
     */
    public static void waitForAction(@Nonnull final OperationManager operationManager,
                                     @Nonnull final Action action) throws Exception {
        waitForEvent(
            () -> !operationManager.getInProgressAction(action.getId()).isPresent()
        );
    }

    /**
     * Wait for an event to complete as determined by a predicate check function.
     *
     * @param predicate The predicate to check.
     * @param <T> The type of the argument to the predicate
     * @throws Exception when something goes wrong
     */
    public static <T> void waitForEvent(final @Nonnull Supplier<Boolean> predicate) throws Exception {
        final long pollIntervalMillis = 10;
        long timePolled = 0;
        final long pollTimeout = TimeUnit.MILLISECONDS.convert(DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Poll until the results are processed or we time out
        while (timePolled < pollTimeout) {
            if (predicate.get()) {
                return;
            }
            Thread.sleep(pollIntervalMillis);
            timePolled += pollIntervalMillis;
        }

        fail();
    }

    /**
     * A helper class for tracking operation progress.
     */
    public static class TrackingOperationListener implements OperationListener {
        @GuardedBy("lock")
        private final Deque<Operation> notifiedOperation = new LinkedList<>();
        private final Object lock = new Object();

        @Override
        public void notifyOperationState(@Nonnull Operation operation) {
            synchronized (lock) {
                notifiedOperation.add(operation);
                lock.notifyAll();
            }
        }

        @Override
        public void notifyOperationsCleared() {
        }

        /**
         * Returns last notification status, if any.
         *
         * @return notification status or empty {@link Optional}
         */
        public Optional<Status> getLastNotifiedStatus() {
            synchronized (lock) {
                return notifiedOperation.isEmpty() ? Optional.empty() : Optional.of(
                        notifiedOperation.getLast().getStatus());
            }
        }

        /**
         * Awaits for an operation to arrive that matches the specified predicate.
         *
         * @param predicate predicate to test operations with
         * @return operation matched
         * @throws InterruptedException if current thread has been interrupted
         * @throws TimeoutException if timed out waiting for a matching operation
         */
        public Operation awaitOperation(@Nonnull Predicate<Operation> predicate)
                throws InterruptedException, TimeoutException {
            final long currentTime = System.currentTimeMillis();
            final long timeout = 30_000;
            synchronized (lock) {
                for (; ; ) {
                    for (Operation operation : notifiedOperation) {
                        if (predicate.test(operation)) {
                            return operation;
                        }
                    }
                    lock.wait(30000);
                    if (System.currentTimeMillis() > currentTime + timeout) {
                        throw new TimeoutException(
                                "Failed to await for matching operation for " + timeout + " ms");
                    }
                }
            }
        }

        /**
         * Get last status matches.
         *
         * @param status status to query
         * @return whether this status has been reported
         */
        public boolean lastStatusMatches(final Status status) {
            return getLastNotifiedStatus()
                .map(notifiedStatus -> notifiedStatus == status)
                .orElse(false);
        }
    }
}
