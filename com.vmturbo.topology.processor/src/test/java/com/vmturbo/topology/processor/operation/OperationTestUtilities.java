package com.vmturbo.topology.processor.operation;

import static junit.framework.TestCase.fail;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

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
            operationManager,
            opManager -> !opManager.getInProgressAction(action.getId()).isPresent()
        );
    }
    /**
     * Wait for an event to complete as determined by a predicate check function.
     *
     * @param checkerArgument The argument to the predicate checker check
     * @param predicateCheck The predicate check.
     * @param <T> The type of the argument to the predicate
     * @throws Exception when something goes wrong
     */
    public static <T> void waitForEvent(final T checkerArgument, Predicate<T> predicateCheck) throws Exception {
        final long pollIntervalMillis = 10;
        long timePolled = 0;
        final long pollTimeout = TimeUnit.MILLISECONDS.convert(DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Poll until the results are processed or we time out
        while (timePolled < pollTimeout) {
            if (predicateCheck.test(checkerArgument)) {
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
        private Optional<Status> lastNotifiedStatus = Optional.empty();
        private Optional<Operation> lastNotifiedOperation = Optional.empty();

        @Override
        public void notifyOperationState(@Nonnull Operation operation) {
            lastNotifiedOperation = Optional.of(operation);
            lastNotifiedStatus = Optional.of(operation.getStatus());
        }

        @Override
        public void notifyOperationsCleared() {
        }

        public Optional<Status> getLastNotifiedStatus() {
            return lastNotifiedStatus;
        }

        public Optional<Operation> getLastNotifiedOperation() {
            return lastNotifiedOperation;
        }

        public boolean lastStatusMatches(final Status status) {
            return lastNotifiedStatus
                .map(notifiedStatus -> notifiedStatus == status)
                .orElse(false);
        }
    }
}
