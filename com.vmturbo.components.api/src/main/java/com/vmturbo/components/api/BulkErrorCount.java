package com.vmturbo.components.api;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Logger;

/**
 * Utility class to gather up errors and print a single summary line.
 */
public class BulkErrorCount implements AutoCloseable {
    private final Logger logger;
    private final String operationName;
    private int count = 0;
    private int errCount = 0;
    private Throwable firstCause = null;

    /**
     * Create a new instance.
     *
     * @param logger The logger to log the error to, if it happens.
     * @param operationName The name of the operation.
     */
    public BulkErrorCount(@Nonnull final Logger logger, @Nonnull final String operationName) {
        this.logger = logger;
        this.operationName = operationName;
    }

    /**
     * Record a successfull operation.
     */
    public void recordSuccess() {
        count++;
    }

    /**
     * Record an error that occurred during the operation.
     *
     * @param error The error.
     */
    public void recordError(@Nonnull final Exception error) {
        if (firstCause == null) {
            firstCause = error;
        }
        count++;
        errCount++;
    }

    @Override
    public void close() {
        if (firstCause != null) {
            logger.error("Operation {} failed {}/{} times. First cause:", operationName, errCount, count, firstCause);
        }
    }
}
