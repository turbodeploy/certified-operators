package com.vmturbo.components.common;

/**
 * Classes implementing this interface require fetching some data (from the DB, consul, or anywhere
 * else) to initialize the in-memory state at component initialization. Use this interface to
 * do the initialization after the Spring context is constructed, to ensure that all migrations have
 * been run.
 *
 * <p>All initializations will be run before the component is considered "running" and healthy,
 * and any fatal error in initialization will result in a component shutdown.
 */
public interface RequiresDataInitialization {

    /**
     * Initialize the data in this class. This will be called after all migrations have been run
     * and the component's context is fully constructed, but before the component is running
     * and accessible by other components.
     *
     * @throws InitializationException If there is an error running the initialization. Fatal errors
     * (or runtime exceptions) will cause the component to shut down. Non-fatal errors will be logged.
     */
    void initialize() throws InitializationException;

    /**
     * Get the priority of the initialization. Classes with higher priority will be initialized
     * first.
     * @return the priority
     */
    default int priority() {
        return -1;
    }

    /**
     * Exception thrown when an initialization operation fails.
     */
    class InitializationException extends Exception {
        private final boolean isFatal;

        private InitializationException(String message, Throwable cause, boolean isFatal) {
            super(message, cause);
            this.isFatal = isFatal;
        }

        /**
         * If true, the component will not function correctly due to this initialization failure
         * and should be shut down.
         *
         * @return Whether or not the error is fatal.
         */
        public boolean isFatal() {
            return isFatal;
        }
    }

}
