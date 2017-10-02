package com.vmturbo.repository.api;

import javax.annotation.Nonnull;

/**
 * Plan orchestrator remote client.
 */
public interface Repository extends AutoCloseable {

    /**
     * Adds listener to receive repository notifications.
     *
     * @param planListener listener to register
     */
    void addListener(@Nonnull RepositoryListener planListener);
}
