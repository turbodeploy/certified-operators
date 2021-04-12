package com.vmturbo.topology.processor.identity;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Manager to handle and expire stale oids.
 */
public interface StaleOidManager {

    /**
     * Initializes the manager.
     * @param getCurrentOids supplier of oids to expire.
     * @return the scheduled future for the oid expiration
     */
    ScheduledFuture<?> initialize(@Nonnull Supplier<Set<Long>> getCurrentOids);

    /**
     * Expire oids asynchronously.
     *
     * @return the number of expired oids
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the expiration oid thread was interrupted
     * @throws TimeoutException if the wait timed out
     * while waiting
     */
    int expireOidsImmediatly() throws InterruptedException, ExecutionException, TimeoutException;
}
