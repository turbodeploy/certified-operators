package com.vmturbo.kvstore;

/**
 * Interfact for distributed lock.
 */
public interface Lock {

    /**
     * Return a distrubuted locked.
     *
     * @param isBlock should the lock blocking other requests?
     * @return disstributed lock.
     * @throws InterruptedException if the lock is interupted
     */
    boolean lock(boolean isBlock) throws InterruptedException;

    /**
     * Release lock.
     *
     * @return true if the lock is release successfully.
     */
    boolean unlock();
}
