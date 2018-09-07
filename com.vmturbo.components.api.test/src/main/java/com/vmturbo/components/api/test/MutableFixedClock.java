package com.vmturbo.components.api.test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link Clock} implementation similar to {@link java.time.Clock.FixedClock}, but allowing
 * changes to the internal instant.
 */
@ThreadSafe
public class MutableFixedClock extends Clock {

    @GuardedBy("clockLock")
    private Clock curFixedClock;

    private final Object clockLock = new Object();

    public MutableFixedClock(@Nonnull final Instant initialInstant, @Nonnull final ZoneId zone) {
        curFixedClock = Clock.fixed(initialInstant, zone);
    }

    @Override
    public ZoneId getZone() {
        synchronized (clockLock) {
            return curFixedClock.getZone();
        }
    }

    @Override
    public Clock withZone(final ZoneId zone) {
        synchronized (clockLock) {
            return curFixedClock.withZone(zone);
        }
    }

    @Override
    public Instant instant() {
        synchronized (clockLock) {
            return curFixedClock.instant();
        }
    }

    /**
     * Change the instant that this clock will return from now on.
     *
     * @param newInstant The new instant to return from this clock.
     */
    public void changeInstant(@Nonnull final Instant newInstant) {
        synchronized (clockLock) {
            curFixedClock = Clock.fixed(newInstant, getZone());
        }
    }
}

