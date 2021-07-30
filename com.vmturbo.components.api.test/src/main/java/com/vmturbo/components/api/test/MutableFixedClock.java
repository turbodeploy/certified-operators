package com.vmturbo.components.api.test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.components.api.TimeUtil;

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

    public MutableFixedClock(@Nonnull final LocalDateTime time) {
        this(TimeUtil.localDateTimeToMilli(time, Clock.systemUTC()));
    }

    public MutableFixedClock(final long millis) {
        curFixedClock = Clock.fixed(Instant.ofEpochMilli(millis), ZoneId.from(ZoneOffset.UTC));
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

    public void addTime(final long amount, @Nonnull final TemporalUnit units) {
        changeInstant(curFixedClock.instant().plus(amount, units));
    }

    public void removeTime(final long amount, @Nonnull final TemporalUnit units) {
        changeInstant(curFixedClock.instant().minus(amount, units));
    }
}
