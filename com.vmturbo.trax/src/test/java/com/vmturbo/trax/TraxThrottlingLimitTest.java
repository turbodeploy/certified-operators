package com.vmturbo.trax;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingThrottlingLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingThrottlingLimitRemainder;
import com.vmturbo.trax.TraxThrottlingLimit.ThrottlingCoin;

/**
 * Tests for {@link TraxThrottlingLimit}.
 */
public class TraxThrottlingLimitTest {

    private final Clock clock = Mockito.mock(Clock.class);
    private final Random random = Mockito.mock(Random.class);

    /**
     * testFlip.
     */
    @Test
    public void testFlip() {
        when(clock.millis()).thenReturn(500L);
        final ThrottlingCoin coin = new ThrottlingCoin(0.5, 100, clock, random);

        when(random.nextDouble()).thenReturn(0.1);
        assertTrue(coin.flip());

        when(random.nextDouble()).thenReturn(0.75);
        assertFalse(coin.flip());
    }

    /**
     * testOddsDoubling.
     */
    @Test
    public void testOddsIncrease() {
        when(clock.millis()).thenReturn(500L);
        final ThrottlingCoin coin = new ThrottlingCoin(0.1, 100, clock, random);

        when(clock.millis()).thenReturn(600L);
        assertEquals(0.1 * (1 + ThrottlingCoin.UPDATE_COEFFICIENT), coin.updateOdds(), 0.1);

        when(clock.millis()).thenReturn(700L);
        assertEquals(0.1 * (1 + ThrottlingCoin.UPDATE_COEFFICIENT) * (1 + ThrottlingCoin.UPDATE_COEFFICIENT),
            coin.updateOdds(), 0.1);
    }

    /**
     * testPositiveFlipReducesOdds.
     */
    @Test
    public void testPositiveFlipReducesOdds() {
        when(clock.millis()).thenReturn(500L);
        final ThrottlingCoin coin = new ThrottlingCoin(0.1, 100, clock, random);
        assertEquals(0.1, coin.updateOdds(), 1e-6);

        when(random.nextDouble()).thenReturn(0.1);
        assertTrue(coin.flip());
        assertEquals(0.085, coin.updateOdds(), 1e-6);
    }

    /**
     * testNegativeFlipDoesNotReduceOdds.
     */
    @Test
    public void testNegativeFlipDoesNotReduceOdds() {
        when(clock.millis()).thenReturn(500L);
        final ThrottlingCoin coin = new ThrottlingCoin(0.1, 100, clock, random);
        assertEquals(0.1, coin.updateOdds(), 1e-6);

        when(random.nextDouble()).thenReturn(0.75);
        assertFalse(coin.flip());
        assertEquals(0.1, coin.updateOdds(), 1e-6);
    }

    /**
     * testIsExhausted.
     */
    @Test
    public void testIsExhausted() {
        final TraxThrottlingLimit limit = new TraxThrottlingLimit(24, clock, random);
        assertFalse(limit.isExhausted());
    }

    /**
     * testTryTracking.
     */
    @Test
    public void testTryTracking() {
        final TraxThrottlingLimit limit = new TraxThrottlingLimit(24, clock, random);

        when(random.nextDouble()).thenReturn(0.0);
        assertTrue(limit.tryTracking());

        when(random.nextDouble()).thenReturn(1.0);
        assertFalse(limit.tryTracking());
    }

    /**
     * testToProto.
     */
    @Test
    public void testToProto() {
        when(clock.millis()).thenReturn(TimeUnit.DAYS.toMillis(1));
        final TraxThrottlingLimit limit = new TraxThrottlingLimit(24, clock, random);

        when(clock.millis()).thenReturn(TimeUnit.DAYS.toMillis(2));
        when(random.nextDouble()).thenReturn(0.0);
        assertTrue(limit.tryTracking());
        when(random.nextDouble()).thenReturn(1.0);
        assertFalse(limit.tryTracking());

        final TrackingLimitRemainder.Builder builder = TrackingLimitRemainder.newBuilder();
        limit.configure(builder);

        assertTrue(builder.hasThrottlingLimit());
        final TrackingThrottlingLimitRemainder remainder = builder.getThrottlingLimit();
        assertEquals(24, remainder.getTargetCalculationsTrackedPerDay());
        assertEquals(2, remainder.getTotalCalculationTrackingAttempts());
        assertEquals(1.0, remainder.getAverageCalculationsTrackedPerDay(), 1e-6);
        assertEquals("1970-01-02T00:00:00Z", remainder.getHumanReadableCreationTime());
        assertEquals("1970-01-03T00:00:00Z", remainder.getHumanReadableLastCalculationTrackedTime());
    }

    /**
     * testFromProto.
     */
    @Test
    public void testFromProto() {
        final TraxThrottlingLimit limit = new TraxThrottlingLimit(TrackingThrottlingLimit.newBuilder()
            .setTargetCalculationsTrackedPerDay(33)
            .build());

        assertEquals(33, limit.getTargetCalculationsTrackedPerDay());
    }
}