package com.vmturbo.trax;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingThrottlingLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.Builder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingThrottlingLimitRemainder;
import com.vmturbo.trax.TraxConfiguration.TraxLimit;

/**
 * This class is thread-safe.
 * <p/>
 * The limit is never exhausted, but not all calls to {@link #tryTracking()} will return true.
 * Attempts to permit tracking on approximately targetCalculationsTrackedPerDay although
 * it probably won't work out to exactly this number because it's probabilistic and influenced
 * by the throughput of {@link #tryTracking()} calls made.
 *  <p/>
 * We use a coin flip whose odds of being true decrease each time it is flipped and turns up
 * true, and whose odds increase over time according to an increase period.
 *  <p/>
 * The theory here is that if tons of requests to track calculations are being made
 * and the odds are fairly high at the moment, we will get a bunch of calculation
 * tracking requests that get permitted. However, each time we permit a tracking,
 * the following requests become less likely. Eventually the odds become unlikely enough
 * that we stop permitting many calculations to be tracked. The odds rise slowly again
 * over time eventually reaching an equilibrium where, if the rate of tracking requests
 * is fairly steady, the lowering of odds due to successful coin flips achieves equilibrium
 * with the raising of the odds over time due to the increase period and this equilibrium
 * point should be about the target rate.
 *  <p/>
 * Empirically, in testing we achieve reasonably close to the target rate across
 * factors such as tracking request throughput, very high messages per day targets
 * (ie 1 per second) and very low message per day targets (ie 1 per hour).
 *  <p/>
 * Some alternative designs considered:
 * 1. Instead of using a probabilistic limit, permit the first tracking call to occur
 *    within some frequency window (ie hourly, daily, every 90 minutes, etc.)
 *    REASON NOT USED: Many calculations happen in a bursty fashion on a list that will arrive in order.
 *    Since it's unlikely for the window to reset in the middle of a burst, we'll almost always process
 *    the first item in the list so instead of sampling lots of calculations at random, we will always
 *    sample the same calculation which is not very useful.
 * 2. Same as above, except select some random number n of calculations to skip before permitting
 *    calculation tracking within a window.
 *    REASON NOT USED: This doesn't scale well to sample uniformly across implementations
 *    that have a throughput of, say, 1 calculation per day, to another that has 1 million calcluations per day.
 *    Since we don't know the calculation throughput apriori, we can't select a good number
 *    of calculations to skip in the window. Even if we were to use a probablistic scheme, we wouldn't know
 *    the odds to pick beforehand.
 */
@ThreadSafe
public class TraxThrottlingLimit implements TraxLimit {

    private final int targetCalculationsTrackedPerDay;
    private final Clock clock;
    private final long creationTimestamp;

    private final AtomicLong lastTrackingTime;
    private final AtomicLong calculationTrackingRequests;
    private final AtomicLong calculationTrackingPermitted;
    private final ThrottlingCoin coin;

    /**
     * Create a new {@link TraxThrottlingLimit}.
     *
     * @param targetCalculationsTrackedPerDay The target number of calculations to track per day.
     * @param clock The clock to use to judge timing.
     * @param random The random number generator to use.
     */
    public TraxThrottlingLimit(final int targetCalculationsTrackedPerDay,
                               @Nonnull final Clock clock,
                               @Nonnull final Random random) {
        Preconditions.checkArgument(targetCalculationsTrackedPerDay >= 0);

        this.targetCalculationsTrackedPerDay = targetCalculationsTrackedPerDay;
        this.clock = Objects.requireNonNull(clock);
        this.creationTimestamp = clock.millis();

        lastTrackingTime = new AtomicLong(-1);
        calculationTrackingRequests = new AtomicLong(0);
        calculationTrackingPermitted = new AtomicLong(0);
        coin = new ThrottlingCoin(1.0 / 32.0,
            increasePeriodMillisForTarget(targetCalculationsTrackedPerDay), clock, random);
    }

    /**
     * Construct a new {@link TraxThrottlingLimit}. The default system clock and a new random number
     * generator will be used.
     *
     * @param limit The definition of the limit.
     */
    public TraxThrottlingLimit(@Nonnull final TrackingThrottlingLimit limit) {
        this(limit.getTargetCalculationsTrackedPerDay(), Clock.systemUTC(),
            new Random(Clock.systemUTC().millis()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(@Nonnull Builder builder) {
        double daysInExistence = (clock.millis() - creationTimestamp) / (double)TimeUnit.DAYS.toMillis(1);
        double averageCalculationsTrackedPerDay = calculationTrackingPermitted.get() / daysInExistence;

        final TrackingThrottlingLimitRemainder remainder = TrackingThrottlingLimitRemainder.newBuilder()
            .setTargetCalculationsTrackedPerDay(targetCalculationsTrackedPerDay)
            .setAverageCalculationsTrackedPerDay(averageCalculationsTrackedPerDay)
            .setTotalCalculationTrackingAttempts(calculationTrackingRequests.get())
            .setHumanReadableCreationTime(Instant.ofEpochMilli(creationTimestamp).toString())
            .setHumanReadableLastCalculationTrackedTime(Instant.ofEpochMilli(lastTrackingTime.get()).toString())
            .build();

        builder.setThrottlingLimit(remainder);
    }

    /**
     * Request permission to track. Returns true if permitted by the current throttling level.
     *
     * @return Whether the limit permits tracking of the current calculation.
     */
    @Override
    public boolean tryTracking() {
        calculationTrackingRequests.incrementAndGet();

        if (coin.flip()) {
            lastTrackingTime.set(clock.millis());
            calculationTrackingPermitted.incrementAndGet();
            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExhausted() {
        return false;
    }

    /**
     * Get the target number of calculations to track per day.
     *
     * @return the target number of calculations to track per day.
     */
    public int getTargetCalculationsTrackedPerDay() {
        return targetCalculationsTrackedPerDay;
    }

    /**
     * Calculate what the odds increase period for our coin should be in order to achieve expected
     * value at equilibrium such that the expected value of calculations tracked per day is near
     * our target.
     *
     * @param targetCalculationsTrackedPerDay The target number of calculations we want to track per day.
     * @return The increase period to use for our {@link ThrottlingCoin} in milliseconds.
     */
    long increasePeriodMillisForTarget(final long targetCalculationsTrackedPerDay) {
        // We assume at equilibrium we will get about one message per increase period
        // Intuition: Since the increase to the odds for each increase period is the inverse of the
        //            decrease in odds each time we track a calculation, we expect the equilibrium
        //            point to be such that the odds increase and get decrease about the same
        //            number of times.
        final long millisPerDay = TimeUnit.DAYS.toMillis(1);

        // This computes the number of milliseconds we want between each calculation to achieve
        // the target calculations per day.
        //
        // millis   calculations   millis      1 day         millis
        // ------ / ------------ = ------ x -----------  = ----------
        // 1 day       1 day        1 day   calculations   calculation
        return Math.max(millisPerDay / targetCalculationsTrackedPerDay, 10);
    }

    /**
     * A coin that returns a boolean value when it is flipped. Each time it returns true
     * from a flip, the odds that the next flip returns true are reduced (cut in half each time).
     * <p/>
     * As time passes since the last true flip, odds increase (they increase each halflife, to a maximum of 1).
     * <p/>
     * This class is thread-safe in that it can be used concurrently, but in order to remain
     * lock-free we forgo certain guarantees that would better guarantee odds checking and updating
     * cannot stomp on each other.
     */
    @ThreadSafe
    static class ThrottlingCoin {

        /**
         * Instead of performing a single update per halflife, we divide the update period into several
         * updates (based on the principles of compound interest), such that after performing
         * UPDATES_PER_INCREASE, the odds are increased.
         * <p/>
         * Empirically, updating more frequently than the increase period tends to get us closer to the
         * target than updating once per increase period. Updating once per increase period seemed to
         * cause the coin to flip() false more frequently than expected.
         * <p/>
         * This lets us perform a number of smaller updates instead of having a large jump where odds increase
         * at the end of the period.
         */
        private static final int UPDATES_PER_INCREASE = 16;

        /**
         * https://www.investopedia.com/terms/c/compoundinterest.asp
         * Based on the calculations for compound interest, if we want to increase the odds at the end of
         * UPDATES_PER_INCREASE updates, we wish to find x such that:.
         * <p/>
         *   (1 + x)^UPDATES_PER_INCREASE - 1 = 0.125
         * <p/>
         * Solving for x:
         * <p/>
         *   x = e^(ln(2) / UPDATES_PER_INCREASE) - 1
         * <p/>
         * Note that 1 / 0.85 is not exactly equal to 1.18 (it's 1.17647...) but empirically
         * I found that we tend to undersample a bit so I bumped up the rate of increase
         * a little.
         */
        static final double UPDATE_COEFFICIENT = Math.exp(Math.log(1.18) / UPDATES_PER_INCREASE) - 1;

        /**
         * When a successful flip occurs, how much to penalize the odds of the next successful flip.
         */
        private static final double SUCCESSFUL_FLIP_PENALTY_COEFFICIENT = 0.85;

        private AtomicDouble currentOdds;

        private final long updatePeriodMilis;
        private final AtomicLong lastOddsUpdateTimestamp;

        private final Clock clock;
        private final Random random;

        /**
         * Create a new {@link com.vmturbo.trax.TraxThrottlingLimit.ThrottlingCoin}.
         *
         * @param initialOdds The initial odds a flip results in a {@code true} value.
         * @param increasePeriodMillis The amount of time until the odds of a {@code true} value increase.
         * @param clock The clock to use for timing.
         * @param random The random number generator to use.
         */
        ThrottlingCoin(final double initialOdds,
                       final long increasePeriodMillis,
                       @Nonnull final Clock clock,
                       @Nonnull final Random random) {
            Preconditions.checkArgument(initialOdds >= 0 && initialOdds <= 1.0);
            Preconditions.checkArgument(increasePeriodMillis > 0);

            this.currentOdds = new AtomicDouble(initialOdds);
            this.updatePeriodMilis = increasePeriodMillis / UPDATES_PER_INCREASE;
            lastOddsUpdateTimestamp = new AtomicLong(clock.millis());

            this.clock = Objects.requireNonNull(clock);
            this.random = Objects.requireNonNull(random);
        }

        /**
         * Package-private method to update odds. Note that we sacrifice the strict correctness
         * of providing consistent updates of the lastOddsUpdateTimestamp and currentOdds
         * parameters together in order to avoid having to introduce any locks.
         * <p/>
         * Implementation note:
         * It is possible that concurrent calls to updateOdds() or even a concurrent call to
         * flip() at the same time as a call to updateOdds() could cause us to set a value
         * to odds that is inconsistent with what we would actually want to set. However,
         * this should be a pretty rare occurrence and when it does occur, the end result will
         * only be slightly more or slightly fewer calculations tracked and we don't make absolute
         * guarantees about these numbers anyway so the sacrifice in correctness for performance
         * seems a fine tradeoff here.
         *
         * @return The updated odds after accounting for the time elapsed since the last
         *         time we boosted the odds due to an update.
         */
        double updateOdds() {
            final long lastTimestamp = lastOddsUpdateTimestamp.get();
            final long currentTimestamp = clock.millis();
            long millisSinceLastUpdate = currentTimestamp - lastTimestamp;
            long updatePeriodsElapsed = millisSinceLastUpdate / updatePeriodMilis;

            if (updatePeriodsElapsed > 0) {
                double odds = currentOdds.get();
                // Increase the odds, never going above 1
                double newOdds = Math.min(1.0,
                    odds + odds * (Math.pow(1.0 + UPDATE_COEFFICIENT, updatePeriodsElapsed) - 1));

                if (lastOddsUpdateTimestamp.compareAndSet(lastTimestamp, currentTimestamp)) {
                    currentOdds.set(newOdds);
                }
            }

            return currentOdds.get();
        }

        /**
         * Flip a coin, returning true or false.
         * <p/>
         * Before performing the flip, we potentially update the odds to account for the odds
         * boosting where we increase the odds
         * <p/>
         * Odds of a successful flip increase over time the longer we go without a successful flip.
         * When a successful flip occurs, we decrease the odds according to the penalty coefficient.
         *
         * @return True or false, with odds decreasing with each successful flip and increasing over
         *         time according to the odds increase period the coin was initialized with.
         */
        public boolean flip() {
            final double odds = updateOdds();

            if (random.nextDouble() <= odds) {
                // If odds changed in the instant in time between when we originally
                // computed them and the line below when we attempt to modify them atomically,
                // pretend the flip was false because we don't want to get the update wrong
                // and trash the update performed by another concurrent operation.
                return currentOdds.compareAndSet(odds, odds * SUCCESSFUL_FLIP_PENALTY_COEFFICIENT);
            }

            return false;
        }
    }
}
