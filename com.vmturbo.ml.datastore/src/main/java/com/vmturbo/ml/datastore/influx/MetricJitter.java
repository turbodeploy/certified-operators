package com.vmturbo.ml.datastore.influx;

import java.util.Random;

import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.core.Preconditions;

@Immutable
/**
 * The jitter is only used for testing purposes to prevent the same value being written
 * for every broadcast when doing a long-running test with a loaded topology. If we write
 * the same value to influx every time, influx can do much higher compression than it would
 * be able to do with a real environment so we jitter the values randomly so that the compression
 * level we see in our tests is a better reflection of what we would expect to see at a customer.
 *
 * Do NOT enable MetricJitter at a customer because doing so will corrupt the metric values.
 */
public class MetricJitter {
    private static final Logger logger = LogManager.getLogger();

    private final double minJitterPercentage;
    private final double maxJitterPercentage;
    private Random random;

    /**
     * Jitter of 0 means no jitter at all. This is the default.
     *
     * @param maxJitterPercentage The most to jitter the value. Applied as a percentage. Should be in range [0,1]
     */
    public MetricJitter(final boolean jitterEnabled,
                        final double maxJitterPercentage) {
        Preconditions.checkArgument(maxJitterPercentage >= 0);

        if (jitterEnabled && maxJitterPercentage > 0) {
            this.minJitterPercentage = -maxJitterPercentage;
            this.maxJitterPercentage = maxJitterPercentage;

            logger.info("Applying metric jitter between {} and {}",
                this.minJitterPercentage, this.maxJitterPercentage);
        } else {
            this.minJitterPercentage = 0;
            this.maxJitterPercentage = 0;

            logger.info("Applying no metric jitter");
        }

        random = new Random();
    }

    public double jitter(double originalValue) {
        if (maxJitterPercentage == 0) {
            return originalValue;
        }

        final double r = random.nextDouble();
        final double lowerBound = originalValue * (1 + minJitterPercentage);
        final double upperBound = originalValue * (1 + maxJitterPercentage);

        // Linear interpolate between lower and upper bounds.
        return (1 - r) * lowerBound + r * upperBound;
    }
}
