package com.vmturbo.topology.processor.history.percentile;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represent a collection of buckets covering the percents range (0-100) - only integer values.
 */
@Immutable
public class PercentileBuckets {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int PERCENT = 100;
    private static final float EPSILON = 0.00001f;

    private final boolean isSinglePercentPrecision;
    /**
     * 101 entries percent number to bucket index starting with 0.
     */
    private final Map<Integer, Integer> percent2bucket = new HashMap<>();
    /**
     * Bucket index (starting with 0) to average value within the bucket.
     */
    private final Map<Integer, Float> bucket2average = new HashMap<>();

    /**
     * Constructs single percent precision bucket.
     */
    public PercentileBuckets() {
        this(StringUtils.EMPTY);
    }
    /**
     * Construct buckets from the CSV specification.
     *
     * @param spec expected string of the monotonously rising integers in 0-100 range inclusive
     *             in the format like, for example, "0,1,2,...50,55,...98,99,100"
     *             - first bucket consists of counts for 0 since we want to "ceil" utilizations
     *             - second is >0 and <=1
     *             - ...
     *             - last bucket is >99 and <=100
     *             null or empty string denotes one percent per bucket
     */
    public PercentileBuckets(String spec) {
        if (StringUtils.isEmpty(spec)) {
            isSinglePercentPrecision = true;
            return;
        }
        String[] parts = spec.split(",");
        int[] values = new int[parts.length];
        for (int i = 0; i < parts.length; ++i) {
            try {
                values[i] = Integer.valueOf(parts[i]);
            } catch (NumberFormatException e) {
                LOGGER.warn("Cannot parse percentile buckets specification " + spec);
                isSinglePercentPrecision = true;
                return;
            }
        }
        if (values[0] != 0 || values[parts.length - 1] != PERCENT) {
            LOGGER.warn("Invalid percentile buckets specification " + spec);
            isSinglePercentPrecision = true;
            return;
        }
        for (int i = 0; i < parts.length; ++i) {
            if (i > 0) {
                for (int j = values[i]; j > values[i - 1]; --j) {
                    percent2bucket.put(j, i);
                }
                bucket2average.put(i, (values[i - 1] + values[i] + EPSILON) / 2.0f);
            } else {
                percent2bucket.put(0, 0);
                bucket2average.put(0, 0f);
            }
        }
        isSinglePercentPrecision = false;
    }

    /**
     * Get the number of percentile buckets (no more than 101).
     *
     * @return buckets count
     */
    public int size() {
        return isSinglePercentPrecision ? PERCENT + 1 : bucket2average.size();
    }

    /**
     * Get the index of a bucket to which percent integer value belongs.
     *
     * @param percent percent
     * @return index starting with 0, null if percent passed is not a valid value
     */
    public Integer index(int percent) {
        return isSinglePercentPrecision ? percent : percent2bucket.get(percent);
    }

    /**
     * Get the average percent value of a bucket of given index.
     *
     * @param index index of a bucket
     * @return average percent, null if not found
     */
    public Float average(int index) {
        return isSinglePercentPrecision ? Math.max(0, index - (1 - EPSILON) / 2) : bucket2average.get(index);
    }

}
