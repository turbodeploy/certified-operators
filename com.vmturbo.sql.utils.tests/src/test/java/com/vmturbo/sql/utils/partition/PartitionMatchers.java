package com.vmturbo.sql.utils.partition;

import java.time.Instant;
import java.util.Collection;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Custom hamcrest matchers for use with tests involving partitions.
 */
public class PartitionMatchers {
    private PartitionMatchers() {}

    /**
     * Construct a {@link CoversInstant} matcher for the given instant.
     *
     * @param t instant to be covered
     * @return matcher instance
     */
    public static CoversInstant coversInstant(Instant t) {
        return new CoversInstant(t);
    }

    /**
     * Matcher class that can be used to test whether a collection of partitions includes a single
     * partition that covers a given timestamp.
     */
    public static class CoversInstant extends TypeSafeMatcher<Collection<Partition<Instant>>> {

        private final Instant t;

        /**
         * Create a new instance.
         *
         * @param t time instant that will be tested
         */
        public CoversInstant(Instant t) {
            this.t = t;
        }

        @Override
        protected boolean matchesSafely(Collection<Partition<Instant>> parts) {
            long containingPartCount = parts.stream()
                    .filter(p -> p.contains(t))
                    .count();
            return containingPartCount == 1L;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("includes exactly one partition that covers instant " + t);
        }
    }
}
