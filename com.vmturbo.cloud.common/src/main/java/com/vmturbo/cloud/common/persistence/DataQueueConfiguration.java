package com.vmturbo.cloud.common.persistence;

import java.time.Duration;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A data queue configuration.
 */
@HiddenImmutableImplementation
@Immutable
public interface DataQueueConfiguration {

    /**
     * The queue name.
     * @return the queue name.
     */
    @Nonnull
    String queueName();

    /**
     * The level of concurrency. Any value less than 1 is treated as 1
     * @return The level of concurrency.
     */
    int concurrency();

    /**
     * If enabled, all failed data sink operations will be retried sequentially prior to closing the queue,
     * in order to avoid concurrent conflicts (e.g. lock timeouts).
     * @return Whether to retry failed data sink operations prior to queue shutdown.
     */
    @Default
    default boolean retryFailedJobsSequentially() {
        return true;
    }

    /**
     * The retry policy for individual data sink operations.
     * @return The retry policy for individual data sink operations.
     */
    @Default
    default DataOperationRetryPolicy retryPolicy() {
        return DataOperationRetryPolicy.DEFAULT;
    }

    /**
     * Checks whether the data operation timeout is set.
     * @return Checks whether the data operation timeout is set.
     */
    default boolean hasDataOperationTimeout() {
        return !dataOperationTimeout().isZero() && !dataOperationTimeout().isNegative();
    }

    /**
     * The timeout for individual data operations, after which the operation will be interrupted. This timeout is
     * applied for individual attempts at a data operation and does not apply across retries.
     * @return The timeout for individual data operations, after which the operation will be interrupted.
     */
    @Default
    default Duration dataOperationTimeout() {
        return Duration.ofSeconds(-1);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link DataQueueConfiguration} instances.
     */
    class Builder extends ImmutableDataQueueConfiguration.Builder {}

    /**
     * The retry policy for data sink operations.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface DataOperationRetryPolicy {

        /**
         * The default data sink operation retry policy.
         */
        DataOperationRetryPolicy DEFAULT = DataOperationRetryPolicy.builder().build();

        /**
         * The number of retry attempts made, before treating a data sink operation as failed.
         * Failed operations may be retried on queue closed sequentially, based on queue configuration.
         * @return the number of retry attempts for a data sink operation.
         */
        @Default
        default int retryAttempts() {
            return 3;
        }

        /**
         * Using a random delay in retrying data operations, this is the minimum delay allowed.
         * @return The minimum delay in attempting a data operation retry.
         */
        @Default
        default Duration minRetryDelay() {
            return Duration.ofSeconds(1);
        }

        /**
         * The maximum delay in attempting a data operation retry.
         * @return The maximum delay in attempting a data operation retry.
         */
        @Default
        default Duration maxRetryDelay() {
            return Duration.ofSeconds(10);
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link DataOperationRetryPolicy} instances.
         */
        class Builder extends ImmutableDataOperationRetryPolicy.Builder {}
    }
}
