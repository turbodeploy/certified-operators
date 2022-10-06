package com.vmturbo.cost.component.billed.cost;

import java.time.Duration;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Configuration for {@link com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData} through
 * {@link com.vmturbo.cost.component.billed.cost.CloudCostStore.BilledCostPersistenceSession}.
 */
@HiddenImmutableImplementation
@Immutable
public interface BilledCostPersistenceConfig {

    /**
     * The maximum concurrent persistence operations.
     * @return The maximum concurrent persistence operations.
     */
    int concurrency();

    /**
     * The allowed duration of any individual persistence operation before it times out. Note that because
     * this duration is eventually translated to milliseconds, large durations (e.g. infinity) meant to disable a timeout
     * may throw an overflow exception.
     * @return The allowed duration of any individual persistence operation before it times out.
     */
    @Nonnull
    Duration persistenceTimeout();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link BilledCostPersistenceConfig} instances.
     */
    class Builder extends ImmutableBilledCostPersistenceConfig.Builder {}
}
