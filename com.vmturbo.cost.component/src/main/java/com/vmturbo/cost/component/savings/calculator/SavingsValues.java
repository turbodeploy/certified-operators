package com.vmturbo.cost.component.savings.calculator;

import java.time.LocalDateTime;

import org.immutables.value.Value;

/**
 * Immutable object definition for holding the result of savings calculation.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface SavingsValues {
    /**
     * Time of the savings values.
     *
     * @return timestamp of the savings values
     */
    LocalDateTime getTimestamp();

    /**
     * Entity OID.
     *
     * @return entity OID
     */
    long getEntityOid();

    /**
     * Savings value.
     *
     * @return savings value
     */
    double getSavings();

    /**
     * Investments value.
     *
     * @return investment value
     */
    double getInvestments();

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableSavingsValues.Builder {}
}
