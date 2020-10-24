package com.vmturbo.cloud.commitment.analysis.pricing;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * Interface responsible for holding RI specific pricing info.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface RIPricingData extends CloudCommitmentPricingData {

    /**
     * The up-front rate for an RI spec, amortized over the term. For example, if the up-front
     * cost is $12 and it's a one year RI spec, this method with return $1.
     *
     * @return The up-front rate, amortized over the life of the RI spec.
     */
    double hourlyUpFrontRate();

    /**
     * The recurring rate for an RI spec.
     *
     * @return The recurring rate.
     */
    double hourlyRecurringRate();

    /**
     * The amortized rate for the target RI spec.
     *
     * @return The amortized rate for the RI spec.
     */
    @Override
    @Value.Derived
    default double amortizedHourlyRate() {
        return hourlyUpFrontRate() + hourlyRecurringRate();
    }

    /**
     * Returns a builder of the RIPricingData class.
     *
     * @return The builder.
     */
    static RIPricingData.Builder builder() {
        return new RIPricingData.Builder();
    }

    /**
     *  Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableRIPricingData.Builder {}
}

