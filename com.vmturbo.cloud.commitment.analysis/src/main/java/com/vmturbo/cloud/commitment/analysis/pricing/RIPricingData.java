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
     * If RI pricing data cant be found for any reason we return empty RI pricing data.
     */
    RIPricingData EMPTY_RI_PRICING_DATA = RIPricingData.builder().reservedInstanceRecurringRate(0f)
            .reservedInstanceUpfrontRate(0f).build();

    /**
     * The up-front rate for an RI spec, amortized over the term. For example, if the up-front
     * cost is $12 and it's a one year RI spec, this method with return $1.
     *
     * @return The up-front rate, amortized over the life of the RI spec.
     */
    float reservedInstanceUpfrontRate();

    /**
     * The recurring rate for an RI spec.
     *
     * @return The recurring rate.
     */
    float reservedInstanceRecurringRate();

    /**
     * The amortized rate for the target RI spec.
     *
     * @return The amortized rate for the RI spec.
     */
    @Value.Derived
    default float reservedInstanceRate() {
        return reservedInstanceUpfrontRate() + reservedInstanceRecurringRate();
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

