package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * This class controls what kind of Reserved Instances the recommendation algorithm will recommend buying.
 * These choices not only pass through into the individual recommendations, but because different options have
 * different price structures, it may also affect the effective discount achieved and thus may affect
 * how many reserved instances are recommended for purchase (or if any are recommended at all).
 *
 * Note that not all combinations may be possible.
 */
public class ReservedInstancePurchaseConstraints {

    // Which class of reserved instance to buy, eg STANDARD or CONVERTIBLE.
    private final OfferingClass offeringClass;

    // How long a term should be purchased, in years (eg 1 year or 3 year term).
    private final int termInYears;

    // How does the customer wish to pay for reserved instances, eg ALL_UPFRONT, PARTIAL_UPFRONT,
    // NO_UPFRONT.
    private final PaymentOption paymentOption;

    public ReservedInstancePurchaseConstraints(@Nonnull OfferingClass offeringClass,
                                               int termInYears,
                                               @Nonnull PaymentOption paymentOption) {
        this.offeringClass = Objects.requireNonNull(offeringClass);
        this.termInYears = termInYears;
        this.paymentOption = Objects.requireNonNull(paymentOption);
    }

    public ReservedInstancePurchaseConstraints(@Nonnull ReservedInstanceType type) {
        Objects.requireNonNull(type);
        this.offeringClass = type.getOfferingClass();
        this.termInYears = type.getTermYears();
        this.paymentOption = type.getPaymentOption();
    }

    /**
     * Get the offering class of the reserved instances to consider purchasing, eg STANDARD or CONVERTIBLE.
     *
     * @return the offering class.
     */
    @Nonnull
    public OfferingClass getOfferingClass() {
        return offeringClass;
    }

    /**
     * Get the term in years to buy when purchasing reserved instances, eg 1 or 3 years.
     *
     * @return the term, in years, for purchased reserved instances.
     */
    public int getTermInYears() {
        return termInYears;
    }

    /**
     * Get the payment option the user expects to use when purchasing reserved instances,
     * eg ALL_UPFRONT, PARTIAL_UPFRONT, NO_UPFRONT.
     *
     * @return the payment option.
     */
    @Nonnull
    public PaymentOption getPaymentOption() {
        return paymentOption;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offeringClass.ordinal(), termInYears, paymentOption.ordinal());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (this.getClass() != o.getClass()) {
            return false;
        }
        ReservedInstancePurchaseConstraints constraints = (ReservedInstancePurchaseConstraints)o;
        return offeringClass == constraints.getOfferingClass()
            && (termInYears == constraints.getTermInYears()
            && paymentOption == constraints.getPaymentOption());
    }

    @Override
    public String toString() {
        return String.format("offeringClass=%s years=%d paymentOption=%s",
                offeringClass.name(), termInYears, paymentOption.name());
    }
}
