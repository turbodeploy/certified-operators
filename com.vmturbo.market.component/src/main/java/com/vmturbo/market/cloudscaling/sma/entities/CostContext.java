package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Objects;

import com.vmturbo.cost.calculation.PricingContext;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

/**
 * Class that holds together all the parameters required for looking up costs.
 */
public class CostContext {

    private final PricingContext pricingContext;
    private final float coverage;

    CostContext(PricingContext pricingContext, float coverage) {
        this.pricingContext = pricingContext;
        this.coverage = coverage;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CostContext) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CostContext that = (CostContext)o;
            return pricingContext.equals(that.pricingContext) && Math.abs(coverage - that.coverage)
                    < SMAUtils.BIG_EPSILON;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(pricingContext, (long)(coverage * SMAUtils.ROUNDING));
    }
}
