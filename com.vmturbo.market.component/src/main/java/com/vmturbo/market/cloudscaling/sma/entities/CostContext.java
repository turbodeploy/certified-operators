package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Objects;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.cost.calculation.PricingContext;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

/**
 * Class that holds together all the parameters required for looking up costs.
 */
public class CostContext {

    private final PricingContext pricingContext;
    private final CloudCommitmentAmount coverage;

    CostContext(PricingContext pricingContext, CloudCommitmentAmount coverage) {
        this.pricingContext = pricingContext;
        this.coverage = coverage;
    }

    public PricingContext getPricingContext() {
        return pricingContext;
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
            return pricingContext.equals(that.pricingContext)
                    && CommitmentAmountCalculator.isSame(coverage, that.coverage, SMAUtils.BIG_EPSILON);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(pricingContext, CommitmentAmountCalculator.hash(coverage));
    }
}
