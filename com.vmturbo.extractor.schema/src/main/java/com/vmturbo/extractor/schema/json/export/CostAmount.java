package com.vmturbo.extractor.schema.json.export;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * The savings for an action.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class CostAmount {
    private String unit;
    private Double amount;

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    /**
     * Create a new {@link CostAmount} message from a {@link CurrencyAmount} protobuf.
     *
     * @param amount The {@link CurrencyAmount}.
     * @return The {@link CostAmount}.
     */
    @Nonnull
    public static CostAmount newAmount(CurrencyAmount amount) {
        CostAmount costAmount = new CostAmount();
        if (amount.hasAmount()) {
            costAmount.setAmount(amount.getAmount());
        }
        if (amount.hasCurrency()) {
            costAmount.setUnit(CostProtoUtil.getCurrencyUnit(amount));
        }
        return costAmount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, amount);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof CostAmount) {
            CostAmount otherMt = (CostAmount)other;
            return Objects.equals(otherMt.amount, amount) && Objects.equals(otherMt.unit, unit);
        } else {
            return false;
        }
    }
}