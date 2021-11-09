package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Class that encapsulates the {@link CurrencyAmount}.
 */
@JsonInclude(Include.NON_EMPTY)
public class CurrencyAmountRepoDTO {

    private Double amount;
    private Integer currency;

    /**
     * Constructor.
     *
     * @param currencyAmount the {@link CurrencyAmount}
     */
    public CurrencyAmountRepoDTO(@Nonnull final CurrencyAmount currencyAmount) {
        if (currencyAmount.hasAmount()) {
            this.amount = currencyAmount.getAmount();
        }
        if (currencyAmount.hasCurrency()) {
            this.currency = currencyAmount.getCurrency();
        }
    }

    /**
     * Creates an instance of {@link CurrencyAmount}.
     *
     * @return the {@link CurrencyAmount}
     */
    @Nonnull
    public CurrencyAmount createCurrencyAmount() {
        final CurrencyAmount.Builder currencyAmount = CurrencyAmount.newBuilder();
        if (this.amount != null) {
            currencyAmount.setAmount(this.amount);
        }
        if (this.currency != null) {
            currencyAmount.setCurrency(this.currency);
        }
        return currencyAmount.build();
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(final Double amount) {
        this.amount = amount;
    }

    public Integer getCurrency() {
        return currency;
    }

    public void setCurrency(final Integer currency) {
        this.currency = currency;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CurrencyAmountRepoDTO that = (CurrencyAmountRepoDTO)o;
        return Objects.equals(amount, that.amount) && Objects.equals(currency, that.currency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(amount, currency);
    }

    @Override
    public String toString() {
        return CurrencyAmountRepoDTO.class.getSimpleName() + "{amount=" + amount + ", currency="
                + currency + '}';
    }
}
