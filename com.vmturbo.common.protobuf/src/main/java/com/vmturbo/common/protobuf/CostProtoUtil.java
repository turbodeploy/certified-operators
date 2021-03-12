package com.vmturbo.common.protobuf;

import java.util.Collections;
import java.util.Currency;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * Utility methods for protobuf messages in Cost.proto.
 */
public class CostProtoUtil {

    private static final Logger logger = LogManager.getLogger();

    public static final int DAYS_IN_YEAR = 365;

    public static final int HOURS_IN_DAY = 24;

    public static final int MONTHS_IN_YEAR = 12;

    public static final int HOURS_IN_YEAR = HOURS_IN_DAY * DAYS_IN_YEAR;

    public static final int HOURS_IN_MONTH = HOURS_IN_YEAR / MONTHS_IN_YEAR;

    /**
     * Get the term of a reserved instance in some kind of time unit.
     *
     * @param riType The {@link ReservedInstanceType} of the reserved instance.
     * @param timeUnit The time unit to return.
     * @return The length of the RI's term in the time unit.
     */
    public static long timeUnitsInTerm(@Nonnull final ReservedInstanceType riType,
                                       @Nonnull final TimeUnit timeUnit) {
        return timeUnit.convert(riType.getTermYears() * DAYS_IN_YEAR, TimeUnit.DAYS);
    }

    /**
     * Get the currency that a {@link ReservedInstanceBought} is expressed in.
     * A single {@link ReservedInstanceBought} should have all costs expressed in a single currency,
     * so there will be a single return number.
     *
     * @param riBought The {@link ReservedInstanceBought}.
     * @return The ISO 4217 numeric code for the currency.
     */
    public static int getRiCurrency(@Nonnull final ReservedInstanceBought riBought) {
        final ReservedInstanceBoughtCost riCost =
                riBought.getReservedInstanceBoughtInfo().getReservedInstanceBoughtCost();

        final int defaultCurrency = CurrencyAmount.getDefaultInstance().getCurrency();
        // We initialize to an empty set, because we pretty much always
        // use the default currency (USD). This lets us avoid object allocation in the
        // majority of cases.
        // We also use a boolean to track whether there was an explicitly-specified default
        // currency.
        // Note: This is probably a textbook example of premature optimization.
        boolean explicitDefault = false;
        Set<Integer> nonDefaultCurrencies = Collections.emptySet();
        if (riCost.getFixedCost().getCurrency() != defaultCurrency) {
            nonDefaultCurrencies = new HashSet<>(1);
            nonDefaultCurrencies.add(riCost.getFixedCost().getCurrency());
        } else if (riCost.hasFixedCost()) {
            explicitDefault = true;
        }

        if (riCost.getRecurringCostPerHour().getCurrency() != defaultCurrency) {
            nonDefaultCurrencies = nonDefaultCurrencies.isEmpty() ?
                    new HashSet<>(1) : nonDefaultCurrencies;
            nonDefaultCurrencies.add(riCost.getRecurringCostPerHour().getCurrency());
        } else if (riCost.hasRecurringCostPerHour()) {
            explicitDefault = true;
        }

        if (riCost.getUsageCostPerHour().getCurrency() != defaultCurrency) {
            nonDefaultCurrencies = nonDefaultCurrencies.isEmpty() ?
                    new HashSet<>(1) : nonDefaultCurrencies;
            nonDefaultCurrencies.add(riCost.getUsageCostPerHour().getCurrency());
        } else if (riCost.hasUsageCostPerHour()) {
            explicitDefault = true;
        }

        if (nonDefaultCurrencies.isEmpty()) {
            return defaultCurrency;
        } else if (nonDefaultCurrencies.size() == 1 && !explicitDefault) {
            return nonDefaultCurrencies.iterator().next();
        } else {
            throw new IllegalArgumentException("RI Bought " + riBought.getId() +
                " has multiple currencies: " +
                    Stream.concat(nonDefaultCurrencies.stream(), Stream.of(defaultCurrency))
                        .map(currInt -> Integer.toString(currInt))
                        .collect(Collectors.joining(",")));
        }
    }

    /**
     * Get the hourly amount of a particular {@link Price}, making the appropriate adjustments
     * based on the unit of the price.
     *
     * @param price The {@link Price}.
     * @return The hourly amount for the given {@link Price}.
     * @throws IllegalArgumentException If the unit is invalid, or has no hourly equivalent.
     */
    public static double getHourlyPriceAmount(@Nonnull final Price price) {
        switch (price.getUnit()) {
            case HOURS:
            case IO_REQUESTS:
                return price.getPriceAmount().getAmount();
            case DAYS:
                return price.getPriceAmount().getAmount() / HOURS_IN_DAY;
            case MONTH:
            case MILLION_IOPS:
            case MBPS_MONTH:
            case GB_MONTH:
                return price.getPriceAmount().getAmount() / HOURS_IN_MONTH;
            case TOTAL:
                throw new IllegalArgumentException("Cannot get hourly amount of TOTAL price.");
            default:
                throw new IllegalArgumentException("Unhandled unit: " + price.getUnit());
        }
    }

    /**
     * Get the unit price amount for a particular {@link Price.Unit} given the hourly price.
     * This is (roughly) the inverse of {@link CostProtoUtil#getHourlyPriceAmount(Price)}.
     *
     * @param unit The unit.
     * @param hourlyPrice The hourly price.
     * @return The unit price.
     */
    public static double getUnitPriceAmount(@Nonnull final Price.Unit unit, final double hourlyPrice) {
        switch (unit) {
            case HOURS:
                return hourlyPrice;
            case DAYS:
                return hourlyPrice * HOURS_IN_DAY;
            case MONTH: case MILLION_IOPS: case GB_MONTH:
                return hourlyPrice * HOURS_IN_MONTH;
            case TOTAL:
                return hourlyPrice;
            default:
                throw new IllegalArgumentException("Unhandled unit: " + unit);
        }
    }

    // Currencies by numeric code map will is a map of numeric code to the Currency.
    // TODO: But the numeric code is not unique. As of writing this, there are a few currencies
    // which share numeric code. They are:
    // Currency code = 946 -> Romanian Leu (RON), Romanian Leu (1952-2006) (ROL)
    // Currency code = 891 -> Serbian Dinar (2002-2006) (CSD), Yugoslavian New Dinar (1994-2002) (YUM)
    // Currency code = 0   -> French UIC-Franc (XFU), French Gold Franc (XFO)
    private static final Map<Integer, Currency> CURRENCIES_BY_NUMERIC_CODE =
            Collections.unmodifiableMap(
                    Currency.getAvailableCurrencies().stream()
                            .collect(Collectors.toMap(Currency::getNumericCode, Function.identity(),
                                    (c1, c2) -> c1)));
    private static final Currency DEFAULT_CURRENCY = Currency.getInstance("USD");

    /**
     * Get the string unit for the given currency, like "$/h".
     *
     * @param currencyAmount the currency amount which contains currency and amount
     * @return unit string
     */
    public static String getCurrencyUnit(@Nonnull CurrencyAmount currencyAmount) {
        final Currency currency;
        if (currencyAmount.hasCurrency()) {
            currency = CURRENCIES_BY_NUMERIC_CODE.getOrDefault(currencyAmount.getCurrency(), DEFAULT_CURRENCY);
        } else {
            currency = DEFAULT_CURRENCY;
        }
        return currency.getSymbol() + "/h";
    }
}
