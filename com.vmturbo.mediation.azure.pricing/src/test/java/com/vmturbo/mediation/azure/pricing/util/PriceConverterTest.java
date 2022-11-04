package com.vmturbo.mediation.azure.pricing.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * Tests for the PriceConverter.
 */
public class PriceConverterTest {
    private static final String PLAN_NAME = "plan";
    private static final String NO_SUCH_PLAN_NAME = "no such plan";

    private PriceConverter converter = new PriceConverter();
    private AzureMeter dollarPerHour = makeMeter("1/Hour", 1.0, 0.0);
    private AzureMeter ninetyCentsPerHourForTen = makeMeter("1/Hour", 0.9, 10.0);

    /**
     * Test handling of methods that require a single price, when that condition is not
     * satisfied.
     */
    @Test
    public void testOnlySinglePriceOrFail() {
        AzureMeterDescriptor descriptor = Mockito.mock(AzureMeterDescriptor.class);

        final ResolvedMeter resolvedMeter = new ResolvedMeter(descriptor);
        resolvedMeter.putPricing(dollarPerHour);
        resolvedMeter.putPricing(ninetyCentsPerHourForTen);

        // The named plan must be present for these methods.

        PriceConversionException ex1 = assertThrows(PriceConversionException.class, () -> {
            converter.getPriceAmount(Unit.HOURS, resolvedMeter, NO_SUCH_PLAN_NAME);
        });

        PriceConversionException ex2 = assertThrows(PriceConversionException.class, () -> {
            converter.getPrice(Unit.HOURS, resolvedMeter, NO_SUCH_PLAN_NAME);
        });

        // These methods only work with a single price.

        PriceConversionException ex3 = assertThrows(PriceConversionException.class, () -> {
            converter.getPriceAmount(Unit.HOURS, resolvedMeter.getPricingByMinimumQuantity(PLAN_NAME));
        });

        PriceConversionException ex4 = assertThrows(PriceConversionException.class, () -> {
            converter.getPrice(Unit.HOURS, resolvedMeter.getPricingByMinimumQuantity(PLAN_NAME));
        });

        PriceConversionException ex5 = assertThrows(PriceConversionException.class, () -> {
            converter.getPriceAmount(Unit.HOURS, resolvedMeter, PLAN_NAME);
        });

        PriceConversionException ex6 = assertThrows(PriceConversionException.class, () -> {
            converter.getPrice(Unit.HOURS, resolvedMeter, PLAN_NAME);
        });

        // The single price must have a minimum quantity of zero.

        final ResolvedMeter resolvedMeter2 = new ResolvedMeter(descriptor);
        resolvedMeter2.putPricing(ninetyCentsPerHourForTen);

        PriceConversionException ex7 = assertThrows(PriceConversionException.class, () -> {
            converter.getPriceAmount(Unit.HOURS, resolvedMeter2.getPricingByMinimumQuantity(PLAN_NAME));
        });

        PriceConversionException ex8 = assertThrows(PriceConversionException.class, () -> {
            converter.getPrice(Unit.HOURS, resolvedMeter2.getPricingByMinimumQuantity(PLAN_NAME));
        });

        PriceConversionException ex9 = assertThrows(PriceConversionException.class, () -> {
            converter.getPriceAmount(Unit.HOURS, resolvedMeter2, PLAN_NAME);
        });

        PriceConversionException ex10 = assertThrows(PriceConversionException.class, () -> {
            converter.getPrice(Unit.HOURS, resolvedMeter2, PLAN_NAME);
        });
    }

    /**
     * Test the success path of price conversion methods for a single price.
     *
     * @throws PriceConversionException if the test fails
     */
    @Test
    public void testOnlySinglePriceSuccess() throws PriceConversionException {
        AzureMeterDescriptor descriptor = Mockito.mock(AzureMeterDescriptor.class);

        final ResolvedMeter resolvedMeter = new ResolvedMeter(descriptor);
        resolvedMeter.putPricing(dollarPerHour);

        // These methods only work with a single price.

        assertEquals(1.0, converter.getPriceAmount(Unit.HOURS,
            resolvedMeter.getPricingByMinimumQuantity(PLAN_NAME)), 0.001);

        assertEquals(1.0, converter.getPriceAmount(Unit.HOURS, resolvedMeter, PLAN_NAME),
            0.001);

        Price price1 = converter.getPrice(Unit.HOURS, resolvedMeter.getPricingByMinimumQuantity(PLAN_NAME));
        assertEquals(Unit.HOURS, price1.getUnit());
        assertEquals(1.0, price1.getPriceAmount().getAmount(), 0.001);

        Price price2 = converter.getPrice(Unit.HOURS, resolvedMeter, PLAN_NAME);
        assertEquals(Unit.HOURS, price2.getUnit());
        assertEquals(1.0, price2.getPriceAmount().getAmount(), 0.001);
    }

    private AzureMeter makeMeter(@Nonnull final String units, double price, double minUnits) {
        AzureMeter meter = Mockito.mock(AzureMeter.class);
        when(meter.getUnitOfMeasure()).thenReturn(units);
        when(meter.getUnitPrice()).thenReturn(price);
        when(meter.getPlanName()).thenReturn(PLAN_NAME);
        when(meter.getTierMinimumUnits()).thenReturn(minUnits);

        return meter;
    }
}
