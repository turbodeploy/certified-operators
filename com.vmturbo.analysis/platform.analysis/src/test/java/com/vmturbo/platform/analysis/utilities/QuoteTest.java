package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.CapacityLimitation;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.RatioBasedResourceDependency;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation.CommodityBundle;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteBelowMinAboveMaxCapacityLimitationQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteRangeBasedResourceDependencyQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteRatioBasedResourceDependencyQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentComputeCommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InsufficientCommodity;
import com.vmturbo.platform.analysis.utilities.Quote.LicenseUnavailableQuote;

/**
 * Tests for {@link Quote} class.
 */
public class QuoteTest {
    final Trader seller = Mockito.mock(Trader.class);
    final CommoditySpecification memSpec = new CommoditySpecification(1);
    final CommoditySpecification cpuSpec = new CommoditySpecification(2);
    final CommoditySpecification ioSpec = new CommoditySpecification(3);

    final CommoditySold memSold = Mockito.mock(CommoditySold.class);
    final CommoditySold cpuSold = Mockito.mock(CommoditySold.class);
    final CommoditySold ioSold = Mockito.mock(CommoditySold.class);

    final ShoppingList shoppingList = Mockito.mock(ShoppingList.class);

    @Before
    public void setup() {
        memSpec.setDebugInfoNeverUseInCode("MEM");
        cpuSpec.setDebugInfoNeverUseInCode("CPU");
        ioSpec.setDebugInfoNeverUseInCode("IO");
        when(seller.toString()).thenReturn("MockTrader");

        when(seller.getCommoditiesSold()).thenReturn(Arrays.asList(memSold, cpuSold, ioSold));
        when(seller.getBasketSold()).thenReturn(new Basket(memSpec, cpuSpec, ioSpec));

        when(memSold.getEffectiveCapacity()).thenReturn(7.0);
        when(memSold.getQuantity()).thenReturn(5.0);

        when(cpuSold.getEffectiveCapacity()).thenReturn(16.0);
        when(cpuSold.getQuantity()).thenReturn(15.0);
    }

    @Test
    public void testQuoteValues() {
        final Quote quote = new CommodityQuote(null, 1.0, 2.0, 3.0);
        assertEquals(1.0, quote.getQuoteValue(), 0);
        assertEquals(2.0, quote.getQuoteMin(), 0);
        assertEquals(3.0, quote.getQuoteMax(), 0);

        assertEquals(quote.getQuoteValue(), quote.getQuoteValues()[0], 0);
        assertEquals(quote.getQuoteMin(), quote.getQuoteValues()[1], 0);
        assertEquals(quote.getQuoteMax(), quote.getQuoteValues()[2], 0);
    }

    @Test
    public void testSeller() {
        final Quote nullQuote = new CommodityQuote(null);
        assertNull(nullQuote.getSeller());

        final Quote sellerQuote = new CommodityQuote(seller);
        assertEquals(seller, sellerQuote.getSeller());
    }

    @Test
    public void testCommodityQuoteRank() {
        final CommodityQuote quote = new CommodityQuote(seller);
        assertEquals(0, quote.getRank());

        quote.addCostToQuote(Double.POSITIVE_INFINITY, 10.0f, memSpec);
        assertEquals(1, quote.getRank());

        quote.addCostToQuote(Double.POSITIVE_INFINITY, 20.0f, cpuSpec);
        assertEquals(2, quote.getRank());

        quote.addCostToQuote(3.0, 20.0f, ioSpec);
        assertEquals(2, quote.getRank());
    }

    @Test
    public void testCommodityQuoteZero() {
        assertEquals(0, CommodityQuote.zero(null).getQuoteValue(), 0);
    }

    @Test
    public void testCommodityQuoteEmptyInsufficientCommodities() {
        final CommodityQuote quote = new CommodityQuote(seller);
        assertTrue(quote.getInsufficientCommodities().isEmpty());
        assertEquals(0, quote.getInsufficientCommodityCount());
    }

    @Test
    public void testCommodityQuoteWithInsufficientCommodities() {
        final CommodityQuote quote = new CommodityQuote(seller);
        assertTrue(quote.getInsufficientCommodities().isEmpty());

        quote.addCostToQuote(Double.POSITIVE_INFINITY, 10.0f, memSpec);
        quote.addCostToQuote(Double.POSITIVE_INFINITY, 20.0f, cpuSpec);
        quote.addCostToQuote(3.0, 20.0f, ioSpec);

        assertEquals(2, quote.getInsufficientCommodityCount());
        assertTrue(quote.getInsufficientCommodities().stream()
            .map(ic -> ic.commodity)
            .anyMatch(ic -> ic == memSpec));
        assertTrue(quote.getInsufficientCommodities().stream()
            .map(ic -> ic.commodity)
            .anyMatch(ic -> ic == cpuSpec));
        assertFalse(quote.getInsufficientCommodities().stream()
            .map(ic -> ic.commodity)
            .anyMatch(ic -> ic == ioSpec));
    }

    @Test
    public void testAddCostToQuoteWithCapacity() {
        final CommodityQuote quote = new CommodityQuote(seller);
        quote.addCostToQuote(Double.POSITIVE_INFINITY, 10.0f, memSpec);
    }

    @Test
    public void testAddCostToQuoteWithSoldIndex() {
        final CommodityQuote commodityQuote = new CommodityQuote(seller);
        commodityQuote.addCostToQuote(Double.POSITIVE_INFINITY, seller, 0);
        commodityQuote.addCostToQuote(Double.POSITIVE_INFINITY, seller, 1);

        assertEquals(2, commodityQuote.getInsufficientCommodityCount());
        final InsufficientCommodity mem = commodityQuote.getInsufficientCommodities().get(0);
        final InsufficientCommodity cpu = commodityQuote.getInsufficientCommodities().get(1);

        assertEquals(memSpec, mem.commodity);
        assertEquals(cpuSpec, cpu.commodity);

        assertEquals(2.0, mem.availableQuantity, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(1.0, cpu.availableQuantity, TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    public void testCommodityQuoteExplanation() {
        final Basket basketBought = new Basket(cpuSpec, memSpec);
        double quantityAvailable = 10.0;

        when(shoppingList.getQuantities()).thenReturn(new double[]{9.0, 15.0, 7.0});
        when(shoppingList.getBasket()).thenReturn(basketBought);

        final CommodityQuote commodityQuote = new CommodityQuote(seller);
        commodityQuote.addCostToQuote(Double.POSITIVE_INFINITY, quantityAvailable, cpuSpec);
        Optional<InfiniteQuoteExplanation> exp = commodityQuote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertEquals(seller, exp.get().seller.get());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertEquals(1, exp.get().commBundle.size());
        CommodityBundle bundle = exp.get().commBundle.iterator().next();
        assertEquals(cpuSpec, bundle.commSpec);
        assertEquals(shoppingList.getQuantity(shoppingList.getBasket().indexOf(cpuSpec)),
                bundle.requestedAmount, 0);
        assertEquals(quantityAvailable, bundle.maxAvailable.get(), 0);
    }

    @Test
    public void testLicenseUnavailableQuote() {
        final CommoditySpecification licenseSpec = new CommoditySpecification(4);
        licenseSpec.setDebugInfoNeverUseInCode("Linux");

        final Quote quote = new LicenseUnavailableQuote(seller, licenseSpec);
        Optional<InfiniteQuoteExplanation> exp = quote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertFalse(exp.get().seller.isPresent());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertEquals(1, exp.get().commBundle.size());
        CommodityBundle bundle = exp.get().commBundle.iterator().next();
        assertEquals(licenseSpec, bundle.commSpec);
        assertEquals(1.0, bundle.requestedAmount, 0);
        assertFalse(bundle.maxAvailable.isPresent());
    }

    /**
     * Test InfiniteRatioBasedResourceDependencyQuote.
     */
    @Test
    public void testInfiniteRatioBasedResourceDependencyQuote() {
        final RatioBasedResourceDependency dependentResourcePair = new RatioBasedResourceDependency(cpuSpec,
                memSpec, 2, false, 0, true);
        double dependentCommodityQuantity = 5.0;

        final Quote quote = new InfiniteRatioBasedResourceDependencyQuote(seller, dependentResourcePair,
                1.0, dependentCommodityQuantity);
        Optional<InfiniteQuoteExplanation> exp = quote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertFalse(exp.get().seller.isPresent());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertFalse(exp.get().seller.isPresent());
        assertEquals(1, exp.get().commBundle.size());
        CommodityBundle bundle = exp.get().commBundle.iterator().next();
        assertEquals(memSpec, bundle.commSpec);
        assertEquals(dependentCommodityQuantity, bundle.requestedAmount, 0);
        assertFalse(bundle.maxAvailable.isPresent());
    }

    /**
     * Test InfiniteBelowMinAboveMaxCapacityLimitationQuote.
     */
    @Test
    public void testInfiniteBelowMinAboveMaxCapacityLimitationQuote() {
        final CapacityLimitation capacityLimitation = new CapacityLimitation(5.0, 10.0, true);
        final double commodityQuantity = 4.0;

        final Quote quote = new InfiniteBelowMinAboveMaxCapacityLimitationQuote(seller, cpuSpec, capacityLimitation, commodityQuantity);
        Optional<InfiniteQuoteExplanation> exp = quote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertFalse(exp.get().seller.isPresent());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertFalse(exp.get().seller.isPresent());
        assertEquals(1, exp.get().commBundle.size());
        CommodityBundle bundle = exp.get().commBundle.iterator().next();
        assertEquals(cpuSpec, bundle.commSpec);
        assertEquals(commodityQuantity, bundle.requestedAmount, 0);
        assertFalse(bundle.maxAvailable.isPresent());
    }

    /**
     * Test InfiniteRangeBasedResourceDependencyQuote.
     */
    @Test
    public void testInfiniteRangeBasedResourceDependencyQuote() {
        double dependentCommodityQuantity = 5.0;

        final Quote quote = new InfiniteRangeBasedResourceDependencyQuote(seller, memSpec,
                cpuSpec, 3.0, 1.0, dependentCommodityQuantity);
        Optional<InfiniteQuoteExplanation> exp = quote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertFalse(exp.get().seller.isPresent());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertFalse(exp.get().seller.isPresent());
        assertEquals(1, exp.get().commBundle.size());
        CommodityBundle bundle = exp.get().commBundle.iterator().next();
        assertEquals(cpuSpec, bundle.commSpec);
        assertEquals(dependentCommodityQuantity, bundle.requestedAmount, 0);
        assertFalse(bundle.maxAvailable.isPresent());
    }

    @Test
    public void testInfiniteDependentComputeCommodityQuote() {
        final Basket basketBought = new Basket(cpuSpec, memSpec);
        double memBought = 2.0;
        double cpuBought = 3.0;
        when(shoppingList.getBasket()).thenReturn(basketBought);

        final Quote quote = new InfiniteDependentComputeCommodityQuote(seller, memSpec, cpuSpec,
                1.0, memBought, cpuBought);
        Optional<InfiniteQuoteExplanation> exp = quote.getExplanation(shoppingList);

        assertTrue(exp.isPresent());
        assertFalse(exp.get().seller.isPresent());
        assertFalse(exp.get().costUnavailable);
        assertTrue(seller.getType() == exp.get().providerType.get());
        assertFalse(exp.get().seller.isPresent());
        assertEquals(2, exp.get().commBundle.size());
        Iterator<CommodityBundle> itr = exp.get().commBundle.iterator();
        CommodityBundle bundle1 = itr.next();
        CommodityBundle bundle2 = itr.next();
        assertTrue((bundle1.commSpec.equals(memSpec) && bundle1.requestedAmount == memBought)
                || (bundle1.commSpec.equals(cpuSpec) && bundle1.requestedAmount == cpuBought));
        assertTrue((bundle2.commSpec.equals(memSpec) && bundle2.requestedAmount == memBought)
                   || (bundle2.commSpec.equals(cpuSpec) && bundle2.requestedAmount == cpuBought));
    }
}