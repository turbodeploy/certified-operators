package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory.DependentResourcePair;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentComputeCommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentResourcePairQuote;
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

        when(shoppingList.getQuantities()).thenReturn(new double[]{9.0, 15.0, 7.0});
        when(shoppingList.getBasket()).thenReturn(basketBought);

        final CommodityQuote commodityQuote = new CommodityQuote(seller);
        commodityQuote.addCostToQuote(Double.POSITIVE_INFINITY, 10.0, cpuSpec);

        assertEquals("CPU (15.0/10.0) on seller MockTrader", commodityQuote.getExplanation(shoppingList));
    }

    @Test
    public void testLicenseUnavailableQuote() {
        final CommoditySpecification licenseSpec = new CommoditySpecification(4);
        licenseSpec.setDebugInfoNeverUseInCode("Linux");

        final Quote quote = new LicenseUnavailableQuote(seller, licenseSpec);
        assertEquals("License Linux unavailable", quote.getExplanation(shoppingList));
    }

    @Test
    public void testInfiniteDependentResourcePairQuote() {
        final DependentResourcePair dependentResourcePair = new DependentResourcePair(cpuSpec, memSpec, 2);

        final Quote quote = new InfiniteDependentResourcePairQuote(dependentResourcePair, 1.0, 5.0);
        assertEquals("Dependent commodity MEM (5.0) exceeds base commodity CPU (1.0) [maxRatio=2.0]",
            quote.getExplanation(shoppingList));
    }

    @Test
    public void testInfiniteDependentComputeCommodityQuote() {
        final Basket basketBought = new Basket(cpuSpec, memSpec);
        when(shoppingList.getBasket()).thenReturn(basketBought);

        final Quote quote = new InfiniteDependentComputeCommodityQuote(0, 1, 1.0, 2.0, 3.0);
        assertEquals("Dependent compute commodities MEM and CPU sum (2.0 + 3.0 = 5.0) exceeds capacity 1.0",
            quote.getExplanation(shoppingList));
    }
}