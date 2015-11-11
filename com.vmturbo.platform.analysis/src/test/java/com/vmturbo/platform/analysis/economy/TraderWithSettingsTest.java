package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the combined TraderWithSettings class.
 *
 * <p>
 *  It tests both Trader and TraderSettings interfaces without casting to the corresponding interface.
 * </p>
 */
@RunWith(JUnitParamsRunner.class)
public final class TraderWithSettingsTest {
    // Fields
    private static final CommoditySpecification A = new CommoditySpecification(0,0,0);
    private static final CommoditySpecification B = new CommoditySpecification(0,0,100);
    private static final CommoditySpecification C = new CommoditySpecification(1,0,100);

    private static final Integer[] validIndices = {0, 1, 100, Integer.MAX_VALUE};
    private static final Integer[] invalidIndices = {-1, -100, Integer.MIN_VALUE};
    private static final Integer[] validTypes = validIndices; // just happens to be the same.
    private static final Integer[] invalidTypes = invalidIndices; // just happens to be the same.
    private static final TraderState[] validStates = TraderState.values();
    private static final Basket[] validBaskets = {
        new Basket(),
        new Basket(A),
        new Basket(B),
        new Basket(A,B),
        new Basket(A,C),
        new Basket(A,B,C)
    };

    private TraderWithSettings fixture_;


    // Methods

    @Before
    public void setUp() {
        fixture_ = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket());
    }

    // Tests for internal methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new TraderWithSettings({0},{1},{2},{3}")
    public final void testTraderWithSettings_NormalInput(int index, int type, @NonNull TraderState state, @NonNull Basket basket) {
        TraderWithSettings trader = new TraderWithSettings(index, type, state, basket);
        assertEquals(index, trader.getEconomyIndex());
        assertEquals(type, trader.getType());
        assertSame(state, trader.getState());
        assertSame(basket, trader.getBasketSold());
        assertEquals(basket.size(), trader.getCommoditiesSold().size());
        for (CommoditySpecification specification : basket) {
            assertNotNull(trader.getCommoditySold(specification));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestTraderWithSettings_NormalInput() {
        Object[][] output = new Object[validIndices.length*validTypes.length*validStates.length*validBaskets.length][];

        int c = 0;
        for (int index : validIndices) {
            for (int type : validTypes) {
                for (TraderState state : validStates) {
                    for (Basket basket : validBaskets) {
                        output[c++] = new Object[]{index,type,state,basket};
                    }
                }
            }
        }

        return output;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: new TraderWithSettings({0},{1},{2},{3}")
    public final void testTraderWithSettings_InvalidInput(int index, int type, @NonNull TraderState state, @NonNull Basket basket) {
        new TraderWithSettings(index, type, state, basket);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestTraderWithSettings_InvalidInput() {
        // just create the minimum of tests for now.
        Object[][] output = new Object[invalidIndices.length+invalidTypes.length][];

        int c = 0;
        for (int index : invalidIndices) {
            output[c++] = new Object[]{index,validIndices[0],validStates[0],validBaskets[0]};
        }

        for (int type : invalidTypes) {
            output[c++] = new Object[]{validIndices[0],type,validStates[0],validBaskets[0]};
        }

        return output;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)EconomyIndex({0})")
    public final void testGetSetEconomyIndex_NormalInput(int index) {
        fixture_.setEconomyIndex(index);
        assertEquals(index, fixture_.getEconomyIndex());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetSetEconomyIndex_NormalInput() {
        return validIndices;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: (set|get)EconomyIndex({0})")
    public final void testGetSetEconomyIndex_InvalidInput(int index) {
        fixture_.setEconomyIndex(index);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetSetEconomyIndex_InvalidInput() {
        return invalidIndices;
    }

    @Test // That the returned list indeed implements all operations.
    public final void testGetMarketsAsBuyer() {
        @NonNull ListMultimap<@NonNull @ReadOnly Market, @NonNull BuyerParticipation> markets = fixture_.getMarketsAsBuyer();
        @NonNull Market market = new Market(new Basket()); // dummy object
        @NonNull BuyerParticipation participation = new BuyerParticipation(0,0,0); // dummy object

        assertTrue(markets.asMap().isEmpty());
        assertTrue(markets.equals(markets));
        assertTrue(markets.get(market).isEmpty());
        assertTrue(markets.removeAll(market).isEmpty());
        assertTrue(markets.replaceValues(market,markets.values()).isEmpty());
        assertFalse(markets.containsEntry(market, null));
        assertFalse(markets.containsKey(market));
        assertFalse(markets.containsValue(null));
        assertTrue(markets.entries().isEmpty());
        assertTrue(markets.isEmpty());
        assertTrue(markets.keys().isEmpty());
        assertTrue(markets.keySet().isEmpty());
        assertTrue(markets.put(market, participation));
        assertTrue(markets.putAll(market, new ArrayList<>(markets.values())));
        assertTrue(markets.putAll(ArrayListMultimap.create(markets)));
        assertTrue(markets.remove(market, participation));
        assertEquals(3, markets.size());
        assertEquals(3, markets.values().size());

        markets.clear();
    }

    @Test // That the returned list indeed implements all operations.
    public final void testGetMarketsAsSeller() {
        @NonNull List<@NonNull Market> markets = fixture_.getMarketsAsSeller();
        @NonNull Market market = new Market(new Basket());

        assertFalse(markets.contains(null));
        assertFalse(markets.containsAll(Arrays.asList(null,null)));
        assertTrue(markets.equals(markets));
        assertEquals(-1, markets.indexOf(null));
        assertTrue(markets.isEmpty());
        assertNotNull(markets.iterator());
        assertEquals(-1, markets.lastIndexOf(null));
        assertNotNull(markets.listIterator());
        assertEquals(0, markets.size());
        assertNotNull(markets.toArray());
        assertTrue(markets.add(market));
        markets.add(0,markets.get(0));
        assertTrue(markets.addAll(markets));
        assertTrue(markets.addAll(0,markets));
        markets.remove(0);
        assertSame(market, markets.set(0, market));
        assertFalse(markets.remove(null));
        assertTrue(markets.removeAll(markets));
        assertFalse(markets.retainAll(markets));
        markets.clear();
    }

    // Tests for Trader methods

    @Test // That the returned list is indeed unmodifiable (part 1)
    public final void testGetCommoditiesSold_ValidOperations() {
        @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> commodities = fixture_.getCommoditiesSold();
        assertFalse(commodities.contains(null));
        assertFalse(commodities.containsAll(Arrays.asList(null,null)));
        assertTrue(commodities.equals(commodities));
        assertEquals(-1, commodities.indexOf(null));
        assertTrue(commodities.isEmpty());
        assertNotNull(commodities.iterator());
        assertEquals(-1, commodities.lastIndexOf(null));
        assertNotNull(commodities.listIterator());
        assertEquals(0, commodities.size());
        assertNotNull(commodities.toArray());
    }

    @Test // That the returned list is indeed unmodifiable (part 2)
    public final void testGetCommoditiesSold_InvalidOperations() {
        @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> commodities = fixture_.getCommoditiesSold();
        @NonNull CommoditySold commodity = new CommoditySoldWithSettings(); // dummy object

        // TODO: may also need to test these on a non-empty buyers list because the API does not
        // guarantee that this exception will be thrown in some cases.
        try{
            commodities.add(commodity);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.add(0,commodity);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.addAll(Arrays.asList(commodity,commodity));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.addAll(0,Arrays.asList(commodity,commodity));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.remove(0);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.remove(commodity);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.removeAll(commodities);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.retainAll(commodities);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            commodities.set(0, commodity);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Trader(?,?,?,{0}).getCommoditySold({1})")
    public final void testGetCommoditySold(Basket basket, CommoditySpecification specification) {
        Trader trader = new TraderWithSettings(0, 0, TraderState.ACTIVE, basket);
        if (basket.contains(specification)) {
            assertSame(trader.getCommoditiesSold().get(basket.indexOf(specification)), trader.getCommoditySold(specification));
        } else {
            assertNull(trader.getCommoditySold(specification));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetCommoditySold() {
        return new Object[][]{
            {new Basket(),A},
            {new Basket(),B},
            {new Basket(A),A},
            {new Basket(A),B},
            {new Basket(A,B),A},
            {new Basket(A,B),B},
            {new Basket(A,B),C},
            {new Basket(A,B,C),A},
            {new Basket(A,B,C),B},
            {new Basket(A,B,C),C},
            {new Basket(A,B,C),new CommoditySpecification(0)},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Trader(?,?,?,{0}).addCommoditySold({1})")
    public final void testAddCommoditySold(Basket basket, CommoditySpecification specification, int newSize) {
        Trader trader = new TraderWithSettings(0, 0, TraderState.ACTIVE, basket);

        assertEquals(trader.getBasketSold().size(), trader.getCommoditiesSold().size());
        assertEquals(newSize == basket.size(), trader.addCommoditySold(specification) == null);

        assertEquals(newSize, trader.getBasketSold().size());
        assertEquals(newSize, trader.getCommoditiesSold().size());

        // assert no aliasing between commodities sold (assumes default equals).
        assertEquals(trader.getBasketSold().size(), new HashSet<>(trader.getCommoditiesSold()).size());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddCommoditySold() {
        return new Object[][]{
            {new Basket(),A,1},
            {new Basket(),B,1},
            {new Basket(A),A,1},
            {new Basket(A),B,2},
            {new Basket(A,B),A,2},
            {new Basket(A,B),B,2},
            {new Basket(A,B),C,3},
            {new Basket(A,B,C),A,3},
            {new Basket(A,B,C),B,3},
            {new Basket(A,B,C),C,3},
            {new Basket(A,B,C),new CommoditySpecification(0),4},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Trader(?,?,?,{0}).removeCommoditySold({1})")
    public final void testRemoveCommoditySold(Basket basket, CommoditySpecification specification, int newSize) {
        Trader trader = new TraderWithSettings(0, 0, TraderState.ACTIVE, basket);

        assertEquals(trader.getBasketSold().size(), trader.getCommoditiesSold().size());
        assertEquals(newSize == basket.size(), trader.removeCommoditySold(specification) == null);

        assertEquals(newSize, trader.getBasketSold().size());
        assertEquals(newSize, trader.getCommoditiesSold().size());

        // assert no aliasing between commodities sold (assumes default equals).
        assertEquals(trader.getBasketSold().size(), new HashSet<>(trader.getCommoditiesSold()).size());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestRemoveCommoditySold() {
        return new Object[][]{
            {new Basket(),A,0},
            {new Basket(),B,0},
            {new Basket(A),A,0},
            {new Basket(A),B,1},
            {new Basket(A,B),A,1},
            {new Basket(A,B),B,1},
            {new Basket(A,B),C,2},
            {new Basket(A,B,C),A,2},
            {new Basket(A,B,C),B,2},
            {new Basket(A,B,C),C,2},
            {new Basket(A,B,C),new CommoditySpecification(0),3},
        };
    }

    @Test
    public final void testGetSettings() {
        assertSame(fixture_, fixture_.getSettings());
    }

    @Test
    @Parameters(source = TraderState.class)
    @TestCaseName("Test #{index}: (set|get)State({0})")
    public final void testGetSetState(TraderState state) {
        fixture_.setState(state);
        assertSame(state, fixture_.getState());
    }

    // Tests for TraderSettings methods

    @Test
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)Suspendable({0})")
    public final void testIsSetSuspendable(boolean suspendable) {
        fixture_.setSuspendable(suspendable);
        assertEquals(suspendable, fixture_.isSuspendable());
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)Cloneable({0})")
    public final void testIsSetCloneable(boolean cloneable) {
        fixture_.setCloneable(cloneable);
        assertEquals(cloneable, fixture_.isCloneable());
    }

    @Test
    @Parameters({"0.0,0.0","0.0,0.5","0.0,1.0",
                 "0.5,0.5","0.5,1.0",
                 "1.0,1.0"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredUtil({1}) while minDesiredUtilization == {0}")
    public final void testGetSetMaxDesiredUtil_NormalInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture_.setMinDesiredUtil(minDesiredUtilization);
        fixture_.setMaxDesiredUtil(maxDesiredUtilization);
        assertEquals(maxDesiredUtilization, fixture_.getMaxDesiredUtil(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"0.0,-1.0","0.0,1.5",
                 "0.5,0.0","0.5,1.5",
                 "1.0,0.5","1.0,1.5"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredUtil({1}) while minDesiredUtilization == {0}")
    public final void testGetSetMaxDesiredUtil_InvalidInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture_.setMinDesiredUtil(minDesiredUtilization);
        fixture_.setMaxDesiredUtil(maxDesiredUtilization);
    }

    @Test
    @Parameters({"0.0,1.0","0.5,1.0","1.0,1.0",
                 "0.0,0.5","0.5,0.5",
                 "0.0,0.0"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredUtil({0}) while maxDesiredUtilization == {1}")
    public final void testGetSetMinDesiredUtil_NormalInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture_.setMaxDesiredUtil(maxDesiredUtilization);
        fixture_.setMinDesiredUtil(minDesiredUtilization);
        assertEquals(minDesiredUtilization, fixture_.getMinDesiredUtil(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1.0,1.0","1.5,1.0",
                 "-1.0,0.5","1.0,0.5",
                 "-1.0,0.0","0.5,0.0"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredUtil({0}) while maxDesiredUtilization == {1}")
    public final void testGetSetMinDesiredUtil_InvalidInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture_.setMaxDesiredUtil(maxDesiredUtilization);
        fixture_.setMinDesiredUtil(minDesiredUtilization);
    }

} // end class TraderWithSettingsTest
