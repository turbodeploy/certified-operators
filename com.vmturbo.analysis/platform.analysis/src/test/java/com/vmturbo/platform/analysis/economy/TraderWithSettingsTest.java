package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utility.CollectionTests;
import com.vmturbo.platform.analysis.utility.ListTests;
import com.vmturbo.platform.analysis.utility.MapTests;
import com.vmturbo.platform.analysis.utility.SetTests;

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
    private static final CommoditySpecification A = new CommoditySpecification(0,1000);
    private static final CommoditySpecification B = new CommoditySpecification(1,1001);
    private static final CommoditySpecification C = new CommoditySpecification(2,1001);

    private static final Integer[] validIndices = {0, 1, 100, Integer.MAX_VALUE};
    private static final Integer[] invalidIndices = {-1, -100, Integer.MIN_VALUE};
    private static final Integer[] validTypes = validIndices; // just happens to be the same.
    private static final Integer[] invalidTypes = invalidIndices; // just happens to be the same.
    private static final TraderState[] validStates = TraderState.values();
    private static final Basket EMPTY = new Basket();
    private static final Basket[] validBaskets = {
        EMPTY,
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

    @Test // That the returned map indeed implements all operations.
    public final void testGetMarketsAsBuyer() {
        MapTests.verifyModifiable(fixture_.getMarketsAsBuyer(), new ShoppingList(fixture_,EMPTY),
                                  new Market(EMPTY));
    }

    @Test
    public final void testGetMarketsAsSeller() {
        ListTests.verifyModifiable(fixture_.getMarketsAsSeller(), new Market(EMPTY));
    }

    // Tests for Trader methods

    @Test
    public final void testGetCommoditiesSold_ValidOperations() {
        ListTests.verifyUnmodifiableValidOperations(fixture_.getCommoditiesSold(), new CommoditySoldWithSettings());
    }

    @Test
    public final void testGetCommoditiesSold_InvalidOperations() {
        ListTests.verifyUnmodifiableInvalidOperations(fixture_.getCommoditiesSold(), new CommoditySoldWithSettings());
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
            {new Basket(A,B,C),new CommoditySpecification(0),3},
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
            {new Basket(A,B),new CommoditySpecification(0),1},
            {new Basket(A,B),A,1},
            {new Basket(A,B),B,1},
            {new Basket(A,B),C,2},
            {new Basket(A,B,C),A,2},
            {new Basket(A,B,C),B,2},
            {new Basket(A,B,C),C,2},
            {new Basket(A,B,C),new CommoditySpecification(0),2},
        };
    }

    @Test
    public final void testGetCliques() {
        SetTests.verifyUnmodifiableValidOperations(fixture_.getCliques(), 42L);
        SetTests.verifyUnmodifiableInvalidOperations(fixture_.getCliques(), 42L);
    }

    @Test
    public final void testGetModifiableCliques() {
        SetTests.verifyModifiable(fixture_.getModifiableCliques(), 42L);
    }

    @Test
    public final void testGetCustomers() {
        ListTests.verifyUnmodifiableValidOperations(fixture_.getCustomers(), new ShoppingList(fixture_,EMPTY));
        ListTests.verifyUnmodifiableInvalidOperations(fixture_.getCustomers(), new ShoppingList(fixture_,EMPTY));
    }

    @Test
    public final void testGetCustomersOfMarket() {
        Basket b = new Basket();
        TraderWithSettings t1 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl1 = new ShoppingList(t1, b);
        TraderWithSettings t2 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl2 = new ShoppingList(t2, b);
        TraderWithSettings t3 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl3 = new ShoppingList(t3, b);
        // Sl1 and Sl2 are buyers in Market
        Market m = new Market(new Basket());
        m.addBuyer(t1, sl1);
        m.addBuyer(t2, sl2);
        // Sl1 and Sl3 are customers of fixture
        fixture_.getModifiableCustomers().add(sl1);
        fixture_.getModifiableCustomers().add(sl3);

        Set<ShoppingList> customersInMarket = fixture_.getCustomers(m);

        assertEquals(1, customersInMarket.size());
        assertEquals(sl1, customersInMarket.iterator().next());
    }

    @Test
    public final void testGetModifiableCustomers() {
        ListTests.verifyModifiable(fixture_.getModifiableCustomers(), new ShoppingList(fixture_,EMPTY));
    }

    @Test
    public final void testGetUniqueCustomers() {
        CollectionTests.verifyUnmodifiableValidOperations(fixture_.getUniqueCustomers(), fixture_);
        CollectionTests.verifyUnmodifiableInvalidOperations(fixture_.getUniqueCustomers(), fixture_);
    }

    @Test
    public final void testGetSettings() {
        assertSame(fixture_, fixture_.getSettings());
    }

    @Test
    public void testClearShoppingAndMarketData() {
        Basket b = new Basket();
        TraderWithSettings t1 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl1 = new ShoppingList(t1, b);
        TraderWithSettings t2 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl2 = new ShoppingList(t2, b);
        TraderWithSettings t3 = new TraderWithSettings(0, 0, TraderState.ACTIVE, b);
        ShoppingList sl3 = new ShoppingList(t3, b);
        // Sl1 and Sl2 are buyers in Market
        Market m = new Market(new Basket());
        m.addBuyer(t1, sl1);
        m.addBuyer(t2, sl2);

        t1.getMarketsAsBuyer().put(sl1, m);
        t3.getMarketsAsSeller().add(m);

        assertEquals(1, t1.getMarketsAsBuyer().size());
        assertEquals(1, t3.getMarketsAsSeller().size());

        t1.clearShoppingAndMarketData();
        t3.clearShoppingAndMarketData();

        assertEquals(0, t1.getMarketsAsBuyer().size());
        assertEquals(0, t3.getMarketsAsSeller().size());
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
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)GuranteedBuyer({0})")
    public final void testIsSetGuranteedBuyer(boolean guranteedBuyer) {
        fixture_.setGuaranteedBuyer(guranteedBuyer);
        assertEquals(guranteedBuyer, fixture_.isGuaranteedBuyer());
    }

    @Test
    @Parameters({"true", "false"})
    @TestCaseName("Test #{index}: setCanAcceptNewCustomers({0})")
    public final void testSetCanAcceptNewCustomers(boolean canAcceptNewCustomers) {
        fixture_.setCanAcceptNewCustomers(canAcceptNewCustomers);
        assertEquals(canAcceptNewCustomers, fixture_.canAcceptNewCustomers());
    }

    @Test
    @Parameters({"0.0,0.0","0.0,0.5","0.0,1.0",
                 "0.5,0.5","0.5,1.0",
                 "1.0,1.0"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredUtil({1}) while minDesiredUtilization == {0}")
    public final void testGetSetMaxDesiredUtil_NormalInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture_.setMinDesiredUtil(minDesiredUtilization);
        fixture_.setMaxDesiredUtil(maxDesiredUtilization);
        assertEquals(maxDesiredUtilization, fixture_.getMaxDesiredUtil(), TestUtils.FLOATING_POINT_DELTA);
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
        assertEquals(minDesiredUtilization, fixture_.getMinDesiredUtil(), TestUtils.FLOATING_POINT_DELTA);
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

    @Test
    public void testTraderOidSet() {
        final TraderWithSettings trader = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket());
        assertFalse(trader.isOidSet());
        trader.setOid(123L);
        assertTrue(trader.isOidSet());
    }
} // end class TraderWithSettingsTest
