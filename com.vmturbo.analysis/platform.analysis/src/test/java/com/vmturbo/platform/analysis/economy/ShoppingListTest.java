package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link ShoppingList} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ShoppingListTest {
    // Fields
    private static final Basket EMPTY = new Basket();
    private static final Trader trader1 = new TraderWithSettings(0, 0, TraderState.ACTIVE, EMPTY);
    private static final Trader trader2 = new TraderWithSettings(0, 0, TraderState.INACTIVE, EMPTY);
    private static final Trader trader3 = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(new CommoditySpecification(0, 1000)));
    private static final Trader trader4 = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket(new CommoditySpecification(0, 1000)));

    private static final Trader[] validBuyers = {trader1, trader2, trader3, trader4};
    private static final Trader[] validSuppliers = {null,trader1, trader2, trader3, trader4};
    private static final Integer[] validSizes = {0,1,100};
    private static final Double[] validQuantities = {0.0,1.0,100.0};
    private static final Double[] invalidQuantities = {-0.1,-1.0,-100.0};
    private static final Integer[] validIndices = {0,1,9}; // with respect to fixture
    private static final Integer[] invalidIndices = {-1,Integer.MIN_VALUE,10,Integer.MAX_VALUE}; // with respect to fixture


    private ShoppingList fixture_;

    // Methods
    @Before
    public void setUp() {
        CommoditySpecification[] commodities = IntStream.range(0, 10).mapToObj(CommoditySpecification::new)
                .toArray(CommoditySpecification[]::new);
        Basket basket = new Basket(commodities);
        fixture_ = new ShoppingList(trader1, basket);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new ShoppingList({0},{1})")
    public final void testShoppingList_NormalInput(Trader buyer, int nCommodities) {
        CommoditySpecification[] commodities = IntStream.range(0, nCommodities).mapToObj(CommoditySpecification::new)
                .toArray(CommoditySpecification[]::new);
        Basket basket = new Basket(commodities);
        ShoppingList shoppingList = new ShoppingList(buyer, basket);
        assertSame(buyer, shoppingList.getBuyer());
        assertNotSame(shoppingList.getQuantities(), shoppingList.getPeakQuantities());
        assertEquals(nCommodities, shoppingList.getQuantities().length);
        assertEquals(nCommodities, shoppingList.getPeakQuantities().length);
    }

    /**
     * Test the move context on the shopping list.
     */
    @Test
    public final void testShoppingListMoveContext() {
        Context moveContext = Context.newBuilder().setRegionId(TestUtils.REGION_COMM_TYPE)
                .setBalanceAccount(BalanceAccountDTO.newBuilder().setId(20L).build()).build();
        fixture_.setContext(moveContext);
        Assert.assertEquals( 20L, fixture_.getContext().get().getBalanceAccount().getId());
        Assert.assertEquals(TestUtils.REGION_COMM_TYPE, fixture_.getContext().get().getRegionId());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestShoppingList_NormalInput() {
        Object[][] output = new Object[validBuyers.length*validSizes.length][];

        int c = 0;
        for(Trader buyer : validBuyers) {
            for(int size : validSizes) {
                output[c++] = new Object[]{buyer,size};
            }
        }

        return output;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)Supplier({0})")
    public final void testSetGetSupplier_NormalInput(Trader supplier) {
        assertSame(fixture_, fixture_.setSupplier(supplier));
        assertSame(supplier, fixture_.getSupplier());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetSupplier_NormalInput() {
        return validSuppliers;
    }

    @Test
    public final void testGetQuantities() {
        double increased = ++fixture_.getQuantities()[2];
        assertEquals(increased, fixture_.getQuantities()[2], TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    public final void testGetPeakQuantities() {
        double increased = ++fixture_.getPeakQuantities()[2];
        assertEquals(increased, fixture_.getPeakQuantities()[2], TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    public final void testGetDebugInfoNeverUseInCode() {
        fixture_.getBuyer().setDebugInfoNeverUseInCode("foo");

        assertEquals("SL_foo|" + fixture_.getShoppingListId(), fixture_.getDebugInfoNeverUseInCode());
    }

    @Test
    public final void testToString() {
        fixture_.getBuyer().setDebugInfoNeverUseInCode("foo");

        assertEquals("SL_foo|" + fixture_.getShoppingListId(), fixture_.toString());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)Quantity({0},{1})")
    public final void testSetGetQuantity_NormalInput(int index, double quantity) {
        assertSame(fixture_, fixture_.setQuantity(index, quantity));
        assertEquals(quantity, fixture_.getQuantity(index), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(quantity, fixture_.getQuantities()[index], TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetQuantity_NormalInput() {
        Number[][] output = new Number[validQuantities.length*validIndices.length][];

        int c = 0;
        for(Double quantity : validQuantities) {
            for(Integer index : validIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test
    @Parameters(method = "parametersForTestSetGetQuantity_NormalInput") // reuse inputs
    @TestCaseName("Test #{index}: (set|get)PeakQuantity({0},{1})")
    public final void testSetGetPeakQuantity_NormalInput(int index, double peakQuantity) {
        assertSame(fixture_, fixture_.setPeakQuantity(index, peakQuantity));
        assertEquals(peakQuantity, fixture_.getPeakQuantity(index), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(peakQuantity, fixture_.getPeakQuantities()[index], TestUtils.FLOATING_POINT_DELTA);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters
    @TestCaseName("Test #{index}: setQuantity({0},{1})")
    public final void testSetQuantity_InvalidIndex(int index, double quantity) {
        fixture_.setQuantity(index, quantity);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetQuantity_InvalidIndex() {
        Number[][] output = new Number[validQuantities.length*invalidIndices.length][];

        int c = 0;
        for(Double quantity : validQuantities) {
            for(Integer index : invalidIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: setQuantity({0},{1})")
    public final void testSetQuantity_InvalidQuantity(int index, double quantity) {
        fixture_.setQuantity(index, quantity);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetQuantity_InvalidQuantity() {
        Number[][] output = new Number[invalidQuantities.length*validIndices.length][];

        int c = 0;
        for(Double quantity : invalidQuantities) {
            for(Integer index : validIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters(method = "parametersForTestSetQuantity_InvalidIndex") // reuse inputs
    @TestCaseName("Test #{index}: setPeakQuantity({0},{1})")
    public final void testSetPeakQuantity_InvalidIndex(int index, double peakQuantity) {
        fixture_.setPeakQuantity(index, peakQuantity);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "parametersForTestSetQuantity_InvalidQuantity") // reuse inputs
    @TestCaseName("Test #{index}: setPeakQuantity({0},{1})")
    public final void testSetPeakQuantity_InvalidQuantity(int index, double peakQuantity) {
        fixture_.setPeakQuantity(index, peakQuantity);
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)Movable({0})")
    public final void testIsSetMovable(boolean movable) {
        fixture_.setMovable(movable);
        assertEquals(movable, fixture_.isMovable());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.move({1})")
    public final void testMove(@NonNull ShoppingList shoppingList, @Nullable TraderWithSettings newSupplier) {
        final @Nullable TraderWithSettings oldSupplier = (TraderWithSettings)shoppingList.getSupplier();
        final int oldSupplierSize = oldSupplier == null ? 0 : oldSupplier.getCustomers().size();
        final int newSupplierSize = newSupplier == null ? 0 : newSupplier.getCustomers().size();

        assertSame(shoppingList, shoppingList.move(newSupplier));
        assertSame(newSupplier, shoppingList.getSupplier());
        assertTrue(oldSupplier == null || !oldSupplier.getCustomers().contains(shoppingList));
        assertTrue(newSupplier == null || newSupplier.getCustomers().contains(shoppingList));

        if (oldSupplier != null)
            assertEquals(oldSupplierSize-1, oldSupplier.getCustomers().size());
        if (newSupplier != null)
            assertEquals(newSupplierSize+1, newSupplier.getCustomers().size());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMove() { // TODO: refactor to make more readable.
        List<Object[]> parameters = new ArrayList<>();
        Basket basket = new Basket(new CommoditySpecification(0));

        for (int i = 0 ; i < 4 ; ++i) {
            for (int j = 0 ; j < 4 ; ++j) {
                ShoppingList shoppingList = new ShoppingList(trader1, basket);

                if (i > 0) {
                    TraderWithSettings oldSupplier = new TraderWithSettings(0, 0, TraderState.ACTIVE, EMPTY);
                    shoppingList.setSupplier(oldSupplier);
                    oldSupplier.getModifiableCustomers().add(shoppingList);

                    if (i > 1) {
                        ShoppingList auxiliary = new ShoppingList(trader1, basket);
                        auxiliary.setSupplier(oldSupplier);
                        oldSupplier.getModifiableCustomers().add(i == 2 ? 0 : 1, auxiliary);
                    }
                }

                TraderWithSettings newSupplier = j == 0 ? null : new TraderWithSettings(0, 0, TraderState.ACTIVE, EMPTY);

                for (int k = 1 ; k < j ; ++k) {
                    ShoppingList auxiliary = new ShoppingList(trader1, basket);
                    auxiliary.setSupplier(newSupplier);
                    newSupplier.getModifiableCustomers().add(auxiliary);
                }

                parameters.add(new Object[]{shoppingList,newSupplier});
            }
        }

        return parameters.toArray();
    }

} // end ShoppingListTest class
