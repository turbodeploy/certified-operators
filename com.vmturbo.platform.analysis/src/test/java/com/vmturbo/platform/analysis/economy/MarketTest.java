package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Market} class.
 */
@RunWith(JUnitParamsRunner.class)
public class MarketTest {

    // Fields

    private static final CommoditySpecification A = new CommoditySpecification((short)0,0,0);
    private static final CommoditySpecification B = new CommoditySpecification((short)0,0,100);
    private static final CommoditySpecification C = new CommoditySpecification((short)1,0,100);
    private static final Basket[] baskets = {
        new Basket(),
        new Basket(A),
        new Basket(A,B),
        new Basket(A,B,C)
    };

    private Market fixture_;

    // Methods

    @Before
    public void setUp() {
        fixture_ = new Market(new Basket());
    }


    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Basket({0}).getBasket() == {0}")
    public final void testMarket_GetBasket(@NonNull Basket basket) {
        assertSame(basket, new Market(basket).getBasket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMarket_GetBasket() {
        return baskets;
    }

    @Test // That the returned list is indeed unmodifiable (part 1)
    public final void testGetSellers_ValidOperations() {
        @NonNull @ReadOnly List<@NonNull Trader> sellers = fixture_.getSellers();
        assertFalse(sellers.contains(null));
        assertFalse(sellers.containsAll(Arrays.asList(null,null)));
        assertTrue(sellers.equals(sellers));
        assertEquals(-1, sellers.indexOf(null));
        assertTrue(sellers.isEmpty());
        assertNotNull(sellers.iterator());
        assertEquals(-1, sellers.lastIndexOf(null));
        assertNotNull(sellers.listIterator());
        assertEquals(0, sellers.size());
        assertNotNull(sellers.toArray());
    }

    @Test // That the returned list is indeed unmodifiable (part 2)
    public final void testGetSellers_InvalidOperations() {
        @NonNull @ReadOnly List<@NonNull Trader> sellers = fixture_.getSellers();
        @NonNull Trader seller = new TraderWithSettings(0,0,TraderState.ACTIVE,new Basket()); // dummy object

        // TODO: may also need to test these on a non-empty buyers list because the API does not
        // guarantee that this exception will be thrown in some cases.
        try{
            sellers.add(seller);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.add(0,seller);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.addAll(Arrays.asList(seller,seller));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.addAll(0,Arrays.asList(seller,seller));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.remove(0);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.remove(seller);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.removeAll(sellers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.retainAll(sellers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            sellers.set(0, seller);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

    @Test
    @Ignore
    public final void testAddSeller() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testRemoveSeller() {
        fail("Not yet implemented");// TODO
    }

    @Test // That the returned list is indeed unmodifiable (part 1)
    public final void testGetBuyers_ValidOperations() {
        @NonNull @ReadOnly List<@NonNull BuyerParticipation> buyers = fixture_.getBuyers();
        assertFalse(buyers.contains(null));
        assertFalse(buyers.containsAll(Arrays.asList(null,null)));
        assertTrue(buyers.equals(buyers));
        assertEquals(-1, buyers.indexOf(null));
        assertTrue(buyers.isEmpty());
        assertNotNull(buyers.iterator());
        assertEquals(-1, buyers.lastIndexOf(null));
        assertNotNull(buyers.listIterator());
        assertEquals(0, buyers.size());
        assertNotNull(buyers.toArray());
    }

    @Test // That the returned list is indeed unmodifiable (part 2)
    public final void testGetBuyers_InvalidOperations() {
        @NonNull @ReadOnly List<@NonNull BuyerParticipation> buyers = fixture_.getBuyers();
        @NonNull BuyerParticipation participation = new BuyerParticipation(0, 0, 0); // dummy object

        // TODO: may also need to test these on a non-empty buyers list because the API does not
        // guarantee that this exception will be thrown in some cases.
        try{
            buyers.add(participation);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.add(0,participation);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.addAll(Arrays.asList(participation,participation));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.addAll(0,Arrays.asList(participation,participation));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.remove(0);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.remove(participation);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.removeAll(buyers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.retainAll(buyers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.set(0, participation);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

    @Test
    @Ignore
    public final void testAddBuyer() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testRemoveBuyerParticipation() {
        fail("Not yet implemented");// TODO
    }

} // end MarketTest class
