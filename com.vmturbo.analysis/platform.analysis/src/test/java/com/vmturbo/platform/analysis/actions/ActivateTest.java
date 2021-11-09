package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utilities.exceptions.ActionCantReplayException;

/**
 * A test case for the {@link Activate} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ActivateTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Expected exception
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3})")
    public final void testActivate(@NonNull Economy economy, @NonNull Trader target,
               @NonNull Basket triggeringBasket, @NonNull Trader modelSeller, boolean unusedFlag) {
        @NonNull Activate activation =
            new Activate(economy, target, triggeringBasket, modelSeller, TestUtils.CPU);

        assertSame(target, activation.getTarget());
        assertSame(triggeringBasket, activation.getTriggeringBasket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestActivate() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t4 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][] {
            {e1, t1, EMPTY, t3, false},
            {e1, t2, EMPTY, t4, true}
        };
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithGuaranteedBuyer() {
        Economy e1 = new Economy();
        Basket basket = new Basket(new CommoditySpecification(0));
        Trader t1 = e1.addTrader(0, TraderState.INACTIVE, basket, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.ACTIVE, basket, EMPTY);

        //t3 is the model seller
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, basket, EMPTY);

        Trader b1 = e1.addTrader(0, TraderState.ACTIVE, basket, basket);
        b1.getSettings().setGuaranteedBuyer(true);
        //b1 is a guaranteedBuyer for t2 and t3
        ShoppingList s2 = e1.addBasketBought(b1, basket);
        s2.move(t2);
        ShoppingList s3 = e1.addBasketBought(b1, basket);
        s3.move(t3);

        return new Object[][]{
            {e1, t1, EMPTY, t3, b1, true},
            {e1, t2, EMPTY, t3, b1, false}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Activate activation,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(activation.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t4 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        oids.put(t1, "id1");
        oids.put(t2, "id2");

        return new Object[][]{
            {new Activate(e1, t1, EMPTY, t3, TestUtils.CPU),
                oid, "<action type=\"activate\" target=\"id1\" />"},
            {new Activate(e1, t2, EMPTY, t4, TestUtils.CPU),
                oid, "<action type=\"activate\" target=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3}).take()  throw == {4}")
    public final void testTake(@NonNull Economy economy, @NonNull Trader target,
                    @NonNull Basket triggeringBasket, @NonNull Trader modelSeller, boolean valid) {
        @NonNull Activate activation =
            new Activate(economy, target, triggeringBasket, modelSeller, TestUtils.CPU);

        try {
            assertSame(activation, activation.take());
            assertTrue(valid);
        } catch (IllegalStateException e){
            assertFalse(valid);
        }
        assertTrue(target.getState().isActive());
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3}).rollback()  throw == {4}")
    public final void testRollback(@NonNull Economy economy, @NonNull Trader target,
            @NonNull Basket triggeringBasket, @NonNull Trader modelSeller, boolean invalid) {
        @NonNull Activate activation =
            new Activate(economy, target, triggeringBasket, modelSeller, TestUtils.CPU);
        // mock the actionTaken flag as if it is being taken
        try {
            Field actionTakenField = ActionImpl.class.getDeclaredField("actionTaken");
            actionTakenField.setAccessible(true);
            actionTakenField.setBoolean(activation, true);
        } catch (Exception e) {
            fail();
        }
        try {
            assertSame(activation, activation.rollback());
            assertFalse(invalid);
        } catch (IllegalStateException e){
            assertTrue(invalid);
        }
        assertFalse(target.getState().isActive());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, activation.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Economy e1 = new Economy();
        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));
        Trader t3 = topology1.addTrader("id3","name3","type3",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t3, Arrays.asList());
        Trader t4 = topology1.addTrader("id4","name4","type4",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t4, Arrays.asList("b"));

        return new Object[][]{
            {new Activate(e1, t1, EMPTY, t3, TestUtils.CPU),
                topology1, "Activate name1 [id1] (#0)."},
            {new Activate(e1, t2, EMPTY, t4, TestUtils.CPU),
                topology1, "Activate name2 [id2] (#1)."},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, activation.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Economy e1 = new Economy();
        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));
        Trader t3 = topology1.addTrader("id3","name3","type3",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t3, Arrays.asList());
        Trader t4 = topology1.addTrader("id4","name4","type4",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t4, Arrays.asList("b"));

        return new Object[][]{
            {new Activate(e1, t1, EMPTY, t3, TestUtils.CPU),
                topology1, "To satisfy increased demand for []."},
            {new Activate(e1, t2, EMPTY, t4, TestUtils.CPU),
                topology1, "To satisfy increased demand for []."},
        };
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestEquals_and_HashCode() {
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Basket b3 = new Basket();
        // t1 t2 are two different traders with same contents, while t3 is another different
        // trader with different contents
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t2 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t3 = e.addTrader(0, TraderState.INACTIVE, b2, b3);
        Trader t4 = e.addTrader(0, TraderState.ACTIVE, b2, b3);

        Activate activate1 = new Activate(e, t3, b2, t4, TestUtils.CPU);
        Activate activate2 = new Activate(e, t3, b2, t4, TestUtils.CPU);
        Activate activate3 = new Activate(e, t4, b2, t3, TestUtils.CPU);
        Activate activate4 = new Activate(e, t4, b2, t4, TestUtils.CPU);
        return new Object[][]{
            {activate1, activate2, true},
            {activate1, activate3, false},
            {activate1, activate4, false}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull Activate activate1,
                    @NonNull Activate activate2, boolean expect) {
        assertEquals(expect, activate1.equals(activate2));
        assertEquals(expect, activate1.hashCode() == activate2.hashCode());
    }

    /**
     * Test extractCommodityIds and createActionWithNewCommodityId.
     *
     * @throws ActionCantReplayException ActionCantReplayException
     */
    @Test
    public void testCreateActionWithNewCommodityId() throws ActionCantReplayException {
        final Basket triggeringBasket = new Basket(
            new CommoditySpecification(1, 2),
            new CommoditySpecification(3, 4));
        final CommoditySpecification reasonCommodity = new CommoditySpecification(5, 6);
        final Activate activate = new Activate(new Economy(), mock(TraderWithSettings.class), triggeringBasket,
            mock(TraderWithSettings.class), reasonCommodity);

        assertEquals(ImmutableSet.of(1, 3, 5), activate.extractCommodityIds());

        // Create mapping
        final Int2IntOpenHashMap commodityIdMapping = new Int2IntOpenHashMap();
        commodityIdMapping.defaultReturnValue(NumericIDAllocator.nonAllocableId);
        commodityIdMapping.put(1, 11);
        commodityIdMapping.put(3, 33);
        commodityIdMapping.put(5, 55);

        final Activate newActivate = activate.createActionWithNewCommodityId(commodityIdMapping);
        assertEquals(new Basket(
                new CommoditySpecification(11, 2),
                new CommoditySpecification(33, 4)),
            newActivate.getTriggeringBasket());
        assertEquals(new CommoditySpecification(55, 6), newActivate.getReason());
    }

    /**
     * Test createActionWithNewCommodityId thorws an exception.
     *
     * @throws ActionCantReplayException ActionCantReplayException
     */
    @Test
    public void testCreateActionWithNewCommodityIdThrowException() throws ActionCantReplayException {
        expectedException.expect(ActionCantReplayException.class);

        final Basket triggeringBasket = new Basket(
            new CommoditySpecification(1, 2),
            new CommoditySpecification(3, 4));
        final CommoditySpecification reasonCommodity = new CommoditySpecification(5, 6);
        final Activate activate = new Activate(new Economy(), mock(TraderWithSettings.class), triggeringBasket,
            mock(TraderWithSettings.class), reasonCommodity);

        assertEquals(ImmutableSet.of(1, 3, 5), activate.extractCommodityIds());

        // Create mapping
        final Int2IntOpenHashMap commodityIdMapping = new Int2IntOpenHashMap();
        commodityIdMapping.defaultReturnValue(NumericIDAllocator.nonAllocableId);
        commodityIdMapping.put(1, 11);
        commodityIdMapping.put(3, 33);

        // Throws an exception
        activate.createActionWithNewCommodityId(commodityIdMapping);
    }
} // end ActivateTest class
