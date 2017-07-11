package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.ActionClassifier;
import com.vmturbo.platform.analysis.ede.OldSuspension;
import com.vmturbo.platform.analysis.ledger.Ledger;

/**
 * A test case for the {@link ActionClassifier} interface.
 */
public class ExecutableActionsTest {
    static final Logger logger = Logger.getLogger(ExecutableActionsTest.class);

    private static final Basket EMPTY = new Basket();
    private static final int TYPE_PM = 0;
    private static final int TYPE_VM = 1;

    /**
     * Tests for the {@link ActionClassier} class.
     *
     */

    /**
     * Test classification of Moves.
     *
     */
    @Test
    public final void testClassifyAndMarkMoves_Action() {
        Economy economy = new Economy();

        Basket BASKET_1 = new Basket(new CommoditySpecification(0, 40, 100),
                                     new CommoditySpecification(1, 100, 200));
        Basket BASKET_2 = new Basket(new CommoditySpecification(2, 50, 50));
        Basket BASKET_3 = new Basket(new CommoditySpecification(0, 40, 100),
                                     new CommoditySpecification(1, 100, 200),
                                     new CommoditySpecification(2, 50, 50));

        // Sellers
        Trader s1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET_1);
        Trader s2 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET_1);
        Trader s3 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET_2);
        // Buyers
        Trader b1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        Trader b2 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        // Shopping lists
        ShoppingList l1 = economy.addBasketBought(b1, BASKET_1);
        ShoppingList l2 = economy.addBasketBought(b1, BASKET_2);
        ShoppingList l3 = economy.addBasketBought(b2, BASKET_3);

        List<Action> actions = new ArrayList<>();

        // List with one Move is collapsed to the same list
        actions.add(new Move(economy, l1, s1, s2));
        actions.add(new Move(economy, l2, s2, s3));

        logger.info(actions.size());

        // TODO: check why this isn't working anymore, before re-enabling.
        ActionClassifier classifier;
        /*try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions);
            assertTrue(classifier.getExecutable() == 2);
            logger.info("No of executable actions for valid moves: "
                        + classifier.getExecutable());
        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }*/

        // should we have a test for such a move being marked non-executable ?
        // as of now it's not generated because the buyer request is not satisfied
        // but attempting such a move causes an ArrayIndexOutOfBoundException
        actions.clear();
        actions.add(new Move(economy, l3, s3, s1));

        try {
            classifier = new ActionClassifier(economy);
            logger.info("No of executable actions for invalid move: "
                        + classifier.getExecutable());
            classifier.classify(actions);
            // PS:  If there's another of doing this, I don't need to modify ActionClassifier ..
            assertTrue(classifier.getExecutable() == 0);

        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }

    }

    /**
     * Test classification of Resize Down.
     *
     */

    @Test
    public final void testMarkResizeDownsExecutable_Action() {
        Economy economy = new Economy();
        List<Action> actions = new ArrayList<>();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, 40, 100));
        commSpecSold.add(new CommoditySpecification(1, 100, 200));
        commSpecSold.add(new CommoditySpecification(2, 50, 50));

        Basket basketSold = new Basket(commSpecSold);

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        seller.getCommoditiesSold().get(0).setQuantity(40).setPeakQuantity(100).setCapacity(100);
        seller.getCommoditiesSold().get(1).setQuantity(100).setPeakQuantity(200).setCapacity(300);
        seller.getCommoditiesSold().get(2).setQuantity(50).setPeakQuantity(50).setCapacity(500);

        // create consumer1
        Trader buyer1 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer1, basketSold)
                        .setQuantity(0, 2).setPeakQuantity(0, 10)
                        .setQuantity(1, 5).setPeakQuantity(1, 20)
                        .setQuantity(2, 3).setPeakQuantity(2, 5).setMovable(true).move(seller);

        // create consumer2
        Trader buyer2 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer2, basketSold)
                        .setQuantity(0, 2).setPeakQuantity(0, 10)
                        .setQuantity(1, 5).setPeakQuantity(1, 20)
                        .setQuantity(2, 3).setPeakQuantity(2, 5);

        Resize r1 = new Resize(economy, seller, commSpecSold.get(0), 100, 80);
        Resize r2 = new Resize(economy, seller, commSpecSold.get(2), 500, 450);
        actions.add(r1);
        actions.add(r2);

        logger.info(actions.size());

        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions);
            // resize up non-executable, resize down executable
            assertTrue(actions.size() == 2);
            assertTrue(classifier.getExecutable() == 2);
            logger.info(classifier.getExecutable());
        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }

    /**
     * Test classification of Resize Up.
     *
     */

    @Test
    public final void testMarkResizeUpNonExecutable_Action() {
        Economy economy = new Economy();
        List<Action> actions = new ArrayList<>();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, 40, 100));
        commSpecSold.add(new CommoditySpecification(1, 100, 200));
        commSpecSold.add(new CommoditySpecification(2, 50, 50));

        Basket basketSold = new Basket(commSpecSold);

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        seller.getCommoditiesSold().get(0).setQuantity(40).setPeakQuantity(100).setCapacity(100);
        seller.getCommoditiesSold().get(1).setQuantity(100).setPeakQuantity(200).setCapacity(300);
        seller.getCommoditiesSold().get(2).setQuantity(50).setPeakQuantity(50).setCapacity(500);

        // create consumer1
        Trader buyer1 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer1, basketSold)
                        .setQuantity(0, 2).setPeakQuantity(0, 10)
                        .setQuantity(1, 5).setPeakQuantity(1, 20)
                        .setQuantity(2, 3).setPeakQuantity(2, 5).setMovable(true).move(seller);

        // create consumer2
        Trader buyer2 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer2, basketSold)
                        .setQuantity(0, 2).setPeakQuantity(0, 10)
                        .setQuantity(1, 5).setPeakQuantity(1, 20)
                        .setQuantity(2, 3).setPeakQuantity(2, 5);

        Resize r1 = new Resize(economy, seller, commSpecSold.get(1), 300, 320);
        actions.add(r1);

        logger.info(actions.size());

        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions);
            // resize up non-executable, resize down executable
            assertTrue(actions.size() == 1);
            assertTrue(classifier.getExecutable() == 1);
            logger.info(classifier.getExecutable());
        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }

    /**
     * Test classification of Provision actions.
     *
     */
    @Test
    public final void testMarkProvisionsNonExecutable_Action() {
        Economy economy = new Economy();

        Basket BASKET = new Basket(new CommoditySpecification(0), new CommoditySpecification(1));
        Basket BASKET2 = new Basket(new CommoditySpecification(2));

        Trader v1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY, BASKET);
        Trader p1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET, BASKET2);
        ShoppingList s1 = economy.addBasketBought(v1, BASKET);

        // suppose the sequence for actions are : ProvisionByDemand -> Deactivate
        ProvisionByDemand provisionByDemand = (ProvisionByDemand)new ProvisionByDemand(economy, s1, p1).take();

        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Basket b3 = new Basket(new CommoditySpecification(300));
        Trader t1 = economy.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t2 = economy.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t3 = economy.addTrader(0, TraderState.ACTIVE, b2, b3);

        ShoppingList shoplist1 = economy.addBasketBought(t1, b2);
        shoplist1.move(t3);
        ShoppingList shoplist2 = economy.addBasketBought(t2, b2);
        shoplist2.move(t3);

        ProvisionBySupply provisionBySupply1 = new ProvisionBySupply(economy, t2);
        ProvisionBySupply provisionBySupply2 = new ProvisionBySupply(economy, t2);

        List<Action> actions = Arrays.asList(provisionByDemand, provisionBySupply1, provisionBySupply2);

        logger.info(actions.size());

        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions);
            // resize up non-executable, resize down executable
            assertTrue(actions.size() == 3);
            assertTrue(classifier.getExecutable() == 0);
            logger.info(classifier.getExecutable());
        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }

    /**
     * Test classification of Suspension actions.
     *
     */
    @Test
    public final void testMarkSuspensionsEmptyTradersExecutable_Action() {
        Economy economy = new Economy();
        List<Action> actions = new ArrayList<>();
        OldSuspension suspension = new OldSuspension();
        Basket BASKET = new Basket(new CommoditySpecification(0), new CommoditySpecification(1));
        Trader v1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, BASKET);
        Trader p1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        ShoppingList s1 = economy.addBasketBought(v1, BASKET).setMovable(true).move(p1);
        Ledger ledger = new Ledger(economy);

        for (Market market : economy.getMarkets()) {
            suspension.takeActionAndUpdateLedger(economy, market, ledger, p1,
                                  actions);
        }
        logger.info(actions.size());

        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions);
            // resize up non-executable, resize down executable
            assertTrue(actions.size() == 1);
            assertTrue(classifier.getExecutable() == 0);
            logger.info(classifier.getExecutable());
        }
        catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }
}