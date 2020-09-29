package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import com.vmturbo.platform.analysis.ede.Provision;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link ActionClassifier} interface.
 */
public class ExecutableActionsTest {
    static final Logger logger = LogManager.getLogger(ExecutableActionsTest.class);

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

        Basket BASKET_1 = new Basket(new CommoditySpecification(0, false),
                                     new CommoditySpecification(1, false));
        Basket BASKET_2 = new Basket(new CommoditySpecification(2, false));
        Basket BASKET_3 = new Basket(new CommoditySpecification(0, false),
                                     new CommoditySpecification(1, false),
                                     new CommoditySpecification(2, false));

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
            classifier.classify(actions, economy);
            // PS:  If there's another of doing this, I don't need to modify ActionClassifier ..
            assertTrue(classifier.getExecutable() == 1);

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
        commSpecSold.add(new CommoditySpecification(0, false));
        commSpecSold.add(new CommoditySpecification(1, false));
        commSpecSold.add(new CommoditySpecification(2, false));

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
            classifier.classify(actions, economy);
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
    public final void testMarkResizeUpExecutable_Action() {
        Economy economy = new Economy();
        List<Action> actions = new ArrayList<>();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, false));
        commSpecSold.add(new CommoditySpecification(1, false));
        commSpecSold.add(new CommoditySpecification(2, false));

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
            classifier.classify(actions, economy);
            // resize up executable, resize down executable
            assertTrue(actions.size() == 1);
            assertTrue(classifier.getExecutable() == 1);
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
            suspension.takeActionAndUpdateLedger(economy, market.getBasket(), ledger, p1,
                                  actions);
        }
        logger.info(actions.size());

        ActionClassifier classifier;
        try {
            classifier = new ActionClassifier(economy);
            classifier.classify(actions, economy);
            // resize up non-executable, resize down executable
            assertTrue(actions.size() == 1);
            assertTrue(classifier.getExecutable() == 0);
            logger.info(classifier.getExecutable());
        } catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }

    /**
     * Test the effect of classifying provision actions generated by provider must clone logic.
     *
     * <p>Service buy 1500 TRANSACTION from App (capacity: 150)
     * App buy 224 VCPU from Container (capacity: 256)
     * Container buy 8192 VMEM from Pod (capacity: 65536)
     * Pod buy 256 VCPU_REQUEST from Node (capacity: 1024)
     *
     * <p>After provision, there should only be two provisioned pods on the existing node.
     * The rest of the provisioned pods should be on cloned nodes, and these provision actions
     * should be marked as non-executable.
     */
    @Test
    public final void testMarketProviderMustCloneExecutableAction() {
        Economy economy = new Economy();
        // One service sells nothing
        Trader service = TestUtils.createTrader(economy, TestUtils.SERVICE_TYPE, Collections.singletonList(0L),
                Collections.emptyList(), new double[]{}, true, true);
        service.setDebugInfoNeverUseInCode("service");
        // One application sells transaction with 150 capacity
        Trader application = TestUtils.createTrader(economy, TestUtils.APP_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.TRANSACTION),
                new double[]{150}, true, false);
        application.getSettings()
                .setProviderMustClone(true)
                .setMaxDesiredUtil(0.75)
                .setMaxDesiredUtil(0.65);
        application.setDebugInfoNeverUseInCode("application");
        // One container sells VCPU with 256 capacity
        Trader container = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU),
                new double[]{256}, true, false);
        container.getSettings().setProviderMustClone(true);
        container.setDebugInfoNeverUseInCode("container");
        // One container pod sells VMEM with 65536 capacity
        Trader containerPod = TestUtils.createTrader(economy, TestUtils.CONTAINER_POD_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VMEM),
                new double[]{65536}, true, false);
        containerPod.setDebugInfoNeverUseInCode("containerPod");
        // One node sells VCPU_REQUEST with 1024 capacity
        Trader node = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU_REQUEST),
                new double[]{1024}, true, false);
        node.setDebugInfoNeverUseInCode("node");
        // The service buys 1500 transaction from application
        Basket basketServiceApplication = new Basket(TestUtils.TRANSACTION);
        ShoppingList slServiceApplication = economy.addBasketBought(service, basketServiceApplication);
        TestUtils.moveSlOnSupplier(economy, slServiceApplication, application, new double[]{1500});
        // The application buys 224 VCPU from container
        Basket basketApplicationContainer = new Basket(TestUtils.VCPU);
        ShoppingList slApplicationContainer = economy.addBasketBought(application, basketApplicationContainer);
        TestUtils.moveSlOnSupplier(economy, slApplicationContainer, container, new double[]{224});
        // The container buys 8192 VMEM from container pod
        Basket basketContainerContainerPod = new Basket(TestUtils.VMEM);
        ShoppingList slContainerContainerPod = economy.addBasketBought(container, basketContainerContainerPod);
        TestUtils.moveSlOnSupplier(economy, slContainerContainerPod, containerPod, new double[]{8192});
        // The container pod buys 256 VCPU_REQUEST from node
        Basket basketContainerPodNode = new Basket(TestUtils.VCPU_REQUEST);
        ShoppingList slContainerPodNode = economy.addBasketBought(containerPod, basketContainerPodNode);
        TestUtils.moveSlOnSupplier(economy, slContainerPodNode, node, new double[]{256});
        try {
            ActionClassifier classifier;
            classifier = new ActionClassifier(economy);
            // Populate markets
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            // Provision
            Ledger ledger = new Ledger(economy);
            List<Action> actions = Provision.provisionDecisions(economy, ledger);
            // Get the executable pod actions before action classification
            List<Action> executablePodClonesBeforeClassifying = actions.stream()
                    .filter(ProvisionBase.class::isInstance)
                    .filter(a -> a.getActionTarget().getType() == TestUtils.CONTAINER_POD_TYPE)
                    .filter(Action::isExecutable).collect(Collectors.toList());
            // The number of clones may change due to different stop criteria, but should be at least
            // greater than 2
            assertTrue(executablePodClonesBeforeClassifying.size() > 2);
            // Classify the actions
            classifier.classify(actions, economy);
            // Get the executable pod actions after action classification
            List<Action> executablePodClonesAfterClassifying = actions.stream()
                    .filter(ProvisionBase.class::isInstance)
                    .filter(a -> a.getActionTarget().getType() == TestUtils.CONTAINER_POD_TYPE)
                    .filter(Action::isExecutable).collect(Collectors.toList());
            assertEquals(2, executablePodClonesAfterClassifying.size());
        } catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }

    /**
     * Test the effect of classifying provision actions generated by provider must clone logic
     * after move actions were generated.
     *
     * <p>Service buy 1500 TRANSACTION from App (capacity: 150)
     * App buy 224 VCPU from Container (capacity: 256)
     * Container buy 8192 VMEM from Pod (capacity: 65536)
     * Pod buy 256 VCPU_REQUEST from Node (capacity: 1025)
     * Pod1 buy 64 VCPU_REQUEST from Node
     * Node1 sells VCPU_REQUEST (capacity: 128)
     *
     * <p>In the beginning, Pod (256) and Pod1 (64) are on Node: (256 + 64 = 320).
     * After moving Pod1 from Node to Node1: Node (256), Node1 (64).
     * After provision, three Pods are cloned on Node: (256 + 256 x 3 = 1024), no cloned pod is on
     * Node1 (it doesn't fit), and the rest of the cloned pods are on cloned nodes which are all
     * marked as non-executable. For the three cloned pods on Node, only two are executable because
     * the Move of Pod1 will not adjust the quantity on Node.
     */
    @Test
    public final void testMarketProviderMustCloneAfterMoveExecutableAction() {
        Economy economy = new Economy();
        // One service sells nothing
        Trader service = TestUtils.createTrader(economy, TestUtils.SERVICE_TYPE, Collections.singletonList(0L),
                Collections.emptyList(), new double[]{}, true, true)
                .setDebugInfoNeverUseInCode("service");
        // One application sells transaction with 150 capacity
        Trader application = TestUtils.createTrader(economy, TestUtils.APP_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.TRANSACTION),
                new double[]{150}, true, false)
                .setDebugInfoNeverUseInCode("application");
        application.getSettings()
                .setProviderMustClone(true)
                .setMaxDesiredUtil(0.75)
                .setMaxDesiredUtil(0.65);
        // One container sells VCPU with 256 capacity
        Trader container = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU),
                new double[]{256}, true, false)
                .setDebugInfoNeverUseInCode("container");
        container.getSettings().setProviderMustClone(true);
        // One container pod sells VMEM with 65536 capacity
        Trader containerPod = TestUtils.createTrader(economy, TestUtils.CONTAINER_POD_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VMEM),
                new double[]{65536}, true, false)
                .setDebugInfoNeverUseInCode("containerPod");
        // One node sells VCPU_REQUEST with 1024 capacity
        Trader node = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU_REQUEST),
                new double[]{1025}, true, false)
                .setDebugInfoNeverUseInCode("node");
        // One container pod sells APPLICATION with 65536 capacity
        Trader containerPod1 = TestUtils.createTrader(economy, TestUtils.CONTAINER_POD_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.APPLICATION),
                new double[]{65536}, true, false)
                .setDebugInfoNeverUseInCode("containerPod1");
        // One node sells VCPU_REQUEST with 1024 capacity
        Trader node1 = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU_REQUEST),
                new double[]{128}, true, false)
                .setDebugInfoNeverUseInCode("node1");
        // The service buys 1500 transaction from application
        Basket basketServiceApplication = new Basket(TestUtils.TRANSACTION);
        ShoppingList slServiceApplication = economy.addBasketBought(service, basketServiceApplication);
        TestUtils.moveSlOnSupplier(economy, slServiceApplication, application, new double[]{1500});
        // The application buys 224 VCPU from container
        Basket basketApplicationContainer = new Basket(TestUtils.VCPU);
        ShoppingList slApplicationContainer = economy.addBasketBought(application, basketApplicationContainer);
        TestUtils.moveSlOnSupplier(economy, slApplicationContainer, container, new double[]{224});
        // The container buys 8192 VMEM from container pod
        Basket basketContainerContainerPod = new Basket(TestUtils.VMEM);
        ShoppingList slContainerContainerPod = economy.addBasketBought(container, basketContainerContainerPod);
        TestUtils.moveSlOnSupplier(economy, slContainerContainerPod, containerPod, new double[]{8192});
        // The container pod buys 256 VCPU_REQUEST from node
        Basket basketContainerPodNode = new Basket(TestUtils.VCPU_REQUEST);
        ShoppingList slContainerPodNode = economy.addBasketBought(containerPod, basketContainerPodNode);
        TestUtils.moveSlOnSupplier(economy, slContainerPodNode, node, new double[]{256});
        // The container pod1 buys 256 VCPU_REQUEST from node
        Basket basketContainerPodNode1 = new Basket(TestUtils.VCPU_REQUEST);
        ShoppingList slContainerPodNode1 = economy.addBasketBought(containerPod1, basketContainerPodNode1);
        TestUtils.moveSlOnSupplier(economy, slContainerPodNode1, node, new double[]{64});
        try {
            ActionClassifier classifier;
            classifier = new ActionClassifier(economy);
            // Populate markets
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            List<Action> actions = new ArrayList<>();
            // Move containerPod1 from node to node1
            Move move = new Move(economy, slContainerPodNode1, node, node1);
            actions.add(move.take());
            // Provision
            Ledger ledger = new Ledger(economy);
            actions.addAll(Provision.provisionDecisions(economy, ledger));
            // Get the executable pod actions before action classification
            List<Action> executablePodClonesBeforeClassifying = actions.stream()
                    .filter(ProvisionBase.class::isInstance)
                    .filter(a -> a.getActionTarget().getType() == TestUtils.CONTAINER_POD_TYPE)
                    .filter(Action::isExecutable)
                    .collect(Collectors.toList());
            // The number of clones may change due to different stop criteria, but should be at least
            // greater than 2
            assertTrue(executablePodClonesBeforeClassifying.size() > 2);
            // Classify the actions
            classifier.classify(actions, economy);
            // Get the executable pod clone actions after action classification
            List<Action> executablePodClonesAfterClassifying = actions.stream()
                    .filter(ProvisionBase.class::isInstance)
                    .filter(a -> a.getActionTarget().getType() == TestUtils.CONTAINER_POD_TYPE)
                    .filter(Action::isExecutable)
                    .collect(Collectors.toList());
            // Confirm we only have 2 clones that are executable
            assertEquals(2, executablePodClonesAfterClassifying.size());
            // Get the executable pod move actions
            List<Action> executablePodMoveAfterClassifying = actions.stream()
                    .filter(MoveBase.class::isInstance)
                    .filter(a -> a.getActionTarget().getType() == TestUtils.CONTAINER_POD_TYPE)
                    .filter(Action::isExecutable)
                    .collect(Collectors.toList());
            // Confirm we have 1 pod move action that is executable
            assertEquals(1, executablePodMoveAfterClassifying.size());
        } catch (Exception e) {
            logger.error("Error classifying actions .." + e);
        }
    }
}
