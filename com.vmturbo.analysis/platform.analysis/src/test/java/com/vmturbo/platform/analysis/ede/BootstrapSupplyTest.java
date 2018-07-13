package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * @author thiru_arun
 *
 */
public class BootstrapSupplyTest {

    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final int ST_TYPE = 2;
    List<Long> CLIQUE0 = Arrays.asList(0l);

    @Test
    public void testShopTogetherBootstrapWithEnoughSupply() {
        Economy economy = new Economy();
        // create two pms, one is smaller another is bigger in terms of cpu capacity
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(1l), 200, 100, false);
        // create two storages with same configuration except the clique
        // st1 only associates with pm1 and st2 only associates with pm2
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(1l), 300, false);
        // create a vm1 that is requesting higher cpu than its current supplier
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{150, 0}, pm1);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrapForIndividualBuyer(economy, vm1,
                                                                                new HashMap<>());

        assertTrue(bootStrapActionList.size() == 1);
        Action compoundMove = bootStrapActionList.get(0);
        assertEquals(ActionType.COMPOUND_MOVE, compoundMove.getType());
        Move expect1 = new Move(economy, sl1, pm1, pm2);
        Move expect2 = new Move(economy, sl2, st1, st2);
        List<Move> moves = ((CompoundMove)compoundMove).getConstituentMoves();
        assertTrue(moves.size() == 2);
        // order of moves in compound move is not deterministic
        assertTrue((expect1.equals(moves.get(0)) && expect2.equals(moves.get(1)))
                        || expect1.equals(moves.get(1)) && expect2.equals(moves.get(0)));
    }

    @Test
    public void testShopTogetherBootstrapWithNewSupply() {
        Economy economy = new Economy();
        // create one pm with smaller capacity
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        // create one storages with adequate capacity
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // create a vm that is requesting higher cpu than any seller in market
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{200, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrapForIndividualBuyer(economy, vm1,
                                                                                new HashMap<>());

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.PROVISION_BY_DEMAND, bootStrapActionList.get(0).getType());
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(1).getType());
        ProvisionByDemand provisionByDemand = (ProvisionByDemand)bootStrapActionList.get(0);
        List<Move> moves = ((CompoundMove)bootStrapActionList.get(1)).getConstituentMoves();
        assertEquals(sl1, provisionByDemand.getModelBuyer());
        assertEquals(pm1, provisionByDemand.getModelSeller());
        Move expect1 = new Move(economy, sl1, pm1, provisionByDemand.getProvisionedSeller());
        assertTrue(expect1.equals(moves.get(0)));
    }

    /**
     * Test cases where buyer fits in seller and buyer does not fit in seller.
     */
    @Test
    public void test_CanBuyerFitInSeller(){
        Economy economy = new Economy();
        // create one pm with smaller capacity than VM requirement
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(TestUtils.CPU, TestUtils.MEM),
                        new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(TestUtils.CPU)).setCapacity(40);
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(TestUtils.CPU, TestUtils.MEM));
        //Buyer does not fit in seller
        sl1.setQuantity(sl1.getBasket().indexOf(TestUtils.CPU), 50);
        assertFalse(BootstrapSupply.canBuyerFitInSeller(sl1, pm1, economy));
        //Buyer fits in seller
        sl1.setQuantity(sl1.getBasket().indexOf(TestUtils.CPU), 30);
        assertTrue(BootstrapSupply.canBuyerFitInSeller(sl1, pm1, economy));
    }

    /**
     * Non Shop together : Create 2 VMs and place them both on PM1.
     * VM1 CPU qty = 60, VM2 CPU qty = 60. PM1 total CPU = 100.
     * Expected result: Should move compute of one of the VMs to PM2.
     */
    @Test
    public void test_NonShopTogetherBootstrap_MoveWithAlreadyPlacedBuyer(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 200, 100, false);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 1);
        assertEquals(ActionType.MOVE, bootStrapActionList.get(0).getType());
        Move expectedMove1 = new Move(economy, sl1, pm1, pm2);
        Move expectedMove2 = new Move(economy, sl2, pm1, pm2);
        assertTrue(expectedMove1.equals(bootStrapActionList.get(0))
                        || expectedMove2.equals(bootStrapActionList.get(0)));
    }

    /**
     * Case: Non Shop together : Test move. Pm1 connected to St1.Vm1 not placed anywhere.
     * Expected result: Place VM on PM1 and ST1.
     */
    @Test
    public void test_NonShopTogetherBootstrap_MoveWithUnplacedBuyer(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, null);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.MOVE, bootStrapActionList.get(0).getType());
        assertEquals(ActionType.MOVE, bootStrapActionList.get(1).getType());
        Move expectedMove1 = new Move(economy, sl1, null, pm1);
        Move expectedMove2 = new Move(economy, sl2, null, st1);
        assertTrue(expectedMove1.equals(bootStrapActionList.get(0))
                        || expectedMove1.equals(bootStrapActionList.get(1)));
        assertTrue(expectedMove2.equals(bootStrapActionList.get(0))
                        || expectedMove2.equals(bootStrapActionList.get(1)));
    }

    /**
     * Case: Non Shop together : Pm1 connected to St1. Vm1 placed on pm1 and st1.
     * Vm2 not placed anywhere, and cannot fit in PM1 together with VM1
     * Expected result: Provision a new PM by supply
     * (because VM2's requirements are satisfied by PM1's total capacity)
     * and move Vm2 to that new machine.
     */
    @Test
    public void test_NonShopTogetherBootstrap_ProvisionBySupplyWithUnplacedBuyer(){
        boolean computeMoved = false, storageMoved = false, computeProvisioned = false;
        int computeMoveActionIndex = -1, provisionActionIndex = -1;
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy,
                        Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy,
                        Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        //Unplaced buyer
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, null);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertEquals(bootStrapActionList.size(), 3);
        Move expectedStorageMove = new Move(economy, sl4, null, st1);
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.stream()
                        .filter(action -> action.getType()
                        .equals(ActionType.PROVISION_BY_SUPPLY)).findFirst().get();
        Move expectedComputeMove = new Move(economy, sl3, null,
                        provisionBySupply.getProvisionedSeller());
        for(int i=0; i < bootStrapActionList.size();i++){
            Action bootstrapAction = bootStrapActionList.get(i);
            switch(bootstrapAction.getType()){
                case MOVE:
                    if(bootstrapAction.equals(expectedComputeMove)){
                        computeMoveActionIndex = i;
                        computeMoved = true;
                    }
                    if(bootstrapAction.equals(expectedStorageMove)){
                        storageMoved = true;
                    }
                    break;
                case PROVISION_BY_SUPPLY:
                    provisionActionIndex = i;
                    assertEquals(pm1, provisionBySupply.getModelSeller());
                    assertEquals(TestUtils.CPU, provisionBySupply.getReason());
                    computeProvisioned = true;
                    break;
                default:
                    fail("An action of type " + bootstrapAction.getType()
                        + " was found which is not expected.");
            }
        }
        //Assert that the compute and storage was moved
        assertTrue("Compute did not move.", computeMoved);
        assertTrue("Storage did not move.",storageMoved);
        assertTrue("Compute was not provisioned.",computeProvisioned);
        //Check that the compute was moved after provisioning it.
        assertTrue("Provision needs to happen before move, but did not.",
                        provisionActionIndex < computeMoveActionIndex);
    }

    /**
     * Case: Non Shop together : Pm1 connected to St1. Vm1 placed on pm1 and st1.
     * Vm2 not placed anywhere.
     * Expected result: Provision a new PM by demand
     * (because VM2's requirements are not satisfied by PM1's total capacity)
     * and move Vm2 to that new machine.
     */
    @Test
    public void test_NonShopTogetherBootstrap_ProvisionByDemandWithUnplacedBuyer(){
        boolean computeMoved = false, storageMoved = false, computeProvisioned = false;
        int computeMoveActionIndex = -1, provisionActionIndex = -1;
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        //Unplaced buyer
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{110, 0}, null);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertEquals(bootStrapActionList.size(), 3);
        Move expectedStorageMove = new Move(economy, sl4, null, st1);
        ProvisionByDemand provisionByDemand = (ProvisionByDemand)bootStrapActionList.stream()
                        .filter(action -> action.getType()
                        .equals(ActionType.PROVISION_BY_DEMAND)).findFirst().get();
        Move expectedComputeMove = new Move(economy, sl3, null,
                        provisionByDemand.getProvisionedSeller());
        for(int i=0; i < bootStrapActionList.size();i++){
            Action bootstrapAction = bootStrapActionList.get(i);
            switch(bootstrapAction.getType()){
                case MOVE:
                    if(bootstrapAction.equals(expectedComputeMove)){
                        computeMoveActionIndex = i;
                        computeMoved = true;
                    }
                    if(bootstrapAction.equals(expectedStorageMove)){
                        storageMoved = true;
                    }
                    break;
                case PROVISION_BY_DEMAND:
                    provisionActionIndex = i;
                    assertEquals(pm1, provisionByDemand.getModelSeller());
                    assertEquals(sl3, provisionByDemand.getModelBuyer());
                    computeProvisioned = true;
                    break;
                default:
                    fail("An action of type " + bootstrapAction.getType()
                        + " was found which is not expected.");
            }
        }
        //Assert that the compute and storage was moved
        assertTrue("Compute did not move.", computeMoved);
        assertTrue("Storage did not move.",storageMoved);
        assertTrue("Compute was not provisioned.",computeProvisioned);
        //Check that the compute was moved after provisioning it.
        assertTrue("Provision needs to happen before move, but did not.",
                        provisionActionIndex < computeMoveActionIndex);
    }

    /**
     * Shop together : Create 2 VMs and place them both on PM1 and DS1.
     * VM1 CPU qty = 60, VM2 CPU qty = 60. PM1 total CPU = 100.
     * Expected result: Should move compute of one of the VMs to PM2.
     */
    @Ignore
    @Test
    public void test_shopTogetherBootstrap_MoveWithAlreadyPlacedBuyer(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(1l), 200, 100, false);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(1l), 300, false);
        st2.setDebugInfoNeverUseInCode("DS2");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, pm1);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 1);
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(0).getType());
        Move expectedComputeMove1 = new Move(economy, sl1, pm1, pm2);
        Move expectedStorageMove1 = new Move(economy, sl2, st1, st2);
        Move expectedComputeMove2 = new Move(economy, sl3, pm1, pm2);
        Move expectedStorageMove2 = new Move(economy, sl4, st1, st2);
        List<Move> moves = ((CompoundMove)bootStrapActionList.get(0)).getConstituentMoves();
        assertTrue( (moves.get(0).equals(expectedComputeMove1) && moves.get(1).equals(expectedStorageMove1))
                       || (moves.get(0).equals(expectedStorageMove1) && moves.get(1).equals(expectedComputeMove1))
                       || (moves.get(0).equals(expectedComputeMove2) && moves.get(1).equals(expectedStorageMove2))
                       || (moves.get(0).equals(expectedStorageMove2) && moves.get(1).equals(expectedComputeMove2)));
    }

    /**
     * Case: Shop together : Pm1 connected to St1. Vm1 and Vm2 are placed on pm1 and st1.
     * Expected result: Provision a new PM by supply and move one of the VMs to that new machine.
     */
    @Test
    public void test_ShopTogetherBootstrap_ProvisionBySupply(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);

        List<Action> bootStrapActionList = BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.PROVISION_BY_SUPPLY, bootStrapActionList.get(0).getType());
        assertEquals(TestUtils.CPU, ((ProvisionBySupply)bootStrapActionList.get(0)).getReason());
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(1).getType());
        //Assert that the provision by supply was modelled off pm1
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.get(0);
        assertEquals(pm1, provisionBySupply.getModelSeller());
        //Assert compound move is for compute of VM1 or VM2 to move to newly provisioned PM
        Action compoundMove = bootStrapActionList.get(1);
        Move expectedMove1 = new Move(economy, sl1, pm1, provisionBySupply.getProvisionedSeller());
        Move expectedMove2 = new Move(economy, sl2, pm1, provisionBySupply.getProvisionedSeller());
        List<Move> moves = ((CompoundMove)compoundMove).getConstituentMoves();
        assertEquals(moves.size(), 1);
        assertTrue(moves.get(0).equals(expectedMove1) || moves.get(0).equals(expectedMove2));
    }

    /**
     * Case: Shop together : Pm1 connected to St1. Pm2 connected to St2.
     * Vm1 and Vm2 are placed on pm1 and st1, but within capacity.
     * Expected result: No actions are generated.
     */
    public void test_ShopTogetherBootstrap_NoActionsWhenNonInfiniteQuote(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(1l), 100, 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(1l), 300, true);
        st2.setDebugInfoNeverUseInCode("DS2");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{10, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{10, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);

        List<Action> bootStrapActionList = BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 0);
    }

    /** Case: Non shop together : Pm1 connected to St1 and St2. Pm2 connected to St1 and St2.
     *  Vm1 and Vm2 are placed on pm1 and st1, but within capacity.
     *  Expected result: No actions are generated.
     */
    @Test
    public void test_NonShopTogetherBootstrap_NoActionsWhenNonInfiniteQuote(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.setDebugInfoNeverUseInCode("DS2");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{150}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{50, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{150}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList = BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 0);
    }

    /**
     * Create one PM and one storage. Place 2 VMs on this PM and Storage. PMs are not cloneable.
     * 2VMs together exceed the CPU capacity of the PM.
     * No provision actions should be produced because the PM is not cloneable.
     */
    @Test
    public void test_bootstrapSupplyDecisions_NoProvisionActionsWhenNoCloneableSellers(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 10}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 10}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);

        List<Action> bootStrapActionList = BootstrapSupply.bootstrapSupplyDecisions(economy);

        assertTrue(bootStrapActionList.size() == 0);
    }

    /**
     * Case: shop together : Pm1 connected to St1.
     * Pm2 connected to St2. Pm2 is inactive.
     * 2 VMs both on PM1 and St1 such that CPU capacity of Pm1 is exceeded.
     * Expected result: Activate Pm2 followed by moving one of the VMs to Pm2 and St2.
     */
    @Test
    public void test_bootstrapShopTogether_reactivateHost(){
        Economy economy = new Economy();
        // change pm1 clique to 1 and pm2 clique to 0, because right now, it always pick the first
        // clique id from common clique list as the cliqueId of shopping list, in order to generate
        // activate pm2 actions, it needs make sure pm2 clique 0 is picked from common list.
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(1l), 100, 100, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(1l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Create PM2 and inactivate it. PM2 is same size of PM1 to make sure only one VM
        // not two VMs will move to PM2
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.changeState(TraderState.INACTIVE);
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st2.setDebugInfoNeverUseInCode("DS2");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, pm1);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(bootStrapActionList.get(0).getActionTarget(), pm2);
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(1).getType());
        Move expectedComputeMove1 = new Move(economy, sl1, pm1, pm2);
        Move expectedStorageMove1 = new Move(economy, sl2, st1, st2);
        Move expectedComputeMove2 = new Move(economy, sl3, pm1, pm2);
        Move expectedStorageMove2 = new Move(economy, sl4, st1, st2);
        List<Move> moves = ((CompoundMove)bootStrapActionList.get(1)).getConstituentMoves();
        assertTrue( (moves.get(0).equals(expectedComputeMove1) && moves.get(1).equals(expectedStorageMove1))
                       || (moves.get(0).equals(expectedStorageMove1) && moves.get(1).equals(expectedComputeMove1))
                       || (moves.get(0).equals(expectedComputeMove2) && moves.get(1).equals(expectedStorageMove2))
                       || (moves.get(0).equals(expectedStorageMove2) && moves.get(1).equals(expectedComputeMove2)));
    }

    /**
     * Case: Non shop together : Pm1 connected to St1 and St2.
     * Pm2 connected to St1 and St2. Pm2 is inactive.
     * 2 VMs both on PM1 and St1 such that CPU capacity of Pm1 is exceeded.
     * Expected result: Activate Pm2 followed by moving one of the VMs to Pm2.
     */
    @Test
    public void test_bootstrapNonShopTogether_reactivateHost(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Create PM2 and inactivate it.
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.changeState(TraderState.INACTIVE);
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.setDebugInfoNeverUseInCode("DS2");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{60, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.ACTIVATE, bootStrapActionList.get(0).getType());
        assertEquals(TestUtils.CPU, ((Activate) bootStrapActionList.get(0)).getReason());
        assertEquals(bootStrapActionList.get(0).getActionTarget(), pm2);
        assertEquals(ActionType.MOVE, bootStrapActionList.get(1).getType());
        Move expectedComputeMove1 = new Move(economy, sl1, pm1, pm2);
        Move expectedComputeMove2 = new Move(economy, sl2, pm1, pm2);
        assertTrue( (bootStrapActionList.get(1).equals(expectedComputeMove1))
                       || (bootStrapActionList.get(1).equals(expectedComputeMove2)) );
    }

    /**
     * Place 3 VMs on 1 PM. PM CPU capacity = 100.
     * VM CPU quantities = 40, 25, 80.
     * Expected result: Provision 1 PM by supply and move VM3 there.
     */
    //Commenting out this test for now.
    //Result of bootstrapSupplyDecisions seems to depend on the order of the VMs added to economy.
    //@Test
    public void test_bootstrapSupplyDecisions_3InfQuoteBuyersOn1Seller(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{40, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{25, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        //Place vm3 on pm1 and st1.
        Trader vm3 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm3, new double[]{80, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm3, new double[]{50}, st1);
        vm3.setDebugInfoNeverUseInCode("VM3");
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.bootstrapSupplyDecisions(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.PROVISION_BY_SUPPLY, bootStrapActionList.get(0).getType());
        assertEquals(TestUtils.CPU, ((ProvisionBySupply)bootStrapActionList.get(0)).getReason());
        assertEquals(ActionType.MOVE, bootStrapActionList.get(1).getType());
        //Assert that the provision by supply was modeled off pm1
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.get(0);
        assertEquals(pm1, provisionBySupply.getModelSeller());
        Move expectedMove = new Move(economy, sl3, pm1, provisionBySupply.getProvisionedSeller());
        assertTrue(bootStrapActionList.get(1).equals(expectedMove));
    }

    /**
     * Tests both shop together and non shop together
     * Case: When there are no sellers, then bootstrapSupplyDecisions should generate a reconfigure action.
     */
    private void test_bootstrapSupplyDecisions_reconfigureWhenNoSellers(Function<Economy, List<Action>> shopper) {
        Economy economy = new Economy();
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{40, 0}, null);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");

        List<Action> bootStrapActionList = shopper.apply(economy);

        assertFalse(bootStrapActionList.isEmpty());
        assertEquals(ActionType.RECONFIGURE, bootStrapActionList.get(0).getType());
    }

    @Test
    public void test_bootstrapSupplyDecisions_reconfigureWhenNoSellers_nonShopTogether() {
        test_bootstrapSupplyDecisions_reconfigureWhenNoSellers(BootstrapSupply::nonShopTogetherBootstrap);
    }

//    @Test
    public void test_bootstrapSupplyDecisions_reconfigureWhenNoSellers_shopTogether() {
        test_bootstrapSupplyDecisions_reconfigureWhenNoSellers(BootstrapSupply::shopTogetherBootstrap);
    }

    /**
     * Tests both shop together and non shop together
     * Case: When there are only guaranteed buyers, then bootstrapSupply does not generate any actions.
     */
    @Test
    public void test_bootstrapSupplyDecisions_noActionsWithOnlyGuaranteedBuyers(){
        Economy economy = new Economy();
        //Create PM1 and
        Trader pm1 = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0l),
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM, TestUtils.CPU_ALLOC, TestUtils.MEM_ALLOC),
                        new double[]{100, 100, 100, 100}, true, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Create VDC1 but do not place anywhere.
        Trader vdc1 = TestUtils.createVDC(economy);
        ShoppingList vdcSl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU_ALLOC, TestUtils.MEM_ALLOC), vdc1, new double[]{50,50}, null);
        vdcSl.setMovable(false);
        vdc1.setDebugInfoNeverUseInCode("VDC1");

        List<Action> bootStrapActionList = BootstrapSupply.bootstrapSupplyDecisions(economy);

        assertEquals(bootStrapActionList.size(), 0);
    }

    /**
     * Case: pm1 and pm2 connected to st1.
     * 2 VMs both on pm1 and st1. A placement policy has been created to place both these VMs on pm2.
     * But pm2 is small and cannot accomodate both VMs.
     * Expected result: Provision pm2 and move both VMs to pm2 and pm2_clone.
     */
    @Test
    public void test_bootstrapSupplyDecisions_provisionWithSegmentation() {
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, CLIQUE0,
                100, 100, true, "PM1");
        Trader st1 = TestUtils.createStorage(economy, CLIQUE0,
                300, true, "DS1");
        // Place vm1 on pm1 and st1. VM1 is looking for segmentation which is not sold by pm1.
        Trader vm1 = TestUtils.createVM(economy, "VM1");
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM,
                        TestUtils.SEGMENTATION_COMMODITY), vm1, new double[]{40, 0, 1}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT),
                vm1, new double[]{100}, st1);
        // Place vm2 on pm1 and st1.  VM2 is looking for segmentation which is not sold by pm1.
        Trader vm2 = TestUtils.createVM(economy, "VM2");
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM,
                        TestUtils.SEGMENTATION_COMMODITY), vm2, new double[]{40, 0, 1}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT),
                vm1, new double[]{100}, st1);
        // Create pm2 which sells the segmentation, but can accomodate only one of the VMs.
        Trader pm2 = TestUtils.createTrader(economy, PM_TYPE, CLIQUE0,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM, TestUtils.SEGMENTATION_COMMODITY),
                new double[]{50, 100, 100}, true, false, "PM2");
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                BootstrapSupply.bootstrapSupplyDecisions(economy);

        // There should be 2 moves and 1 provision by supply of pm2. Order does not matter
        // as long as the actions are present.
        assertEquals(bootStrapActionList.size(), 3);
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.stream()
                .filter(a -> a.getType() == ActionType.PROVISION_BY_SUPPLY)
                .findFirst().get();
        assertEquals(TestUtils.CPU, provisionBySupply.getReason());
        // Assert that the provision by supply was modeled off pm2
        assertEquals(pm2, provisionBySupply.getModelSeller());
        // Check that both VMs moved to the pm2 and pm2_clone
        List<Move> moves = bootStrapActionList.stream()
                .filter(a -> a.getType() == ActionType.MOVE)
                .map(a -> (Move)a)
                .collect(Collectors.toList());
        Move sl1Move = moves.stream().filter(m -> m.getTarget().equals(sl1)).findFirst().get();
        Move sl2Move = moves.stream().filter(m -> m.getTarget().equals(sl2)).findFirst().get();
        Move sl1PossibleMove1 = new Move(economy, sl1, pm1, pm2);
        Move sl2PossibleMove1 = new Move(economy, sl2, pm1, provisionBySupply.getProvisionedSeller());
        Move sl1PossibleMove2 = new Move(economy, sl1, pm1, provisionBySupply.getProvisionedSeller());
        Move sl2PossibleMove2 = new Move(economy, sl2, pm1, pm2);
        assertTrue(sl1Move.equals(sl1PossibleMove1) && sl2Move.equals(sl2PossibleMove1)
            || sl1Move.equals(sl1PossibleMove2) && sl2Move.equals(sl2PossibleMove2));
    }
}